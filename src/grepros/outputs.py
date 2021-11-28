# -*- coding: utf-8 -*-
"""
Outputs for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    27.11.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs
from __future__ import print_function
import atexit
import collections
import csv
import copy
import json
import os
try: import queue  # Py3
except ImportError: import Queue as queue  # Py2
import re
import sqlite3
import sys
import threading

import yaml

from . common import ConsolePrinter, MatchMarkers, TextWrapper, filter_fields, format_bytes, \
                     merge_spans, plural, quote, unique_path, wildcard_to_regex
from . import rosapi
from . vendor import step


class SinkBase(object):
    """Output base class."""

    def __init__(self, args):
        """
        @param   args        arguments object like argparse.Namespace
        @param   args.META   whether to print metainfo
        """
        self._args = copy.deepcopy(args)
        self._batch_meta = {}  # {source batch: "source metadata"}
        self._counts     = {}  # {topic: count}

        ## inputs.SourceBase instance bound to this sink
        self.source = None

    def emit_meta(self):
        """Prints source metainfo like bag header as debug stream, if not already printed."""
        batch = self._args.META and self.source.get_batch()
        if self._args.META and batch not in self._batch_meta:
            meta = self._batch_meta[batch] = self.source.format_meta()
            meta and ConsolePrinter.debug(meta)

    def emit(self, topic, index, stamp, msg, match):
        """
        Outputs ROS message.

        @param   topic  full name of ROS topic the message is from
        @param   index  message index in topic
        @param   msg    ROS message
        @param   match  ROS message with values tagged with match markers if matched, else None
        """
        self._counts[topic] = self._counts.get(topic, 0) + 1

    def bind(self, source):
        """Attaches source to sink."""
        self.source = source

    def validate(self):
        """Returns whether sink prerequisites are met (like ROS environment set if TopicSink)."""
        return True

    def close(self):
        """Shuts down output, closing any files or connections."""
        self._batch_meta.clear()
        self._counts.clear()

    def flush(self):
        """Writes out any pending data to disk."""

    def thread_excepthook(self, exc):
        """Handles exception, used by background threads."""
        ConsolePrinter.error(exc)


class TextSinkMixin(object):
    """Provides message formatting as text."""

    ## Default highlight wrappers if not color output
    NOCOLOR_HIGHLIGHT_WRAPPERS = "**", "**"


    def __init__(self, args):
        """
        @param   args                       arguments object like argparse.Namespace
        @param   args.COLOR                 "never" for not using colors in replacements
        @param   args.PRINT_FIELDS          message fields to use in output if not all
        @param   args.NOPRINT_FIELDS        message fields to skip in output
        @param   args.MAX_FIELD_LINES       maximum number of lines to output per field
        @param   args.START_LINE            message line number to start output from
        @param   args.END_LINE              message line number to stop output at
        @param   args.MAX_MESSAGE_LINES     maximum number of lines to output per message
        @param   args.LINES_AROUND_MATCH    number of message lines around matched fields to output
        @param   args.MATCHED_FIELDS_ONLY   output only the fields where match was found
        @param   args.WRAP_WIDTH            character width to wrap message YAML output at
        @param   args.MATCH_WRAPPER         string to wrap around matched values,
                                            both sides if one value, start and end if more than one,
                                            or no wrapping if zero values
        """
        self._prefix       = ""    # Put before each message line (filename if grepping 1+ files)
        self._wrapper      = None  # TextWrapper instance
        self._patterns     = {}    # {key: [(() if any field else ('path', ), re.Pattern), ]}
        self._format_repls = {}    # {text to replace if highlight: replacement text}
        self._styles = collections.defaultdict(str)  # {label: ANSI code string}

        self._configure(args)


    def format_message(self, msg, highlight=False):
        """Returns message as formatted string, optionally highlighted for matches."""
        text = self.message_to_yaml(msg).rstrip("\n")

        if self._prefix or self._args.START_LINE or self._args.END_LINE \
        or self._args.MAX_MESSAGE_LINES or (self._args.LINES_AROUND_MATCH and highlight):
            lines = text.splitlines()

            if self._args.START_LINE or self._args.END_LINE or self._args.MAX_MESSAGE_LINES:
                start = self._args.START_LINE or 0
                start = max(start, -len(lines)) - (start > 0)  # <0 to sanity, >0 to 0-base
                lines = lines[start:start + (self._args.MAX_MESSAGE_LINES or len(lines))]
                lines = lines and (lines[:-1] + [lines[-1] + self._styles["rst"]])

            if self._args.LINES_AROUND_MATCH and highlight:
                spans, NUM = [], self._args.LINES_AROUND_MATCH
                for i, l in enumerate(lines):
                    if MatchMarkers.START in l:
                        spans.append([max(0, i - NUM), min(i + NUM + 1, len(lines))])
                    if MatchMarkers.END in l and spans:
                        spans[-1][1] = min(i + NUM + 1, len(lines))
                lines = sum((lines[a:b - 1] + [lines[b - 1] + self._styles["rst"]]
                             for a, b in merge_spans(spans)), [])

            if self._prefix:
                lines = [self._prefix + l for l in lines]

            text = "\n".join(lines)

        for a, b in self._format_repls.items() if highlight else ():
            text = re.sub(r"(%s)\1+" % re.escape(a), r"\1", text)  # Remove consecutive duplicates
            text = text.replace(a, b)

        return text


    def message_to_yaml(self, val, top=(), typename=None):
        """Returns ROS message or other value as YAML."""
        # Refactored from genpy.message.strify_message().
        unquote = lambda v, t: v[1:-1] if "string" != t and v[:1] == v[-1:] == '"' else v

        def retag_match_lines(lines):
            """Adds match tags to lines where wrapping separated start and end."""
            PH = self._wrapper.placeholder
            for i, l in enumerate(lines):
                startpos0, endpos0 = l.find (MatchMarkers.START), l.find (MatchMarkers.END)
                startpos1, endpos1 = l.rfind(MatchMarkers.START), l.rfind(MatchMarkers.END)
                if endpos0 >= 0 and (startpos0 < 0 or startpos0 > endpos0):
                    lines[i] = l = re.sub(r"^(\s*)", r"\1" + MatchMarkers.START, l)
                if startpos1 >= 0 and endpos1 < startpos1 and i + 1 < len(lines):
                    lines[i + 1] = re.sub(r"^(\s*)", r"\1" + MatchMarkers.START, lines[i + 1])
                if startpos1 >= 0 and startpos1 > endpos1:
                    CUT, EXTRA = (-len(PH), PH) if PH and l.endswith(PH) else (len(l), "")
                    lines[i] = l[:CUT] + MatchMarkers.END + EXTRA
            return lines

        indent = "  " * len(top)
        if isinstance(val, (int, float, bool)):
            return str(val)
        if isinstance(val, str):
            if val in ("", MatchMarkers.EMPTY):
                return MatchMarkers.EMPTY_REPL if val else "''"
            # default_style='"' avoids trailing "...\n"
            return yaml.safe_dump(val, default_style='"', width=sys.maxsize).rstrip("\n")
        if isinstance(val, (list, tuple)):
            if not val:
                return "[]"
            if "string" == rosapi.scalar(typename):
                yaml_str = yaml.safe_dump(val).rstrip('\n')
                return "\n" + "\n".join(indent + line for line in yaml_str.splitlines())
            vals = [x for v in val for x in [self.message_to_yaml(v, top, typename)] if x]
            if rosapi.scalar(typename) in rosapi.ROS_NUMERIC_TYPES:
                return "[%s]" % ", ".join(unquote(str(v), typename) for v in vals)
            return ("\n" + "\n".join(indent + "- " + v for v in vals)) if vals else ""
        if rosapi.is_ros_message(val):
            MATCHED_ONLY = self._args.MATCHED_FIELDS_ONLY and not self._args.LINES_AROUND_MATCH
            vals, fieldmap = [], rosapi.get_message_fields(val)
            prints, noprints = self._patterns["print"], self._patterns["noprint"]
            fieldmap = filter_fields(fieldmap, top, include=prints, exclude=noprints)
            for k, t in fieldmap.items():
                v = self.message_to_yaml(rosapi.get_message_value(val, k, t), top + (k, ), t)
                if not v or MATCHED_ONLY and MatchMarkers.START not in v:
                    continue  # for k, t

                v = unquote(v, t)  # Strip quotes from non-string types cast to "<match>v</match>"
                if rosapi.scalar(t) in rosapi.ROS_BUILTIN_TYPES:
                    is_strlist = t.endswith("]") and rosapi.scalar(t) in rosapi.ROS_STRING_TYPES
                    is_num = rosapi.scalar(t) in rosapi.ROS_NUMERIC_TYPES
                    extra_indent = indent if is_strlist else " " * len(indent + k + ": ")
                    self._wrapper.reserve_width(self._prefix + extra_indent)
                    self._wrapper.drop_whitespace = t.endswith("]") and not is_strlist
                    self._wrapper.break_long_words = not is_num
                    v = ("\n" + extra_indent).join(retag_match_lines(self._wrapper.wrap(v)))
                    if is_strlist: v = "\n" + v
                vals.append("%s%s: %s" % (indent, k, rosapi.format_message_value(val, k, v)))
            return ("\n" if indent and vals else "") + "\n".join(vals)

        return str(val)


    @classmethod
    def make_full_yaml_args(cls, args):
        """
        Returns init arguments that provide message full YAMLs with no markers or colors.

        @param   args                       arguments object like argparse.Namespace
        @param   args.COLOR                 set to "never" in result
        @param   args.PRINT_FIELDS          blanked to [] in result
        @param   args.NOPRINT_FIELDS        blanked to [] in result
        @param   args.MAX_FIELD_LINES       blanked to None in result
        @param   args.START_LINE            blanked to None in result
        @param   args.END_LINE              blanked to None in result
        @param   args.MAX_MESSAGE_LINES     blanked to None in result
        @param   args.LINES_AROUND_MATCH    blanked to None in result
        @param   args.MATCHED_FIELDS_ONLY   blanked to False in result
        @param   args.MATCH_WRAPPER         set to [""] in result
        @param   args.WRAP_WIDTH            set to 120 in result
        """
        DEFAULTS = {"PRINT_FIELDS": [], "NOPRINT_FIELDS": [], "MAX_FIELD_LINES": None,
                    "START_LINE": None, "END_LINE": None, "MAX_MESSAGE_LINES": None,
                    "LINES_AROUND_MATCH": None, "MATCHED_FIELDS_ONLY": False,
                    "COLOR": "never", "MATCH_WRAPPER": [""], "WRAP_WIDTH": 120}
        args = copy.deepcopy(args)
        for k, v in DEFAULTS.items(): setattr(args, k, v)
        return args


    def _configure(self, args):
        """Initializes output settings."""
        prints, noprints = args.PRINT_FIELDS, args.NOPRINT_FIELDS
        for key, vals in [("print", prints), ("noprint", noprints)]:
            self._patterns[key] = [(tuple(v.split(".")), wildcard_to_regex(v)) for v in vals]

        if "never" != args.COLOR:
            self._styles.update({"hl0":  ConsolePrinter.STYLE_HIGHLIGHT,
                                 "ll0":  ConsolePrinter.STYLE_LOWLIGHT,
                                 "pfx0": ConsolePrinter.STYLE_SPECIAL, # Content line prefix start
                                 "sep0": ConsolePrinter.STYLE_SPECIAL2})
            self._styles.default_factory = lambda: ConsolePrinter.STYLE_RESET

        WRAPS = args.MATCH_WRAPPER
        if WRAPS is None and "never" == args.COLOR: WRAPS = self.NOCOLOR_HIGHLIGHT_WRAPPERS
        WRAPS = ((WRAPS or [""]) * 2)[:2]
        self._styles["hl0"] = self._styles["hl0"] + WRAPS[0]
        self._styles["hl1"] = WRAPS[1] + self._styles["hl1"]

        custom_widths = {MatchMarkers.START: len(WRAPS[0]), MatchMarkers.END:     len(WRAPS[1]),
                         self._styles["ll0"]:            0, self._styles["ll1"]:  0,
                         self._styles["pfx0"]:           0, self._styles["pfx1"]: 0,
                         self._styles["sep0"]:           0, self._styles["sep1"]: 0}
        wrapargs = dict(max_lines=args.MAX_FIELD_LINES,
                        placeholder="%s ...%s" % (self._styles["ll0"], self._styles["ll1"]))
        if args.WRAP_WIDTH is not None: wrapargs.update(width=args.WRAP_WIDTH)
        self._wrapper = TextWrapper(custom_widths=custom_widths, **wrapargs)
        self._format_repls = {MatchMarkers.START: self._styles["hl0"],
                              MatchMarkers.END:   self._styles["hl1"]}



class ConsoleSink(SinkBase, TextSinkMixin):
    """Prints messages to console."""

    META_LINE_TEMPLATE   = "{ll0}{sep} {line}{ll1}"
    MESSAGE_SEP_TEMPLATE = "{ll0}{sep}{ll1}"
    PREFIX_TEMPLATE      = "{pfx0}{batch}{pfx1}{sep0}{sep}{sep1}"
    MATCH_PREFIX_SEP     = ":"    # Printed after bag filename for matched message lines
    CONTEXT_PREFIX_SEP   = "-"    # Printed after bag filename for context message lines
    SEP                  = "---"  # Prefix of message separators and metainfo lines



    def __init__(self, args):
        """
        @param   args                       arguments object like argparse.Namespace
        @param   args.META                  whether to print metainfo
        @param   args.PRINT_FIELDS          message fields to print in output if not all
        @param   args.NOPRINT_FIELDS        message fields to skip in output
        @param   args.LINE_PREFIX           print source prefix like bag filename on each message line
        @param   args.MAX_FIELD_LINES       maximum number of lines to print per field
        @param   args.START_LINE            message line number to start output from
        @param   args.END_LINE              message line number to stop output at
        @param   args.MAX_MESSAGE_LINES     maximum number of lines to output per message
        @param   args.LINES_AROUND_MATCH    number of message lines around matched fields to output
        @param   args.MATCHED_FIELDS_ONLY   output only the fields where match was found
        @param   args.WRAP_WIDTH            character width to wrap message YAML output at
        """
        if args.WRAP_WIDTH is None:
            args = copy.deepcopy(args)
            args.WRAP_WIDTH = ConsolePrinter.WIDTH

        super(ConsoleSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)


    def emit_meta(self):
        """Prints source metainfo like bag header, if not already printed."""
        batch = self._args.META and self.source.get_batch()
        if self._args.META and batch not in self._batch_meta:
            meta = self._batch_meta[batch] = self.source.format_meta()
            kws = dict(self._styles, sep=self.SEP)
            meta = "\n".join(x and self.META_LINE_TEMPLATE.format(**dict(kws, line=x))
                             for x in meta.splitlines())
            meta and ConsolePrinter.print(meta)


    def emit(self, topic, index, stamp, msg, match):
        """Prints separator line and message text."""
        self._prefix = ""
        if self._args.LINE_PREFIX and self.source.get_batch():
            sep = self.MATCH_PREFIX_SEP if match else self.CONTEXT_PREFIX_SEP
            kws = dict(self._styles, sep=sep, batch=self.source.get_batch())
            self._prefix = self.PREFIX_TEMPLATE.format(**kws)
        kws = dict(self._styles, sep=self.SEP)
        if self._args.META:
            meta = self.source.format_message_meta(topic, index, stamp, msg)
            meta = "\n".join(x and self.META_LINE_TEMPLATE.format(**dict(kws, line=x))
                             for x in meta.splitlines())
            meta and ConsolePrinter.print(meta)
        elif self._counts:
            sep = self.MESSAGE_SEP_TEMPLATE.format(**kws)
            sep and ConsolePrinter.print(sep)
        ConsolePrinter.print(self.format_message(match or msg, highlight=bool(match)))
        super(ConsoleSink, self).emit(topic, index, stamp, msg, match)



class BagSink(SinkBase):
    """Writes messages to bagfile."""

    def __init__(self, args):
        """
        @param   args           arguments object like argparse.Namespace
        @param   args.META      whether to print metainfo
        @param   args.OUTFILE   name of ROS bagfile to create or append to
        @param   args.VERBOSE   whether to print debug information
        """
        super(BagSink, self).__init__(args)
        self._bag = None
        self._close_printed = False

        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output bagfile."""
        if not self._bag:
            if self._args.VERBOSE:
                sz = os.path.isfile(self._args.OUTFILE) and os.path.getsize(self._args.OUTFILE)
                ConsolePrinter.debug("%s %s%s.", "Appending to" if sz else "Creating",
                                     self._args.OUTFILE, (" (%s)" % format_bytes(sz)) if sz else "")
            self._bag = rosapi.create_bag_writer(self._args.OUTFILE)

        if topic not in self._counts and self._args.VERBOSE:
            ConsolePrinter.debug("Adding topic %s.", topic)

        self._bag.write(topic, msg, stamp)
        super(BagSink, self).emit(topic, index, stamp, msg, match)

    def validate(self):
        """Returns whether ROS environment is set, prints error if not."""
        return rosapi.validate()

    def close(self):
        """Closes output bagfile, if any."""
        self._bag and self._bag.close()
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), self._args.OUTFILE,
                                 format_bytes(os.path.getsize(self._args.OUTFILE)))
        super(BagSink, self).close()


class CsvSink(SinkBase):
    """Writes messages to CSV files, each topic to a separate file."""

    def __init__(self, args):
        """
        @param   args           arguments object like argparse.Namespace
        @param   args.OUTFILE   base name of CSV file to write,
                                will add topic name like "name.__my__topic.csv" for "/my/topic",
                                will add counter like "name.__my__topic.2.csv" if exists
        @param   args.VERBOSE   whether to print debug information
        """
        super(CsvSink, self).__init__(args)
        self._filebase      = args.OUTFILE  # Filename base, will be made unique
        self._files         = {}            # {topic: file()}
        self._writers       = {}            # {topic: csv.writer}
        self._lasttopic     = None          # Last topic emitted
        self._close_printed = False

        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output file."""
        data = [v for _, v in self._iter_fields(msg)]
        metadata = [rosapi.to_sec(stamp), rosapi.to_datetime(stamp), rosapi.get_message_type(msg)]
        self._make_writer(topic, msg).writerow(self._format_row(metadata + data))
        super(CsvSink, self).emit(topic, index, stamp, msg, match)

    def close(self):
        """Closes output file(s), if any."""
        names = {t: f.name for t, f in self._files.items()}
        for t in names:
            self._files.pop(t).close()
        self._writers.clear()
        if not self._close_printed and self._counts:
            self._close_printed = True
            sizes = {t: os.path.getsize(n) for t, n in names.items()}
            ConsolePrinter.debug("Wrote %s in %s to file (%s):",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)),
                                 format_bytes(sum(sizes.values())))
            for topic, name in names.items():
                ConsolePrinter.debug("- %s (%s, %s)", name,
                                    format_bytes(sizes[topic]),
                                    plural("message", self._counts[topic]))
        super(CsvSink, self).close()

    def _make_writer(self, topic, msg):
        """
        Returns a csv.writer for writing topic data.

        File is populated with header if 
        """
        if self._lasttopic and topic != self._lasttopic:
            self._files[self._lasttopic].close()  # Avoid hitting ulimit
        if topic not in self._files or self._files[topic].closed:
            name = self._files[topic].name if topic in self._files else None
            if not name:
                base, ext = os.path.splitext(self._filebase)
                name = unique_path("%s.%s%s" % (base, topic.lstrip("/").replace("/", "__"), ext))
            flags = {"mode": "ab"} if sys.version_info < (3, 0) else {"mode": "a", "newline": ""}
            f = open(name, **flags)
            w = csv.writer(f)
            if topic not in self._files:
                if self._args.VERBOSE:
                    ConsolePrinter.debug("Creating %s.", name)
                header = [topic + "/" + ".".join(map(str, p)) for p, _ in self._iter_fields(msg)]
                metaheader = ["__time", "__datetime", "__type"]
                w.writerow(self._format_row(metaheader + header))
            self._files[topic], self._writers[topic] = f, w
        self._lasttopic = topic
        return self._writers[topic]

    def _format_row(self, data):
        """Returns row suitable for writing to CSV."""
        data = [int(v) if isinstance(v, bool) else v for v in data]
        if sys.version_info < (3, 0):  # Py2, CSV is written in binary mode
            data = [v.encode("utf-8") if isinstance(v, unicode) else v for v in data]
        return data

    def _iter_fields(self, msg, top=()):
        """
        Yields ((nested, path), scalar value) from ROS message.

        Lists are returned as ((nested, path, index), value), e.g. (("data", 0), 666).
        """
        fieldmap = rosapi.get_message_fields(msg)
        for k, t in fieldmap.items() if fieldmap != msg else ():
            v, path = rosapi.get_message_value(msg, k, t), top + (k, )
            is_sublist = isinstance(v, (list, tuple)) and \
                         rosapi.scalar(t) not in rosapi.ROS_BUILTIN_TYPES
            if isinstance(v, (list, tuple)) and not is_sublist:
                for i, lv in enumerate(v):
                    yield path + (i, ), rosapi.to_sec(lv)
            elif is_sublist:
                for i, lmsg in enumerate(v):
                    for lp, lv in self._iter_fields(lmsg, path + (i, )):
                        yield lp, lv
            elif rosapi.is_ros_message(v, ignore_time=True):
                for mp, mv in self._iter_fields(v, path):
                    yield mp, mv
            else:
                yield path, rosapi.to_sec(v)


class HtmlSink(SinkBase, TextSinkMixin):
    """Writes messages to bagfile."""

    ## HTML template path
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "templates", "html.tpl")

    ## Character wrap width for message YAML
    WRAP_WIDTH = 120

    def __init__(self, args):
        """
        @param   args                    arguments object like argparse.Namespace
        @param   args.META               whether to print metainfo
        @param   args.OUTFILE            name of HTML file to write,
                                         will add counter like .2 to filename if exists
        @param   args.OUTFILE_TEMPLATE   path to custom HTML template, if any
        @param   args.VERBOSE            whether to print debug information
        @param   args.MATCH_WRAPPER      string to wrap around matched values,
                                         both sides if one value, start and end if more than one,
                                         or no wrapping if zero values
        @param   args.ORDERBY            "topic" or "type" if any to group results by
        """
        args = copy.deepcopy(args)
        args.WRAP_WIDTH = self.WRAP_WIDTH
        args.COLOR = "always"

        super(HtmlSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)
        self._queue    = queue.Queue()
        self._writer   = None          # threading.Thread running _stream()
        self._filename = args.OUTFILE  # Filename base, will be made unique
        self._template_path = args.OUTFILE_TEMPLATE or self.TEMPLATE_PATH
        self._close_printed = False

        WRAPS = ((args.MATCH_WRAPPER or [""]) * 2)[:2]
        self._tag_repls = {MatchMarkers.START:            '<span class="match">' +
                                                          step.escape_html(WRAPS[0]),
                           MatchMarkers.END:              step.escape_html(WRAPS[1]) + '</span>',
                           ConsolePrinter.STYLE_LOWLIGHT: '<span class="lowlight">',
                           ConsolePrinter.STYLE_RESET:    '</span>'}
        self._tag_rgx = re.compile("(%s)" % "|".join(map(re.escape, self._tag_repls)))

        self._format_repls.clear()
        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output file."""
        self._queue.put((topic, index, stamp, msg, match))
        if not self._writer:
            self._writer = threading.Thread(target=self._stream)
            self._writer.start()
        if self._queue.qsize() > 100: self._queue.join()

    def validate(self):
        """Returns whether ROS environment is set, prints error if not."""
        return rosapi.validate()

    def close(self):
        """Closes output file, if any."""
        if self._writer:
            writer, self._writer = self._writer, None
            self._queue.put(None)
            writer.is_alive() and writer.join()
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), self._filename,
                                 format_bytes(os.path.getsize(self._filename)))
        super(HtmlSink, self).close()

    def flush(self):
        """Writes out any pending data to disk."""
        self._queue.join()

    def format_message(self, msg, highlight=False):
        """Returns message as formatted string, optionally highlighted for matches."""
        text = TextSinkMixin.format_message(self, msg, highlight)
        text = "".join(self._tag_repls.get(x) or step.escape_html(x)
                       for x in self._tag_rgx.split(text))
        return text

    def _stream(self):
        """Writer-loop, streams HTML template to file."""
        if not self._writer:
            return

        try:
            with open(self._template_path, "r") as f: tpl = f.read()
            template = step.Template(tpl, escape=True, strip=False)
            ns = dict(source=self.source, sink=self, args=["grepros"] + sys.argv[1:],
                      timeline=not self._args.ORDERBY, messages=self._produce())
            self._filename = unique_path(self._filename, empty_ok=True)
            if self._args.VERBOSE:
                ConsolePrinter.debug("Creating %s.", self._filename)
            with open(self._filename, "wb") as f:
                template.stream(f, ns, unbuffered=True)
        except Exception as e:
            self.thread_excepthook(e)
        finally:
            self._writer = None

    def _produce(self):
        """Yields messages from emit queue, as (topic, index, stamp, msg, match)."""
        while True:
            entry = self._queue.get()
            self._queue.task_done()
            if entry is None:
                break  # while
            (topic, index, stamp, msg, match) = entry
            if self._args.VERBOSE and topic not in self._counts:
                ConsolePrinter.debug("Adding topic %s.", topic)
            yield entry
            super(HtmlSink, self).emit(topic, index, stamp, msg, match)
        try:
            while self._queue.get_nowait() or True: self._queue.task_done()
        except queue.Empty: pass



class SqliteSink(SinkBase, TextSinkMixin):
    """
    Writes messages to an SQLite database.

    Output will have:
    - table "messages", with all messages as YAML and serialized binary
    - table "types", with message definitions
    - table "topics", with topic information

    plus:
    - table "pkg/MsgType" for each message type, with detailed fields,
      and array fields to linking to subtypes
    - view "/topic/full/name" for each topic,
      selecting from the message type table

    """

    ## SQL statements for populating database base schema
    BASE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS messages (
      id           INTEGER   PRIMARY KEY,
      dt           TIMESTAMP NOT NULL,
      topic        TEXT      NOT NULL,
      type         TEXT      NOT NULL,
      yaml         TEXT      NOT NULL,
      data         BLOB      NOT NULL,
      topic_id     INTEGER   NOT NULL,
      timestamp    INTEGER   NOT NULL
    );

    CREATE TABLE IF NOT EXISTS types (
      id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      type       TEXT    NOT NULL,
      definition TEXT    NOT NULL,
      subtypes   JSON
    );

    CREATE TABLE IF NOT EXISTS topics (
      id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name                 TEXT    NOT NULL,
      type                 TEXT    NOT NULL,
      count                INTEGER NOT NULL DEFAULT 0,
      dt_first             TIMESTAMP,
      dt_last              TIMESTAMP,
      timestamp_first      INTEGER,
      timestamp_last       INTEGER,
      serialization_format TEXT DEFAULT "cdr",
      offered_qos_profiles TEXT DEFAULT ""
    );
    """

    ## SQL statement for inserting messages
    INSERT_MESSAGE = """
    INSERT INTO messages (dt, topic, type, yaml, data, topic_id, timestamp)
    VALUES (:dt, :topic, :type, :yaml, :data, :topic_id, :timestamp)
    """

    ## SQL statement for inserting topics
    INSERT_TOPIC = "INSERT INTO topics (name, type) VALUES (:name, :type)"

    ## SQL statement for updating topics with latest message
    UPDATE_TOPIC = """
    UPDATE topics SET count = count + 1,
    dt_first = MIN(COALESCE(dt_first, :dt), :dt),
    dt_last  = MAX(COALESCE(dt_last,  :dt), :dt),
    timestamp_first = MIN(COALESCE(timestamp_first, :timestamp), :timestamp),
    timestamp_last  = MAX(COALESCE(timestamp_last,  :timestamp), :timestamp)
    WHERE name = :name AND type = :type
    """

    ## SQL statement for inserting types
    INSERT_TYPE = """
    INSERT INTO types (type, definition, subtypes)
    VALUES (:type, :definition, :subtypes)
    """

    ## SQL statement for creating a view for topic
    CREATE_TOPIC_VIEW = """
    CREATE VIEW %(name)s AS
    SELECT %(cols)s
    FROM %(type)s
    WHERE _topic = %(topic)s
    """

    ## SQL statement template for inserting message types
    INSERT_MESSAGE_TYPE = """
    INSERT INTO %(type)s (_topic, _dt, _timestamp, _parent_type, _parent_id%(cols)s)
    VALUES (:_topic, :_dt, :_timestamp, :_parent_type, :_parent_id%(vals)s)
    """

    ## Default columns for pkg/MsgType tables
    MESSAGE_TYPE_BASECOLS = [("_topic",       "TEXT"),
                             ("_dt",          "TIMESTAMP"),
                             ("_timestamp",   "INTEGER"),
                             ("_id",          "INTEGER NOT NULL "
                                              "PRIMARY KEY AUTOINCREMENT"),
                             ("_parent_type", "TEXT"),
                             ("_parent_id",   "INTEGER"), ]


    def __init__(self, args):
        """
        @param   args              arguments object like argparse.Namespace
        @param   args.META         whether to print metainfo
        @param   args.OUTFILE      name of SQLite file to write,
                                   will be appended to if exists
        @param   args.WRAP_WIDTH   character width to wrap message YAML output at
        @param   args.VERBOSE      whether to print debug information
        """
        args = TextSinkMixin.make_full_yaml_args(args)

        super(SqliteSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)

        self._filename      = args.OUTFILE
        self._db            = None   # sqlite3.Connection
        self._close_printed = False

        self._topics = {}  # {(topic, typename): {topics-row}}
        self._types  = {}  # {typename: {types-row}}
        # {"view": {topic: {typename: True}}, "table": {typename: {cols}}}
        self._schema = collections.defaultdict(dict)

        self._format_repls.update({k: "" for k in self._format_repls})
        atexit.register(self.close)


    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output file."""
        if not self._db:
            self._init_db()
        typename = rosapi.get_message_type(msg)
        self._process_topic(topic, typename, msg)
        self._process_message(topic, typename, msg, stamp)
        super(SqliteSink, self).emit(topic, index, stamp, msg, match)


    def close(self):
        """Closes output file, if any."""
        if self._db:
            self._db.close()
            self._db = None
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), self._filename,
                                 format_bytes(os.path.getsize(self._filename)))



    def _init_db(self):
        """Opens the database file and populates schema if not already existing."""
        for t in (dict, list, tuple): sqlite3.register_adapter(t, json.dumps)
        if self._args.VERBOSE:
            sz = os.path.exists(self._filename) and os.path.getsize(self._filename)
            ConsolePrinter.debug("%s %s%s.", "Adding to" if sz else "Creating", self._filename,
                                 (" (%s)" % format_bytes(sz)) if sz else "")
        self._db = sqlite3.connect(self._filename, isolation_level=None, check_same_thread=False)
        self._db.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))
        self._db.executescript(self.BASE_SCHEMA)
        for row in self._db.execute("SELECT * FROM topics"):
            topickey = (row["name"], row["type"])
            self._topics[topickey] = row
        for row in self._db.execute("SELECT * FROM types"):
            self._types[row["type"]] = row
        for row in self._db.execute("SELECT name FROM sqlite_master "
                                    "WHERE type = 'table' AND name LIKE '%/%'"):
            cols = self._db.execute("PRAGMA table_info(%s)" % quote(row["name"])).fetchall()
            cols = [c for c in cols if not c["name"].startswith("_")]
            self._schema["table"][row["name"]] = {c["name"]: c for c in cols}
        for row in self._db.execute("SELECT sql FROM sqlite_master "
                                    "WHERE type = 'view' AND name LIKE '%/%'"):
            try:
                topic = re.search(r'WHERE _topic = "([^"]+)"', row["sql"]).group(1)
                typename = re.search(r'FROM "([^"]+)"', row["sql"]).group(1)
                self._schema["view"].setdefault(topic, {})[typename] = True
            except Exception as e:
                ConsolePrinter.error("Error parsing view %s: %s.", row, e)


    def _process_topic(self, topic, typename, msg):
        """Inserts topic and message rows and tables and views if not already existing."""
        topickey = (topic, typename)
        if topickey not in self._topics:
            targs = dict(name=topic, type=typename)
            targs["id"] = self._db.execute(self.INSERT_TOPIC, targs).lastrowid
            self._topics[topickey] = targs

        self._process_type(typename, msg)

        if topic not in self._schema["view"] or typename not in self._schema["view"][topic]:
            cols = ["_id", "_dt"] + list(self._schema["table"][typename])
            fargs = dict(topic=quote(topic), cols=", ".join(map(quote, cols)), type=quote(typename))
            suffix = " (%s)" % typename if topic in self._schema["view"] else ""
            fargs["name"] = quote(topic + suffix)
            sql = self.CREATE_TOPIC_VIEW % fargs
            if self._args.VERBOSE:
                ConsolePrinter.debug("Adding topic %s.", topic)
            self._db.execute(sql)
            self._schema["view"].setdefault(topic, {})[typename] = True


    def _process_type(self, typename, msg):
        """Inserts type rows and creates pkg/MsgType tables if not already existing."""
        if typename not in self._types:
            msgdef = self.source.get_message_definition(typename)
            targs = dict(type=typename, definition=msgdef, subtypes={})
            self._types[typename] = targs
            for path, submsgs, subtype in self._iter_fields(msg, messages_only=True):
                subtype = rosapi.scalar(subtype)
                targs["subtypes"][".".join(path)] = subtype

                if not isinstance(submsgs, (list, tuple)): submsgs = [submsgs]
                elif not submsgs: submsgs = [rosapi.get_message_class(subtype)()]
                for submsg in submsgs:
                    submsg = submsg or self.source.get_message_class(subtype)()  # List may be empty
                    self._process_type(subtype, submsg)

            targs["id"] = self._db.execute(self.INSERT_TYPE, targs).lastrowid

        if typename not in self._schema["table"]:
            cols = []
            for path, value, subtype in self._iter_fields(msg):
                suffix = "[]" if isinstance(value, (list, tuple)) else ""
                cols += [(".".join(path), quote(rosapi.scalar(subtype) + suffix))]
            coldefs = ["%s %s" % (quote(n), t) for n, t in cols + self.MESSAGE_TYPE_BASECOLS]
            sql = "CREATE TABLE %s (%s)" % (quote(typename), ", ".join(coldefs))
            self._schema["table"][typename] = collections.OrderedDict(cols)
            self._db.execute(sql)


    def _process_message(self, topic, typename, msg, stamp):
        """Inserts message to messages-table, and to pkg/MsgType tables."""
        topic_id = self._topics[(topic, typename)]["id"]
        margs = dict(dt=rosapi.to_datetime(stamp), timestamp=rosapi.to_nsec(stamp),
                     topic=topic, name=topic, topic_id=topic_id, type=typename,
                     yaml=self.format_message(msg), data=rosapi.get_message_data(msg))
        self._db.execute(self.INSERT_MESSAGE, margs)
        self._db.execute(self.UPDATE_TOPIC,   margs)
        self._populate_type(topic, typename, msg, stamp)


    def _populate_type(self, topic, typename, msg, stamp, parent_type=None, parent_id=None):
        """
        Inserts pkg/MsgType row for this message, and sub-rows for subtypes in message.

        Returns inserted ID.
        """
        args = dict(_topic=topic, _dt=rosapi.to_datetime(stamp),
                    _timestamp=rosapi.to_nsec(stamp),
                    _parent_type=parent_type, _parent_id=parent_id)
        cols, vals = [], []
        for p, v, t in self._iter_fields(msg):
            if isinstance(v, (list, tuple)) and rosapi.scalar(t) not in rosapi.ROS_BUILTIN_TYPES:
                continue  # for p, v, t
            cols += [".".join(p)]
            vals += [re.sub(r"\W", "_", cols[-1])]
            args[vals[-1]] = v
        inter = ", " if cols else ""
        fargs = dict(type=quote(typename), cols=inter + ", ".join(map(quote, cols)),
                     vals=inter + ", ".join(":" + c for c in vals))
        sql = self.INSERT_MESSAGE_TYPE % fargs
        myid = args["_id"] = self._db.execute(sql, args).lastrowid

        subids = {}  # {path: [ids]}
        for subpath, submsgs, subtype in self._iter_fields(msg, messages_only=True):
            subtype = rosapi.scalar(subtype)
            if isinstance(submsgs, (list, tuple)):
                subids[subpath] = []
            for submsg in submsgs if isinstance(submsgs, (list, tuple)) else [submsgs]:
                subid = self._populate_type(topic, subtype, submsg, stamp, typename, myid)
                if isinstance(submsgs, (list, tuple)):
                    subids[subpath].append(subid)
        if subids:
            sql = "UPDATE %s SET " % quote(typename)
            for i, (path, subids) in enumerate(subids.items()):
                sql += "%s%s = :%s" % (", " if i else "", quote(".".join(path)), "_".join(path))
                args["_".join(path)] = subids
            sql += " WHERE _id = :_id"
            self._db.execute(sql, args)
        return myid


    def _iter_fields(self, msg, messages_only=False, top=()):
        """
        Yields ((nested, path), value, typename) from ROS message.

        @param  messages_only  whether to yield only values that are ROS messages themselves
                               or lists of ROS messages, else will yield scalar and list values
        """
        fieldmap = rosapi.get_message_fields(msg)
        for k, t in fieldmap.items() if fieldmap != msg else ():
            v, path = rosapi.get_message_value(msg, k, t), top + (k, )
            is_true_msg, is_msg_or_time = (rosapi.is_ros_message(v, ignore_time=x) for x in (1, 0))
            is_sublist = isinstance(v, (list, tuple)) and \
                         rosapi.scalar(t) not in rosapi.ROS_COMMON_TYPES
            if (is_true_msg or is_sublist) if messages_only else not is_msg_or_time:
                yield path, v, t
            if is_msg_or_time:
                for p2, v2, t2 in self._iter_fields(v, messages_only, top=path):
                    yield p2, v2, t2



class TopicSink(SinkBase):
    """Publishes messages to ROS topics."""

    def __init__(self, args):
        """
        @param   args                   arguments object like argparse.Namespace
        @param   args.META              whether to print metainfo
        @param   args.QUEUE_SIZE_OUT    publisher queue size
        @param   args.PUBLISH_PREFIX    output topic prefix, prepended to input topic
        @param   args.PUBLISH_SUFFIX    output topic suffix, appended to output topic
        @param   args.PUBLISH_FIXNAME   single output topic name to publish to,
                                        overrides prefix and suffix if given
        @param   args.VERBOSE           whether to print debug information
        """
        super(TopicSink, self).__init__(args)
        self._pubs = {}  # {(intopic, cls): ROS publisher}
        self._close_printed = False

    def emit(self, topic, index, stamp, msg, match):
        """Publishes message to output topic."""
        key, cls = (topic, type(msg)), type(msg)
        if key not in self._pubs:
            topic2 = self._args.PUBLISH_PREFIX + topic + self._args.PUBLISH_SUFFIX
            topic2 = self._args.PUBLISH_FIXNAME or topic2
            if self._args.VERBOSE:
                ConsolePrinter.debug("Publishing from %s to %s.", topic, topic2)

            pub = None
            if self._args.PUBLISH_FIXNAME:
                pub = next((v for (_, c), v in self._pubs.items() if c == cls), None)
            pub = pub or rosapi.create_publisher(topic2, cls, queue_size=self._args.QUEUE_SIZE_OUT)
            self._pubs[key] = pub
            self._counts.setdefault(topic, 0)

        self._counts[topic] += 1
        self._pubs[key].publish(msg)

    def bind(self, source):
        """Attaches source to sink and blocks until connected to ROS."""
        SinkBase.bind(self, source)
        rosapi.init_node()

    def validate(self):
        """Returns whether ROS environment is set, prints error if not."""
        return rosapi.validate(live=True)

    def close(self):
        """Shuts down publishers."""
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Published %s to %s.",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(set(self._pubs.values()))))
        for t in list(self._pubs):
            pub = self._pubs.pop(t)
            # ROS1 prints errors when closing a publisher with subscribers
            not pub.get_num_connections() and pub.unregister()
        super(TopicSink, self).close()
        rosapi.shutdown_node()


class MultiSink(SinkBase):
    """Combines any number of sinks."""

    ## Autobinding between argument flags and sink classes
    FLAG_CLASSES = {"PUBLISH": TopicSink, "CONSOLE": ConsoleSink}

    ## Autobinding between argument flags+subflags and sink classes
    SUBFLAG_CLASSES = {"OUTFILE": {"OUTFILE_FORMAT": {"bag": BagSink, "html":   HtmlSink,
                                                      "csv": CsvSink, "sqlite": SqliteSink}}}

    ## Autobinding between flag value file extension and subflags
    SUBFLAG_AUTODETECTS = {"OUTFILE": {"OUTFILE_FORMAT": {
        "bag":  lambda: rosapi.BAG_EXTENSIONS, "csv": (".csv", ),  # Need late binding
        "html": (".htm", ".html"),             "sqlite": (".sqlite", ".sqlite3")}
    }}

    def __init__(self, args):
        """
        @param   args                  arguments object like argparse.Namespace
        @param   args.CONSOLE          print matches to console
        @param   args.OUTFILE          write matches to output file
        @param   args.OUTFILE_FORMAT   output file format, "bag" or "html"
        @param   args.PUBLISH          publish matches to live topics
        """
        super(MultiSink, self).__init__(args)

        ## List of all combined sinks
        self.sinks = [cls(args) for flag, cls in self.FLAG_CLASSES.items()
                      if getattr(args, flag, None)]
        for flag, opts in self.SUBFLAG_CLASSES.items():
            if not getattr(args, flag, None): continue  # for glag

            found = None
            for subflag, binding in opts.items():
                ext = os.path.splitext(getattr(args, flag))[-1].lower()
                found = False if getattr(args, subflag, None) else None
                for cls in filter(bool, [binding.get(getattr(args, subflag, None))]):
                    self.sinks += [cls(args)]
                    found = True
                    break  # for cls
                if found is None and subflag in self.SUBFLAG_AUTODETECTS.get(flag, {}):
                    subopts = self.SUBFLAG_AUTODETECTS[flag][subflag]
                    subflagval = next((k for k, v in subopts.items()
                                       if ext in (v() if callable(v) else v)), None)
                    if subflagval in opts[subflag]:
                        cls = opts[subflag][subflagval]
                        self.sinks += [cls(args)]
                        found = True
            if not found:
                raise Exception("Unknown output format.")

    def emit_meta(self):
        """Outputs source metainfo in one sink, if not already emitted."""
        sink = next((s for s in self.sinks if isinstance(s, ConsoleSink)), None)
        # Print meta in one sink only, prefer console
        sink = sink or self.sinks[0] if self.sinks else None
        sink and sink.emit_meta()

    def emit(self, topic, index, stamp, msg, match):
        """Outputs ROS message to all sinks."""
        for sink in self.sinks:
            sink.emit(topic, index, stamp, msg, match)

    def bind(self, source):
        """Attaches source to all sinks."""
        SinkBase.bind(self, source)
        for sink in self.sinks:
            sink.bind(source)

    def validate(self):
        """Returns whether prerequisites are met for all sinks."""
        return all([sink.validate() for sink in self.sinks])

    def close(self):
        """Closes all sinks."""
        for sink in self.sinks:
            sink.close()

    def flush(self):
        """Flushes all sinks."""
        for sink in self.sinks:
            sink.flush()

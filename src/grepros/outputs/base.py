# -*- coding: utf-8 -*-
"""
Main outputs for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    12.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs.base
from __future__ import print_function
import atexit
import collections
import copy
import os
import re
import sys

import yaml

from .. common import ConsolePrinter, MatchMarkers, TextWrapper, \
                      filter_fields, format_bytes, merge_spans, plural, wildcard_to_regex
from .. import rosapi


class SinkBase(object):
    """Output base class."""

    ## Auto-detection file extensions for subclasses, as (".ext", )
    FILE_EXTENSIONS = ()

    def __init__(self, args):
        """
        @param   args        arguments object like argparse.Namespace
        @param   args.META   whether to print metainfo
        """
        self._args = copy.deepcopy(args)
        self._batch_meta = {}  # {source batch: "source metadata"}
        self._counts     = {}  # {(topic, typehash): count}

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
        topickey = (topic, self.source.get_message_type_hash(msg))
        self._counts[topickey] = self._counts.get(topickey, 0) + 1

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

    def is_highlighting(self):
        """Returns whether this sink requires highlighted matches."""
        return False

    @classmethod
    def autodetect(cls, dump_target):
        """Returns true if dump_target is recognizable as output for this sink class."""
        ext = os.path.splitext(dump_target or "")[-1].lower()
        return ext in cls.FILE_EXTENSIONS


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
                    if is_strlist and self._wrapper.strip(v) != "[]": v = "\n" + v
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
                                 "pfx0": ConsolePrinter.STYLE_SPECIAL,  # Content line prefix start
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


    def is_highlighting(self):
        """Returns True (requires highlighted matches)."""
        return True



class BagSink(SinkBase):
    """Writes messages to bagfile."""

    def __init__(self, args):
        """
        @param   args               arguments object like argparse.Namespace
        @param   args.META          whether to print metainfo
        @param   args.DUMP_TARGET   name of ROS bagfile to create or append to
        @param   args.VERBOSE       whether to print debug information
        """
        super(BagSink, self).__init__(args)
        self._bag = None
        self._close_printed = False

        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output bagfile."""
        if not self._bag:
            if self._args.VERBOSE:
                sz = os.path.isfile(self._args.DUMP_TARGET) and \
                     os.path.getsize(self._args.DUMP_TARGET)
                ConsolePrinter.debug("%s %s%s.", "Appending to" if sz else "Creating",
                                     self._args.DUMP_TARGET,
                                     (" (%s)" % format_bytes(sz)) if sz else "")
            self._bag = rosapi.create_bag_writer(self._args.DUMP_TARGET)

        topickey = (topic, self.source.get_message_type_hash(msg))
        if topickey not in self._counts and self._args.VERBOSE:
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
                                 plural("topic", len(self._counts)), self._args.DUMP_TARGET,
                                 format_bytes(os.path.getsize(self._args.DUMP_TARGET)))
        super(BagSink, self).close()

    @classmethod
    def autodetect(cls, dump_target):
        """Returns true if dump_target is recognizable as a ROS bag."""
        ext = os.path.splitext(dump_target or "")[-1].lower()
        return ext in rosapi.BAG_EXTENSIONS


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
        topickey, cls = (topic, self.source.get_message_type_hash(msg)), type(msg)
        if topickey not in self._pubs:
            topic2 = self._args.PUBLISH_PREFIX + topic + self._args.PUBLISH_SUFFIX
            topic2 = self._args.PUBLISH_FIXNAME or topic2
            if self._args.VERBOSE:
                ConsolePrinter.debug("Publishing from %s to %s.", topic, topic2)

            pub = None
            if self._args.PUBLISH_FIXNAME:
                pub = next((v for (_, c), v in self._pubs.items() if c == cls), None)
            pub = pub or rosapi.create_publisher(topic2, cls, queue_size=self._args.QUEUE_SIZE_OUT)
            self._pubs[topickey] = pub

        self._pubs[topickey].publish(msg)
        super(TopicSink, self).emit(topic, index, stamp, msg, match)

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
        for k in list(self._pubs):
            pub = self._pubs.pop(k)
            # ROS1 prints errors when closing a publisher with subscribers
            not pub.get_num_connections() and pub.unregister()
        super(TopicSink, self).close()
        rosapi.shutdown_node()

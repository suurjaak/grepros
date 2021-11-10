# -*- coding: utf-8 -*-
"""
Outputs for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    10.11.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs
from __future__ import print_function
import atexit
import copy
import os
try: import queue  # Py3
except ImportError: import Queue as queue  # Py2
import re
import sys
import threading

import yaml

from . common import ConsolePrinter, MatchMarkers, TextWrapper, \
                     filter_fields, merge_spans, unique_path, wildcard_to_regex
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


class TextSinkMixin(object):
    """Provides message formatting as text."""


    def __init__(self, args):
        """
        @param   args                       arguments object like argparse.Namespace
        @param   args.PRINT_FIELDS          message fields to use in output if not all
        @param   args.NOPRINT_FIELDS        message fields to skip in output
        @param   args.MAX_FIELD_LINES       maximum number of lines to output per field
        @param   args.START_LINE            message line number to start output from
        @param   args.END_LINE              message line number to stop output at
        @param   args.MAX_MESSAGE_LINES     maximum number of lines to output per message
        @param   args.LINES_AROUND_MATCH    number of message lines around matched fields to output
        @param   args.MATCHED_FIELDS_ONLY   output only the fields where match was found
        @param   args.WRAP_WIDTH            character width to wrap message YAML output at
        """
        self._prefix   = ""    # Put before each message line (filename if grepping 1+ files)
        self._wrapper  = None  # TextWrapper instance
        self._patterns = {}    # {key: [(() if any field else ('nested', 'path'), re.Pattern), ]}
        self._format_repls = {MatchMarkers.START: ConsolePrinter.HIGHLIGHT_START,
                              MatchMarkers.END:   ConsolePrinter.HIGHLIGHT_END}

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
                lines = lines and (lines[:-1] + [lines[-1] + ConsolePrinter.STYLE_RESET])

            if self._args.LINES_AROUND_MATCH and highlight:
                spans, NUM = [], self._args.LINES_AROUND_MATCH
                for i, l in enumerate(lines):
                    if MatchMarkers.START in l:
                        spans.append([max(0, i - NUM), min(i + NUM + 1, len(lines))])
                    if MatchMarkers.END in l and spans:
                        spans[-1][1] = min(i + NUM + 1, len(lines))
                lines = sum((lines[a:b - 1] + [lines[b - 1] + ConsolePrinter.STYLE_RESET]
                             for a, b in merge_spans(spans)), [])

            if self._prefix:
                lines = [self._prefix + l for l in lines]

            text = "\n".join(lines)

        for a, b in self._format_repls.items() if highlight else ():
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
                    lines[i] = l = MatchMarkers.START + l
                if startpos1 >= 0 and endpos1 < startpos1 and i + 1 < len(lines):
                    lines[i + 1] = MatchMarkers.START + lines[i + 1]
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
                    extra_indent = indent if is_strlist else " " * len(indent + k + ": ")
                    self._wrapper.reserve_width(self._prefix + extra_indent)
                    self._wrapper.drop_whitespace = t.endswith("]") and not is_strlist
                    v = ("\n" + extra_indent).join(retag_match_lines(self._wrapper.wrap(v)))
                    if is_strlist: v = "\n" + v
                vals.append("%s%s: %s" % (indent, k, rosapi.format_message_value(val, k, v)))
            return ("\n" if indent and vals else "") + "\n".join(vals)

        return str(val)


    def _configure(self, args):
        """Initializes output settings."""
        prints, noprints = args.PRINT_FIELDS, args.NOPRINT_FIELDS
        for key, vals in [("print", prints), ("noprint", noprints)]:
            self._patterns[key] = [(tuple(v.split(".")), wildcard_to_regex(v)) for v in vals]

        HL0, HL1 = ConsolePrinter.HIGHLIGHT_START, ConsolePrinter.HIGHLIGHT_END
        LL0, LL1 = ConsolePrinter.LOWLIGHT_START,  ConsolePrinter.LOWLIGHT_END
        custom_widths = {
            MatchMarkers.START:            len(HL0.replace(ConsolePrinter.STYLE_HIGHLIGHT, "")),
            MatchMarkers.END:              len(HL1.replace(ConsolePrinter.STYLE_RESET,     "")),
            ConsolePrinter.LOWLIGHT_START: 0,
            ConsolePrinter.LOWLIGHT_END:   0,
            ConsolePrinter.ERROR_START:    0,
            ConsolePrinter.ERROR_END:      0,
            ConsolePrinter.PREFIX_START:   0,
            ConsolePrinter.PREFIX_END:     0,
        }
        width = ConsolePrinter.WIDTH if args.WRAP_WIDTH is None else args.WRAP_WIDTH
        wrapargs = dict(width=width, max_lines=args.MAX_FIELD_LINES,
                        placeholder="%s ...%s" % (LL0, LL1))
        self._wrapper = TextWrapper(custom_widths=custom_widths, **wrapargs)



class ConsoleSink(SinkBase, TextSinkMixin):
    """Prints messages to console."""

    META_LINE_TEMPLATE   = "{coloron}{sep} {line}{coloroff}"
    MESSAGE_SEP_TEMPLATE = "{coloron}{sep}{coloroff}"
    PREFIX_TEMPLATE      = "{coloron}{batch}{coloroff}{sepcoloron}{sep}{sepcoloroff}"
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
        super(ConsoleSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)

        self._configure(args)


    def emit_meta(self):
        """Prints source metainfo like bag header, if not already printed."""
        batch = self._args.META and self.source.get_batch()
        if self._args.META and batch not in self._batch_meta:
            meta = self._batch_meta[batch] = self.source.format_meta()
            kws = dict(coloron=ConsolePrinter.LOWLIGHT_START,
                       coloroff=ConsolePrinter.LOWLIGHT_END, sep=self.SEP)
            meta = "\n".join(x and self.META_LINE_TEMPLATE.format(**dict(kws, line=x))
                             for x in meta.splitlines())
            meta and ConsolePrinter.print(meta)


    def emit(self, topic, index, stamp, msg, match):
        """Prints separator line and message text."""
        self._prefix = ""
        if self._args.LINE_PREFIX and self.source.get_batch():
            sep = self.MATCH_PREFIX_SEP if match else self.CONTEXT_PREFIX_SEP
            kws = dict(coloron=ConsolePrinter.PREFIX_START, sep=sep,
                       coloroff=ConsolePrinter.PREFIX_END, batch=self.source.get_batch(),
                       sepcoloron=ConsolePrinter.SEP_START, sepcoloroff=ConsolePrinter.SEP_END)
            self._prefix = self.PREFIX_TEMPLATE.format(**kws)
        kws = dict(coloron=ConsolePrinter.LOWLIGHT_START,
                   coloroff=ConsolePrinter.LOWLIGHT_END, sep=self.SEP)
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
        @param   args          arguments object like argparse.Namespace
        @param   args.META     whether to print metainfo
        @param   args.OUTFILE  name of ROS bagfile to create or append to
        """
        super(BagSink, self).__init__(args)
        self._bag    = None
        self._close_printed = False

        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output bagfile."""
        if not self._bag:
            if self._args.META:
                a = os.path.isfile(self._args.OUTFILE) and os.path.getsize(self._args.OUTFILE)
                ConsolePrinter.debug("%s %s.", "Appending to" if a else "Creating",
                                     self._args.OUTFILE)
            self._bag = rosapi.create_bag_writer(self._args.OUTFILE)

        if topic not in self._counts:
            ConsolePrinter.debug("Adding topic %s.", topic)

        self._bag.write(topic, msg, stamp)
        super(BagSink, self).emit(topic, index, stamp, msg, match)

    def validate(self):
        """Returns whether ROS environment is set, prints error if not."""
        return rosapi.validate()

    def close(self):
        """Closes output bagfile, if any."""
        self._bag and self._bag.close()
        if not self._close_printed and self._counts and self._args.META:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s message(s) in %s topic(s) to %s.",
                                 sum(self._counts.values()), len(self._counts), self._args.OUTFILE)



class HtmlSink(SinkBase, TextSinkMixin):
    """Writes messages to bagfile."""

    ## HTML template path
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "templates", "html.tpl")

    def __init__(self, args):
        """
        @param   args                       arguments object like argparse.Namespace
        @param   args.META                  whether to print metainfo
        @param   args.OUTFILE               name of HTML file to write,
                                            will add counter like (2) to filename if exists
        @param   args.OUTFILE_TEMPLATE      path to custom HTML template, if any
        @param   args.PRINT_FIELDS          message fields to use in output if not all
        @param   args.NOPRINT_FIELDS        message fields to skip in output
        @param   args.MAX_FIELD_LINES       maximum number of lines to output per field
        @param   args.START_LINE            message line number to start output from
        @param   args.END_LINE              message line number to stop output at
        @param   args.MAX_MESSAGE_LINES     maximum number of lines to output per message
        @param   args.LINES_AROUND_MATCH    number of message lines around matched fields to output
        @param   args.MATCHED_FIELDS_ONLY   output only the fields where match was found
        @param   args.WRAP_WIDTH            character width to wrap message YAML output at
        """
        super(HtmlSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)
        self._queue    = queue.Queue()
        self._writer   = None          # threading.Thread running _stream()
        self._filename = args.OUTFILE  # Filename base, will be made unique
        self._template_path = args.OUTFILE_TEMPLATE or self.TEMPLATE_PATH
        self._close_printed = False
        self._tag_repls = {MatchMarkers.START:            '<span class="match">',
                           MatchMarkers.END:              '</span>',
                           ConsolePrinter.LOWLIGHT_START: '<span class="lowlight">',
                           ConsolePrinter.LOWLIGHT_END:   '</span>'}
        self._tag_rgx = re.compile("(%s)" % "|".join(map(re.escape, self._tag_repls)))

        self._format_repls.clear()
        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output file."""
        if not self._writer:
            self._writer = threading.Thread(target=self._stream)
            self._writer.start()
        self._queue.put((topic, index, stamp, msg, match))

    def validate(self):
        """Returns whether ROS environment is set, prints error if not."""
        return rosapi.validate()

    def close(self):
        """Closes output file, if any."""
        if self._writer:
            writer, self._writer = self._writer, None
            self._queue.put(None)
            writer.is_alive() and writer.join()
        if not self._close_printed and self._counts and self._args.META:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s message(s) in %s topic(s) to %s.",
                                 sum(self._counts.values()), len(self._counts), self._filename)

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

        with open(self._template_path, "r") as f: tpl = f.read()
        template = step.Template(tpl, escape=True, strip=False)
        ns = dict(source=self.source, sink=self, args=["grepros"] + sys.argv[1:],
                  messages=self._produce())
        self._filename = unique_path(self._filename, empty_ok=True)
        if self._args.META:
            ConsolePrinter.debug("Creating %s.", self._filename)
        with open(self._filename, "wb") as f:
            template.stream(f, ns, unbuffered=True)
        self._writer = None

    def _produce(self):
        """Yields messages from emit queue, as (topic, index, stamp, msg, match)."""
        while True:
            entry = self._queue.get()
            if entry is None:
                break  # while
            (topic, index, stamp, msg, match) = entry
            if self._args.META and topic not in self._counts:
                ConsolePrinter.debug("Adding topic %s.", topic)
            yield entry
            super(HtmlSink, self).emit(topic, index, stamp, msg, match)


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
            self._args.META and ConsolePrinter.debug("Publishing from %s to %s.", topic, topic2)

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
        return rosapi.validate()

    def close(self):
        """Shuts down publishers."""
        if not self._close_printed and self._counts and self._args.META:
            self._close_printed = True
            ConsolePrinter.debug("Published %s message(s) to %s topic(s).",
                                 sum(self._counts.values()), len(set(self._pubs.values())))
        for t in list(self._pubs):
            pub = self._pubs.pop(t)
            # ROS1 prints errors when closing a publisher with subscribers
            not pub.get_num_connections() and pub.unregister()
        rosapi.shutdown_node()



class MultiSink(SinkBase):
    """Combines any number of sinks."""

    ## Autobinding between argument flags and sink classes
    FLAG_CLASSES = {"PUBLISH": TopicSink, "CONSOLE": ConsoleSink}

    ## Autobinding between argument flags+subflags and sink classes
    SUBFLAG_CLASSES = {"OUTFILE": {"OUTFILE_FORMAT": {"bag": BagSink, "html": HtmlSink}}}

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
            for subflag, binding in opts.items() if getattr(args, flag, None) else ():
                for cls in filter(bool, [binding.get(getattr(args, subflag, None))]):
                    self.sinks += [cls(args)]

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

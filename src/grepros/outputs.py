# -*- coding: utf-8 -*-
"""
Outputs for grep.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS message content.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    23.10.2021
------------------------------------------------------------------------------
"""
import atexit
import collections
import contextlib
import copy
import os
import sys

import genpy
import rosbag
import rospy
import yaml

from . common import ConsolePrinter, MatchMarkers, TextWrapper
from . common import ROS_NUMERIC_TYPES, ROS_BUILTIN_TYPES, filter_fields, get_message_fields, \
                     get_message_value, merge_spans, wildcard_to_regex
from . import inputs


class SinkBase:
    """Output base class."""

    def __init__(self, args):
        self._args = copy.deepcopy(args)

        self.source = None

    def emit_source(self):
        """
        Outputs source metainfo if not already emitted, e.g. bag header for console output.
        """

    def emit(self, topic, index, stamp, msg, match):
        """
        Outputs ROS message.

        @param   topic  full name of ROS topic the message is from
        @param   index  message index in topic
        @param   msg    ROS message
        @param   match  ROS message with matched values tagged with markers, if matched
        """

    def bind(self, source):
        """Attaches source to sink"""
        self.source = source

    def close(self):
        """Shuts down output, closing any files or connections."""


class ConsoleSink(SinkBase):
    """Prints messages to console."""

    BAG_META_TEMPLATE = (
        "\n{pref}{sep} File {file} ({size}), {tcount} topics, {mcount:,d} messages{suff}\n"
        "{pref}{sep} File span {delta} ({start} - {end}){suff}"
    )

    MESSAGE_META_TEMPLATE = (
        "{pref}{sep} Topic {topic} message {index} ({type}, {stamp}){suff}"
    )
    MESSAGE_META_TOTAL_TEMPLATE = (
        "{pref}{sep} Topic {topic} message {index}/{total} ({type}, {stamp}){suff}"
    )

    MESSAGE_SEP_TEMPLATE = (
        "{pref}{sep}{suff}"
    )

    META_SEP = "---"  # Prefix of metainfo lines


    def __init__(self, args):
        super().__init__(args)
        self.source = None

        self._use_prefix = False  # Whether to use bagfile prefix in output
        self._prefix     = ""     # Printed before each message line (filename if grepping 1+ files)
        self._wrapper    = None   # TextWrapper instance
        self._meta_done  = set()  # {bagfile path, }
        self._patterns   = {}     # {key: [(() if any field else ('nested', 'path'), re.Pattern), ]}
        self._printed    = collections.defaultdict(int)  # {topic: count}

        # {topic: {None: count processed, True: count matched, False: count printed as context}}
        self._counts = collections.defaultdict(lambda: collections.defaultdict(int))

        atexit.register(self._on_exit)
        self._configure(args)


    def emit_source(self):
        """
        Outputs source metainfo if not already emitted: bag header if source is `BagSource`.
        """
        if not self._args.META or not isinstance(self.source, inputs.BagSource) \
        or self.source.filename in self._meta_done:
            return

        kws = dict(pref=ConsolePrinter.LOWLIGHT_START, suff=ConsolePrinter.LOWLIGHT_END,
                   sep=self.META_SEP, **self.source.get_meta())
        print(self.BAG_META_TEMPLATE.format(**kws))
        self._meta_done.add(self.source.filename)


    def emit(self, topic, index, stamp, msg, match):
        """Prints separator line and message text."""
        self._prefix = ""
        if self._use_prefix and isinstance(self.source, inputs.BagSource):
            self._prefix = "%s%s:%s" % (ConsolePrinter.PREFIX_START, self.source.filename,
                                        ConsolePrinter.PREFIX_END)
        kws = dict(pref=ConsolePrinter.LOWLIGHT_START, suff=ConsolePrinter.LOWLIGHT_END,
                   sep=self.META_SEP, **self.source.get_message_meta(topic, index, stamp, msg))
        if self._args.META:
            tpl = self.MESSAGE_META_TOTAL_TEMPLATE if "total" in kws else self.MESSAGE_META_TEMPLATE
            print(tpl.format(**kws))
        elif any(x[True] or x[False] for x in self._counts.values()):
            # @todo see on tiba vale nüüd.
            print(self.MESSAGE_SEP_TEMPLATE.format(**kws))
        print(self.format_message(match or msg, highlight=bool(match)))


    def format_message(self, msg, highlight=False):
        """Returns message as formatted string, optionally highlighted for matches."""
        text = self.message_to_yaml(msg).rstrip("\n")

        if self._use_prefix or self._args.START_LINE or self._args.END_LINE \
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

            if self._use_prefix:
                lines = [self._prefix + l for l in lines]

            text = "\n".join(lines)

        if highlight:  # Cannot use ANSI codes before YAML, they get transformed
            text = text.replace(MatchMarkers.START, ConsolePrinter.HIGHLIGHT_START)
            text = text.replace(MatchMarkers.END,   ConsolePrinter.HIGHLIGHT_END)

        return text


    def message_to_yaml(self, val, top=(), typename=None):
        """Returns ROS message (or other value) as YAML."""
        # Refactored from genpy.message.strify_message().
        scalar  = lambda n: n[:n.index("[")] if "[" in n else n  # Returns bool from bool[10]
        unquote = lambda v, t: v[1:-1] if "string" != t and v[:1] == v[-1:] == '"' else v

        def retag_match_lines(lines):
            """Adds match tags to lines where wrapping separated start and end."""
            for i, l in enumerate(lines):
                startpos0, endpos0 = l.find (MatchMarkers.START), l.find (MatchMarkers.END)
                startpos1, endpos1 = l.rfind(MatchMarkers.START), l.rfind(MatchMarkers.END)
                if endpos0 >= 0 and (startpos0 < 0 or startpos0 > endpos0):
                    lines[i] = MatchMarkers.START + l
                if startpos1 >= 0 and endpos1 < startpos1 and i + 1 < len(lines):
                    lines[i + 1] = MatchMarkers.START + lines[i + 1]
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
            if "string" == scalar(typename):
                yaml_str = yaml.safe_dump(val).rstrip('\n')
                return "\n" + "\n".join(indent + line for line in yaml_str.splitlines())
            vals = [x for v in val for x in [self.message_to_yaml(v, top, typename)] if x]
            if scalar(typename) in ROS_NUMERIC_TYPES:
                return "[%s]" % ", ".join(unquote(str(v), typename) for v in vals)
            return ("\n" + "\n".join(indent + "- " + v for v in vals)) if vals else ""
        if isinstance(val, (genpy.Message, genpy.TVal)):
            FMTS = {"secs": "%10s", "nsecs": "%9s"} if isinstance(val, genpy.TVal) else {}
            MATCHED_ONLY = self._args.MATCHED_FIELDS_ONLY and not self._args.LINES_AROUND_MATCH
            vals, fieldmap = [], get_message_fields(val)
            prints, noprints = self._patterns["print"], self._patterns["noprint"]
            fieldmap = filter_fields(fieldmap, top, include=prints, exclude=noprints)
            for k, t in fieldmap.items():
                v = self.message_to_yaml(get_message_value(val, k, t), top + (k, ), t)
                if not v or MATCHED_ONLY and MatchMarkers.START not in v:
                    continue  # for k, t

                v = unquote(v, t)  # Strip quotes from non-string types cast to "<match>v</match>"
                if scalar(t) in ROS_BUILTIN_TYPES:
                    extra_indent = " " * len(indent + k + ": ")
                    self._wrapper.reserve_width(self._prefix + extra_indent)
                    v = ("\n" + extra_indent).join(retag_match_lines(self._wrapper.wrap(v)))
                vals.append("%s%s: %s" % (indent, k, FMTS.get(k, "%s") % v))
            return ("\n" if indent and vals else "") + "\n".join(vals)

        return str(val)


    def _on_exit(self):
        """atexit callback, finalizes console output."""
        with contextlib.suppress(Exception):
            # Piping cursed output to `more` remains paging if nothing is printed
            not self._printed and not sys.stdout.isatty() and print()


    def _configure(self, args):
        """Initializes output settings."""
        prints, noprints = args.PRINT_FIELDS, args.NOPRINT_FIELDS
        for key, vals in [("print", prints), ("noprint", noprints)]:
            self._patterns[key] = [(tuple(v.split(".")), wildcard_to_regex(v)) for v in vals]

        self._use_prefix = (args.RECURSE or len(args.FILES) != 1 or
                            any("*" in x for x in args.FILES)) if args.FILENAME else False

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
        wrapargs = dict(max_lines=args.MAX_FIELD_LINES, width=ConsolePrinter.WIDTH,
                        placeholder="%s ...%s" % (LL0, LL1))
        self._wrapper = TextWrapper(custom_widths, **wrapargs)



class BagSink(SinkBase):
    """Writes messages to bagfile."""

    def __init__(self, args):
        super().__init__(args)
        self._bag  = None
        self._counts = {}  # {topic: count}
        self._close_printed = False

        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        if not self._bag:
            if os.path.isfile(self._args.OUTBAG) and os.path.getsize(self._args.OUTBAG):
                ConsolePrinter.debug("Appending to bag %s.", self._args.OUTBAG)
                self._bag = rosbag.Bag(self._args.OUTBAG, "a")
            else:
                ConsolePrinter.debug("Creating bag %s.", self._args.OUTBAG)
                self._bag = rosbag.Bag(self._args.OUTBAG, "w")

        if topic not in self._counts:
            ConsolePrinter.debug("Adding topic %s.", topic)
            self._counts[topic] = 0

        self._counts[topic] += 1
        self._bag.write(topic, msg, stamp)

    def close(self):
        """Closes output bag, if any."""
        self._bag and self._bag.close()
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s message(s) in %s topic(s) to %s.",
                                 sum(self._counts.values()), len(self._counts), self._args.OUTBAG)



class TopicSink(SinkBase):
    """Publishes messages to ROS topics."""

    def __init__(self, args):
        """
        @param   args.QUEUE_SIZE_OUT    publisher queue size
        @param       .PUBLISH_PREFIX    output topic prefix, prepended to input topic
        @param       .PUBLISH_SUFFIX    output topic suffix, appended to output topic
        @param       .PUBLISH_FIXNAME   single output topic name to publish to,
                                        overrides prefix and suffix if given
        """
        super().__init__(args)
        self._pubs   = {}  # {(intopic, cls): rospy.Publisher}
        self._counts = {}  # {topic: count}
        self._close_printed = False

    def emit(self, topic, index, stamp, msg, match):
        """Publishes message to output topic."""
        rospy.init_node("grepros", anonymous=True, disable_signals=True)

        key, cls = (topic, type(msg)), type(msg)
        if key not in self._pubs:
            topic2 = self._args.PUBLISH_PREFIX + topic + self._args.PUBLISH_SUFFIX
            topic2 = self._args.PUBLISH_FIXNAME or topic2
            ConsolePrinter.debug("Publishing from %s to %s.", topic, topic2)

            pub = None
            if self._args.PUBLISH_FIXNAME:
                pub = next((v for (_, c), v in self._pubs.items() if c == cls), None)
            pub = pub or rospy.Publisher(topic2, cls, queue_size=self._args.QUEUE_SIZE_OUT)
            self._pubs[key] = pub
            self._counts.setdefault(topic, 0)

        self._counts[topic] += 1
        self._pubs[key].publish(msg)

    def close(self):
        """Shuts down publishers."""
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Published %s message(s) to %s topic(s).",
                                 sum(self._counts.values()), len(set(self._pubs.values())))
        for t in list(self._pubs):
            self._pubs.pop(t).unregister()

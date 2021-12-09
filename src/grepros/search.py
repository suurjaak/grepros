# -*- coding: utf-8 -*-
"""
Search core.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     28.09.2021
@modified    09.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.search
import copy
import collections
import re

from . common import MatchMarkers, filter_fields, merge_spans, wildcard_to_regex
from . import rosapi


class Searcher(object):
    """ROS message grepper."""

    ## Match patterns for global any-match
    ANY_MATCHES = [((), re.compile("(.*)", re.DOTALL)), (), re.compile("(.?)", re.DOTALL)]


    def __init__(self, args):
        """
        @param   args                     arguments object like argparse.Namespace
        @param   args.PATTERNS            pattern(s) to find in message field values
        @param   args.RAW                 PATTERNS are ordinary strings, not regular expressions
        @param   args.CASE                use case-sensitive matching in PATTERNS
        @param   args.INVERT              select non-matching messages
        @param   args.BEFORE              number of messages of leading context to emit before match
        @param   args.AFTER               number of messages of trailing context to emit after match
        @param   args.MAX_MATCHES         number of matched messages to emit (per file if bag input)
        @param   args.MAX_TOPIC_MATCHES   number of matched messages to emit from each topic
        @param   args.MAX_TOPICS          number of topics to print matches from
        @param   args.SELECT_FIELDS       message fields to use in matching if not all
        @param   args.NOSELECT_FIELDS     message fields to skip in matching
        """
        self._args     = copy.deepcopy(args)
        # {key: [(() if any field else ('nested', 'path') or re.Pattern, re.Pattern), ]}
        self._patterns = {}
        # {(topic, type): {message ID: message}}
        self._messages = collections.defaultdict(collections.OrderedDict)
        # {(topic, type): {message ID: ROS time}}
        self._stamps   = collections.defaultdict(collections.OrderedDict)
        # {(topic, type): {None: count processed, True: count matched, False: count emitted as context}}
        self._counts   = collections.defaultdict(collections.Counter)
        # {(topic, type): {message ID: True if matched else False if emitted else None}
        self._statuses = collections.defaultdict(collections.OrderedDict)
        # {(topic, type): (message hash over all fields used in matching)}
        self._hashes = collections.defaultdict(set)
        # Patterns to check in message plaintext and skip full matching if not found
        self._brute_prechecks = []
        self._source = None  # SourceBase instance
        self._sink   = None  # SinkBase instance

        self._parse_patterns()


    def search(self, source, sink):
        """
        Greps messages yielded from source and emits matched content to sink.

        @param   source  inputs.SourceBase instance
        @param   sink    outputs.SinkBase instance
        @return          count matched
        """
        for d in (self._messages, self._stamps, self._counts, self._statuses, self._hashes):
            d.clear()
        self._source, self._sink = source, sink
        source.bind(sink), sink.bind(source)

        counter, total, batch, any_matched = 0, 0, None, False
        for topic, msg, stamp in source.read():
            if batch != source.get_batch():
                total += sum(x[True] for x in self._counts.values())
                for d in (self._counts, self._messages, self._stamps, self._statuses, self._hashes):
                    d.clear()
                counter, batch, any_matched = 0, source.get_batch(), False

            msgid = counter = counter + 1
            topickey = (topic, rosapi.get_message_type(msg))
            self._counts[topickey][None] += 1
            self._messages[topickey][msgid] = msg
            self._stamps  [topickey][msgid] = stamp
            self._statuses[topickey][msgid] = None

            matched = self._is_processable(topic, stamp, msg) and self.get_match(msg)
            if matched:
                self._statuses[topickey][msgid] = True
                self._counts[topickey][True] += 1
                sink.emit_meta()
                for msgid2, i, s, m in self._get_context(topickey, before=True):
                    self._counts[topickey][False] += 1
                    sink.emit(topic, i, s, m, None)
                    self._statuses[topickey][msgid2] = False
                sink.emit(topic, self._counts[topickey][None], stamp, msg, matched)
            elif self._args.AFTER and self._has_in_window(topickey, self._args.AFTER + 1, status=True):
                for msgid2, i, s, m in self._get_context(topickey, before=False):
                    self._counts[topickey][False] += 1
                    sink.emit(topic, i, s, m, None)
                    self._statuses[topickey][msgid2] = False
            source.notify(matched)
            any_matched = any_matched or bool(matched)

            self._prune_data(topickey)
            if any_matched and self._is_max_done():
                sink.flush()
                source.close_batch()

        source.close(), sink.close()
        return total + sum(x[True] for x in self._counts.values())


    def _is_processable(self, topic, stamp, msg):
        """
        Returns whether processing current message in topic is acceptable:
        that topic or total maximum count has not been reached,
        and current message in topic is in configured range, if any.
        """
        topickey = (topic, rosapi.get_message_type(msg))
        if self._args.MAX_MATCHES \
        and sum(x[True] for x in self._counts.values()) >= self._args.MAX_MATCHES:
            return False
        if self._args.MAX_TOPIC_MATCHES \
        and self._counts[topickey][True] >= self._args.MAX_TOPIC_MATCHES:
            return False
        if self._args.MAX_TOPICS:
            topics_matched = [k for k, vv in self._counts.items() if vv[True]]
            if topickey not in topics_matched and len(topics_matched) >= self._args.MAX_TOPICS:
                return False
        if not self._source.is_processable(topic, self._counts[topickey][None], stamp, msg):
            return False
        if self._args.UNIQUE:
            include, exclude = self._patterns["select"], self._patterns["noselect"]
            msghash = rosapi.make_message_hash(msg, include, exclude)
            if msghash in self._hashes[topickey]:
                return False
            self._hashes[topickey].add(msghash)
        return True


    def _prune_data(self, topickey):
        """Drops history older than context window."""
        WINDOW = max(self._args.BEFORE, self._args.AFTER) + 1
        for dct in (self._messages, self._stamps, self._statuses):
            while len(dct[topickey]) > WINDOW:
                dct[topickey].pop(next(iter(dct[topickey])))


    def _parse_patterns(self):
        """Parses pattern arguments into re.Patterns."""
        NOBRUTE_SIGILS = r"\A", r"\Z", "?("  # Regex specials ruling out brute precheck
        BRUTE, FLAGS = not self._args.INVERT, re.DOTALL | (0 if self._args.CASE else re.I)
        self._patterns.clear()
        del self._brute_prechecks[:]
        contents = []
        for v in self._args.PATTERNS:
            split = v.find("=", 1, -1)
            v, path = (v[split + 1:], v[:split]) if split > 0 else (v, ())
            # Special case if '' or "": add pattern for matching empty string
            v = (re.escape(v) if self._args.RAW else v) + ("|^$" if v in ("''", '""') else "")
            path = re.compile(r"(^|\.)%s($|\.)" % ".*".join(map(re.escape, path.split("*")))) \
                   if path else ()
            contents.append((path, re.compile("(%s)" % v, FLAGS)))
            if BRUTE and (self._args.RAW or not any(x in v for x in NOBRUTE_SIGILS)):
                self._brute_prechecks.append(re.compile(v, re.I | re.M))
        if not self._args.PATTERNS:  # Add match-all pattern
            contents.append(self.ANY_MATCHES[0])
        self._patterns["content"] = contents

        selects, noselects = self._args.SELECT_FIELDS, self._args.NOSELECT_FIELDS
        for key, vals in [("select", selects), ("noselect", noselects)]:
            self._patterns[key] = [(tuple(v.split(".")), wildcard_to_regex(v)) for v in vals]


    def _get_context(self, topickey, before=False):
        """Returns unemitted context for latest match, as [(msgid, index, timestamp, message)]."""
        result = []
        count = self._args.BEFORE + 1 if before else self._args.AFTER
        candidates = list(self._statuses[topickey])[-count:]
        current_index = self._counts[topickey][None]
        for i, msgid in enumerate(candidates) if count else ():
            if self._statuses[topickey][msgid] is None:
                idx = current_index + i - (len(candidates) - 1 if before else 1)
                stamp, msg = self._stamps[topickey][msgid], self._messages[topickey][msgid]
                result += [(msgid, idx, stamp, msg)]
        return result


    def _is_max_done(self):
        """Returns whether max match count has been reached (and message after-context emitted)."""
        result, is_maxed = False, False
        if self._args.MAX_MATCHES:
            is_maxed = sum(vv[True] for vv in self._counts.values()) >= self._args.MAX_MATCHES
        if not is_maxed and self._args.MAX_TOPIC_MATCHES:
            count_required = self._args.MAX_TOPICS or len(self._source.topics)
            count_maxed = sum(vv[True] >= self._args.MAX_TOPIC_MATCHES
                              or vv[None] >= (self._source.topics.get(k) or 0)
                              for k, vv in self._counts.items())
            is_maxed = (count_maxed >= count_required)
        if is_maxed:
            result = not self._args.AFTER or \
                     not any(self._has_in_window(k, self._args.AFTER, status=True, full=True)
                             for k in self._counts)
        return result


    def _has_in_window(self, topickey, length, status, full=False):
        """Returns whether given status exists in recent message window."""
        if not length or full and len(self._statuses[topickey]) < length:
            return False
        return status in list(self._statuses[topickey].values())[-length:]


    def get_match(self, msg):
        """
        Returns transformed message if all patterns find a match in message, else None.

        Matching field values are converted to strings and surrounded by markers.
        Returns original message if any-match and sink does not require highlighting.
        """

        def wrap_matches(v, top, is_collection=False):
            """Returns string with parts matching patterns wrapped in marker tags."""
            spans = []
            # Omit collection brackets from match unless empty: allow matching "[]"
            v1 = v2 = v[1:-1] if is_collection and v != "[]" else v
            topstr = ".".join(top)
            for i, (path, p) in enumerate(self._patterns["content"]):
                if not path or path.search(topstr):
                    for match in (m for m in p.finditer(v1) if not v1 or m.start() != m.end()):
                        matched[i] = True
                        spans.append(match.span())
                        if self._args.INVERT:
                            break  # for match
            spans = merge_spans(spans) if not self._args.INVERT else \
                    [] if spans else [(0, len(v1))] if v1 or not is_collection else []
            for a, b in reversed(spans):  # Work from last to first, indices stay the same
                v2 = v2[:a] + MatchMarkers.START + v2[a:b] + MatchMarkers.END + v2[b:]
            return "[%s]" % v2 if is_collection and v != "[]" else v2

        def decorate_message(obj, top=()):
            """Recursively converts field values to pattern-matched strings."""
            selects, noselects = self._patterns["select"], self._patterns["noselect"]
            fieldmap = fieldmap0 = rosapi.get_message_fields(obj)  # Returns obj if not ROS message
            if fieldmap != obj:
                fieldmap = filter_fields(fieldmap, top, include=selects, exclude=noselects)
            for k, t in fieldmap.items() if fieldmap != obj else ():
                v, path = rosapi.get_message_value(obj, k, t), top + (k, )
                is_collection = isinstance(v, (list, tuple))
                if rosapi.is_ros_message(v):
                    decorate_message(v, path)
                elif v and is_collection and rosapi.scalar(t) not in rosapi.ROS_NUMERIC_TYPES:
                    rosapi.set_message_value(obj, k, [decorate_message(x, path) for x in v])
                else:
                    v1 = str(list(v) if isinstance(v, (bytes, tuple)) else v)
                    v2 = wrap_matches(v1, path, is_collection)
                    if len(v1) != len(v2):
                        rosapi.set_message_value(obj, k, v2)
            if not rosapi.is_ros_message(obj):
                v1 = str(list(obj) if isinstance(obj, bytes) else obj)
                v2 = wrap_matches(v1, top)
                obj = v2 if len(v1) != len(v2) else obj
            if not top and not matched and not selects and not fieldmap0 and not self._args.INVERT \
            and set(self._patterns["content"]) <= set(self.ANY_MATCHES):  # Ensure Empty any-match
                matched.update({i: True for i, _ in enumerate(self._patterns["content"])})
            return obj

        if not self._sink.is_highlighting() \
        and not self._patterns["select"] and not self._patterns["noselect"] \
        and not self._args.INVERT and set(self._patterns["content"]) <= set(self.ANY_MATCHES):
            return msg  # Skip decorating if highlighting not required and message matches

        if self._brute_prechecks:
            text  = "\n".join("%r" % (v, ) for _, v, _ in rosapi.iter_message_fields(msg))
            if not all(any(p.finditer(text)) for p in self._brute_prechecks):
                return None  # Skip detailed matching if patterns not present at all

        result, matched = copy.deepcopy(msg), {}  # {pattern index: True}
        decorate_message(result)
        yes = not matched if self._args.INVERT else len(matched) == len(self._patterns["content"])
        return result if yes else None

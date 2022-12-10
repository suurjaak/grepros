# -*- coding: utf-8 -*-
"""
Search core.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     28.09.2021
@modified    10.12.2022
------------------------------------------------------------------------------
"""
## @namespace grepros.search
import copy
import collections
import re

from . common import MatchMarkers, ensure_namespace, filter_fields, merge_spans, wildcard_to_regex
from . import inputs
from . import rosapi


class Searcher(object):
    """ROS message grepper."""

    ## Match patterns for global any-match
    ANY_MATCHES = [((), re.compile("(.*)", re.DOTALL)), (), re.compile("(.?)", re.DOTALL)]

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(PATTERNS=(), CASE=False, RAW=False, INVERT=False, NTH_MATCH=1,
                        BEFORE=0, AFTER=0, MAX_MATCHES=0, MAX_TOPIC_MATCHES=0, MAX_TOPICS=0,
                        SELECT_FIELDS=(), NOSELECT_FIELDS=())


    def __init__(self, args=None, **kwargs):
        """
        @param   args                     arguments as namespace or dictionary, case-insensitive
        @param   args.PATTERNS            pattern(s) to find in message field values
        @param   args.RAW                 PATTERNS are ordinary strings, not regular expressions
        @param   args.CASE                use case-sensitive matching in PATTERNS
        @param   args.INVERT              select non-matching messages
        @param   args.BEFORE              number of messages of leading context to emit before match
        @param   args.AFTER               number of messages of trailing context to emit after match
        @param   args.MAX_MATCHES         number of matched messages to emit (per file if bag input)
        @param   args.MAX_TOPIC_MATCHES   number of matched messages to emit from each topic
        @param   args.MAX_TOPICS          number of topics to print matches from
        @param   args.NTH_MATCH           emit every Nth match in topic
        @param   args.SELECT_FIELDS       message fields to use in matching if not all
        @param   args.NOSELECT_FIELDS     message fields to skip in matching
        @param   kwargs                   any and all arguments as keyword overrides, case-insensitive
        """
        # {key: [(() if any field else ('nested', 'path') or re.Pattern, re.Pattern), ]}
        self._patterns = {}
        # {(topic, typename, typehash): {message ID: message}}
        self._messages = collections.defaultdict(collections.OrderedDict)
        # {(topic, typename, typehash): {message ID: ROS time}}
        self._stamps   = collections.defaultdict(collections.OrderedDict)
        # {(topic, typename, typehash): {None: processed, True: matched, False: emitted as context}}
        self._counts   = collections.defaultdict(collections.Counter)
        # {(topic, typename, typehash): {message ID: True if matched else False if emitted else None}}
        self._statuses = collections.defaultdict(collections.OrderedDict)
        # Patterns to check in message plaintext and skip full matching if not found
        self._brute_prechecks = []
        self._idcounter       = 0      # Counter for unique message IDs
        self._highlight       = False  # Highlight matched values in message fields
        self._passthrough     = False  # Pass all messages to sink, skip matching and highlighting
        self._source = None  # SourceBase instance
        self._sink   = None  # SinkBase instance

        self.args = copy.deepcopy(ensure_namespace(args, Searcher.DEFAULT_ARGS, **kwargs))
        self._parse_patterns()


    def find(self, source, highlight=False):
        """
        Yields matched and context messages from source.

        @param   source     inputs.SourceBase or rosapi.Bag instance
        @param   highlight  whether to highlight matched values in message fields
        @return             tuples of (topic, msg, stamp, matched optionally highlighted msg)
        """
        if not isinstance(source, inputs.SourceBase):
            source = inputs.BagSource(self.args, bag=source)
        self._prepare(source, highlight=highlight)
        for topic, msg, stamp, matched, _ in self._generate():
            yield topic, msg, stamp, matched
        source.close()


    def match(self, topic, msg, stamp, highlight=False):
        """
        Returns matched message if message matches search filters.

        @param   topic      topic name
        @param   msg        ROS message
        @param   stamp      message ROS time
        @param   highlight  whether to highlight matched values in message fields
        @return             original or highlighted message on match else `None`
        """
        result = None
        if not isinstance(self._source, inputs.AppSource):
            self._prepare(inputs.AppSource(), highlight=highlight)
        if self._highlight != bool(highlight): self._set_passthrough()

        self._source.push(topic, msg, stamp)
        item = self._source.read_queue()
        if item is not None:
            msgid = self._idcounter = self._idcounter + 1
            topickey = rosapi.TypeMeta.make(msg, topic).topickey
            self._register_message(topickey, msgid, msg, stamp)
            matched = self._is_processable(topic, stamp, msg) and self.get_match(msg)

            self._source.notify(matched)
            if matched and not self._counts[topickey][True] % (self.args.NTH_MATCH or 1):
                self._statuses[topickey][msgid] = True
                self._counts[topickey][True] += 1
                result = matched
            elif matched:  # Not NTH_MATCH, skip emitting
                self._statuses[topickey][msgid] = True
                self._counts[topickey][True] += 1
            self._prune_data(topickey)
            self._source.mark_queue(topic, msg, stamp)
        return result


    def work(self, source, sink):
        """
        Greps messages yielded from source and emits matched content to sink.

        @param   source  inputs.SourceBase instance
        @param   sink    outputs.SinkBase instance
        @return          count matched
        """
        self._prepare(source, sink)
        total_matched = 0
        for topic, msg, stamp, matched, index in self._generate():
            if matched: sink.emit_meta()
            sink.emit(topic, msg, stamp, matched, index)
            total_matched += bool(matched)
        source.close(), sink.close()
        return total_matched


    def _generate(self):
        """
        Yields matched and context messages from source.
        
        @return  tuples of (topic, msg, stamp, matched optionally highlighted msg, index in topic)
        """
        batch_matched, batch = False, None
        for topic, msg, stamp in self._source.read():
            if batch != self._source.get_batch():
                batch, batch_matched = self._source.get_batch(), False
                if self._counts: self._clear_data()

            msgid = self._idcounter = self._idcounter + 1
            topickey = rosapi.TypeMeta.make(msg, topic).topickey
            self._register_message(topickey, msgid, msg, stamp)
            matched = self._is_processable(topic, stamp, msg) and self.get_match(msg)

            self._source.notify(matched)
            if matched and not self._counts[topickey][True] % (self.args.NTH_MATCH or 1):
                self._statuses[topickey][msgid] = True
                self._counts[topickey][True] += 1
                for x in self._generate_context(topickey, before=True): yield x
                yield (topic, msg, stamp, matched, self._counts[topickey][None])
            elif matched:  # Not NTH_MATCH, skip emitting
                self._statuses[topickey][msgid] = True
                self._counts[topickey][True] += 1
            elif self.args.AFTER \
            and self._has_in_window(topickey, self.args.AFTER + 1, status=True):
                for x in self._generate_context(topickey, before=False): yield x
            batch_matched = batch_matched or bool(matched)

            self._prune_data(topickey)
            if batch_matched and self._is_max_done():
                self._sink and self._sink.flush()
                self._source.close_batch()


    def _is_processable(self, topic, stamp, msg):
        """
        Returns whether processing current message in topic is acceptable:
        that topic or total maximum count has not been reached,
        and current message in topic is in configured range, if any.
        """
        topickey = rosapi.TypeMeta.make(msg, topic).topickey
        if self.args.MAX_MATCHES \
        and sum(x[True] for x in self._counts.values()) >= self.args.MAX_MATCHES:
            return False
        if self.args.MAX_TOPIC_MATCHES \
        and self._counts[topickey][True] >= self.args.MAX_TOPIC_MATCHES:
            return False
        if self.args.MAX_TOPICS:
            topics_matched = [k for k, vv in self._counts.items() if vv[True]]
            if topickey not in topics_matched and len(topics_matched) >= self.args.MAX_TOPICS:
                return False
        if self._source \
        and not self._source.is_processable(topic, self._counts[topickey][None], stamp, msg):
            return False
        return True


    def _generate_context(self, topickey, before=False):
        """Yields before/after context for latest match."""
        count = self.args.BEFORE + 1 if before else self.args.AFTER
        candidates = list(self._statuses[topickey])[-count:]
        current_index = self._counts[topickey][None]
        for i, msgid in enumerate(candidates) if count else ():
            if self._statuses[topickey][msgid] is None:
                idx = current_index + i - (len(candidates) - 1 if before else 1)
                stamp, msg = self._stamps[topickey][msgid], self._messages[topickey][msgid]
                self._counts[topickey][False] += 1
                yield topickey[0], msg, stamp, None, idx
                self._statuses[topickey][msgid] = False


    def _clear_data(self):
        """Clears local structures."""
        for d in (self._counts, self._messages, self._stamps, self._statuses):
            d.clear()
        rosapi.TypeMeta.clear()


    def _prepare(self, source, sink=None, highlight=False):
        """Clears local structures, binds and registers source and sink, if any."""
        self._clear_data()
        self._source, self._sink = source, sink
        source.bind(sink), sink and sink.bind(source)
        self._highlight = sink.is_highlighting() if sink else bool(highlight)
        self._set_passthrough()


    def _prune_data(self, topickey):
        """Drops history older than context window."""
        WINDOW = max(self.args.BEFORE, self.args.AFTER) + 1
        for dct in (self._messages, self._stamps, self._statuses):
            while len(dct[topickey]) > WINDOW:
                msgid = next(iter(dct[topickey]))
                value = dct[topickey].pop(msgid)
                dct is self._messages and rosapi.TypeMeta.discard(value)


    def _parse_patterns(self):
        """Parses pattern arguments into re.Patterns."""
        NOBRUTE_SIGILS = r"\A", r"\Z", "?("  # Regex specials ruling out brute precheck
        BRUTE, FLAGS = not self.args.INVERT, re.DOTALL | (0 if self.args.CASE else re.I)
        self._patterns.clear()
        del self._brute_prechecks[:]
        contents = []
        for v in self.args.PATTERNS:
            split = v.find("=", 1, -1)
            v, path = (v[split + 1:], v[:split]) if split > 0 else (v, ())
            # Special case if '' or "": add pattern for matching empty string
            v = (re.escape(v) if self.args.RAW else v) + ("|^$" if v in ("''", '""') else "")
            path = re.compile(r"(^|\.)%s($|\.)" % ".*".join(map(re.escape, path.split("*")))) \
                   if path else ()
            contents.append((path, re.compile("(%s)" % v, FLAGS)))
            if BRUTE and (self.args.RAW or not any(x in v for x in NOBRUTE_SIGILS)):
                self._brute_prechecks.append(re.compile(v, re.I | re.M))
        if not self.args.PATTERNS:  # Add match-all pattern
            contents.append(self.ANY_MATCHES[0])
        self._patterns["content"] = contents

        selects, noselects = self.args.SELECT_FIELDS, self.args.NOSELECT_FIELDS
        for key, vals in [("select", selects), ("noselect", noselects)]:
            self._patterns[key] = [(tuple(v.split(".")), wildcard_to_regex(v)) for v in vals]


    def _register_message(self, topickey, msgid, msg, stamp):
        """Registers message with local structures."""
        self._counts[topickey][None] += 1
        self._messages[topickey][msgid] = msg
        self._stamps  [topickey][msgid] = stamp
        self._statuses[topickey][msgid] = None


    def _set_passthrough(self):
        """Sets passthrough flag from current settings."""
        self._passthrough = not self._highlight and not self._patterns["select"] \
                            and not self._patterns["noselect"] and not self.args.INVERT \
                            and set(self._patterns["content"]) <= set(self.ANY_MATCHES)


    def _is_max_done(self):
        """Returns whether max match count has been reached (and message after-context emitted)."""
        result, is_maxed = False, False
        if self.args.MAX_MATCHES:
            is_maxed = sum(vv[True] for vv in self._counts.values()) >= self.args.MAX_MATCHES
        if not is_maxed and self.args.MAX_TOPIC_MATCHES:
            count_required = self.args.MAX_TOPICS or len(self._source.topics)
            count_maxed = sum(vv[True] >= self.args.MAX_TOPIC_MATCHES
                              or vv[None] >= (self._source.topics.get(k) or 0)
                              for k, vv in self._counts.items())
            is_maxed = (count_maxed >= count_required)
        if is_maxed:
            result = not self.args.AFTER or \
                     not any(self._has_in_window(k, self.args.AFTER, status=True, full=True)
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
            """Returns string with matching parts wrapped in marker tags; updates `matched`."""
            spans = []
            # Omit collection brackets from match unless empty: allow matching "[]"
            v1 = v2 = v[1:-1] if is_collection and v != "[]" else v
            topstr = ".".join(top)
            for i, (path, p) in enumerate(self._patterns["content"]):
                if not path or path.search(topstr):
                    for match in (m for m in p.finditer(v1) if not v1 or m.start() != m.end()):
                        matched[i] = True
                        spans.append(match.span())
                        if self.args.INVERT:
                            break  # for match
            spans = merge_spans(spans) if not self.args.INVERT else \
                    [] if spans else [(0, len(v1))] if v1 or not is_collection else []
            for a, b in reversed(spans):  # Work from last to first, indices stay the same
                v2 = v2[:a] + MatchMarkers.START + v2[a:b] + MatchMarkers.END + v2[b:]
            return "[%s]" % v2 if is_collection and v != "[]" else v2

        def process_message(obj, top=()):
            """Recursively converts field values to pattern-matched strings; updates `matched`."""
            selects, noselects = self._patterns["select"], self._patterns["noselect"]
            fieldmap = fieldmap0 = rosapi.get_message_fields(obj)  # Returns obj if not ROS message
            if fieldmap != obj:
                fieldmap = filter_fields(fieldmap, top, include=selects, exclude=noselects)
            for k, t in fieldmap.items() if fieldmap != obj else ():
                v, path = rosapi.get_message_value(obj, k, t), top + (k, )
                is_collection = isinstance(v, (list, tuple))
                if rosapi.is_ros_message(v):
                    process_message(v, path)
                elif v and is_collection and rosapi.scalar(t) not in rosapi.ROS_NUMERIC_TYPES:
                    rosapi.set_message_value(obj, k, [process_message(x, path) for x in v])
                else:
                    v1 = str(list(v) if isinstance(v, (bytes, tuple)) else v)
                    v2 = wrap_matches(v1, path, is_collection)
                    if len(v1) != len(v2):
                        rosapi.set_message_value(obj, k, v2)
            if not rosapi.is_ros_message(obj):
                v1 = str(list(obj) if isinstance(obj, bytes) else obj)
                v2 = wrap_matches(v1, top)
                obj = v2 if len(v1) != len(v2) else obj
            if not top and not matched and not selects and not fieldmap0 and not self.args.INVERT \
            and set(self._patterns["content"]) <= set(self.ANY_MATCHES):  # Ensure Empty any-match
                matched.update({i: True for i, _ in enumerate(self._patterns["content"])})
            return obj

        if self._passthrough: return msg

        if self._brute_prechecks:
            text  = "\n".join("%r" % (v, ) for _, v, _ in rosapi.iter_message_fields(msg))
            if not all(any(p.finditer(text)) for p in self._brute_prechecks):
                return None  # Skip detailed matching if patterns not present at all

        result, matched = copy.deepcopy(msg), {}  # {pattern index: True}
        process_message(result)
        yes = not matched if self.args.INVERT else len(matched) == len(self._patterns["content"])
        return (result if self._highlight else msg) if yes else None

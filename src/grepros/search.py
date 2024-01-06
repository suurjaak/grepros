# -*- coding: utf-8 -*-
"""
Search core.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     28.09.2021
@modified    13.01.2024
------------------------------------------------------------------------------
"""
## @namespace grepros.search
import copy
import collections
import re

import six

from . import api
from . import common
from . import inputs


class Scanner(object):
    """
    ROS message grepper.

    In highlighted results, message field values that match search criteria are modified
    to wrap the matching parts in {@link grepros.common.MatchMarkers MatchMarkers} tags,
    with numeric field values converted to strings beforehand.
    """

    ## Namedtuple of (topic name, ROS message, ROS time object, message if matched, index in topic).
    GrepMessage = collections.namedtuple("BagMessage", "topic message timestamp match index")

    ## Match patterns for global any-match
    ANY_MATCHES = [((), re.compile("(.*)", re.DOTALL)), (), re.compile("(.?)", re.DOTALL)]

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(PATTERN=(), CASE=False, FIXED_STRING=False, INVERT=False, EXPRESSION=False,
                        HIGHLIGHT=False, NTH_MATCH=1, BEFORE=0, AFTER=0, CONTEXT=0, MAX_COUNT=0,
                        MAX_PER_TOPIC=0, MAX_TOPICS=0, SELECT_FIELD=(), NOSELECT_FIELD=(),
                        MATCH_WRAPPER="**")


    def __init__(self, args=None, **kwargs):
        """
        @param   args                     arguments as namespace or dictionary, case-insensitive
        @param   args.pattern             pattern(s) to find in message field values
        @param   args.fixed_string        pattern contains ordinary strings, not regular expressions
        @param   args.case                use case-sensitive matching in pattern
        @param   args.invert              select messages not matching pattern
        @param   args.expression          pattern(s) are a logical expression
                                          like 'this AND (this2 OR NOT "skip this")',
                                          with elements as patterns to find in message fields
        @param   args.highlight           highlight matched values
        @param   args.before              number of messages of leading context to emit before match
        @param   args.after               number of messages of trailing context to emit after match
        @param   args.context             number of messages of leading and trailing context to emit
                                          around match, overrides args.before and args.after
        @param   args.max_count           number of matched messages to emit (per file if bag input)
        @param   args.max_per_topic       number of matched messages to emit from each topic
        @param   args.max_topics          number of topics to emit matches from
        @param   args.nth_match           emit every Nth match in topic
        @param   args.select_field        message fields to use in matching if not all
        @param   args.noselect_field      message fields to skip in matching
        @param   args.match_wrapper       string to wrap around matched values in find() and match(),
                                          both sides if one value, start and end if more than one,
                                          or no wrapping if zero values (default "**")
        @param   kwargs                   any and all arguments as keyword overrides, case-insensitive
        <!--sep-->

        Additional arguments when using match() or find(grepros.api.Bag):

        @param   args.topic               ROS topics to read if not all
        @param   args.type                ROS message types to read if not all
        @param   args.skip_topic          ROS topics to skip
        @param   args.skip_type           ROS message types to skip
        @param   args.start_time          earliest timestamp of messages to read
        @param   args.end_time            latest timestamp of messages to read
        @param   args.start_index         message index within topic to start from
        @param   args.end_index           message index within topic to stop at
        @param   args.unique              emit messages that are unique in topic
        @param   args.nth_message         read every Nth message in topic
        @param   args.nth_interval        minimum time interval between messages in topic
        @param   args.condition           Python expressions that must evaluate as true
                                          for message to be processable, see ConditionMixin
        @param   args.progress            whether to print progress bar
        @param   args.stop_on_error       stop execution on any error like unknown message type
        """
        # {key: [(() if any field else ('nested', 'path') or re.Pattern, re.Pattern), ]}
        self._patterns = {}
        self._expression = None # Nested [op, val] like ["NOT", ["VAL", "skip this"]]
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
        self._highlight       = None   # Highlight matched values in message fields
        self._passthrough     = False  # Emit messages without pattern-matching and highlighting

        ## Source instance
        self.source = None
        ## Sink instance
        self.sink   = None

        self.args = common.ensure_namespace(args, Scanner.DEFAULT_ARGS, **kwargs)
        if self.args.CONTEXT: self.args.BEFORE = self.args.AFTER = self.args.CONTEXT
        self._parse_patterns()


    def find(self, source, highlight=None):
        """
        Yields matched and context messages from source.

        @param   source     inputs.Source or api.Bag instance
        @param   highlight  whether to highlight matched values in message fields,
                            defaults to flag from constructor
        @return             GrepMessage namedtuples of
                            (topic, message, timestamp, match, index in topic),
                            where match is matched optionally highlighted message
                            or `None` if yielding a context message
        """
        if isinstance(source, api.Bag):
            source = inputs.BagSource(source, **vars(self.args))
        self._prepare(source, highlight=highlight)
        for topic, msg, stamp, matched, index in self._generate():
            yield self.GrepMessage(topic, msg, stamp, matched, index)


    def match(self, topic, msg, stamp, highlight=None):
        """
        Returns matched message if message matches search filters.

        @param   topic      topic name
        @param   msg        ROS message
        @param   stamp      message ROS timestamp
        @param   highlight  whether to highlight matched values in message fields,
                            defaults to flag from constructor
        @return             original or highlighted message on match else `None`
        """
        result = None
        if not isinstance(self.source, inputs.AppSource):
            self._prepare(inputs.AppSource(self.args), highlight=highlight)
        if self._highlight != bool(highlight): self._configure_flags(highlight=highlight)

        self.source.push(topic, msg, stamp)
        item = self.source.read_queue()
        if item is not None:
            msgid = self._idcounter = self._idcounter + 1
            topickey = api.TypeMeta.make(msg, topic).topickey
            self._register_message(topickey, msgid, msg, stamp)
            matched = self._is_processable(topic, msg, stamp) and self.get_match(msg)

            self.source.notify(matched)
            if matched and not self._counts[topickey][True] % (self.args.NTH_MATCH or 1):
                self._statuses[topickey][msgid] = True
                self._counts[topickey][True] += 1
                result = matched
            elif matched:  # Not NTH_MATCH, skip emitting
                self._statuses[topickey][msgid] = True
                self._counts[topickey][True] += 1
            self._prune_data(topickey)
            self.source.mark_queue(topic, msg, stamp)
        return result


    def work(self, source, sink):
        """
        Greps messages yielded from source and emits matched content to sink.

        @param   source  inputs.Source or api.Bag instance
        @param   sink    outputs.Sink instance
        @return          count matched
        """
        if isinstance(source, api.Bag):
            source = inputs.BagSource(source, **vars(self.args))
        self._prepare(source, sink, highlight=self.args.HIGHLIGHT)
        total_matched = 0
        for topic, msg, stamp, matched, index in self._generate():
            sink.emit_meta()
            sink.emit(topic, msg, stamp, matched, index)
            total_matched += bool(matched)
        return total_matched


    def __enter__(self):
        """Context manager entry, does nothing, returns self."""
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit, does nothing."""
        return self


    def _generate(self):
        """
        Yields matched and context messages from source.

        @return  tuples of (topic, msg, stamp, matched optionally highlighted msg, index in topic)
        """
        batch_matched, batch = False, None
        for topic, msg, stamp in self.source.read():
            if batch != self.source.get_batch():
                batch, batch_matched = self.source.get_batch(), False
                if self._counts: self._clear_data()

            msgid = self._idcounter = self._idcounter + 1
            topickey = api.TypeMeta.make(msg, topic).topickey
            self._register_message(topickey, msgid, msg, stamp)
            matched = self._is_processable(topic, msg, stamp) and self.get_match(msg)

            self.source.notify(matched)
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
                if self.sink: self.sink.flush()
                self.source.close_batch()


    def _is_processable(self, topic, msg, stamp):
        """
        Returns whether processing current message in topic is acceptable:
        that topic or total maximum count has not been reached,
        and current message in topic is in configured range, if any.
        """
        topickey = api.TypeMeta.make(msg, topic).topickey
        if self.args.MAX_COUNT \
        and sum(x[True] for x in self._counts.values()) >= self.args.MAX_COUNT:
            return False
        if self.args.MAX_PER_TOPIC and self._counts[topickey][True] >= self.args.MAX_PER_TOPIC:
            return False
        if self.args.MAX_TOPICS:
            topics_matched = [k for k, vv in self._counts.items() if vv[True]]
            if topickey not in topics_matched and len(topics_matched) >= self.args.MAX_TOPICS:
                return False
        if self.source \
        and not self.source.is_processable(topic, msg, stamp, self._counts[topickey][None]):
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
                msg, stamp = self._messages[topickey][msgid], self._stamps[topickey][msgid]
                self._counts[topickey][False] += 1
                yield topickey[0], msg, stamp, None, idx
                self._statuses[topickey][msgid] = False


    def _clear_data(self):
        """Clears local structures."""
        for d in (self._counts, self._messages, self._stamps, self._statuses):
            d.clear()
        api.TypeMeta.clear()


    def _prepare(self, source, sink=None, highlight=None):
        """Clears local structures, binds and registers source and sink, if any."""
        self._clear_data()
        self.source, self.sink = source, sink
        source.bind(sink), sink and sink.bind(source)
        source.preprocess = False
        self._configure_flags(highlight=highlight)


    def _prune_data(self, topickey):
        """Drops history older than context window."""
        WINDOW = max(self.args.BEFORE, self.args.AFTER) + 1
        for dct in (self._messages, self._stamps, self._statuses):
            while len(dct[topickey]) > WINDOW:
                msgid = next(iter(dct[topickey]))
                value = dct[topickey].pop(msgid)
                dct is self._messages and api.TypeMeta.discard(value)


    def _parse_patterns(self):
        """Parses pattern arguments into re.Patterns. Raises on invalid pattern."""
        NOBRUTE_SIGILS = r"\A", r"\Z", "?("  # Regex specials ruling out brute precheck
        BRUTE, FLAGS = not self.args.INVERT, re.DOTALL | (0 if self.args.CASE else re.I)
        self._patterns.clear()
        self._expression = None
        del self._brute_prechecks[:]
        contents = []

        def make_pattern(v):
            """Returns (path Pattern or (), value Pattern)."""
            split = v.find("=", 1, -1)
            v, path = (v[split + 1:], v[:split]) if split > 0 else (v, ())
            # Special case if '' or "": add pattern for matching empty string
            v = "|^$" if v in ("''", '""') else (re.escape(v) if self.args.FIXED_STRING else v)
            path = re.compile(r"(^|\.)%s($|\.)" % ".*".join(map(re.escape, path.split("*")))) \
                   if path else ()
            try: return (path, re.compile("(%s)" % v, FLAGS))
            except Exception as e:
                raise ValueError("Invalid regular expression\n  '%s': %s" % (v, e))

        if self.args.EXPRESSION and self.args.PATTERN:
            self._expression = self._parse_expression(" ".join(self.args.PATTERN), make_pattern)
        for v in self.args.PATTERN if not self._expression else ():
            contents.append(make_pattern(v))
            if BRUTE and (self.args.FIXED_STRING or not any(x in v for x in NOBRUTE_SIGILS)):
                self._brute_prechecks.append(re.compile(v, re.I | re.M))
        if not self.args.PATTERN:  # Add match-all pattern
            contents.append(self.ANY_MATCHES[0])
        self._patterns["content"] = contents

        selects, noselects = self.args.SELECT_FIELD, self.args.NOSELECT_FIELD
        for key, vals in [("select", selects), ("noselect", noselects)]:
            self._patterns[key] = [(tuple(v.split(".")), common.wildcard_to_regex(v)) for v in vals]


    def _parse_expression(self, text, parse_text=None):
        """
        Returns a logical expression like "a AND (b OR NOT c)" parsed into a binary tree.

        Binary tree like ["AND", [["VAL", "a"], ["OR", [["VAL", "b"], ["NOT", [["VAL", "c"]]]]]]].
        Operators AND OR NOT are case-insensitive. Raises on invalid expression.

        @param   parse_text  callback(text) returning tree value for text nodes
        """
        QUOTES, ESCAPE, LPAREN, RPAREN, WHITESPACE = "'\"", "\\", "(", ")", " \n\r\t"
        AND, OR, NOT, VAL, ERRLABEL = "AND", "OR", "NOT", "VAL", "Invalid logical expression: "
        RANK = {OR: 4, AND: 3, NOT: 2, VAL: 1}

        def mark(i): return "\n%s\n%s^" % (text, " " * i)  # Return expression text marked at pos

        def missing(op, first=False):  # Return error text for missing operand in AND OR NOT
            pos = "" if NOT == op else "1st " if first else "2nd "
            return ERRLABEL + "missing %selement for %s-operator" % (pos, op)

        def add_node(op, val):
            """Adds new node to tree, updates root, returns (node to use as last, root)."""
            node0, (op0, val0), myroot, reparent = node, (node or [None, None]), root, None

            if op in (AND, OR):  # Attach last child or root to new if needed
                if stacki is None and (RANK[op] <= RANK[op0] and val0[-1] is not None):
                    reparent = node0  # Chain of AND/OR, last is full, new ranks equal or lower
                    val = [val0[-1], None]  # Take last child of last node into new
                elif RANK[op] >= RANK[root[0]]:  # Root has equal or lower rank
                    reparent = root
                    val = [root, None]  # Take root into new
            newnode = [op, parse_text(val) if parse_text and VAL == op else val]
            if reparent or node0: parents[id(reparent or newnode)] = newnode if reparent else node0

            if node0 and stacki is None and (val0[-1] is None  # Last is unfinished
            or op0 in (AND, OR) and RANK[op0] >= RANK[op]):  # Last AND/OR ranks higher or same
                val0[-1] = newnode  # Attach new node to last
            elif not root or (RANK[op] > RANK[root[0]]) if stacki is None else (root == node0):
                myroot = newnode  # Replace root if new outranks it, or expression so far was braced
            latest = node0 if node0 and op == VAL else newnode
            while NOT == latest[0] and latest[1][-1] is not None and id(latest) in parents:
                latest = parents[id(latest)]  # Walk up filled NOT-nodes until AND/OR/root
            return latest, myroot

        root, node, stack, buf, quote, nodei, stacki, parents = [], [], [], "", "", None, None, {}
        for i, char in enumerate(text + " "):  # Ensure terminating whitespace
            # First pass: start/end quotes, or handle explicit/implicit word ends and operators
            if quote:
                if char == quote and buf[-1:] != ESCAPE:  # End quote
                    (node, root), buf, quote, char = add_node(VAL, buf), "", "", ""
            elif char in QUOTES:
                quote, char = char, ""  # Start quoted string, consume quotemark
            elif char in WHITESPACE + LPAREN + RPAREN:
                op = buf.upper() if len(buf) in (2, 3) and buf.upper() in (AND, OR, NOT) else None
                if op:  # Explicit AND OR NOT
                    if op in (AND, OR) and (not node or node[1][-1] is None):
                        raise ValueError(missing(node[0] if node else op, first=not node) + mark(i))
                    (node, root), nodei = add_node(op, [None] if op == NOT else [node, None]), i
                else:
                    if (buf or char == LPAREN) and node and node[1][-1] is not None:
                        (node, root), nodei = add_node(AND, [node, None]), i  # Insert implicit AND
                    if buf: node, root = add_node(VAL, buf)  # Operand
                buf, stacki = "", (None if op or buf else stacki)
            # Second pass: enter/exit bracket groups, or accumulate text buffer
            if quote or char not in WHITESPACE + LPAREN + RPAREN:
                buf += char  # Accumulate text while not consumable
            elif char == LPAREN:  # Enter (..), stack current nodes
                root, node, _, _ = [], [], i, stack.append((root, node, i))
            elif char == RPAREN:  # Exit (..), unstack previous nodes, bind nested to previous
                if not stack: raise ValueError(ERRLABEL + "bracket end has no start" + mark(i))
                if not node: raise ValueError(ERRLABEL + "empty bracket" + mark(i))
                if node[0] in (AND, OR, NOT) and node[1][-1] is None:
                    raise ValueError(missing(node[0]) + mark(nodei))
                (root, node, stacki), root2, node2 = stack.pop(), root, node
                if node: node[1][-1], node, parents[id(root2)], stacki = root2, root, node, None
                elif not root: root, node = root2, node2  # Replace empty root with nested
        if stack: raise ValueError(ERRLABEL + "unterminated bracket" + mark(stack[-1][-1]))
        if node and node[0] != VAL and node[1][-1] is None:
            raise ValueError(missing(node[0]) + mark(nodei))
        return root


    def _register_message(self, topickey, msgid, msg, stamp):
        """Registers message with local structures."""
        self._counts[topickey][None] += 1
        self._messages[topickey][msgid] = msg
        self._stamps  [topickey][msgid] = stamp
        self._statuses[topickey][msgid] = None


    def _configure_flags(self, highlight=None):
        """Sets highlight and passthrough flags from current settings."""
        self._highlight = bool(highlight if highlight is not None else
                               False if self.sink and not self.sink.is_highlighting() else
                               self.args.HIGHLIGHT)
        self._passthrough = not self._highlight and not self._expression \
                            and not self._patterns["select"] \
                            and not self._patterns["noselect"] and not self.args.INVERT \
                            and set(self._patterns["content"]) <= set(self.ANY_MATCHES)


    def _is_max_done(self):
        """Returns whether max match count has been reached (and message after-context emitted)."""
        result, is_maxed = False, False
        if self.args.MAX_COUNT:
            is_maxed = sum(vv[True] for vv in self._counts.values()) >= self.args.MAX_COUNT
        if not is_maxed and self.args.MAX_PER_TOPIC:
            count_required = self.args.MAX_TOPICS or len(self.source.topics)
            count_maxed = sum(vv[True] >= self.args.MAX_PER_TOPIC
                              or vv[None] >= (self.source.topics.get(k) or 0)
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

        def process_value(v, parent, top, patterns):
            """
            Populates `field_matches` for patterns matching given string value.
            Returns set of pattern indexes that found a match.
            """
            indexes, spans, topstr = set(), [], ".".join(map(str, top))
            topstrn = ".".join(x for x in top if not isinstance(x, int))  # Without array indexes
            v2 = str(list(v) if isinstance(v, LISTIFIABLES) else v)
            if v and isinstance(v, (list, tuple)): v2 = v2[1:-1]  # Omit collection braces leave []
            for i, (path, p) in enumerate(patterns):
                if path and not path.search(topstr) and not path.search(topstrn): continue  # for
                matches = [next(p.finditer(v2), None)] if PLAIN_INVERT else list(p.finditer(v2))
                # Join consecutive zero-length matches, extend remaining zero-lengths to end of value
                matchspans = common.merge_spans([x.span() for x in matches if x], join_blanks=True)
                matchspans = [(a, b if a != b else len(v2)) for a, b in matchspans]
                if matchspans: indexes.add(i), spans.extend(matchspans)
            if PLAIN_INVERT: spans = [(0, len(v2))] if v2 and not spans else []
            if spans: field_matches.setdefault(top, (parent, v, v2, []))[-1].extend(spans)
            return indexes

        def populate_matches(obj, patterns, top=()):
            """
            Recursively populates `field_matches` for message fields matching patterns.
            Returns set of pattern indexes that found a match.
            """
            indexes = set()
            selects, noselects = self._patterns["select"], self._patterns["noselect"]
            fieldmap = api.get_message_fields(obj)  # Returns obj if not ROS message
            if fieldmap != obj:
                fieldmap = api.filter_fields(fieldmap, top, include=selects, exclude=noselects)
            for k, t in fieldmap.items() if fieldmap != obj else ():
                v, path = api.get_message_value(obj, k, t), top + (k, )
                if api.is_ros_message(v):
                    indexes |= populate_matches(v, patterns, path)
                elif v and isinstance(v, (list, tuple)) and api.scalar(t) not in api.ROS_NUMERIC_TYPES:
                    for i, x in enumerate(v): indexes |= populate_matches(x, patterns, path + (i, ))
                else:
                    indexes |= process_value(v, obj, path, patterns)
            if not api.is_ros_message(obj):
                indexes |= process_value(v, obj, top, patterns)
            return indexes

        def wrap_matches(matches):
            """Replaces result-message field values with matched parts wrapped in marker tags."""
            for path, (parent, v1, v2, spans) in matches.items() if any(WRAPS) else ():
                is_collection = isinstance(v1, (list, tuple))
                for a, b in reversed(common.merge_spans(spans)):  # Backwards for stable indexes
                    v2 = v2[:a] + WRAPS[0] + v2[a:b] + WRAPS[1] + v2[b:]
                if v1 and is_collection: v2 = "[%s]" % v2  # Readd collection braces
                if isinstance(parent, list) and isinstance(path[-1], int): parent[path[-1]] = v2
                else: api.set_message_value(parent, path[-1], v2)

        def process_message(obj, patterns):
            """Returns whether message matches patterns, wraps matches in marker tags if so."""
            indexes = populate_matches(obj, patterns)
            is_match = not indexes if self.args.INVERT else len(indexes) == len(patterns)
            if is_match:
                wrap_matches(field_matches)
            if not self.args.INVERT and not indexes and not self._patterns["select"] \
            and set(self._patterns["content"]) <= set(self.ANY_MATCHES) \
            and not api.get_message_fields(obj):  # Ensure any-match for messages with no fields
                is_match = True
            return is_match

        def eval_expression(tree):
            """Returns whether expression tree matches result-message, populates `field_matches`."""
            op, val = tree
            if   "AND" == op: return eval_expression(val[0]) and eval_expression(val[1])
            elif "OR"  == op:
                first = eval_expression(val[0])
                return first and not WRAPS or eval_expression(val[1]) or first
            elif "NOT" == op: return not eval_expression(val[0])
            else:             return bool(populate_matches(result, [val]))


        if self._passthrough: return msg

        if self._brute_prechecks:
            text  = "\n".join("%r" % (v, ) for _, v, _ in api.iter_message_fields(msg, flat=True))
            if not all(any(p.finditer(text)) for p in self._brute_prechecks):
                return None  # Skip detailed matching if patterns not present at all

        WRAPS = [] if not self._highlight else self.args.MATCH_WRAPPER if not self.sink else \
                (common.MatchMarkers.START, common.MatchMarkers.END)
        WRAPS = WRAPS if isinstance(WRAPS, (list, tuple)) else [] if WRAPS is None else [WRAPS]
        WRAPS = ((WRAPS or [""]) * 2)[:2]

        LISTIFIABLES = (bytes, tuple) if six.PY3 else (tuple, )
        PLAIN_INVERT = self.args.INVERT and not self._expression
        field_matches = {}  # {field path: (parent, original value, stringified value, [(span), ])}

        result, is_match = copy.deepcopy(msg) if self._highlight else msg, False
        if self._expression:
            if eval_expression(self._expression) != self.args.INVERT:
                is_match, _ = True, wrap_matches(field_matches)
        else:
            is_match = process_message(result, self._patterns["content"])
        return (result if self._highlight else msg) if is_match else None


__all__ = ["Scanner"]

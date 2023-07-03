# -*- coding: utf-8 -*-
"""
ROS2 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     02.11.2021
@modified    03.07.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.ros2
import array
import collections
import datetime
import decimal
import enum
import inspect
import io
import os
import re
import sqlite3
import threading
import time

import builtin_interfaces.msg
try: import numpy
except Exception: numpy = None
import rclpy
import rclpy.clock
import rclpy.duration
import rclpy.executors
import rclpy.serialization
import rclpy.time
import rosidl_parser.parser
import rosidl_parser.definition
import rosidl_runtime_py.utilities
import yaml

from . import api
from . common import PATH_TYPES, ConsolePrinter, MatchMarkers, memoize


## Bagfile extensions to seek
BAG_EXTENSIONS  = (".db3", )

## Bagfile extensions to skip
SKIP_EXTENSIONS = ()

## ROS2 time/duration message types
ROS_TIME_TYPES = ["builtin_interfaces/Time", "builtin_interfaces/Duration"]

## ROS2 time/duration types and message types mapped to type names
ROS_TIME_CLASSES = {rclpy.time.Time:                 "builtin_interfaces/Time",
                    builtin_interfaces.msg.Time:     "builtin_interfaces/Time",
                    rclpy.duration.Duration:         "builtin_interfaces/Duration",
                    builtin_interfaces.msg.Duration: "builtin_interfaces/Duration"}

## ROS2 time/duration types mapped to message types
ROS_TIME_MESSAGES = {rclpy.time.Time:          builtin_interfaces.msg.Time,
                     rclpy.duration.Duration:  builtin_interfaces.msg.Duration}

## Mapping between type aliases and real types, like {"byte": "uint8"}
ROS_ALIAS_TYPES = {"byte": "uint8", "char": "int8"}

## Data Distribution Service types to ROS builtins
DDS_TYPES = {"boolean":             "bool",
             "float":               "float32",
             "double":              "float64",
             "octet":               "byte",
             "short":               "int16",
             "unsigned short":      "uint16",
             "long":                "int32",
             "unsigned long":       "uint32",
             "long long":           "int64",
             "unsigned long long":  "uint64", }

## rclpy.node.Node instance
node = None

## rclpy.context.Context instance
context = None

## rclpy.executors.Executor instance
executor = None



class ROS2Bag(api.BaseBag):
    """ROS2 bag reader and writer (SQLite format), providing most of rosbag.Bag interface."""

    ## Whether bag supports reading or writing stream objects, overridden in subclasses
    STREAMABLE = False

    ## ROS2 bag SQLite schema
    CREATE_SQL = """
CREATE TABLE IF NOT EXISTS messages (
  id        INTEGER PRIMARY KEY,
  topic_id  INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  data      BLOB    NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
  id                   INTEGER PRIMARY KEY,
  name                 TEXT    NOT NULL,
  type                 TEXT    NOT NULL,
  serialization_format TEXT    NOT NULL,
  offered_qos_profiles TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS timestamp_idx ON messages (timestamp ASC);

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
    """


    def __init__(self, filename, mode="a", *_, **__):
        """
        @param   filename  bag file path to open
        @param   mode      file will be overwritten if "w"
        """
        if not isinstance(filename, PATH_TYPES):
            raise ValueError("invalid filename %r" % type(filename))
        if mode not in self.MODES: raise ValueError("invalid mode %r" % mode)

        self._db     = None  # sqlite3.Connection instance
        self._mode   = mode
        self._topics = {}    # {(topic, typename): {id, name, type}}
        self._counts = {}    # {(topic, typename, typehash): message count}
        self._qoses  = {}    # {(topic, typename): [{qos profile dict}]}
        self._iterer = None  # Generator from read_messages() for next()
        self._ttinfo = None  # Cached result for get_type_and_topic_info()
        self._filename = str(filename)
        self._stop_on_error = True

        self._ensure_open(populate=("r" != mode))


    def get_message_count(self, topic_filters=None):
        """
        Returns the number of messages in the bag.

        @param   topic_filters  list of topics or a single topic to filter by, if any
        """
        if self._db and self._has_table("messages"):
            sql, where = "SELECT COUNT(*) AS count FROM messages", ""
            if topic_filters:
                self._ensure_topics()
                topics = topic_filters
                topics = topics if isinstance(topics, (dict, list, set, tuple)) else [topics]
                topic_ids = [x["id"] for (topic, _), x in self._topics.items() if topic in topics]
                where = " WHERE topic_id IN (%s)" % ", ".join(map(str, topic_ids))
            return self._db.execute(sql + where).fetchone()["count"]
        return None


    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp, or None if bag empty."""
        if self._db and self._has_table("messages"):
            row = self._db.execute("SELECT MIN(timestamp) AS val FROM messages").fetchone()
            if row["val"] is None: return None
            secs, nsecs = divmod(row["val"], 10**9)
            return secs + nsecs / 1E9
        return None


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp, or None if bag empty."""
        if self._db and self._has_table("messages"):
            row = self._db.execute("SELECT MAX(timestamp) AS val FROM messages").fetchone()
            if row["val"] is None: return None
            secs, nsecs = divmod(row["val"], 10**9)
            return secs + nsecs / 1E9
        return None


    def get_topic_info(self, counts=True, ensure_types=True):
        """
        Returns topic and message type metainfo as {(topic, typename, typehash): count}.

        Can skip retrieving message counts, as this requires a full table scan.
        Can skip looking up message type classes, as those might be unavailable in ROS2 environment.

        @param   counts        whether to return actual message counts instead of None
        @param   ensure_types  whether to look up type classes instead of returning typehash as None
        """
        self._ensure_topics()
        if counts: self._ensure_counts()
        if ensure_types: self._ensure_types()
        return dict(self._counts)


    def get_type_and_topic_info(self, topic_filters=None):
        """
        Returns thorough metainfo on topic and message types.

        @param   topic_filters  list of topics or a single topic to filter returned topics-dict by,
                                if any
        @return                 TypesAndTopicsTuple(msg_types, topics) namedtuple,
                                msg_types as dict of {typename: typehash},
                                topics as a dict of {topic: TopicTuple() namedtuple}.
        """
        topics = topic_filters
        topics = topics if isinstance(topics, (list, set, tuple)) else [topics] if topics else []
        if self._ttinfo and (not topics or set(topics) == set(t for t, _, _ in self._counts)):
            return self._ttinfo
        if self.closed: raise ValueError("I/O operation on closed file.")

        counts = self.get_topic_info()
        msgtypes = {n: h for t, n, h in counts}
        topicdict = {}

        def median(vals):
            """Returns median value from given sorted numbers."""
            vlen = len(vals)
            return None if not vlen else vals[vlen // 2] if vlen % 2 else \
                   float(vals[vlen // 2 - 1] + vals[vlen // 2]) / 2

        for (t, n, _), c in sorted(counts.items(), key=lambda x: x[0][:2]):
            if topics and t not in topics: continue  # for
            mymedian = None
            if c > 1:
                args = (self._topics[(t, n)]["id"], )
                cursor = self._db.execute("SELECT timestamp FROM messages WHERE topic_id = ?", args)
                stamps = sorted(x["timestamp"] / 1E9 for x in cursor)
                mymedian = median(sorted(s1 - s0 for s1, s0 in zip(stamps[1:], stamps[:-1])))
            freq = 1.0 / mymedian if mymedian else None
            topicdict[t] = self.TopicTuple(n, c, len(self.get_qoses(t, n) or []), freq)
        result = self.TypesAndTopicsTuple(msgtypes, topicdict)
        if not topics or set(topics) == set(t for t, _, _ in self._counts): self._ttinfo = result
        return result


    def get_qoses(self, topic, typename):
        """Returns topic Quality-of-Service profiles as a list of dicts, or None if not available."""
        topickey = (topic, typename)
        if topickey not in self._qoses and topickey in self._topics:
            topicrow = self._topics[topickey]
            try:
                if topicrow.get("offered_qos_profiles"):
                    self._qoses[topickey] = yaml.safe_load(topicrow["offered_qos_profiles"])
            except Exception as e:
                ConsolePrinter.warn("Error parsing quality of service for topic %r: %r", topic, e)
        self._qoses.setdefault(topickey, None)
        return self._qoses[topickey]


    def get_message_class(self, typename, typehash=None):
        """Returns ROS2 message type class, or None if unknown message type for bag."""
        self._ensure_topics()
        if any(n == typename for _, n, in self._topics) and typehash is not None \
        and not any((n, h) == (typename, typehash) for _, n, h in self._counts):
            self._ensure_types([t for t, n in self._topics if n == typename])
        if any((typename, typehash) in [(n, h), (n, None)] for _, n, h in self._counts):
            return get_message_class(typename)
        return None


    def get_message_definition(self, msg_or_type):
        """
        Returns ROS2 message type definition full text, including subtype definitions.

        Returns None if unknown message type for bag.
        """
        self._ensure_topics()
        typename = msg_or_type if isinstance(msg_or_type, str) else get_message_type(msg_or_type)
        if any(n == typename for _, n, _ in self._counts):
            return get_message_definition(msg_or_type)
        return None


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS2 message type MD5 hash, or None if unknown message type for bag."""
        typename = msg_or_type if isinstance(msg_or_type, str) else get_message_type(msg_or_type)
        self._ensure_types([t for t, n in self._topics if n == typename])
        return next((h for _, n, h in self._counts if n == typename), None)


    def read_messages(self, topics=None, start_time=None, end_time=None, raw=False, **__):
        """
        Yields messages from the bag, optionally filtered by topic and timestamp.

        @param   topics      list of topics or a single topic to filter by, if any
        @param   start_time  earliest timestamp of message to return, as ROS time or convertible
                             (int/float/duration/datetime/decimal)
        @param   end_time    latest timestamp of message to return, as ROS time or convertible
                             (int/float/duration/datetime/decimal)
        @param   raw         if True, then returned messages are tuples of
                             (typename, bytes, typehash, typeclass).
                             If message type unavailable, returns None for hash and class.
        @return              BagMessage namedtuples of
                             (topic, message, timestamp as rclpy.time.Time)
        """
        if self.closed: raise ValueError("I/O operation on closed file.")
        if "w" == self._mode: raise io.UnsupportedOperation("read")

        self._ensure_topics()
        if not self._topics or (topics is not None and not topics):
            return

        sql, exprs, args = "SELECT * FROM messages", [], ()
        if topics:
            topics = topics if isinstance(topics, (list, tuple)) else [topics]
            topic_ids = [x["id"] for (topic, _), x in self._topics.items() if topic in topics]
            exprs += ["topic_id IN (%s)" % ", ".join(map(str, topic_ids))]
        if start_time is not None:
            exprs += ["timestamp >= ?"]
            args  += (to_nsec(to_time(start_time)), )
        if end_time is not None:
            exprs += ["timestamp <= ?"]
            args  += (to_nsec(to_time(end_time)), )
        sql += ((" WHERE " + " AND ".join(exprs)) if exprs else "")
        sql += " ORDER BY timestamp"

        topicmap   = {v["id"]: v for v in self._topics.values()}
        msgtypes   = {}  # {typename: cls or None if unavailable}
        topicset   = set(topics or [t for t, _ in self._topics])
        typehashes = {n: h for _, n, h in self._counts} # {typename: typehash or None or ""}
        for row in self._db.execute(sql, args):
            tdata = topicmap[row["topic_id"]]
            topic, typename = tdata["name"], canonical(tdata["type"])
            if not raw and msgtypes.get(typename, typename) is None: continue  # for row
            if typehashes.get(typename) is None:
                self._ensure_types([topic])
                selector = (h for t, n, h in self._counts if (t, n) == (topic, typename))
                typehash = typehashes[typename] = next(selector, None)
            else: typehash = typehashes[typename]

            try:
                cls = msgtypes.get(typename) or \
                      msgtypes.setdefault(typename, get_message_class(typename))
                if raw: msg = (typename, row["data"], typehash or None, cls)
                else:   msg = rclpy.serialization.deserialize_message(row["data"], cls)
            except Exception as e:
                reportfunc = ConsolePrinter.error if self._stop_on_error else ConsolePrinter.warn
                reportfunc("Error loading type %s in topic %s: %%s" % (typename, topic),
                           "message class not found." if cls is None else e,
                           __once=not self._stop_on_error)
                if self._stop_on_error: raise
                if raw: msg = (typename, row["data"], typehash or None, msgtypes.get(typename))
                elif set(n for n, c in msgtypes.items() if c is None) == topicset:
                    break  # for row
                continue  # for row
            stamp = rclpy.time.Time(nanoseconds=row["timestamp"])

            api.TypeMeta.make(msg, topic, self)
            yield self.BagMessage(topic, msg, stamp)
            if not self._db:  # Bag has been closed in the meantime
                break  # for row


    def write(self, topic, msg, t=None, raw=False, qoses=None, **__):
        """
        Writes a message to the bag.

        @param   topic  name of topic
        @param   msg    ROS2 message
        @param   t      message timestamp if not using wall time, as ROS time or convertible
                        (int/float/duration/datetime/decimal)
        @param   qoses  topic Quality-of-Service settings, if any, as a list of dicts
        """
        if self.closed: raise ValueError("I/O operation on closed file.")
        if "r" == self._mode: raise io.UnsupportedOperation("write")

        self._ensure_topics()
        if raw:
            typename, binary, typehash = msg[:3]
        else:
            typename = get_message_type(msg)
            typehash = get_message_type_hash(msg)
            binary   = serialize_message(msg)
        topickey = (topic, typename)
        cursor = self._db.cursor()
        if topickey not in self._topics:
            full_typename = make_full_typename(typename)
            sql = "INSERT INTO topics (name, type, serialization_format, offered_qos_profiles) " \
                  "VALUES (?, ?, ?, ?)"
            qoses = yaml.safe_dump(qoses) if isinstance(qoses, list) else ""
            args = (topic, full_typename, "cdr", qoses)
            cursor.execute(sql, args)
            tdata = {"id": cursor.lastrowid, "name": topic, "type": full_typename,
                     "serialization_format": "cdr", "offered_qos_profiles": qoses}
            self._topics[topickey] = tdata

        timestamp = (time.time_ns() if hasattr(time, "time_ns") else int(time.time() * 10**9)) \
                    if t is None else to_nsec(to_time(t))
        sql = "INSERT INTO messages (topic_id, timestamp, data) VALUES (?, ?, ?)"
        args = (self._topics[topickey]["id"], timestamp, binary)
        cursor.execute(sql, args)
        countkey = (topic, typename, typehash)
        if self._counts.get(countkey, self) is not None:
            self._counts[countkey] = self._counts.get(countkey, 0) + 1
        self._ttinfo = None


    def open(self):
        """Opens the bag file if not already open."""
        self._ensure_open()


    def close(self):
        """Closes the bag file."""
        if self._db:
            self._db.close()
            self._db     = None
            self._mode   = None
            self._iterer = None


    @property
    def closed(self):
        """Returns whether file is closed."""
        return not self._db


    @property
    def topics(self):
        """Returns the list of topics in bag, in alphabetic order."""
        return sorted((t for t, _, _ in self._topics), key=str.lower)


    @property
    def filename(self):
        """Returns bag file path."""
        return self._filename


    @property
    def size(self):
        """Returns current file size in bytes (including journaling files)."""
        result = os.path.getsize(self._filename) if os.path.isfile(self._filename) else None
        for suffix in ("-journal", "-wal") if result else ():
            path = "%s%s" % (self._filename, suffix)
            result += os.path.getsize(path) if os.path.isfile(path) else 0
        return result


    @property
    def mode(self):
        """Returns file open mode."""
        return self._mode


    def __contains__(self, key):
        """Returns whether bag contains given topic."""
        return any(key == t for t, _, _ in self._topics)


    def __next__(self):
        """Retrieves next message from bag as (topic, message, timestamp)."""
        if self.closed: raise ValueError("I/O operation on closed file.")
        if self._iterer is None: self._iterer = self.read_messages()
        return next(self._iterer)


    def _ensure_open(self, populate=False):
        """Opens bag database if not open, populates schema if specified."""
        if not self._db:
            if "w" == self._mode and os.path.exists(self._filename):
                os.remove(self._filename)
            self._db = sqlite3.connect(self._filename, detect_types=sqlite3.PARSE_DECLTYPES,
                                       isolation_level=None, check_same_thread=False)
            self._db.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))
        if populate:
            self._db.executescript(self.CREATE_SQL)


    def _ensure_topics(self):
        """Populates local topic struct from database, if not already available."""
        if not self._db or self._topics or not self._has_table("topics"): return
        for row in self._db.execute("SELECT * FROM topics ORDER BY id"):
            topickey = (topic, typename) = row["name"], canonical(row["type"])
            self._topics[(topic, typename)] = row
            self._counts[(topic, typename, None)] = None


    def _ensure_counts(self):
        """Populates local counts values from database, if not already available."""
        if not self._db or all(v is not None for v in self._counts.values()) \
        or not self._has_table("messages"): return
        self._ensure_topics()
        topickeys = {self._topics[(t, n)]["id"]: (t, n, h) for (t, n, h) in self._counts}
        self._counts.clear()
        for row in self._db.execute("SELECT topic_id, COUNT(*) AS count FROM messages "
                                    "GROUP BY topic_id").fetchall():
            if row["topic_id"] in topickeys:
                self._counts[topickeys[row["topic_id"]]] = row["count"]


    def _ensure_types(self, topics=None):
        """
        Populates local type definitions and classes from database, if not already available.

        @param   topics  selected topics to ensure types for, if not all
        """
        if not self._db or (not topics and topics is not None) or not self._has_table("topics") \
        or not any(h is None for t, _, h in self._counts if topics is None or t in topics):
            return
        self._ensure_topics()
        for countkey, count in list(self._counts.items()):
            (topic, typename, typehash) = countkey
            if typehash is None and (topics is None or topic in topics):
                typehash = get_message_type_hash(typename)
                self._counts.pop(countkey)
                self._counts[(topic, typename, typehash)] = count


    def _has_table(self, name):
        """Returns whether specified table exists in database."""
        sql = "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?"
        return bool(self._db.execute(sql, ("table", name)).fetchone())
Bag = ROS2Bag



def init_node(name):
    """Initializes a ROS2 node if not already initialized."""
    global node, context, executor
    if node or not validate(live=True):
        return

    def spin_loop():
        while context and context.ok():
            executor.spin_once(timeout_sec=1)

    context = rclpy.Context()
    try: rclpy.init(context=context)
    except Exception: pass  # Must not be called twice at runtime
    node_name = "%s_%s_%s" % (name, os.getpid(), int(time.time() * 1000))
    node = rclpy.create_node(node_name, context=context, use_global_arguments=False,
                             enable_rosout=False, start_parameter_services=False)
    executor = rclpy.executors.MultiThreadedExecutor(context=context)
    executor.add_node(node)
    spinner = threading.Thread(target=spin_loop)
    spinner.daemon = True
    spinner.start()


def shutdown_node():
    """Shuts down live ROS2 node."""
    global node, context, executor
    if context:
        context_, executor_ = context, executor
        context = executor = node = None
        executor_.shutdown()
        context_.shutdown()


def validate(live=False):
    """
    Returns whether ROS2 environment is set, prints error if not.

    @param   live  whether environment must support launching a ROS node
    """
    missing = [k for k in ["ROS_VERSION"] if not os.getenv(k)]
    if missing:
        ConsolePrinter.error("ROS environment not sourced: missing %s.",
                             ", ".join(sorted(missing)))
    if "2" != os.getenv("ROS_VERSION", "2"):
        ConsolePrinter.error("ROS environment not supported: need ROS_VERSION=2.")
        missing = True
    return not missing


@memoize
def canonical(typename, unbounded=False):
    """
    Returns "pkg/Type" for "pkg/msg/Type", standardizes various ROS2 formats.

    Converts DDS types like "octet" to "byte", and "sequence<uint8, 100>" to "uint8[100]".

    @param  unbounded  drop constraints like array and string bounds,
                       e.g. returning "uint8[]" for "uint8[10]" and "string" for "string<=8"
    """
    if not typename: return typename
    is_array, bound, dimension = False, "", ""

    if "<" in typename:
        match = re.match("sequence<(.+)>", typename)
        if match:  # "sequence<uint8, 100>" or "sequence<uint8>"
            is_array = True
            typename = match.group(1)
            match = re.match(r"([^,]+)?,\s?(\d+)", typename)
            if match:  # sequence<uint8, 10>
                typename = match.group(1)
                if match.lastindex > 1: dimension = match.group(2)

        match = re.match("(w?string)<(.+)>", typename)
        if match:  # string<5>
            typename, bound = match.groups()

    if "[" in typename:  # "string<=5[<=10]" or "string<=5[10]" or "byte[10]" or "byte[]"
        dimension = typename[typename.index("[") + 1:typename.index("]")]
        typename, is_array = typename[:typename.index("[")], True

    if "<=" in typename:  # "string<=5"
        typename, bound = typename.split("<=")

    if typename.count("/") > 1:
        typename = "%s/%s" % tuple((x[0], x[-1]) for x in [typename.split("/")])[0]

    if unbounded: suffix = "[]" if is_array else ""
    else: suffix = ("<=%s" % bound if bound else "") + ("[%s]" % dimension if is_array else "")
    return DDS_TYPES.get(typename, typename) + suffix


def create_publisher(topic, cls_or_typename, queue_size):
    """Returns an rclpy.Publisher instance, with .get_num_connections() and .unregister()."""
    cls = cls_or_typename
    if isinstance(cls, str): cls = get_message_class(cls)
    qos = rclpy.qos.QoSProfile(depth=queue_size)
    pub = node.create_publisher(cls, topic, qos)
    pub.get_num_connections = pub.get_subscription_count
    pub.unregister = pub.destroy
    return pub


def create_subscriber(topic, cls_or_typename, handler, queue_size):
    """
    Returns an rclpy.Subscription.

    Supplemented with .get_message_class(), .get_message_definition(),
    .get_message_type_hash(), .get_qoses(), and.unregister().
    """
    cls = typename = cls_or_typename
    if isinstance(cls, str): cls = get_message_class(cls)
    else: typename = get_message_type(cls)

    qos = rclpy.qos.QoSProfile(depth=queue_size)
    qoses = [x.qos_profile for x in node.get_publishers_info_by_topic(topic)
             if canonical(x.topic_type) == typename]
    rels, durs = zip(*[(x.reliability, x.durability) for x in qoses]) if qoses else ([], [])
    # If subscription demands stricter QoS than publisher offers, no messages are received
    if rels: qos.reliability = max(rels)  # DEFAULT < RELIABLE < BEST_EFFORT
    if durs: qos.durability  = max(durs)  # DEFAULT < TRANSIENT_LOCAL < VOLATILE

    qosdicts = [qos_to_dict(x) for x in qoses] or None
    sub = node.create_subscription(cls, topic, handler, qos)
    sub.get_message_class      = lambda: cls
    sub.get_message_definition = lambda: get_message_definition(cls)
    sub.get_message_type_hash  = lambda: get_message_type_hash(cls)
    sub.get_qoses              = lambda: qosdicts
    sub.unregister             = sub.destroy
    return sub


def format_message_value(msg, name, value):
    """
    Returns a message attribute value as string.

    Result is at least 10 chars wide if message is a ROS2 time/duration
    (aligning seconds and nanoseconds).
    """
    LENS = {"sec": 13, "nanosec": 9}
    v = "%s" % (value, )
    if not isinstance(msg, tuple(ROS_TIME_CLASSES)) or name not in LENS:
        return v

    EXTRA = sum(v.count(x) * len(x) for x in (MatchMarkers.START, MatchMarkers.END))
    return ("%%%ds" % (LENS[name] + EXTRA)) % v  # Default %10s/%9s for secs/nanosecs


@memoize
def get_message_class(typename):
    """Returns ROS2 message class, or None if unknown type."""
    try: return rosidl_runtime_py.utilities.get_message(make_full_typename(typename))
    except Exception: return None


def get_message_definition(msg_or_type):
    """
    Returns ROS2 message type definition full text, including subtype definitions.

    Returns None if unknown type.
    """
    typename = msg_or_type if isinstance(msg_or_type, str) else get_message_type(msg_or_type)
    return _get_message_definition(canonical(typename))


def get_message_type_hash(msg_or_type):
    """Returns ROS2 message type MD5 hash, or "" if unknown type."""
    typename = msg_or_type if isinstance(msg_or_type, str) else get_message_type(msg_or_type)
    return _get_message_type_hash(canonical(typename))


@memoize
def _get_message_definition(typename):
    """Returns ROS2 message type definition full text, or None on error (internal caching method)."""
    try:
        texts, pkg = collections.OrderedDict(), typename.rsplit("/", 1)[0]
        try:
            typepath = rosidl_runtime_py.get_interface_path(make_full_typename(typename) + ".msg")
            with open(typepath) as f:
                texts[typename] = f.read()
        except Exception:  # .msg file unavailable: parse IDL
            texts[typename] = get_message_definition_idl(typename)
        for line in texts[typename].splitlines():
            if not line or not line[0].isalpha():
                continue  # for line
            linetype = scalar(canonical(re.sub(r"^([a-zA-Z][^\s]+)(.+)", r"\1", line)))
            if linetype in api.ROS_BUILTIN_TYPES:
                continue  # for line
            linetype = linetype if "/" in linetype else "std_msgs/Header" \
                       if "Header" == linetype else "%s/%s" % (pkg, linetype)
            linedef = None if linetype in texts else get_message_definition(linetype)
            if linedef: texts[linetype] = linedef

        basedef = texts.pop(next(iter(texts)))
        subdefs = ["%s\nMSG: %s\n%s" % ("=" * 80, k, v) for k, v in texts.items()]
        return basedef + ("\n" if subdefs else "") + "\n".join(subdefs)
    except Exception as e:
        ConsolePrinter.warn("Error collecting type definition of %s: %s", typename, e)
        return None


@memoize
def get_message_definition_idl(typename):
    """
    Returns ROS2 message type definition parsed from IDL file.

    @since   version 0.4.2
    """

    def format_comment(text):
        """Returns annotation text formatted with comment prefixes and escapes."""
        ESCAPES = {"\n":   "\\n", "\t":   "\\t", "\x07": "\\a",
                   "\x08": "\\b", "\x0b": "\\v", "\x0c": "\\f"}
        repl = lambda m: ESCAPES[m.group(0)]
        return "#" + "\n#".join(re.sub("|".join(map(re.escape, ESCAPES)), repl, l)
                                for l in text.split("\\n"))

    def format_type(typeobj, msgpackage, constant=False):
        """Returns canonical type name, like "uint8" or "string<=5" or "nav_msgs/Path"."""
        result = None
        if isinstance(typeobj, rosidl_parser.definition.AbstractNestedType):
            # Array, BoundedSequence, UnboundedSequence
            valuetype = format_type(typeobj.value_type, msgpackage, constant)
            size, bounding = "", ""
            if isinstance(typeobj, rosidl_parser.definition.Array):
                size = typeobj.size
            elif typeobj.has_maximum_size():
                size = typeobj.maximum_size
            if isinstance(typeobj, rosidl_parser.definition.BoundedSequence):
                bounding = "<="
            result = "%s[%s%s]" % (valuetype, bounding, size) # type[], type[N], type[<=N]
        elif isinstance(typeobj, rosidl_parser.definition.AbstractWString):
            result = "wstring"
        elif isinstance(typeobj, rosidl_parser.definition.AbstractString):
            result = "string"
        elif isinstance(typeobj, rosidl_parser.definition.NamespacedType):
            nameparts = typeobj.namespaced_name()
            result = canonical("/".join(nameparts))
            if nameparts[0].value == msgpackage or "std_msgs/Header" == result:
                result = canonical("/".join(nameparts[-1:]))  # Omit package if local or Header
        else:  # Primitive like int8
            result = DDS_TYPES.get(typeobj.typename, typeobj.typename)

        if isinstance(typeobj, rosidl_parser.definition.AbstractGenericString) \
        and typeobj.has_maximum_size() and not constant:  # Constants get parsed into "string<=N"
            result += "<=%s" % typeobj.maximum_size

        return result

    def get_comments(obj):
        """Returns all comments for annotatable object, as [text, ]."""
        return [v.get("text", "") for v in obj.get_annotation_values("verbatim")
                if "comment" == v.get("language")]

    typepath = rosidl_runtime_py.get_interface_path(make_full_typename(typename) + ".idl")
    with open(typepath) as f:
        idlcontent = rosidl_parser.parser.parse_idl_string(f.read())
    msgidl = idlcontent.get_elements_of_type(rosidl_parser.definition.Message)[0]
    package = msgidl.structure.namespaced_type.namespaces[0]
    DUMMY = rosidl_parser.definition.EMPTY_STRUCTURE_REQUIRED_MEMBER_NAME

    lines = []
    # Add general comments
    lines.extend(map(format_comment, get_comments(msgidl.structure)))
    # Add blank line between general comments and constants
    if lines and msgidl.constants: lines.append("")
    # Add constants
    for c in msgidl.constants:
        ctype = format_type(c.type, package, constant=True)
        lines.extend(map(format_comment, get_comments(c)))
        lines.append("%s %s=%s" % (ctype, c.name, c.value))
    # Parser adds dummy placeholder if constants-only message
    if not (len(msgidl.structure.members) == 1 and DUMMY == msgidl.structure[0].name):
        # Add blank line between constants and fields
        if msgidl.constants and msgidl.structure.members: lines.append("")
        # Add fields
        for m in msgidl.structure.members:
            lines.extend(map(format_comment, get_comments(m)))
            lines.append("%s %s" % (format_type(m.type, package), m.name))
    return "\n".join(lines)


@memoize
def _get_message_type_hash(typename):
    """Returns ROS2 message type MD5 hash (internal caching method)."""
    msgdef = get_message_definition(typename)
    return "" if msgdef is None else api.calculate_definition_hash(typename, msgdef)


def get_message_fields(val):
    """Returns OrderedDict({field name: field type name}) if ROS2 message, else {}."""
    if not is_ros_message(val): return {}
    fields = ((k, canonical(v)) for k, v in val.get_fields_and_field_types().items())
    return collections.OrderedDict(fields)


def get_message_type(msg_or_cls):
    """Returns ROS2 message type name, like "std_msgs/Header"."""
    cls = msg_or_cls if inspect.isclass(msg_or_cls) else type(msg_or_cls)
    return canonical("%s/%s" % (cls.__module__.split(".")[0], cls.__name__))


def get_message_value(msg, name, typename):
    """Returns object attribute value, with numeric arrays converted to lists."""
    v, scalartype = getattr(msg, name), scalar(typename)
    if isinstance(v, (bytes, array.array)): v = list(v)
    elif numpy and isinstance(v, (numpy.generic, numpy.ndarray)):
        v = v.tolist()  # Returns value as Python type, either scalar or list
    if v and isinstance(v, (list, tuple)) and scalartype in ("byte", "uint8"):
        if isinstance(v[0], bytes):
            v = list(map(ord, v))  # In ROS2, a byte array like [0, 1] is [b"\0", b"\1"]
        elif scalartype == typename:
            v = v[0]  # In ROS2, single byte values are given as bytes()
    return v


def get_rostime(fallback=False):
    """
    Returns current ROS2 time, as rclpy.time.Time.

    @param   fallback  use wall time if node not initialized
    """
    try: return node.get_clock().now()
    except Exception:
        if fallback: return make_time(time.time())
        raise


def get_topic_types():
    """
    Returns currently available ROS2 topics, as [(topicname, typename)].

    Omits topics that the current ROS2 node itself has published.
    """
    result = []
    myname, myns = node.get_name(), node.get_namespace()
    mytypes = {}  # {topic: [typename, ]}
    for topic, typenames in node.get_publisher_names_and_types_by_node(myname, myns):
        mytypes.setdefault(topic, []).extend(typenames)
    for t in ("/parameter_events", "/rosout"):  # Published by all nodes
        mytypes.pop(t, None)
    for topic, typenames in node.get_topic_names_and_types():  # [(topicname, [typename, ])]
        for typename in typenames:
            if topic not in mytypes or typename not in mytypes[topic]:
                result += [(topic, canonical(typename))]
    return result


def is_ros_message(val, ignore_time=False):
    """
    Returns whether value is a ROS2 message or special like ROS2 time/duration class or instance.

    @param  ignore_time  whether to ignore ROS2 time/duration types
    """
    is_message = rosidl_runtime_py.utilities.is_message(val)
    if is_message and ignore_time: is_message = not is_ros_time(val)
    return is_message


def is_ros_time(val):
    """Returns whether value is a ROS2 time/duration class or instance."""
    if inspect.isclass(val): return issubclass(val, tuple(ROS_TIME_CLASSES))
    return isinstance(val, tuple(ROS_TIME_CLASSES))


def make_duration(secs=0, nsecs=0):
    """Returns an rclpy.duration.Duration."""
    return rclpy.duration.Duration(seconds=secs, nanoseconds=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS2 time, as rclpy.time.Time."""
    return rclpy.time.Time(seconds=secs, nanoseconds=nsecs)


def make_full_typename(typename):
    """Returns "pkg/msg/Type" for "pkg/Type"."""
    if "/msg/" in typename or "/" not in typename:
        return typename
    return "%s/msg/%s" % tuple((x[0], x[-1]) for x in [typename.split("/")])[0]


def make_subscriber_qos(topic, typename, queue_size=10):
    """
    Returns rclpy.qos.QoSProfile that matches the most permissive publisher.

    @param   queue_size  QoSProfile.depth
    """
    qos = rclpy.qos.QoSProfile(depth=queue_size)
    infos = node.get_publishers_info_by_topic(topic)
    rels, durs = zip(*[(x.qos_profile.reliability, x.qos_profile.durability)
                       for x in infos if canonical(x.topic_type) == typename])
    # If subscription demands stricter QoS than publisher offers, no messages are received
    if rels: qos.reliability = max(rels)  # DEFAULT < RELIABLE < BEST_EFFORT
    if durs: qos.durability  = max(durs)  # DEFAULT < TRANSIENT_LOCAL < VOLATILE
    return qos


def qos_to_dict(qos):
    """Returns rclpy.qos.QoSProfile as dictionary."""
    result = {}
    if qos:
        QOS_TYPES = (bool, int, enum.Enum) + tuple(ROS_TIME_CLASSES)
        for name in (n for n in dir(qos) if not n.startswith("_")):
            val = getattr(qos, name)
            if name.startswith("_") or not isinstance(val, QOS_TYPES):
                continue  # for name
            if isinstance(val, enum.Enum):
                val = val.value
            elif isinstance(val, tuple(ROS_TIME_CLASSES)):
                val = dict(zip(["sec", "nsec"], to_sec_nsec(val)))
            result[name] = val
    return [result]


def serialize_message(msg):
    """Returns ROS2 message as a serialized binary."""
    with api.TypeMeta.make(msg) as m:
        if m.data is not None: return m.data
    return rclpy.serialization.serialize_message(msg)


def deserialize_message(raw, cls_or_typename):
    """Returns ROS2 message or service request/response instantiated from serialized binary."""
    cls = cls_or_typename
    if isinstance(cls, str): cls = get_message_class(cls)
    return rclpy.serialization.deserialize_message(raw, cls)


@memoize
def scalar(typename):
    """
    Returns unbounded scalar type from ROS2 message data type

    Like "uint8" from "uint8[]", or "string" from "string<=10[<=5]".
    Returns type unchanged if not a collection or bounded type.
    """
    if typename and "["  in typename: typename = typename[:typename.index("[")]
    if typename and "<=" in typename: typename = typename[:typename.index("<=")]
    return typename


def set_message_value(obj, name, value):
    """Sets message or object attribute value."""
    if is_ros_message(obj):
        # Bypass setter as it does type checking
        fieldmap = obj.get_fields_and_field_types()
        if name in fieldmap:
            name = obj.__slots__[list(fieldmap).index(name)]
    setattr(obj, name, value)


def time_message(val, to_message=True, clock_type=None):
    """
    Converts ROS2 time/duration between `rclpy` and `builtin_interfaces` objects.

    @param   val         ROS2 time/duration object from `rclpy` or `builtin_interfaces`
    @param   to_message  whether to convert from `rclpy` to `builtin_interfaces` or vice versa
    @param   clock_type  ClockType for converting to `rclpy.Time`, defaults to `ROS_TIME`
    @return              value converted to appropriate type, or original value if not convertible
    """
    to_message, clock_type = bool(to_message), (clock_type or rclpy.clock.ClockType.ROS_TIME)
    if isinstance(val, tuple(ROS_TIME_CLASSES)):
        rcl_cls = next(k for k, v in ROS_TIME_MESSAGES.items() if isinstance(val, (k, v)))
        is_rcl = isinstance(val, tuple(ROS_TIME_MESSAGES))
        name = "to_msg" if to_message and is_rcl else "from_msg" if to_message == is_rcl else None
        args = [val] + ([clock_type] if rcl_cls is rclpy.time.Time and "from_msg" == name else [])
        return getattr(rcl_cls, name)(*args) if name else val
    return val


def to_nsec(val):
    """Returns value in nanoseconds if value is ROS2 time/duration, else value."""
    if not isinstance(val, tuple(ROS_TIME_CLASSES)):
        return val
    if hasattr(val, "nanoseconds"):  # rclpy.Time/Duration
        return val.nanoseconds
    return val.sec * 10**9 + val.nanosec  # builtin_interfaces.msg.Time/Duration


def to_sec(val):
    """Returns value in seconds if value is ROS2 time/duration, else value."""
    if not isinstance(val, tuple(ROS_TIME_CLASSES)):
        return val
    if hasattr(val, "nanoseconds"):  # rclpy.Time/Duration
        secs, nsecs = divmod(val.nanoseconds, 10**9)
        return secs + nsecs / 1E9
    return val.sec + val.nanosec / 1E9  # builtin_interfaces.msg.Time/Duration


def to_sec_nsec(val):
    """Returns value as (seconds, nanoseconds) if value is ROS2 time/duration, else value."""
    if not isinstance(val, tuple(ROS_TIME_CLASSES)):
        return val
    if hasattr(val, "seconds_nanoseconds"):  # rclpy.Time
        return val.seconds_nanoseconds()
    if hasattr(val, "nanoseconds"):  # rclpy.Duration
        return divmod(val.nanoseconds, 10**9)
    return (val.sec, val.nanosec)  # builtin_interfaces.msg.Time/Duration


def to_time(val):
    """
    Returns value as ROS2 time if convertible, else value.

    Convertible types: int/float/duration/datetime/decimal/builtin_interfaces.Time.
    """
    result = val
    if isinstance(val, decimal.Decimal):
        result = make_time(int(val), float(val % 1) * 10**9)
    elif isinstance(val, datetime.datetime):
        result = make_time(int(val.timestamp()), 1000 * val.microsecond)
    elif isinstance(val, (float, int)):
        result = make_time(val)
    elif isinstance(val, rclpy.duration.Duration):
        result = make_time(nsecs=val.nanoseconds)
    elif isinstance(val, tuple(ROS_TIME_MESSAGES.values())):
        result = make_time(val.sec, val.nanosec)
    return result


__all__ = [
    "BAG_EXTENSIONS", "DDS_TYPES", "ROS_ALIAS_TYPES", "ROS_TIME_CLASSES", "ROS_TIME_MESSAGES",
    "ROS_TIME_TYPES", "SKIP_EXTENSIONS", "Bag", "ROS2Bag", "context", "executor", "node",
    "canonical", "create_publisher", "create_subscriber", "deserialize_message",
    "format_message_value", "get_message_class", "get_message_definition",
    "get_message_definition_idl", "get_message_fields", "get_message_type",
    "get_message_type_hash", "get_message_value", "get_rostime", "get_topic_types", "init_node",
    "is_ros_message", "is_ros_time", "make_duration", "make_full_typename", "make_subscriber_qos",
    "make_time", "qos_to_dict", "scalar", "serialize_message", "set_message_value", "shutdown_node",
    "time_message", "to_nsec", "to_sec", "to_sec_nsec", "to_time", "validate",
]

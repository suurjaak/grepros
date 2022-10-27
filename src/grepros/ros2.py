# -*- coding: utf-8 -*-
"""
ROS2 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     02.11.2021
@modified    27.10.2022
------------------------------------------------------------------------------
"""
## @namespace grepros.ros2
import array
import collections
import enum
import inspect
import os
import re
import sqlite3
import threading
import time

import builtin_interfaces.msg
import rclpy
import rclpy.duration
import rclpy.executors
import rclpy.serialization
import rclpy.time
import rosidl_parser.parser
import rosidl_parser.definition
import rosidl_runtime_py.utilities
import yaml

from . common import ConsolePrinter, MatchMarkers, memoize
from . import rosapi


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



class Bag(object):
    """ROS2 bag interface, partially mimicking rosbag.Bag."""

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
    """


    def __init__(self, filename, mode="a", *_, **__):
        """
        @param   filename  bag file path to open
        @param   mode      file will be overwritten if "w"
        """
        self._db     = None  # sqlite3.Connection instance
        self._mode   = mode
        self._topics = {}    # {(topic, typename): {id, name, type}}
        self._counts = {}    # {(topic, typename, typehash): message count}
        self._qoses  = {}    # {(topic, typename): [{qos profile dict}]}

        ## Bagfile path
        self.filename = filename


    def get_message_count(self):
        """Returns the number of messages in the bag."""
        self._ensure_open()
        if self._has_table("messages"):
            row = self._db.execute("SELECT COUNT(*) AS count FROM messages").fetchone()
            return row["count"]
        return None


    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp."""
        self._ensure_open()
        if self._has_table("messages"):
            row = self._db.execute("SELECT MIN(timestamp) AS val FROM messages").fetchone()
            if row["val"] is None: return None
            secs, nsecs = divmod(row["val"], 10**9)
            return secs + nsecs / 1E9
        return None


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp."""
        self._ensure_open()
        if self._has_table("messages"):
            row = self._db.execute("SELECT MAX(timestamp) AS val FROM messages").fetchone()
            if row["val"] is None: return None
            secs, nsecs = divmod(row["val"], 10**9)
            return secs + nsecs / 1E9
        return None


    def get_topic_info(self, counts=False):
        """
        Returns topic and message type metainfo as {(topic, typename, typehash): count}.

        @param   counts  whether to return actual message counts instead of None
        """
        self._ensure_open()
        DEFAULTCOUNT = 0 if counts else None
        if not self._counts and self._has_table("topics"):
            for row in self._db.execute("SELECT * FROM topics ORDER BY id").fetchall():
                topic, typename = row["name"], canonical(row["type"])
                typehash = get_message_type_hash(typename)
                self._topics[(topic, typename)] = row
                self._counts[(topic, typename, typehash)] = DEFAULTCOUNT

        if counts and self._has_table("messages") and not any(self._counts.values()):
            topickeys = {v["id"]: (t, n, get_message_type_hash(n))
                         for (t, n), v in self._topics.items()}
            for row in self._db.execute("SELECT topic_id, COUNT(*) AS count FROM messages "
                                        "GROUP BY topic_id").fetchall():
                if row["topic_id"] in topickeys:
                    self._counts[topickeys[row["topic_id"]]] = row["count"]

        return dict(self._counts)


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
        """Returns ROS2 message type class."""
        return get_message_class(typename)


    def get_message_definition(self, msg_or_type):
        """Returns ROS2 message type definition full text, including subtype definitions."""
        return get_message_definition(msg_or_type)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS2 message type MD5 hash."""
        return get_message_type_hash(msg_or_type)


    def read_messages(self, topics=None, start_time=None, end_time=None, raw=False, *_, **__):
        """
        Yields messages from the bag, optionally filtered by topic and timestamp.

        @param   topics      list of topics or a single topic to filter by, if at all
        @param   start_time  earliest timestamp of message to return, as UNIX timestamp
        @param   end_time    latest timestamp of message to return, as UNIX timestamp
        @param   raw         if True, then returned messages are tuples of
                             (typename, bytes, typehash, typeclass)
        @return              generator of (topic, message, rclpy.time.Time) tuples
        """
        self.get_topic_info()
        if not self._topics or (topics is not None and not topics):
            return

        sql, exprs, args = "SELECT * FROM messages", [], ()
        if topics:
            topics = topics if isinstance(topics, (list, tuple)) else [topics]
            topic_ids = [x["id"] for (topic, _), x in self._topics.items() if topic in topics]
            exprs += ["topic_id IN (%s)" % ", ".join(map(str, topic_ids))]
        if start_time is not None:
            exprs += ["timestamp >= ?"]
            args  += (start_time.nanoseconds, )
        if end_time is not None:
            exprs += ["timestamp <= ?"]
            args  += (end_time.nanoseconds, )
        sql += ((" WHERE " + " AND ".join(exprs)) if exprs else "")
        sql += " ORDER BY timestamp"

        topicmap   = {v["id"]: v for v in self._topics.values()}
        msgtypes   = {}  # {typename: cls}
        topicset   = set(topics or [t for t, _ in self._topics])
        errortypes = set()  # {typename failed to instantiate, }
        for row in self._db.execute(sql, args):
            tdata = topicmap[row["topic_id"]]
            topic, typename = tdata["name"], canonical(tdata["type"])
            if not raw and msgtypes.get(typename, typename) is None: continue # for row
            typehash = next(h for t, n, h in self._counts if (t, n) == (topic, typename))

            try:
                cls = msgtypes.get(typename) or \
                      msgtypes.setdefault(typename, get_message_class(typename))
                if raw: msg = (typename, row["data"], typehash, cls)
                else:   msg = rclpy.serialization.deserialize_message(row["data"], cls)
            except Exception as e:
                errortypes.add(typename)
                ConsolePrinter.warn("Error loading type %s in topic %s: %%s" %
                                    (typename, topic), e, __once=True)
                if raw: msg = (typename, row["data"], typehash, None)
                elif set(n for n, c in msgtypes.items() if c is None) == topicset:
                    break  # for row
                continue  # for row
            errortypes.discard(typename)
            stamp = rclpy.time.Time(nanoseconds=row["timestamp"])

            yield topic, msg, stamp
            if not self._db:
                break  # for row


    def write(self, topic, msg, stamp, meta=None):
        """
        Writes a message to the bag.

        @param   topic  name of topic
        @param   msg    ROS2 message
        @param   stamp  rclpy.time.Time of message publication
        @param   meta   message metainfo dict (meta["qoses"] added to topics-table, if any)
        """
        self._ensure_open(populate=True)
        self.get_topic_info()

        cursor = self._db.cursor()
        typename = get_message_type(msg)
        topickey = (topic, typename)
        if topickey not in self._topics:
            full_typename = make_full_typename(get_message_type(msg))
            sql = "INSERT INTO topics (name, type, serialization_format, offered_qos_profiles) " \
                  "VALUES (?, ?, ?, ?)"
            qoses = yaml.safe_dump(meta["qoses"], ) if meta and meta.get("qoses") else ""
            args = (topic, full_typename, "cdr", qoses)
            cursor.execute(sql, args)
            tdata = {"id": cursor.lastrowid, "name": topic, "type": full_typename,
                     "serialization_format": "cdr", "offered_qos_profiles": qoses}
            self._topics[topickey] = tdata

        sql = "INSERT INTO messages (topic_id, timestamp, data) VALUES (?, ?, ?)"
        args = (self._topics[topickey]["id"], stamp.nanoseconds, get_message_data(msg))
        cursor.execute(sql, args)


    def close(self):
        """Closes the bag file."""
        if self._db:
            self._db.close()
            self._db   = None
            self._mode = None


    @property
    def size(self):
        """Returns current file size in bytes (including journaling files)."""
        result = os.path.getsize(self.filename) if os.path.isfile(self.filename) else None
        for suffix in ("-journal", "-wal") if result else ():
            path = self.filename + suffix
            result += os.path.getsize(path) if os.path.isfile(path) else 0
        return result


    def _ensure_open(self, populate=False):
        """Opens bag database if not open, can populate schema if not present."""
        if self._db:
            return
        if "w" == self._mode and os.path.exists(self.filename):
            os.remove(self.filename)
        self._db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES,
                                   isolation_level=None, check_same_thread=False)
        self._db.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))
        if populate:
            self._db.executescript(self.CREATE_SQL)


    def _has_table(self, name):
        """Returns whether specified table exists in database."""
        sql = "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?"
        return bool(self._db.execute(sql, ("table", name)).fetchone())



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
def canonical(typename):
    """
    Returns "pkg/Type" for "pkg/msg/Type", standardizes various ROS2 formats.

    Converts DDS types like "octet" to "byte", and "sequence<uint8, 100>" to "uint8[100]".
    """
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

    suffix = ("<=%s" % bound if bound else "") + ("[%s]" % dimension if is_array else "")
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


def deserialize_message(raw, cls):
    """Returns ROS2 message or service request/response instantiated from serialized binary."""
    return rclpy.serialization.deserialize_message(raw, cls)


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
    """Returns ROS2 message class."""
    return rosidl_runtime_py.utilities.get_message(make_full_typename(typename))


def get_message_data(msg):
    """Returns ROS2 message as a serialized binary."""
    with rosapi.TypeMeta.make(msg) as m:
        if m.data is not None: return m.data
    return rclpy.serialization.serialize_message(msg)


def get_message_definition(msg_or_type):
    """Returns ROS2 message type definition full text, including subtype definitions."""
    typename = msg_or_type if isinstance(msg_or_type, str) else get_message_type(msg_or_type)
    return _get_message_definition(canonical(typename))


def get_message_type_hash(msg_or_type):
    """Returns ROS2 message type MD5 hash."""
    typename = msg_or_type if isinstance(msg_or_type, str) else get_message_type(msg_or_type)
    return _get_message_type_hash(canonical(typename))


@memoize
def _get_message_definition(typename):
    """Returns ROS2 message type definition full text, or "" on error (internal caching method)."""
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
            if linetype in rosapi.ROS_BUILTIN_TYPES:
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
        return ""


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
    return rosapi.calculate_definition_hash(typename, msgdef)


def get_message_fields(val):
    """Returns OrderedDict({field name: field type name}) if ROS2 message, else {}."""
    if not is_ros_message(val): return {}
    fields = {k: canonical(v) for k, v in val.get_fields_and_field_types().items()}
    return collections.OrderedDict(fields)


def get_message_type(msg_or_cls):
    """Returns ROS2 message type name, like "std_msgs/Header"."""
    cls = msg_or_cls if inspect.isclass(msg_or_cls) else type(msg_or_cls)
    return canonical("%s/%s" % (cls.__module__.split(".")[0], cls.__name__))


def get_message_value(msg, name, typename):
    """Returns object attribute value, with numeric arrays converted to lists."""
    v, scalartype = getattr(msg, name), scalar(typename)
    if isinstance(v, (bytes, array.array)) \
    or "numpy.ndarray" == "%s.%s" % (v.__class__.__module__, v.__class__.__name__):
        v = list(v)
    if v and isinstance(v, (list, tuple)) and scalartype in ("byte", "uint8"):
        if isinstance(v[0], bytes):
            v = list(map(ord, v))  # In ROS2, a byte array like [0, 1] is [b"\0", b"\1"]
        elif scalartype == typename:
            v = v[0]  # In ROS2, single byte values are given as bytes()
    return v


def get_rostime():
    """Returns current ROS2 time, as rclpy.time.Time."""
    return node.get_clock().now()


def get_topic_types():
    """
    Returns currently available ROS2 topics, as [(topicname, typename)].

    Omits topics that the current ROS2 node itself has published.
    """
    result = []
    myname, myns = node.get_name(), node.get_namespace()
    mytypes = {}  # {topic: [typename, ]}
    for topic, typename in node.get_publisher_names_and_types_by_node(myname, myns):
        mytypes.setdefault(topic, []).append(typename)
    for t in ("/parameter_events", "/rosout"):  # Published by all nodes
        mytypes.pop(t, None)
    for topic, typenames in node.get_topic_names_and_types():  # [(topicname, [typename, ])]
        for typename in typenames:
            if topic not in mytypes or typename not in mytypes[topic]:
                result += [(topic, canonical(typename))]
    return result


def is_ros_message(val, ignore_time=False):
    """
    Returns whether value is a ROS2 message or special like ROS2 time/duration.

    @param  ignore_time  whether to ignore ROS2 time/duration types
    """
    is_message = rosidl_runtime_py.utilities.is_message(val)
    if is_message and ignore_time:
        is_message = not isinstance(val, tuple(ROS_TIME_CLASSES))
    return is_message


def is_ros_time(val):
    """Returns whether value is a ROS2 time/duration."""
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


@memoize
def scalar(typename):
    """
    Returns unbounded scalar type from ROS2 message data type

    Like "uint8" from "uint8[]", or "string" from "string<=10[<=5]".
    Returns type unchanged if not a collection or bounded type.
    """
    if "["  in typename: typename = typename[:typename.index("[")]
    if "<=" in typename: typename = typename[:typename.index("<=")]
    return typename


def set_message_value(obj, name, value):
    """Sets message or object attribute value."""
    if is_ros_message(obj):
        # Bypass setter as it does type checking
        fieldmap = obj.get_fields_and_field_types()
        if name in fieldmap:
            name = obj.__slots__[list(fieldmap).index(name)]
    setattr(obj, name, value)


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

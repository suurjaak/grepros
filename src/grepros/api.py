# -*- coding: utf-8 -*-
"""
ROS interface, shared facade for ROS1 and ROS2.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    02.07.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.api
import abc
import collections
import datetime
import decimal
import hashlib
import inspect
import os
import re
import sys
import time

import six

from . common import ConsolePrinter, LenIterable, format_bytes, memoize
#from . import ros1, ros2  # Imported conditionally


## Node base name for connecting to ROS (will be anonymized).
NODE_NAME = "grepros"

## Bagfile extensions to seek, including leading dot, populated after init
BAG_EXTENSIONS  = ()

## Bagfile extensions to skip, including leading dot, populated after init
SKIP_EXTENSIONS = ()

## Flag denoting ROS1 environment, populated on validate()
ROS1 = None

## Flag denoting ROS2 environment, populated on validate()
ROS2 = None

## ROS version from environment, populated on validate() as integer
ROS_VERSION = None

## ROS Python module family, "rospy" or "rclpy", populated on validate()
ROS_FAMILY = None

## All built-in numeric types in ROS
ROS_NUMERIC_TYPES = ["byte", "char", "int8", "int16", "int32", "int64", "uint8",
                     "uint16", "uint32", "uint64", "float32", "float64", "bool"]

## All built-in string types in ROS
ROS_STRING_TYPES = ["string", "wstring"]

## All built-in basic types in ROS
ROS_BUILTIN_TYPES = ROS_NUMERIC_TYPES + ROS_STRING_TYPES

## Python constructors for ROS built-in types, as {ROS name: type class}
ROS_BUILTIN_CTORS = {"byte":   int,  "char":   int, "int8":    int,   "int16":   int,
                     "int32":  int,  "int64":  int, "uint8":   int,   "uint16":  int,
                     "uint32": int,  "uint64": int, "float32": float, "float64": float,
                     "bool":   bool, "string": str, "wstring": str}

## ROS time/duration types, populated after init
ROS_TIME_TYPES = []

## ROS1 time/duration types mapped to type names, populated after init
ROS_TIME_CLASSES = {}

## All built-in basic types plus time types in ROS, populated after init
ROS_COMMON_TYPES = []

## Mapping between type aliases and real types, like {"byte": "int8"} in ROS1
ROS_ALIAS_TYPES = {}

## Module grepros.ros1 or grepros.ros2
realapi = None


class BaseBag(object):
    """
    ROS bag interface.

    %Bag can be used a context manager, is an iterable providing (topic, message, timestamp) tuples
    and supporting `len(bag)`; and supports topic-based membership
    (`if mytopic in bag`, `for t, m, s in bag[mytopic]`, `len(bag[mytopic])`).

    Extra methods and properties compared with rosbag.Bag: Bag.get_message_class(),
    Bag.get_message_definition(), Bag.get_message_type_hash(), Bag.get_topic_info();
    Bag.closed and Bag.topics.
    """

    ## Returned from read_messages() as (topic name, ROS message, ROS timestamp object).
    BagMessage = collections.namedtuple("BagMessage", "topic message timestamp")

    ## Returned from get_type_and_topic_info() as
    ## (typename, message count, connection count, median frequency).
    TopicTuple = collections.namedtuple("TopicTuple", ["msg_type", "message_count",
                                                       "connections", "frequency"])

    ## Returned from get_type_and_topic_info() as ({typename: typehash}, {topic name: TopicTuple}).
    TypesAndTopicsTuple = collections.namedtuple("TypesAndTopicsTuple", ["msg_types", "topics"])

    ## Supported opening modes, overridden in subclasses
    MODES = ("r", "w", "a")

    ## Whether bag supports reading or writing stream objects, overridden in subclasses
    STREAMABLE = True

    def __iter__(self):
        """Iterates over all messages in the bag."""
        return self.read_messages()

    def __enter__(self):
        """Context manager entry, opens bag if not open."""
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit, closes bag."""
        self.close()

    def __len__(self):
        """Returns the number of messages in the bag."""
        return self.get_message_count()

    def __next__(self):
        """Retrieves next message from bag as (topic, message, timestamp)."""
        raise NotImplementedError
    if sys.version_info < (3, ): next = __next__

    def __nonzero__(self): return True  # Iterables by default use len() for bool() [Py2]

    def __bool__   (self): return True  # Iterables by default use len() for bool() [Py3]

    def __contains__(self, key):
        """Returns whether bag contains given topic."""
        raise NotImplementedError

    def __copy__(self): return self

    def __deepcopy__(self, memo=None): return self

    def __getitem__(self, key):
        """Returns an iterator yielding messages from the bag in given topic, supporting len()."""
        if key not in self: return LenIterable([], 0)
        get_count = lambda: sum(c for (t, _, _), c in self.get_topic_info().items() if t == key)
        return LenIterable(self.read_messages(key), get_count)

    def __str__(self):
        """Returns informative text for bag, with a full overview of topics and types."""

        indent  = lambda s, n: ("\n%s" % (" " * n)).join(s.splitlines())
        # Returns UNIX timestamp as "Dec 22 2021 23:13:44.44"
        fmttime = lambda x: datetime.datetime.fromtimestamp(x).strftime("%b %d %Y %H:%M:%S.%f")[:-4]
        def fmtdur(secs):
            """Returns duration seconds as text like "1hr 1:12s (3672s)" or "51.8s"."""
            result = ""
            hh, rem = divmod(secs, 3600)
            mm, ss  = divmod(rem, 60)
            if hh: result += "%dhr " % hh
            if mm: result += "%d:"   % mm
            result += "%ds" % ss
            if hh or mm: result += " (%ds)" % secs
            return result

        entries = {}
        counts = self.get_topic_info()
        start, end = self.get_start_time(), self.get_end_time()

        entries["path"] = self.filename or "<stream>"
        if None not in (start, end):
            entries["duration"] = fmtdur(end - start)
            entries["start"] = "%s (%.2f)" % (fmttime(start), start)
            entries["end"]   = "%s (%.2f)" % (fmttime(end),   end)
        entries["size"] = format_bytes(self.size)
        if any(counts.values()):
            entries["messages"] = str(sum(counts.values()))
        if counts:
            nhs = sorted(set((n, h) for _, n, h in counts))
            namew = max(len(n) for n, _ in nhs)
            # "pkg/Msg <PAD>[typehash]"
            entries["types"] = "\n".join("%s%s [%s]" % (n, " " * (namew - len(n)), h) for n, h in nhs)
        if counts:
            topicw = max(len(t) for t, _, _ in counts)
            typew  = max(len(n) for _, n, _ in counts)
            countw = max(len(str(c)) for c in counts.values())
            lines = []
            for (t, n, _), c in sorted(counts.items()):
                qq = self.get_qoses(t, n) or []
                # "/my/topic<PAD>"
                line  = "%s%s" % (t, " " * (topicw - len(t)))
                # "   <PAD>13 msgs" or "   <PAD>1 msg "
                line += "   %s%s msg%s" % (" " * (countw - len(str(c))), c, " " if 1 == c else "s")
                # "    : pkg/Msg"
                line += "    : %s" % n
                # "<PAD> (2 connections)" if >1 connections
                line += "%s (%s connections)" % (" " * (typew - len(n)), len(qq)) if len(qq) > 1 else ""
                lines.append(line)
            entries["topics"] = "\n".join(lines)

        labelw = max(map(len, entries))
        return "\n".join("%s:%s %s" % (k, " " * (labelw - len(k)), indent(v, labelw + 2))
                         for k, v in entries.items())

    def get_message_count(self, topic_filters=None):
        """
        Returns the number of messages in the bag.

        @param   topic_filters  list of topics or a single topic to filter by, if any
        """
        raise NotImplementedError

    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp, or None if bag empty."""
        raise NotImplementedError

    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp, or None if bag empty."""
        raise NotImplementedError

    def get_topic_info(self, counts=True):
        """
        Returns topic and message type metainfo as {(topic, typename, typehash): count}.

        @param   counts  if false, counts may be returned as None if lookup is costly
        """
        raise NotImplementedError

    def get_type_and_topic_info(self, topic_filters=None):
        """
        Returns thorough metainfo on topic and message types.

        @param   topic_filters  list of topics or a single topic to filter returned topics-dict by,
                                if any
        @return                 TypesAndTopicsTuple(msg_types, topics) namedtuple,
                                msg_types as dict of {typename: typehash},
                                topics as a dict of {topic: TopicTuple() namedtuple}.
        """
        raise NotImplementedError

    def get_qoses(self, topic, typename):
        """
        Returns topic Quality-of-Service profiles as a list of dicts, or None if not available.

        Functional only in ROS2.
        """
        return None

    def get_message_class(self, typename, typehash=None):
        """Returns ROS message type class, or None if unknown message type for bag."""
        return None

    def get_message_definition(self, msg_or_type):
        """
        Returns ROS message type definition full text, including subtype definitions.

        Returns None if unknown message type for bag.
        """
        return None

    def get_message_type_hash(self, msg_or_type):
        """Returns ROS message type MD5 hash, or None if unknown message type for bag."""
        return None

    def read_messages(self, topics=None, start_time=None, end_time=None, raw=False, **__):
        """
        Yields messages from the bag, optionally filtered by topic and timestamp.

        @param   topics      list of topics or a single topic to filter by, if any
        @param   start_time  earliest timestamp of message to return, as ROS time or convertible
                             (int/float/duration/datetime/decimal)
        @param   end_time    latest timestamp of message to return, as ROS time
                             (int/float/duration/datetime/decimal)
        @param   raw         if true, then returned messages are tuples of
                             (typename, bytes, typehash, typeclass)
                             or (typename, bytes, typehash, position, typeclass),
                             depending on file format
        @return              BagMessage namedtuples of (topic, message, timestamp as ROS time)
        """
        raise NotImplementedError

    def write(self, topic, msg, t=None, raw=False, **kwargs):
        """
        Writes a message to the bag.

        @param   topic   name of topic
        @param   msg     ROS message
        @param   t       message timestamp if not using current wall time, as ROS time
                         or convertible (int/float/duration/datetime/decimal)
        @param   raw     if true, `msg` is in raw format, (typename, bytes, typehash, typeclass)
        @param   kwargs  ROS version-specific arguments,
                         like `connection_header` for ROS1 or `qoses` for ROS2
        """
        raise NotImplementedError

    def open(self):
        """Opens the bag file if not already open."""
        raise NotImplementedError

    def close(self):
        """Closes the bag file."""
        raise NotImplementedError

    def flush(self):
        """Ensures all changes are written to bag file."""

    @property
    def topics(self):
        """Returns the list of topics in bag, in alphabetic order."""
        raise NotImplementedError

    @property
    def filename(self):
        """Returns bag file path."""
        raise NotImplementedError

    @property
    def size(self):
        """Returns current file size in bytes."""
        raise NotImplementedError

    @property
    def mode(self):
        """Returns file open mode."""
        raise NotImplementedError

    @property
    def closed(self):
        """Returns whether file is closed."""
        raise NotImplementedError

    @property
    def stop_on_error(self):
        """Whether raising read error on unknown message type (ROS2 SQLite .db3 specific)."""
        return getattr(self, "_stop_on_error", None)

    @stop_on_error.setter
    def stop_on_error(self, flag):
        """Sets whether to raise read error on unknown message type (ROS2 SQLite .db3 specific)."""
        setattr(self, "_stop_on_error", flag)


@six.add_metaclass(abc.ABCMeta)
class Bag(BaseBag):
    """
    %Bag factory metaclass.

    Result is a format-specific class instance, auto-detected from file extension or content:
    an extended rosbag.Bag for ROS1 bags, otherwise an object with a conforming interface.

    E.g. {@link grepros.plugins.mcap.McapBag McapBag} if {@link grepros.plugins.mcap mcap}
    plugin loaded and file recognized as MCAP format.

    User plugins can add their own format support to READER_CLASSES and WRITER_CLASSES.
    Classes can have a static/class method `autodetect(filename)`
    returning whether given file is in recognizable format for the plugin class.
    """

    ## Bag reader classes, as {Cls, }
    READER_CLASSES = set()

    ## Bag writer classes, as {Cls, }
    WRITER_CLASSES = set()

    def __new__(cls, f, mode="r", reindex=False, progress=False, **kwargs):
        """
        Returns an object for reading or writing ROS bags.

        Suitable Bag class is auto-detected by file extension or content.

        @param   f         bag file path, or a stream object
                           (streams not supported for ROS2 .db3 SQLite bags)
        @param   mode      return reader if "r" else writer
        @param   reindex   reindex unindexed bag (ROS1 only), making a backup if indexed format
        @param   progress  show progress bar with reindexing status
        @param   kwargs    additional keyword arguments for format-specific Bag constructor,
                           like `compression` for ROS1 bag
        """
        classes, errors = set(cls.READER_CLASSES if "r" == mode else cls.WRITER_CLASSES), []
        for detect, bagcls in ((d, c) for d in (True, False) for c in list(classes)):
            use, discard = not detect, False  # Try auto-detecting suitable class first
            try:
                if detect and callable(getattr(bagcls, "autodetect", None)):
                    use, discard = bagcls.autodetect(f), True
                if use:
                    return bagcls(f, mode, reindex=reindex, progress=progress, **kwargs)
            except Exception as e:
                discard = True
                errors.append("Failed to open %r for %s with %s: %s." %
                              (f, "reading" if "r" == mode else "writing", bagcls, e))
            discard and classes.discard(bagcls)
        for err in errors: ConsolePrinter.warn(err)
        raise Exception("No suitable %s class available" % ("reader" if "r" == mode else "writer"))


    @classmethod
    def autodetect(cls, f):
        """Returns registered bag class auto-detected from file, or None."""
        for bagcls in cls.READER_CLASSES | cls.WRITER_CLASSES:
            if callable(vars(bagcls).get("autodetect")) and bagcls.autodetect(f):
                return bagcls
        return None


    @classmethod
    def __subclasshook__(cls, C):
        return True if issubclass(C, BaseBag) else NotImplemented


class TypeMeta(object):
    """
    Container for caching and retrieving message type metadata.

    All property values are lazy-loaded upon request.
    """

    ## Seconds before auto-clearing message from cache, <=0 disables
    LIFETIME = 2

    ## Max size to constrain cache to, <=0 disables
    POPULATION = 0

    ## {id(msg): TypeMeta()}
    _CACHE = {}

    ## {id(msg): [id(nested msg), ]}
    _CHILDREN = {}

    ## {id(msg): time.time() of registering}
    _TIMINGS = {}

    ## time.time() of last cleaning of stale messages
    _LASTSWEEP = time.time()

    def __init__(self, msg, topic=None, source=None, data=None):
        self._msg      = msg
        self._topic    = topic
        self._source   = source
        self._data     = data
        self._type     = None  # Message typename as "pkg/MsgType"
        self._def      = None  # Message type definition with full subtype definitions
        self._hash     = None  # Message type definition MD5 hash
        self._cls      = None  # Message class object
        self._topickey = None  # (topic, typename, typehash)
        self._typekey  = None  # (typename, typehash)

    def __enter__(self, *_, **__):
        """Allows using instance as a context manager (no other effect)."""
        return self

    def __exit__(self, *_, **__): pass

    @property
    def typename(self):
        """Returns message typename, as "pkg/MsgType"."""
        if not self._type:
            self._type = realapi.get_message_type(self._msg)
        return self._type

    @property
    def typehash(self):
        """Returns message type definition MD5 hash."""
        if not self._hash:
            hash = self._source and self._source.get_message_type_hash(self._msg)
            self._hash = hash or realapi.get_message_type_hash(self._msg)
        return self._hash

    @property
    def definition(self):
        """Returns message type definition text with full subtype definitions."""
        if not self._def:
            typedef = self._source and self._source.get_message_definition(self._msg)
            self._def = typedef or realapi.get_message_definition(self._msg)
        return self._def

    @property
    def data(self):
        """Returns message serialized binary, as bytes(), or None if not cached."""
        return self._data

    @property
    def typeclass(self):
        """Returns message class object."""
        if not self._cls:
            cls = type(self._msg) if realapi.is_ros_message(self._msg) else \
                  self._source and self._source.get_message_class(self.typename, self.typehash)
            self._cls = cls or realapi.get_message_class(self.typename)
        return self._cls

    @property
    def topickey(self):
        """Returns (topic, typename, typehash) for message."""
        if not self._topickey:
            self._topickey = (self._topic, self.typename, self.typehash)
        return self._topickey

    @property
    def typekey(self):
        """Returns (typename, typehash) for message."""
        if not self._typekey:
            self._typekey = (self.typename, self.typehash)
        return self._typekey

    @classmethod
    def make(cls, msg, topic=None, source=None, root=None, data=None):
        """
        Returns TypeMeta instance, registering message in cache if not present.

        Other parameters are only required for first registration.

        @param   topic   topic the message is in if root message
        @param   source  message source like TopicSource or Bag,
                         for looking up message type metadata
        @param   root    root message that msg is a nested value of, if any
        @param   data    message serialized binary, if any
        """
        msgid = id(msg)
        if msgid not in cls._CACHE:
            cls._CACHE[msgid] = TypeMeta(msg, topic, data)
            if root and root is not msg:
                cls._CHILDREN.setdefault(id(root), set()).add(msgid)
            cls._TIMINGS[msgid] = time.time()
        result = cls._CACHE[msgid]
        cls.sweep()
        return result

    @classmethod
    def discard(cls, msg):
        """Discards message metadata from cache, if any, including nested messages."""
        msgid = id(msg)
        cls._CACHE.pop(msgid, None), cls._TIMINGS.pop(msgid, None)
        for childid in cls._CHILDREN.pop(msgid, []):
            cls._CACHE.pop(childid, None), cls._TIMINGS.pop(childid, None)
        cls.sweep()

    @classmethod
    def sweep(cls):
        """Discards stale or surplus messages from cache."""
        if cls.POPULATION > 0 and len(cls._CACHE) > cls.POPULATION:
            count = len(cls._CACHE) - cls.POPULATION
            for msgid, tm in sorted(x[::-1] for x in cls._TIMINGS.items())[:count]:
                cls._CACHE.pop(msgid, None), cls._TIMINGS.pop(msgid, None)
                for childid in cls._CHILDREN.pop(msgid, []):
                    cls._CACHE.pop(childid, None), cls._TIMINGS.pop(childid, None)

        now = time.time()
        if cls.LIFETIME <= 0 or cls._LASTSWEEP < now - cls.LIFETIME: return

        for msgid, tm in list(cls._TIMINGS.items()):
            drop = (tm > now) or (tm < now - cls.LIFETIME)
            drop and (cls._CACHE.pop(msgid, None), cls._TIMINGS.pop(msgid, None))
            for childid in cls._CHILDREN.pop(msgid, []) if drop else ():
                cls._CACHE.pop(childid, None), cls._TIMINGS.pop(childid, None)
        cls._LASTSWEEP = now

    @classmethod
    def clear(cls):
        """Clears entire cache."""
        cls._CACHE.clear()
        cls._CHILDREN.clear()
        cls._TIMINGS.clear()



def init_node(name=None):
    """
    Initializes a ROS1 or ROS2 node if not already initialized.

    In ROS1, blocks until ROS master available.
    """
    validate() and realapi.init_node(name or NODE_NAME)


def shutdown_node():
    """Shuts down live ROS node."""
    realapi and realapi.shutdown_node()


def validate(live=False):
    """
    Initializes ROS bindings, returns whether ROS environment set, prints or raises error if not.

    @param   live  whether environment must support launching a ROS node
    """
    global realapi, BAG_EXTENSIONS, SKIP_EXTENSIONS, ROS1, ROS2, ROS_VERSION, ROS_FAMILY, \
           ROS_COMMON_TYPES, ROS_TIME_TYPES, ROS_TIME_CLASSES, ROS_ALIAS_TYPES
    if realapi:
        return True

    success, version = False, os.getenv("ROS_VERSION")
    if "1" == version:
        from . import ros1
        realapi = ros1
        success = realapi.validate()
        ROS1, ROS2, ROS_VERSION, ROS_FAMILY = True, False, 1, "rospy"
    elif "2" == version:
        from . import ros2
        realapi = ros2
        success = realapi.validate(live)
        ROS1, ROS2, ROS_VERSION, ROS_FAMILY = False, True, 2, "rclpy"
    elif not version:
        ConsolePrinter.error("ROS environment not set: missing ROS_VERSION.")
    else:
        ConsolePrinter.error("ROS environment not supported: unknown ROS_VERSION %r.", version)
    if success:
        BAG_EXTENSIONS, SKIP_EXTENSIONS = realapi.BAG_EXTENSIONS, realapi.SKIP_EXTENSIONS
        ROS_COMMON_TYPES = ROS_BUILTIN_TYPES + realapi.ROS_TIME_TYPES
        ROS_TIME_TYPES   = realapi.ROS_TIME_TYPES
        ROS_TIME_CLASSES = realapi.ROS_TIME_CLASSES
        ROS_ALIAS_TYPES  = realapi.ROS_ALIAS_TYPES
        Bag.READER_CLASSES.add(realapi.Bag)
        Bag.WRITER_CLASSES.add(realapi.Bag)
    return success


@memoize
def calculate_definition_hash(typename, msgdef, extradefs=()):
    """
    Returns MD5 hash for message type definition.

    @param   extradefs  additional subtype definitions as ((typename, msgdef), )
    """
    # "type name (= constvalue)?" or "type name (defaultvalue)?" (ROS2 format)
    FIELD_RGX = re.compile(r"^([a-z][^\s:]+)\s+([^\s=]+)(\s*=\s*([^\n]+))?(\s+([^\n]+))?", re.I)
    STR_CONST_RGX = re.compile(r"^w?string\s+([^\s=#]+)\s*=")
    lines, pkg = [], typename.rsplit("/", 1)[0]
    subtypedefs = dict(extradefs, **parse_definition_subtypes(msgdef))
    extradefs = tuple(subtypedefs.items())

    # First pass: write constants
    for line in msgdef.splitlines():
        if set(line) == set("="):  # Subtype separator
            break  # for line
        # String constants cannot have line comments
        if "#" in line and not STR_CONST_RGX.match(line): line = line[:line.index("#")]
        match = FIELD_RGX.match(line)
        if match and match.group(3):
            lines.append("%s %s=%s" % (match.group(1), match.group(2), match.group(4).strip()))
    # Second pass: write fields and subtype hashes
    for line in msgdef.splitlines():
        if set(line) == set("="):  # Subtype separator
            break  # for line
        if "#" in line and not STR_CONST_RGX.match(line): line = line[:line.index("#")]
        match = FIELD_RGX.match(line)
        if match and not match.group(3):  # Not constant
            scalartype, namestr = scalar(match.group(1)), match.group(2)
            if scalartype in ROS_COMMON_TYPES:
                typestr = match.group(1)
                if match.group(5): namestr = (namestr + " " + match.group(6)).strip()
            else:
                subtype = scalartype if "/" in scalartype else "std_msgs/Header" \
                          if "Header" == scalartype else "%s/%s" % (pkg, scalartype)
                typestr = calculate_definition_hash(subtype, subtypedefs[subtype], extradefs)
            lines.append("%s %s" % (typestr, namestr))
    return hashlib.md5("\n".join(lines).encode()).hexdigest()


def canonical(typename, unbounded=False):
    """
    Returns "pkg/Type" for "pkg/subdir/Type", standardizes various ROS2 formats.

    Converts ROS2 DDS types like "octet" to "byte", and "sequence<uint8, 100>" to "uint8[100]".

    @param   unbounded  drop constraints like array bounds, and string bounds in ROS2,
                        e.g. returning "uint8[]" for "uint8[10]"
    """
    return realapi.canonical(typename, unbounded)


def create_publisher(topic, cls_or_typename, queue_size):
    """Returns a ROS publisher instance, with .get_num_connections() and .unregister()."""
    return realapi.create_publisher(topic, cls_or_typename, queue_size)


def create_subscriber(topic, cls_or_typename, handler, queue_size):
    """
    Returns a ROS subscriber instance.

    Supplemented with .unregister(), .get_message_class(), .get_message_definition(),
    .get_message_type_hash(), and .get_qoses().
    """
    return realapi.create_subscriber(topic, cls_or_typename, handler, queue_size)


def filter_fields(fieldmap, top=(), include=(), exclude=()):
    """
    Returns fieldmap filtered by include and exclude patterns.

    @param   fieldmap   {field name: field type name}
    @param   top        parent path as (rootattr, ..)
    @param   include    [((nested, path), re.Pattern())] to require in parent path
    @param   exclude    [((nested, path), re.Pattern())] to reject in parent path
    """
    NESTED_RGX = re.compile(".+/.+|" + "|".join("^%s$" % re.escape(x) for x in ROS_TIME_TYPES))
    result = type(fieldmap)() if include or exclude else fieldmap
    for k, v in fieldmap.items() if not result else ():
        trailstr = ".".join(map(str, top + (k, )))
        for is_exclude, patterns in enumerate((include, exclude)):
            # Nested fields need filtering on deeper level
            matches = not is_exclude and NESTED_RGX.match(v) \
                      or any(r.match(trailstr) for _, r in patterns)
            if patterns and (not matches if is_exclude else matches):
                result[k] = v
            elif patterns and is_exclude and matches:
                result.pop(k, None)
            if include and exclude and k not in result:  # Failing to include takes precedence
                break  # for is_exclude
    return result


def format_message_value(msg, name, value):
    """
    Returns a message attribute value as string.

    Result is at least 10 chars wide if message is a ROS time/duration
    (aligning seconds and nanoseconds).
    """
    return realapi.format_message_value(msg, name, value)


def get_message_class(typename):
    """Returns ROS message class, or None if unavailable."""
    return realapi.get_message_class(typename)


def get_message_definition(msg_or_type):
    """
    Returns ROS message type definition full text, including subtype definitions.

    Returns None if unknown type.
    """
    return realapi.get_message_definition(msg_or_type)


def get_message_type_hash(msg_or_type):
    """Returns ROS message type MD5 hash, or "" if unknown type."""
    return realapi.get_message_type_hash(msg_or_type)


def get_message_fields(val):
    """
    Returns OrderedDict({field name: field type name}) if ROS message, else {}.

    @param   val  ROS message class or instance
    """
    return realapi.get_message_fields(val)


def get_message_type(msg_or_cls):
    """Returns ROS message type name, like "std_msgs/Header"."""
    return realapi.get_message_type(msg_or_cls)


def get_message_value(msg, name, typename):
    """Returns object attribute value, with numeric arrays converted to lists."""
    return realapi.get_message_value(msg, name, typename)


def get_rostime(fallback=False):
    """
    Returns current ROS time, as rospy.Time or rclpy.time.Time.

    @param   fallback  use wall time if node not initialized
    """
    return realapi.get_rostime(fallback=fallback)


def get_ros_time_category(msg_or_type):
    """Returns "time" or "duration" for time/duration type or instance, else original argument."""
    cls = msg_or_type if inspect.isclass(msg_or_type) else \
          type(msg_or_type) if is_ros_message(msg_or_type) else None
    if cls is None:
        cls = next((x for x in ROS_TIME_CLASSES if get_message_type(x) == msg_or_type), None)
    if cls in ROS_TIME_CLASSES:
        return "duration" if "duration" in ROS_TIME_CLASSES[cls].lower() else "time"
    return msg_or_type


def get_topic_types():
    """
    Returns currently available ROS topics, as [(topicname, typename)].

    Omits topics that the current ROS node itself has published.
    """
    return realapi.get_topic_types()


def get_type_alias(typename):
    """
    Returns alias like "char" for ROS built-in type, if any; reverse of get_type_alias().

    In ROS1, byte and char are aliases for int8 and uint8; in ROS2 the reverse.
    """
    return next((k for k, v in ROS_ALIAS_TYPES.items() if v == typename), None)


def get_alias_type(typename):
    """
    Returns ROS built-in type for alias like "char", if any; reverse of get_alias_type().

    In ROS1, byte and char are aliases for int8 and uint8; in ROS2 the reverse.
    """
    return ROS_ALIAS_TYPES.get(typename)


def is_ros_message(val, ignore_time=False):
    """
    Returns whether value is a ROS message or special like ROS time/duration class or instance.

    @param   ignore_time  whether to ignore ROS time/duration types
    """
    return realapi.is_ros_message(val, ignore_time)


def is_ros_time(val):
    """Returns whether value is a ROS time/duration class or instance."""
    return realapi.is_ros_time(val)


def iter_message_fields(msg, messages_only=False, scalars=(), include=(), exclude=(), top=()):
    """
    Yields ((nested, path), value, typename) from ROS message.

    @param   messages_only  whether to yield only values that are ROS messages themselves
                            or lists of ROS messages, else will yield scalar and list values
    @param   scalars        sequence of ROS types to consider as scalars, like ("time", duration")
    @param   include        [((nested, path), re.Pattern())] to require in field path, if any
    @param   exclude        [((nested, path), re.Pattern())] to reject in field path, if any
    @param   top            internal recursion helper
    """
    fieldmap = realapi.get_message_fields(msg)
    if include or exclude: fieldmap = filter_fields(fieldmap, (), include, exclude)
    if not fieldmap: return
    if messages_only:
        for k, t in fieldmap.items():
            v, scalart = realapi.get_message_value(msg, k, t), realapi.scalar(t)
            is_sublist = isinstance(v, (list, tuple)) and scalart not in ROS_COMMON_TYPES
            is_forced_scalar = get_ros_time_category(scalart) in scalars
            if not is_forced_scalar and realapi.is_ros_message(v):
                for p2, v2, t2 in iter_message_fields(v, True, scalars, top=top + (k, )):
                    yield p2, v2, t2
            if is_forced_scalar or is_sublist or realapi.is_ros_message(v, ignore_time=True):
                yield top + (k, ), v, t
    else:
        for k, t in fieldmap.items():
            v = realapi.get_message_value(msg, k, t)
            is_forced_scalar = get_ros_time_category(realapi.scalar(t)) in scalars
            if not is_forced_scalar and realapi.is_ros_message(v):
                for p2, v2, t2 in iter_message_fields(v, False, scalars, top=top + (k, )):
                    yield p2, v2, t2
            else:
                yield top + (k, ), v, t


def make_full_typename(typename, category="msg"):
    """
    Returns "pkg/msg/Type" for "pkg/Type".

    @param   category  type category like "msg" or "srv"
    """
    INTER, FAMILY = "/%s/" % category, "rospy" if ROS1 else "rclpy"
    if INTER in typename or "/" not in typename or typename.startswith("%s/" % FAMILY):
        return typename
    return INTER.join(next((x[0], x[-1]) for x in [typename.split("/")]))


def make_bag_time(stamp, bag):
    """
    Returns value as ROS timestamp, conditionally as relative to bag start/end time.

    Stamp is interpreted as relative offset from bag start/end time
    if numeric string with sign prefix, or timedelta, or ROS duration.

    @param   stamp   converted to ROS timestamp if int/float/str/duration/datetime/timedelta/decimal
    @param   bag     an open bag to use for relative start/end time
    """
    shift = 0
    if is_ros_time(stamp):
        if "duration" != get_ros_time_category(stamp): return stamp
        shift = bag.get_start_time() if stamp >= 0 else bag.get_end_time()
    elif isinstance(stamp, datetime.datetime):
        stamp = time.mktime(stamp.timetuple()) + stamp.microsecond / 1E6
    elif isinstance(stamp, datetime.timedelta):
        stamp = stamp.total_seconds()
        shift = bag.get_start_time() if stamp >= 0 else bag.get_end_time()
    elif isinstance(stamp, (six.binary_type, six.text_type)):
        sign = stamp[0] in ("+", b"+") if six.text_type(stamp[0]) in "+-" else None
        shift = 0 if sign is None else bag.get_start_time() if sign else bag.get_end_time()
        stamp = float(stamp)
    return make_time(stamp + shift)


def make_live_time(stamp):
    """
    Returns value as ROS timestamp, conditionally as relative to system time.

    Stamp is interpreted as relative offset from system time
    if numeric string with sign prefix, or timedelta, or ROS duration.

    @param   stamp   converted to ROS timestamp if int/float/str/duration/datetime/timedelta/decimal
    """
    shift = 0
    if is_ros_time(stamp):
        if "duration" != get_ros_time_category(stamp): return stamp
        shift = time.time()
    elif isinstance(stamp, datetime.datetime):
        stamp = time.mktime(stamp.timetuple()) + stamp.microsecond / 1E6
    elif isinstance(stamp, datetime.timedelta):
        stamp, shift = stamp.total_seconds(), time.time()
    elif isinstance(stamp, (six.binary_type, six.text_type)):
        sign = stamp[0] in ("+", b"+") if six.text_type(stamp[0]) in "+-" else None
        stamp, shift = float(stamp), (0 if sign is None else time.time())
    return make_time(stamp + shift)


def make_duration(secs=0, nsecs=0):
    """Returns a ROS duration."""
    return realapi.make_duration(secs=secs, nsecs=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS time."""
    return realapi.make_time(secs=secs, nsecs=nsecs)


def make_message_hash(msg, include=(), exclude=()):
    """
    Returns hashcode for ROS message, as a hex digest.

    @param   include   message fields to include if not all, as [((nested, path), re.Pattern())]
    @param   exclude   message fields to exclude, as [((nested, path), re.Pattern())]
    """
    hasher = hashlib.md5()

    def walk_message(obj, top=()):
        fieldmap = get_message_fields(obj)
        fieldmap = filter_fields(fieldmap, include=include, exclude=exclude)
        for k, t in fieldmap.items():
            v, path = get_message_value(obj, k, t), top + (k, )
            if is_ros_message(v):
                walk_message(v, path)
            elif isinstance(v, (list, tuple)) and scalar(t) not in ROS_BUILTIN_TYPES:
                for x in v: walk_message(x, path)
            else:
                s = "%s=%s" % (path, v)
                hasher.update(s.encode("utf-8", errors="backslashreplace"))
        if not hasattr(obj, "__slots__"):
            s = "%s=%s" % (top, obj)
            hasher.update(s.encode("utf-8", errors="backslashreplace"))

    walk_message(msg)
    return hasher.hexdigest()


def message_to_dict(msg, replace=None):
    """
    Returns ROS message as nested Python dictionary.

    @param   replace  mapping of {value: replaced value},
                      e.g. {math.nan: None, math.inf: None}
    """
    result = {} if realapi.is_ros_message(msg) else msg
    for name, typename in realapi.get_message_fields(msg).items():
        v = realapi.get_message_value(msg, name, typename)
        if realapi.is_ros_time(v):
            v = dict(zip(realapi.get_message_fields(v), realapi.to_sec_nsec(v)))
        elif realapi.is_ros_message(v):
            v = message_to_dict(v)
        elif isinstance(v, (list, tuple)):
            if realapi.scalar(typename) not in ROS_BUILTIN_TYPES:
                v = [message_to_dict(x) for x in v]
            elif replace:
                v = [replace.get(x, x) for x in v]
        elif replace:
            v = replace.get(v, v)
        result[name] = v
    return result


def dict_to_message(dct, msg):
    """
    Returns given ROS message populated from Python dictionary.

    Raises TypeError on attribute value type mismatch.
    """
    for name, typename in realapi.get_message_fields(msg).items():
        if name not in dct:
            continue  # for
        v, msgv = dct[name], realapi.get_message_value(msg, name, typename)

        if realapi.is_ros_message(msgv):
            v = dict_to_message(v, msgv)
        elif isinstance(msgv, (list, tuple)):
            scalarname = realapi.scalar(typename)
            if scalarname in ROS_BUILTIN_TYPES:
                cls = ROS_BUILTIN_CTORS[scalarname]
                v = [x if isinstance(x, cls) else cls(x) for x in v]
            else:
                cls = realapi.get_message_class(scalarname)
                v = [dict_to_message(x, cls()) for x in v]
        else:
            v = type(msgv)(v)

        setattr(msg, name, v)
    return msg


@memoize
def parse_definition_fields(typename, typedef):
    """
    Returns field names and type names from a message definition text.

    Does not recurse into subtypes.

    @param   typename  ROS message type name, like "my_pkg/MyCls"
    @param   typedef   ROS message definition, like "Header header\nbool a\nMyCls2 b"
    @return            ordered {field name: type name},
                       like {"header": "std_msgs/Header", "a": "bool", "b": "my_pkg/MyCls2"}
    """
    result = collections.OrderedDict()  # {subtypename: subtypedef}

    FIELD_RGX = re.compile(r"^([a-z][^\s:]+)\s+([^\s=]+)(\s*=\s*([^\n]+))?(\s+([^\n]+))?", re.I)
    STR_CONST_RGX = re.compile(r"^w?string\s+([^\s=#]+)\s*=")
    pkg = typename.rsplit("/", 1)[0]
    for line in filter(bool, typedef.splitlines()):
        if set(line) == set("="):  # Subtype separator
            break  # for line
        if "#" in line and not STR_CONST_RGX.match(line): line = line[:line.index("#")]
        match = FIELD_RGX.match(line)
        if not match or match.group(3):  # Constant or not field
            continue  # for line

        name, typename, scalartype = match.group(2), match.group(1), scalar(match.group(1))
        if scalartype not in ROS_COMMON_TYPES:
            pkg2 = "" if "/" in scalartype else "std_msgs" if "Header" == scalartype else pkg
            typename = "%s/%s" % (pkg2, typename) if pkg2 else typename
        result[name] = typename
    return result


@memoize
def parse_definition_subtypes(typedef, nesting=False):
    """
    Returns subtype names and type definitions from a full message definition.

    @param   typedef    message type definition including all subtype definitions
    @param   nesting    whether to additionally return type nesting information as
                        {typename: [typename contained in parent]}
    @return             {"pkg/MsgType": "full definition for MsgType including subtypes"}
                        or ({typedefs}, {nesting}) if nesting
    """
    result  = collections.OrderedDict()      # {subtypename: subtypedef}
    nesteds = collections.defaultdict(list)  # {subtypename: [subtypename2, ]})

    # Parse individual subtype definitions from full definition
    curtype, curlines = "", []
    # Separator line, and definition header like 'MSG: std_msgs/MultiArrayLayout'
    rgx = re.compile(r"^((=+)|(MSG: (.+)))$")  # Group 2: separator, 4: new type
    for line in typedef.splitlines():
        m = rgx.match(line)
        if m and m.group(2) and curtype:  # Separator line between nested definitions
            result[curtype] = "\n".join(curlines)
            curtype, curlines = "", []
        elif m and m.group(4):  # Start of nested definition "MSG: pkg/MsgType"
            curtype, curlines = m.group(4), []
        elif not m and curtype:  # Definition content
            curlines.append(line)
    if curtype:
        result[curtype] = "\n".join(curlines)

    # "type name (= constvalue)?" or "type name (defaultvalue)?" (ROS2 format)
    FIELD_RGX = re.compile(r"^([a-z][^\s]+)\s+([^\s=]+)(\s*=\s*([^\n]+))?(\s+([^\n]+))?", re.I)
    # Concatenate nested subtype definitions to parent subtype definitions
    for subtype, subdef in list(result.items()):
        pkg, seen = subtype.rsplit("/", 1)[0], set()
        for line in subdef.splitlines():
            m = FIELD_RGX.match(line)
            if m and m.group(1):
                scalartype, fulltype = realapi.scalar(m.group(1)), None
                if scalartype not in ROS_COMMON_TYPES:
                    fulltype = scalartype if "/" in scalartype else "std_msgs/Header" \
                               if "Header" == scalartype else "%s/%s" % (pkg, scalartype)
                if fulltype in result and fulltype not in seen:
                    addendum = "%s\nMSG: %s\n%s" % ("=" * 80, fulltype, result[fulltype])
                    result[subtype] = result[subtype].rstrip() + ("\n\n%s\n" % addendum)
                    nesteds[subtype].append(fulltype)
                    seen.add(fulltype)
    return (result, nesteds) if nesting else result


def serialize_message(msg):
    """Returns ROS message as a serialized binary."""
    return realapi.serialize_message(msg)


def deserialize_message(msg, cls_or_typename):
    """Returns ROS message or service request/response instantiated from serialized binary."""
    return realapi.deserialize_message(msg, cls_or_typename)


def scalar(typename):
    """
    Returns scalar type from ROS message data type, like "uint8" from uint8-array.

    Returns type unchanged if an ordinary type. In ROS2, returns unbounded type,
    e.g. "string" from "string<=10[<=5]".
    """
    return realapi.scalar(typename)


def set_message_value(obj, name, value):
    """Sets message or object attribute value."""
    realapi.set_message_value(obj, name, value)


def time_message(val, to_message=True, clock_type=None):
    """
    Converts ROS2 time/duration between `rclpy` and `builtin_interfaces` objects.

    Returns input value as-is in ROS1.

    @param   val         ROS2 time/duration object from `rclpy` or `builtin_interfaces`
    @param   to_message  whether to convert from `rclpy` to `builtin_interfaces` or vice versa
    @param   clock_type  ClockType for converting to `rclpy.Time`, defaults to `ROS_TIME`
    @return              value converted to appropriate type, or original value if not convertible
    """
    if ROS1: return val
    return realapi.time_message(val, to_message, clock_type=clock_type)


def to_datetime(val):
    """Returns value as datetime.datetime if value is ROS time/duration, else value."""
    sec = realapi.to_sec(val)
    return datetime.datetime.fromtimestamp(sec) if sec is not val else val


def to_decimal(val):
    """Returns value as decimal.Decimal if value is ROS time/duration, else value."""
    if realapi.is_ros_time(val):
        return decimal.Decimal("%d.%09d" % realapi.to_sec_nsec(val))
    return val


def to_nsec(val):
    """Returns value in nanoseconds if value is ROS time/duration, else value."""
    return realapi.to_nsec(val)


def to_sec(val):
    """Returns value in seconds if value is ROS time/duration, else value."""
    return realapi.to_sec(val)


def to_sec_nsec(val):
    """Returns value as (seconds, nanoseconds) if value is ROS time/duration, else value."""
    return realapi.to_sec_nsec(val)


def to_time(val):
    """Returns value as ROS time if convertible (int/float/duration/datetime/decimal), else value."""
    return realapi.to_time(val)


__all___ = [
    "BAG_EXTENSIONS", "NODE_NAME", "ROS_ALIAS_TYPES", "ROS_BUILTIN_CTORS", "ROS_BUILTIN_TYPES",
    "ROS_COMMON_TYPES", "ROS_FAMILY", "ROS_NUMERIC_TYPES", "ROS_STRING_TYPES", "ROS_TIME_CLASSES",
    "ROS_TIME_TYPES", "SKIP_EXTENSIONS", "Bag", "BaseBag", "TypeMeta",
    "calculate_definition_hash", "canonical", "create_publisher", "create_subscriber",
    "deserialize_message", "dict_to_message", "filter_fields", "format_message_value",
    "get_alias_type", "get_message_class", "get_message_definition", "get_message_fields",
    "get_message_type", "get_message_type_hash", "get_message_value", "get_ros_time_category",
    "get_rostime", "get_topic_types", "get_type_alias", "init_node", "is_ros_message",
    "is_ros_time", "iter_message_fields", "make_bag_time", "make_duration", "make_live_time",
    "make_message_hash", "make_time", "message_to_dict", "parse_definition_fields",
    "parse_definition_subtypes", "scalar", "deserialize_message", "set_message_value",
    "shutdown_node", "time_message", "to_datetime", "to_decimal", "to_nsec", "to_sec",
    "to_sec_nsec", "to_time", "validate",
]

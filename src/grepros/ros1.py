# -*- coding: utf-8 -*-
"""
ROS1 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    03.07.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.ros1
import collections
import datetime
import decimal
import inspect
import logging
import io
import os
import shutil
import time

import genpy
import genpy.dynamic
import rosbag
import roslib
import rospy
import six

from . import api
from . import common
from . api import TypeMeta, calculate_definition_hash, parse_definition_subtypes
from . common import ConsolePrinter, memoize


## Bagfile extensions to seek
BAG_EXTENSIONS  = (".bag", ".bag.active")

## Bagfile extensions to skip
SKIP_EXTENSIONS = (".bag.orig.active", )

## ROS1 time/duration types
ROS_TIME_TYPES = ["time", "duration"]

## ROS1 time/duration types mapped to type names
ROS_TIME_CLASSES = {rospy.Time: "time", rospy.Duration: "duration"}

## Mapping between type aliases and real types, like {"byte": "int8"}
ROS_ALIAS_TYPES = {"byte": "int8", "char": "uint8"}

## {(typename, typehash): message type class}
TYPECLASSES = {}

## Seconds between checking whether ROS master is available.
SLEEP_INTERVAL = 0.5

## rospy.MasterProxy instance
master = None


class ROS1Bag(rosbag.Bag, api.BaseBag):
    """
    ROS1 bag reader and writer.

    Extends `rosbag.Bag` with more conveniences, smooths over the rosbag bug of ignoring
    topic and time filters in format v1.2, and smooths over the rosbag bug
    of yielding messages of wrong type, if message types in different topics
    have different packages but identical fields and hashes.

    Does **not** smooth over the rosbag bug of writing different types to one topic.

    rosbag does allow writing messages of different types to one topic,
    just like live ROS topics can have multiple message types published
    to one topic. And their serialized bytes will actually be in the bag,
    but rosbag will only register the first type for this topic (unless it is
    explicitly given another connection header with metadata on the other type).

    All messages yielded will be deserialized by rosbag as that first type,
    and whether reading will raise an exception or not depends on whether
    the other type has enough bytes to be deserialized as that first type.
    """

    # {(typename, typehash): message type class}
    __TYPES    = {}

    ## {(typename, typehash): type definition text}
    __TYPEDEFS = {}

    # {(typename, typehash): whether subtype definitions parsed}
    __PARSEDS = {}


    def __init__(self, *args, **kwargs):
        """
        @param   f           bag file path, or a stream object
        @param   mode        mode to open bag in, defaults to "r" (read mode)
        @param   reindex     if true and bag is unindexed, make a copy
                             of the file (unless unindexed format) and reindex original
        @param   progress    show progress bar with reindexing status
        @param   kwargs      additional keyword arguments for `rosbag.Bag`, like `compression`
        """
        self.__topics = {}    # {(topic, typename, typehash): message count}
        self.__iterer = None  # Generator from read_messages() for next()
        f,    args = (args[0] if args else kwargs.pop("f")), args[1:]
        mode, args = (args[0] if args else kwargs.pop("mode", "r")), args[1:]
        if mode not in self.MODES: raise ValueError("invalid mode %r" % mode)

        kwargs.setdefault("skip_index", True)
        reindex, progress = (kwargs.pop(k, False) for k in ("reindex", "progress"))
        getargspec = getattr(inspect, "getfullargspec", inspect.getargspec)
        for n in set(kwargs) - set(getargspec(rosbag.Bag).args): kwargs.pop(n)

        if common.is_stream(f):
            if not common.verify_io(f, mode):
                raise io.UnsupportedOperation({"r": "read", "w": "write", "a": "append"}[mode])
            super(ROS1Bag, self).__init__(f, mode, *args, **kwargs)
            self._populate_meta()
            return

        f = str(f)
        if "a" == mode and (not os.path.exists(f) or not os.path.getsize(f)):
            mode = "w"  # rosbag raises error on append if no file or empty file
            os.path.exists(f) and os.remove(f)

        try:
            super(ROS1Bag, self).__init__(f, mode, *args, **kwargs)
        except rosbag.ROSBagUnindexedException:
            if not reindex: raise
            Bag.reindex_file(f, progress, *args, **kwargs)
            super(ROS1Bag, self).__init__(f, mode, *args, **kwargs)
        self._populate_meta()


    def get_message_definition(self, msg_or_type):
        """Returns ROS1 message type definition full text from bag, including subtype definitions."""
        if is_ros_message(msg_or_type):
            return self.__TYPEDEFS.get((msg_or_type._type, msg_or_type._md5sum))
        typename = msg_or_type
        return next((d for (n, h), d in self.__TYPEDEFS.items() if n == typename), None)


    def get_message_class(self, typename, typehash=None):
        """
        Returns rospy message class for typename, or None if unknown type.

        Generates class dynamically if not already generated.

        @param   typehash  message type definition hash, if any
        """
        if (typename, typehash) not in self.__TYPES: self._ensure_typedef(typename, typehash)
        typehash = typehash or next((h for n, h in self.__TYPEDEFS if n == typename), None)
        typekey = (typename, typehash)
        if typekey not in self.__TYPES and typekey in self.__TYPEDEFS:
            for n, c in genpy.dynamic.generate_dynamic(typename, self.__TYPEDEFS[typekey]).items():
                self.__TYPES[(n, c._md5sum)] = c
        return self.__TYPES.get(typekey)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS1 message type MD5 hash, or None if unknown type."""
        if is_ros_message(msg_or_type): return msg_or_type._md5sum
        typename = msg_or_type
        typehash = next((h for n, h in self.__TYPEDEFS if n == typename), None)
        if not typehash:
            self._ensure_typedef(typename)
            typehash = next((h for n, h in self.__TYPEDEFS if n == typename), None)
        return typehash


    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp, or None if bag empty."""
        try: return super(ROS1Bag, self).get_start_time()
        except Exception: return None


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp, or None if bag empty."""
        try: return super(ROS1Bag, self).get_end_time()
        except Exception: return None


    def get_topic_info(self, *_, **__):
        """Returns topic and message type metainfo as {(topic, typename, typehash): count}."""
        return dict(self.__topics)


    def read_messages(self, topics=None, start_time=None, end_time=None,
                      raw=False, connection_filter=None, **__):
        """
        Yields messages from the bag, optionally filtered by topic, timestamp and connection details.

        @param   topics             list of topics or a single topic.
                                    If an empty list is given, all topics will be read.
        @param   start_time         earliest timestamp of messages to return,
                                    as ROS time or convertible (int/float/duration/datetime/decimal)
        @param   end_time           latest timestamp of messages to return,
                                    as ROS time or convertible (int/float/duration/datetime/decimal)
        @param   connection_filter  function to filter connections to include
        @param   raw                if true, then returned messages are tuples of
                                    (typename, bytes, typehash, typeclass)
                                    or (typename, bytes, typehash, position, typeclass),
                                    depending on file format
        @return                     BagMessage namedtuples of
                                    (topic, message, timestamp as rospy.Time)
        """
        if self.closed: raise ValueError("I/O operation on closed file.")
        if "w" == self._mode: raise io.UnsupportedOperation("read")

        hashtypes = {}
        for n, h in self.__TYPEDEFS: hashtypes.setdefault(h, []).append(n)
        read_topics = topics if isinstance(topics, (dict, list, set, tuple)) else \
                      [topics] if topics else None
        dupes = {t: (n, h) for t, n, h in self.__topics
                 if (read_topics is None or t in read_topics) and len(hashtypes.get(h, [])) > 1}

        # Workaround for rosbag.Bag ignoring topic and time filters in format v1.2
        if self.version != 102 or (not topics and start_time is None and end_time is None):
            in_range = lambda *_: True
        else: in_range = lambda t, s: ((not read_topics or t in read_topics) and
                                       (start_time is None or s >= start_time) and
                                       (end_time is None or s <= end_time))

        start_time, end_time = map(to_time, (start_time, end_time))
        kwargs = dict(topics=topics, start_time=start_time, end_time=end_time,
                      connection_filter=connection_filter, raw=raw)
        if not dupes:
            for topic, msg, stamp in super(ROS1Bag, self).read_messages(**kwargs):
                if in_range(topic, stamp):
                    TypeMeta.make(msg, topic, self)
                    yield self.BagMessage(topic, msg, stamp)
                if self.closed: break  # for topic
            return

        for topic, msg, stamp in super(ROS1Bag, self).read_messages(**kwargs):
            if not in_range(topic, stamp): continue  # for topic

            # Workaround for rosbag bug of using wrong type for identical type hashes
            if topic in dupes:
                typename, typehash = (msg[0], msg[2]) if raw else (msg._type, msg._md5sum)
                if dupes[topic] != (typename, typehash):
                    if raw:
                        msg = msg[:-1] + (self.get_message_class(typename, typehash), )
                    else:
                        msg = self._convert_message(msg, *dupes[topic])
            TypeMeta.make(msg, topic, self)
            yield self.BagMessage(topic, msg, stamp)
            if self.closed: break  # for topic


    def write(self, topic, msg, t=None, raw=False, connection_header=None, **__):
        """
        Writes a message to the bag.

        Populates connection header if topic already in bag but with a different message type.

        @param   topic              name of topic
        @param   msg                ROS1 message
        @param   t                  message timestamp if not using current wall time,
                                    as ROS time or convertible (int/float/duration/datetime/decimal)
        @param   raw                if true, `msg` is in raw format,
                                    (typename, bytes, typehash, typeclass)
        @param   connection_header  custom connection record for topic,
                                    as {"topic", "type", "md5sum", "message_definition"}
        """
        if self.closed: raise ValueError("I/O operation on closed file.")
        if "r" == self._mode: raise io.UnsupportedOperation("write")
        self._register_write(topic, msg, raw, connection_header)
        return super(ROS1Bag, self).write(topic, msg, to_time(t), raw, connection_header)


    def open(self):
        """Opens the bag file if not already open."""
        if not self._file:
            self._open(self.filename, self.mode, allow_unindexed=True)


    def close(self):
        """Closes the bag file."""
        if self._file:
            super(ROS1Bag, self).close()
            self._iterer = None
            self._clear_index()


    def __contains__(self, key):
        """Returns whether bag contains given topic."""
        return any(key == t for t, _, _ in self.__topics)


    def __next__(self):
        """Retrieves next message from bag as (topic, message, timestamp)."""
        if self.closed: raise ValueError("I/O operation on closed file.")
        if self.__iterer is None: self.__iterer = self.read_messages()
        return next(self.__iterer)


    @property
    def topics(self):
        """Returns the list of topics in bag, in alphabetic order."""
        return sorted((t for t, _, _ in self.__topics), key=str.lower)


    @property
    def closed(self):
        """Returns whether file is closed."""
        return not self._file


    def _convert_message(self, msg, typename2, typehash2=None):
        """Returns message converted to given type; fields must match."""
        msg2 = self.get_message_class(typename2, typehash2)()
        fields2 = get_message_fields(msg2)
        for fname, ftypename in get_message_fields(msg).items():
            v1 = v2 = getattr(msg, fname)
            if ftypename != fields2.get(fname, ftypename):
                v2 = self._convert_message(v1, fields2[fname])
            setattr(msg2, fname, v2)
        return msg2


    def _populate_meta(self):
        """Populates topics and message type definitions and hashes."""
        result = collections.Counter()  # {(topic, typename, typehash): count}
        counts = collections.Counter()  # {connection ID: count}
        for c in self._chunks:
            for c_id, count in c.connection_counts.items():
                counts[c_id] += count
        for c in self._connections.values():
            result[(c.topic, c.datatype, c.md5sum)] += counts[c.id]
            self.__TYPEDEFS[(c.datatype, c.md5sum)] = c.msg_def
        self.__topics = dict(result)


    def _register_write(self, topic, msg, raw=False, connection_header=None):
        """Registers message in local metadata, writes connection header if new type for topic."""
        if raw: typename, typehash, typeclass = msg[0], msg[2], msg[3]
        else:   typename, typehash, typeclass = msg._type, msg._md5sum, type(msg)
        topickey, typekey = (topic, typename, typehash), (typename, typehash)

        if topickey not in self.__topics \
        and any(topic == t and typekey != (n, h) for t, n, h in self.__topics):
            # Write connection header if topic already present but with different type
            if not connection_header:
                connection_header = {"topic": topic, "type": typename, "md5sum": typehash,
                                     "message_definition": typeclass._full_text}
            conn_id, bagself = len(self._connections), super(ROS1Bag, self)
            connection_info = rosbag.bag._ConnectionInfo(conn_id, topic, connection_header)
            bagself._write_connection_record(connection_info, encrypt=False)
            bagself._connections[conn_id] = bagself._topic_connections[topic] = connection_info

        self.__TYPES.setdefault(typekey, typeclass)
        self.__TYPEDEFS.setdefault(typekey, typeclass._full_text)
        self.__topics[topickey] = self.__topics.get(topickey, 0) + 1


    def _ensure_typedef(self, typename, typehash=None):
        """Parses subtype definition from any full definition where available, if not loaded."""
        typehash = typehash or next((h for n, h in self.__TYPEDEFS if n == typename), None)
        typekey = (typename, typehash)
        if typekey not in self.__TYPEDEFS:
            for (roottype, roothash), rootdef in list(self.__TYPEDEFS.items()):
                rootkey = (roottype, roothash)
                if self.__PARSEDS.get(rootkey): continue  # for (roottype, roothash)

                subdefs = tuple(parse_definition_subtypes(rootdef).items())  # ((typename, typedef), )
                subhashes = {n: calculate_definition_hash(n, d, subdefs) for n, d in subdefs}
                self.__TYPEDEFS.update(((n, subhashes[n]), d) for n, d in subdefs)
                self.__PARSEDS.update(((n, h), True) for n, h in subhashes.items())
                self.__PARSEDS[rootkey] = True
                if typekey in self.__TYPEDEFS:
                    break  # for (roottype, roothash)
            self.__TYPEDEFS.setdefault(typekey, "")


    @staticmethod
    def reindex_file(f, progress, *args, **kwargs):
        """
        Reindexes bag, making a backup copy in file directory.

        @param   progress  show progress bar for reindexing status
        """
        KWS = ["mode", "compression", "chunk_threshold",
               "allow_unindexed", "options", "skip_index"]
        kwargs.update(zip(KWS, args), allow_unindexed=True)
        copied, bar, f2 = False, None, None
        if progress:
            fmt = lambda s: common.format_bytes(s, strip=False)
            name, size = os.path.basename(f), os.path.getsize(f)
            aftertemplate = " Reindexing %s (%s): {afterword}" % (name, fmt(size))
            bar = common.ProgressBar(size, interval=0.1, pulse=True, aftertemplate=aftertemplate)

        ConsolePrinter.warn("Unindexed bag %s, reindexing.", f)
        bar and bar.update(0).start()  # Start progress pulse
        try:
            with rosbag.Bag(f, **kwargs) as inbag:
                inplace = (inbag.version > 102)

            f2 = "%s.orig%s" % os.path.splitext(f)
            shutil.copy(f, f2)
            copied = True

            inbag, outbag = None, None
            outkwargs = dict(kwargs, mode="a" if inplace else "w", allow_unindexed=True)
            try:
                inbag  = rosbag.Bag(f2, **dict(kwargs, mode="r"))
                outbag = rosbag.Bag(f, **outkwargs)
                Bag._run_reindex(inbag, outbag, bar)
                bar and bar.update(bar.max)
            except BaseException:  # Ensure steady state on KeyboardInterrupt/SystemExit
                inbag and inbag.close()
                outbag and outbag.close()
                copied and shutil.move(f2, f)
                raise
        finally: bar and bar.update(bar.value, flush=True).stop()
        inbag.close()
        outbag.close()


    @staticmethod
    def _run_reindex(inbag, outbag, bar=None):
        """Runs reindexing, showing optional progress bar."""
        update_bar = noop = lambda s: None
        indexbag, writebag = (inbag, outbag) if inbag.version == 102 else (outbag, None)
        if bar:
            fmt = lambda s: common.format_bytes(s, strip=False)
            update_bar = lambda s: (setattr(bar, "afterword", fmt(s)),
                                    setattr(bar, "pulse", False), bar.update(s).stop())
        # v102: build index from inbag, write all messages to outbag.
        # Other versions: re-build index in outbag file in-place.
        progress = update_bar if not writebag else noop  # Incremental progress during re-build
        for offset in indexbag.reindex():
            progress(offset)
        if not writebag:
            return

        progress = update_bar if bar else noop  # Incremental progress during re-write
        for (topic, msg, t, header) in indexbag.read_messages(return_connection_header=True):
            writebag.write(topic, msg, t, connection_header=header)
            progress(indexbag._file.tell())
Bag = ROS1Bag



def init_node(name):
    """
    Initializes a ROS1 node if not already initialized.

    Blocks until ROS master available.
    """
    if not validate(live=True):
        return

    global master
    if not master:
        master = rospy.client.get_master()
        available = None
        while not available:
            try: uri = master.getUri()[-1]
            except Exception:
                if available is None:
                    ConsolePrinter.log(logging.ERROR,
                                       "Unable to register with master. Will keep trying.")
                available = False
                time.sleep(SLEEP_INTERVAL)
            else:
                ConsolePrinter.debug("Connected to ROS master at %s.", uri)
                available = True
    try: rospy.get_rostime()
    except Exception:  # Init node only if not already inited
        rospy.init_node(name, anonymous=True, disable_signals=True)


def shutdown_node():
    """Shuts down live ROS1 node."""
    global master
    if master:
        master = None
        rospy.signal_shutdown("Close grepros")


def validate(live=False):
    """
    Returns whether ROS1 environment is set, prints error if not.

    @param   live  whether environment must support launching a ROS node
    """
    VARS = ["ROS_MASTER_URI", "ROS_ROOT", "ROS_VERSION"] if live else ["ROS_VERSION"]
    missing = [k for k in VARS if not os.getenv(k)]
    if missing:
        ConsolePrinter.error("ROS environment not sourced: missing %s.",
                             ", ".join(sorted(missing)))
    if "1" != os.getenv("ROS_VERSION", "1"):
        ConsolePrinter.error("ROS environment not supported: need ROS_VERSION=1.")
        missing = True
    return not missing


@memoize
def canonical(typename, unbounded=False):
    """
    Returns "pkg/Type" for "pkg/subdir/Type".

    @param  unbounded  drop array bounds, e.g. returning "uint8[]" for "uint8[10]"
    """
    if typename and typename.count("/") > 1:
        typename = "%s/%s" % tuple((x[0], x[-1]) for x in [typename.split("/")])[0]
    if unbounded and typename and "[" in typename:
        typename = typename[:typename.index("[")] + "[]"
    return typename


def create_publisher(topic, cls_or_typename, queue_size):
    """Returns a rospy.Publisher."""
    def pub_unregister():
        # ROS1 prints errors when closing a publisher with subscribers
        if not pub.get_num_connections(): super(rospy.Publisher, pub).unregister()

    cls = cls_or_typename
    if isinstance(cls, common.TEXT_TYPES): cls = get_message_class(cls)
    pub = rospy.Publisher(topic, cls, queue_size=queue_size)
    pub.unregister = pub_unregister
    return pub


def create_subscriber(topic, typename, handler, queue_size):
    """
    Returns a rospy.Subscriber.

    Local message packages are not required. Subscribes as AnyMsg,
    creates message class dynamically from connection info,
    and deserializes message before providing to handler.

    Supplemented with .get_message_class(), .get_message_definition(),
    .get_message_type_hash(), and .get_qoses().

    The supplementary .get_message_xyz() methods should only be invoked after at least one message
    has been received from the topic, as they get populated from live connection metadata.
    """
    def myhandler(msg):
        if msg._connection_header["type"] != typename:
            return
        typekey = (typename, msg._connection_header["md5sum"])
        if typename not in TYPECLASSES:
            typedef = msg._connection_header["message_definition"]
            for name, cls in genpy.dynamic.generate_dynamic(typename, typedef).items():
                TYPECLASSES.setdefault((name, cls._md5sum), cls)
        handler(TYPECLASSES[typekey]().deserialize(msg._buff))

    sub = rospy.Subscriber(topic, rospy.AnyMsg, myhandler, queue_size=queue_size)
    sub.get_message_class      = lambda: next(c for (n, h), c in TYPECLASSES.items()
                                              if n == typename)
    sub.get_message_definition = lambda: next(get_message_definition(c)
                                              for (n, h), c in TYPECLASSES.items() if n == typename)
    sub.get_message_type_hash  = lambda: next(h for n, h in TYPECLASSES if n == typename)
    sub.get_qoses              = lambda: None
    return sub


def format_message_value(msg, name, value):
    """
    Returns a message attribute value as string.

    Result is at least 10 chars wide if message is a ROS time/duration
    (aligning seconds and nanoseconds).
    """
    LENS = {"secs": 10, "nsecs": 9}
    v = "%s" % (value, )
    if not isinstance(msg, genpy.TVal) or name not in LENS:
        return v

    EXTRA = sum(v.count(x) * len(x) for x in (common.MatchMarkers.START, common.MatchMarkers.END))
    return ("%%%ds" % (LENS[name] + EXTRA)) % v  # Default %10s/%9s for secs/nsecs


@memoize
def get_message_class(typename):
    """Returns ROS1 message class."""
    if typename in ROS_TIME_TYPES:
        return next(k for k, v in ROS_TIME_CLASSES.items() if v == typename)
    if typename in ("genpy/Time", "rospy/Time"):
        return rospy.Time
    if typename in ("genpy/Duration", "rospy/Duration"):
        return rospy.Duration
    try: return roslib.message.get_message_class(typename)
    except Exception: return None


def get_message_definition(msg_or_type):
    """
    Returns ROS1 message type definition full text, including subtype definitions.

    Returns None if unknown type.
    """
    msg_or_cls = msg_or_type if is_ros_message(msg_or_type) else get_message_class(msg_or_type)
    return getattr(msg_or_cls, "_full_text", None)


def get_message_type_hash(msg_or_type):
    """Returns ROS message type MD5 hash, or "" if unknown type."""
    msg_or_cls = msg_or_type if is_ros_message(msg_or_type) else get_message_class(msg_or_type)
    return getattr(msg_or_cls, "_md5sum", "")


def get_message_fields(val):
    """
    Returns OrderedDict({field name: field type name}) if ROS1 message, else {}.

    @param   val  ROS1 message class or instance
    """
    names, types = (getattr(val, k, []) for k in ("__slots__", "_slot_types"))
    # Bug in genpy: class slot types defined as "int32", but everywhere else types use "uint32"
    if isinstance(val, genpy.TVal): names, types = genpy.TVal.__slots__, ["uint32", "uint32"]
    return collections.OrderedDict(zip(names, types))


def get_message_type(msg_or_cls):
    """Returns ROS1 message type name, like "std_msgs/Header"."""
    if is_ros_time(msg_or_cls):
        cls = msg_or_cls if inspect.isclass(msg_or_cls) else type(msg_or_cls)
        return "duration" if "duration" in cls.__name__.lower() else "time"
    return msg_or_cls._type


def get_message_value(msg, name, typename):
    """Returns object attribute value, with numeric arrays converted to lists."""
    v = getattr(msg, name)
    return list(v) if typename.startswith("uint8[") and isinstance(v, bytes) else v


def get_rostime(fallback=False):
    """
    Returns current ROS1 time, as rospy.Time.

    @param   fallback  use wall time if node not initialized
    """
    try: return rospy.get_rostime()
    except Exception:
        if fallback: return make_time(time.time())
        raise


def get_topic_types():
    """
    Returns currently available ROS1 topics, as [(topicname, typename)].

    Omits topics that the current ROS1 node itself has published.
    """
    result, myname = [], rospy.get_name()
    pubs, _, _ = master.getSystemState()[-1]
    mypubs = [t for t, nn in pubs if myname in nn and t not in ("/rosout", "/rosout_agg")]
    for topic, typename in master.getTopicTypes()[-1]:
        if topic not in mypubs:
            result.append((topic, typename))
    return result


def is_ros_message(val, ignore_time=False):
    """
    Returns whether value is a ROS1 message or special like ROS1 time/duration class or instance.

    @param  ignore_time  whether to ignore ROS1 time/duration types
    """
    isfunc = issubclass if inspect.isclass(val) else isinstance
    return isfunc(val, genpy.Message if ignore_time else (genpy.Message, genpy.TVal))


def is_ros_time(val):
    """Returns whether value is a ROS1 time/duration class or instance."""
    return issubclass(val, genpy.TVal) if inspect.isclass(val) else isinstance(val, genpy.TVal)


def make_duration(secs=0, nsecs=0):
    """Returns a ROS1 duration, as rospy.Duration."""
    return rospy.Duration(secs=secs, nsecs=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS1 time, as rospy.Time."""
    return rospy.Time(secs=secs, nsecs=nsecs)


def serialize_message(msg):
    """Returns ROS1 message as a serialized binary."""
    with TypeMeta.make(msg) as m:
        if m.data is not None: return m.data
    buf = io.BytesIO()
    msg.serialize(buf)
    return buf.getvalue()


def deserialize_message(raw, cls_or_typename):
    """Returns ROS1 message or service request/response instantiated from serialized binary."""
    cls = cls_or_typename
    if isinstance(cls, common.TEXT_TYPES): cls = get_message_class(cls)
    return cls().deserialize(raw)


@memoize
def scalar(typename):
    """
    Returns scalar type from ROS message data type, like "uint8" from "uint8[100]".

    Returns type unchanged if already a scalar.
    """
    return typename[:typename.index("[")] if typename and "[" in typename else typename


def set_message_value(obj, name, value):
    """Sets message or object attribute value."""
    setattr(obj, name, value)


def to_nsec(val):
    """Returns value in nanoseconds if value is ROS time/duration, else value."""
    return val.to_nsec() if isinstance(val, genpy.TVal) else val


def to_sec(val):
    """Returns value in seconds if value is ROS1 time/duration, else value."""
    return val.to_sec() if isinstance(val, genpy.TVal) else val


def to_sec_nsec(val):
    """Returns value as (seconds, nanoseconds) if value is ROS1 time/duration, else value."""
    return (val.secs, val.nsecs) if isinstance(val, genpy.TVal) else val


def to_time(val):
    """Returns value as ROS1 time if convertible (int/float/duration/datetime/decimal), else value."""
    result = val
    if isinstance(val, decimal.Decimal):
        result = rospy.Time(int(val), float(val % 1) * 10**9)
    elif isinstance(val, datetime.datetime):
        result = rospy.Time(int(val.timestamp()), 1000 * val.microsecond)
    elif isinstance(val, six.integer_types + (float, )):
        result = rospy.Time(val)
    elif isinstance(val, genpy.Duration):
        result = rospy.Time(val.secs, val.nsecs)
    return result


__all__ = [
    "BAG_EXTENSIONS", "ROS_ALIAS_TYPES", "ROS_TIME_CLASSES", "ROS_TIME_TYPES", "SKIP_EXTENSIONS",
    "SLEEP_INTERVAL", "TYPECLASSES", "Bag", "ROS1Bag", "master",
    "canonical", "create_publisher", "create_subscriber", "deserialize_message",
    "format_message_value", "get_message_class", "get_message_definition", "get_message_fields",
    "get_message_type", "get_message_type_hash", "get_message_value", "get_rostime",
    "get_topic_types", "init_node", "is_ros_message", "is_ros_time", "make_duration", "make_time",
    "scalar", "serialize_message", "set_message_value", "shutdown_node", "to_nsec", "to_sec",
    "to_sec_nsec", "to_time", "validate",
]

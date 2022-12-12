# -*- coding: utf-8 -*-
"""
ROS1 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    12.12.2022
------------------------------------------------------------------------------
"""
## @namespace grepros.ros1
import collections
import inspect
import io
import os
import shutil
import time

import genpy
import genpy.dynamic
import rosbag
import roslib
import rospy

from . import rosapi
from . common import ConsolePrinter, MatchMarkers, ProgressBar, format_bytes, memoize
from . rosapi import TypeMeta, calculate_definition_hash, parse_definition_subtypes


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


class ROS1Bag(rosbag.Bag, rosapi.Bag):
    """
    ROS1 bag reader and writer.

    Extends `rosbag.Bag` with more conveniences, and smooths over the rosbag bug
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
        @param   mode        mode to open bag in, defaults to "r" (read mode)
        @param   reindex     if true and bag is unindexed, make a copy
                             of the file (unless unindexed format) and reindex original
        @param   progress    show progress bar with reindexing status
        """
        kwargs.setdefault("skip_index", True)
        reindex, progress = (kwargs.pop(k, False) for k in ("reindex", "progress"))
        filename, args = (args[0] if args else kwargs.pop("f")), args[1:]
        mode,     args = (args[0] if args else kwargs.pop("mode", "r")), args[1:]
        for n in set(kwargs) - set(inspect.getargspec(rosbag.Bag).args): kwargs.pop(n)
        try:
            super(Bag, self).__init__(filename, mode, *args, **kwargs)
        except rosbag.ROSBagUnindexedException:
            if not reindex: raise
            Bag.reindex_file(filename, progress, *args, **kwargs)
            super(Bag, self).__init__(filename, mode, *args, **kwargs)

        self.__topics = {}  # {(topic, typename, typehash): message count}

        self.__populate_meta()


    def __iter__(self):
        """Iterates over all messages in the bag."""
        return self.read_messages()


    def __enter__(self):
        """Context manager entry."""
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit, closes bag."""
        self.close()


    def __len__(self):
        """Returns the number of messages in the bag."""
        return self.get_message_count()


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
        self.__ensure_typedef(typename, typehash)
        typehash = typehash or next((h for n, h in self.__TYPEDEFS if n == typename), None)
        typekey = (typename, typehash)
        if typekey not in self.__TYPES and typekey in self.__TYPEDEFS:
            for n, c in genpy.dynamic.generate_dynamic(typename, self.__TYPEDEFS[typekey]).items():
                self.__TYPES[(n, c._md5sum)] = c
        return self.__TYPES.get(typekey) or get_message_class(typename)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS1 message type MD5 hash."""
        if is_ros_message(msg_or_type): return msg_or_type._md5sum
        typename = msg_or_type
        typehash = next((h for n, h in self.__TYPEDEFS if n == typename), None)
        if not typehash:
            self.__ensure_typedef(typename)
            typehash = next((h for n, h in self.__TYPEDEFS if n == typename), None)
        return typehash or get_message_type_hash(typename)


    def get_qoses(self, topic, typename):
        """Returns None."""
        return None


    def get_topic_info(self):
        """Returns topic and message type metainfo as {(topic, typename, typehash): count}."""
        return dict(self.__topics)


    def read_messages(self, topics=None, start_time=None, end_time=None, connection_filter=None, raw=False):
        """
        Yields messages from the bag, optionally filtered by topic, timestamp and connection details.

        @param   topics             list of topics or a single topic.
                                    If an empty list is given, all topics will be read.
        @param   start_time         earliest timestamp of messages to return
        @param   end_time           latest timestamp of messages to return
        @param   connection_filter  function to filter connections to include
        @param   raw                if True, then generate tuples of
                                    (datatype, (data, md5sum, position), pytype)
        @return                     generator of (topic, message, timestamp) tuples
        """
        hashtypes = {}
        for n, h in self.__TYPEDEFS: hashtypes.setdefault(h, []).append(n)
        read_topics = topics if isinstance(topics, list) else [topics] if topics else None
        dupes = {t: (n, h) for t, n, h in self.__topics
                 if (read_topics is None or t in read_topics) and len(hashtypes.get(h, [])) > 1}

        kwargs = dict(topics=topics, start_time=start_time, end_time=end_time,
                      connection_filter=connection_filter, raw=raw)
        if not dupes:
            for topic, msg, stamp in super(Bag, self).read_messages(**kwargs):
                yield topic, msg, stamp
            return

        for topic, msg, stamp in super(Bag, self).read_messages(**kwargs):
            # Workaround for rosbag bug of using wrong type for identical type hashes
            if topic in dupes:
                typename, typehash = (msg[0], msg[2]) if raw else (msg._type, msg._md5sum)
                if dupes[topic] != (typename, typehash):
                    if raw:
                        msg = msg[:-1] + (self.get_message_class(typename, typehash), )
                    else:
                        msg = self.__convert_message(msg, *dupes[topic])
            yield topic, msg, stamp


    def write(self, topic, msg, stamp, *_, **__):
        """Writes a message to the bag."""
        return super(Bag, self).write(topic, msg, stamp)


    def __convert_message(self, msg, typename2, typehash2=None):
        """Returns message converted to given type; fields must match."""
        msg2 = self.get_message_class(typename2, typehash2)()
        fields2 = get_message_fields(msg2)
        for fname, ftypename in get_message_fields(msg).items():
            v1 = v2 = getattr(msg, fname)
            if ftypename != fields2.get(fname, ftypename):
                v2 = self.__convert_message(v1, fields2[fname])
            setattr(msg2, fname, v2)
        return msg2


    def __populate_meta(self):
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


    def __ensure_typedef(self, typename, typehash=None):
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
            fmt = lambda s: format_bytes(s, strip=False)
            name, size = os.path.basename(f), os.path.getsize(f)
            aftertemplate = " Reindexing %s (%s): {afterword}" % (name, fmt(size))
            bar = ProgressBar(size, interval=0.1, pulse=True, aftertemplate=aftertemplate)

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
            fmt = lambda s: format_bytes(s, strip=False)
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
            try: master.getSystemState()
            except Exception:
                if available is None:
                    ConsolePrinter.warn("Unable to register with master. Will keep trying.")
                available = False
                time.sleep(SLEEP_INTERVAL)
            else: available = True
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


def create_publisher(topic, cls_or_typename, queue_size):
    """Returns a rospy.Publisher."""
    def pub_unregister():
        # ROS1 prints errors when closing a publisher with subscribers
        if not pub.get_num_connections(): super(rospy.Publisher, pub).unregister()

    cls = cls_or_typename
    if isinstance(cls, str): cls = get_message_class(cls)
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

    EXTRA = sum(v.count(x) * len(x) for x in (MatchMarkers.START, MatchMarkers.END))
    return ("%%%ds" % (LENS[name] + EXTRA)) % v  # Default %10s/%9s for secs/nsecs


@memoize
def get_message_class(typename):
    """Returns ROS1 message class."""
    return roslib.message.get_message_class(typename)


def get_message_data(msg):
    """Returns ROS1 message as a serialized binary."""
    with TypeMeta.make(msg) as m:
        if m.data is not None: return m.data
    buf = io.BytesIO()
    msg.serialize(buf)
    return buf.getvalue()


def get_message_definition(msg_or_type):
    """Returns ROS1 message type definition full text, including subtype definitions."""
    msg_or_cls = msg_or_type if is_ros_message(msg_or_type) else get_message_class(msg_or_type)
    return msg_or_cls._full_text


def get_message_type_hash(msg_or_type):
    """Returns ROS message type MD5 hash."""
    msg_or_cls = msg_or_type if is_ros_message(msg_or_type) else get_message_class(msg_or_type)
    return msg_or_cls._md5sum


def get_message_fields(val):
    """Returns OrderedDict({field name: field type name}) if ROS1 message, else {}."""
    names = getattr(val, "__slots__", [])
    if isinstance(val, tuple(ROS_TIME_CLASSES)):  # Empty __slots__
        names = genpy.TVal.__slots__
    return collections.OrderedDict(zip(names, getattr(val, "_slot_types", [])))


def get_message_type(msg_or_cls):
    """Returns ROS1 message type name, like "std_msgs/Header"."""
    return msg_or_cls._type


def get_message_value(msg, name, typename):
    """Returns object attribute value, with numeric arrays converted to lists."""
    v = getattr(msg, name)
    return list(v) if typename.startswith("uint8[") and isinstance(v, bytes) else v


def get_rostime():
    """Returns current ROS1 time, as rospy.Time."""
    return rospy.get_rostime()


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
    Returns whether value is a ROS1 message or special like ROS1 time/duration.

    @param  ignore_time  whether to ignore ROS1 time/duration types
    """
    return isinstance(val, genpy.Message if ignore_time else (genpy.Message, genpy.TVal))


def is_ros_time(val):
    """Returns whether value is a ROS1 time/duration."""
    return isinstance(val, genpy.TVal)


def make_duration(secs=0, nsecs=0):
    """Returns a ROS1 duration, as rospy.Duration."""
    return rospy.Duration(secs=secs, nsecs=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS1 time, as rospy.Time."""
    return rospy.Time(secs=secs, nsecs=nsecs)


@memoize
def scalar(typename):
    """
    Returns scalar type from ROS message data type, like "uint8" from "uint8[100]".

    Returns type unchanged if already a scalar.
    """
    return typename[:typename.index("[")] if "[" in typename else typename


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

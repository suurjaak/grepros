# -*- coding: utf-8 -*-
"""
ROS1 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    17.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.ros1
import collections
import io
import os
import re
import shutil
import time

try: import embag
except ImportError: embag = None
import genpy
import genpy.dynamic
import rosbag
import roslib
import rospy

from . common import ConsolePrinter, MatchMarkers, memoize
from . rosapi import ROS_BUILTIN_TYPES, calculate_definition_hash, parse_definition_subtypes


## Bagfile extensions to seek
BAG_EXTENSIONS  = (".bag", ".bag.active")

## Bagfile extensions to skip
SKIP_EXTENSIONS = (".bag.orig.active", )

## ROS1 time/duration types
ROS_TIME_TYPES = ["time", "duration"]

## ROS1 time/duration type names to type classes
ROS_TIME_TYPES_MAP = {"time": rospy.Time, "duration": rospy.Duration}

## {"pkg/msg/Msg": message type class}
TYPECLASSES = {}

## Seconds between checking whether ROS master is available.
SLEEP_INTERVAL = 0.5

## rospy.MasterProxy instance
master = None


class BagReader(rosbag.Bag):
    """Add message type getters to rosbag.Bag."""


    def __init__(self, *args, **kwargs):
        """
        @param   allow_unindexed  if true and bag is unindexed, makes a copy
                                  of the file (unless unindexed format) and reindexes original
        """
        reindex = kwargs.pop("allow_unindexed", False)
        try:
            super(BagReader, self).__init__(*args, **kwargs)
        except rosbag.ROSBagUnindexedException:
            if not reindex: raise
            filename, args = (args[0] if args else kwargs.pop("f")), args[1:]
            BagReader.reindex(filename, *args, **kwargs)
            super(BagReader, self).__init__(filename, *args, **kwargs)

        self.__topics     = {}  # {(topic, typename, typehash): message count}
        self.__types      = {}  # {(typename, typehash): message type class}
        self.__typedefs   = {}  # {(typename, typehash): type definition text}
        self.__parseds    = {}  # {(typename, typehash): whether subtype definitions parsed}

        self.__populate_meta()


    def get_message_definition(self, msg_or_type):
        """Returns ROS1 message type definition full text from bag, including subtype definitions."""
        if is_ros_message(msg_or_type):
            return self.__typedefs.get((msg_or_type._type, msg_or_type._md5sum))
        typename = msg_or_type
        return next((d for (n, h), d in self.__typedefs.items() if n == typename), None)


    def get_message_class(self, typename, typehash=None):
        """
        Returns rospy message class for typename, or None if unknown type.

        Generates class dynamically if not already generated.

        @param   typehash  message type definition hash, if any
        """
        typekey = (typename, typehash)
        self.__ensure_typedef(typename, typehash)
        if typekey not in self.__types and typekey in self.__typedefs:
            for n, c in genpy.dynamic.generate_dynamic(typename, self.__typedefs[typekey]).items():
                self.__types[(n, c._md5sum)] = c
        return self.__types.get(typekey) or get_message_class(typename)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS1 message type MD5 hash."""
        if is_ros_message(msg_or_type): return msg_or_type._md5sum
        typename = msg_or_type
        return next((h for n, h in self.__typedefs if n == typename), None) \
               or get_message_type_hash(typename)


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
        @return                     generator of BagMessage(topic, message, timestamp) namedtuples
                                    for each message in the bag file
        """
        hashtypes = {}
        for n, h in self.__typedefs: hashtypes.setdefault(h, []).append(n)
        read_topics = topics if isinstance(topics, list) else [topics] if topics else None
        dupes = {t: (n, h) for t, n, h in self.__topics
                 if (read_topics is None or t in read_topics) and len(hashtypes.get(n, [])) > 1}

        kwargs = dict(topics=topics, start_time=start_time, end_time=end_time,
                      connection_filter=connection_filter, raw=raw)
        if not dupes:
            for topic, msg, stamp in super(BagReader, self).read_messages(**kwargs):
                yield topic, msg, stamp
            return

        for topic, msg, stamp in super(BagReader, self).read_messages(**kwargs):
            # Workaround for rosbag bug of using wrong type for identical type hashes
            if dupes.get(topic, msg._type) != msg._type:
                msg = self.__convert_message(msg, *dupes[topic])
            yield topic, msg, stamp


    def __convert_message(self, msg, typename2, typehash2=None):
        """Returns message converted to given type; fields must match."""
        msg2 = self.get_message_class(typename2, typehash2)
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
            self.__typedefs[(c.datatype, c.md5sum)] = c.msg_def
        self.__topics = dict(result)


    def __ensure_typedef(self, typename, typehash):
        """Parses subtype definition from any full definition where available, if not loaded."""
        typekey = (typename, typehash)
        if typekey not in self.__typedefs:
            for (roottype, roothash), rootdef in list(self.__typedefs.items()):
                rootkey = (roottype, roothash)
                if self.__parseds.get(rootkey): continue  # for (roottype, roothash)

                subdefs = tuple(parse_definition_subtypes(rootdef))  # ((typename, typedef), )
                subhashes = {n: calculate_definition_hash(n, d, subdefs) for n, d in subdefs}
                self.__typedefs.update(((n, subhashes[n]), d) for n, d in subdefs)
                self._parseds[rootkey] = True
                if typekey in subdefs:
                    break  # for (roottype, roothash)
            self.__typedefs.setdefault(typekey, "")


    @staticmethod
    def reindex(f, *args, **kwargs):
        """Reindexes bag (making a backup copy if indexed format)."""
        KWS = ["mode", "compression", "chunk_threshold",
               "allow_unindexed", "options", "skip_index"]
        kwargs.update(zip(args, KWS), allow_unindexed=True)

        ConsolePrinter.warn("Unindexed bag %s, reindexing.", f)
        inbag = rosbag.Bag(f, **kwargs)
        do_copy = (inbag.version > 102)
        inbag.close()

        f2 = "%s.orig%s" % os.path.splitext(f) if do_copy else f
        do_copy and shutil.copy(f, f2)

        inbag, outbag = None, None
        outkwargs = dict(kwargs, mode="a" if do_copy else "w", allow_unindexed=do_copy)
        try:
            inbag  = rosbag.Bag(f2, **dict(kwargs, mode="r"))
            outbag = rosbag.Bag(f, **outkwargs)
            rosbag.rosbag_main.reindex_op(inbag, outbag, quiet=True)
        except BaseException:
            inbag and inbag.close()
            outbag and outbag.close()
            do_copy and os.remove(f2)
            raise
        inbag.close()
        outbag.close()



class EmbagReader(object):
    """embag reader interface, partially mimicking rosbag.Bag."""


    def __init__(self, filename):
        self._topics   = {}  # {(topic, typename, typehash): message count}
        self._types    = {}  # {(typename, typehash): message type class}
        self._hashdefs = {}  # {(topic, typehash): typename}
        self._typedefs = {}  # {(typename, typehash): type definition text}
        self._view = embag.View(filename)

        ## Bagfile path
        self.filename = filename

        self._populate_meta()


    def get_message_count(self):
        """Returns the number of messages in the bag."""
        return sum(self._topics.values())


    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp."""
        return self._view.getStartTime().to_sec()


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp."""
        return self._view.getEndTime().to_sec()


    def get_message_class(self, typename, typehash=None):
        """
        Returns rospy message class for typename, or None if unknown type.

        Generates class dynamically if not already generated.

        @param   typehash  message type definition hash, if any
        """
        typekey = (typename, typehash)
        if typekey not in self._types and typekey in self._typedefs:
            for n, c in genpy.dynamic.generate_dynamic(typename, self._typedefs[typekey]).items():
                self._types[(n, c._md5sum)] = c
        return self._types.get(typekey) or get_message_class(typename)


    def get_message_definition(self, msg_or_type):
        """Returns ROS1 message type definition full text from bag, including subtype definitions."""
        if is_ros_message(msg_or_type):
            return self._typedefs.get((msg_or_type._type, msg_or_type._md5sum))
        typename = msg_or_type
        return next((d for (n, h), d in self._typedefs.items() if n == typename), None)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS1 message type MD5 hash."""
        if is_ros_message(msg_or_type): return msg_or_type._md5sum
        typename = msg_or_type
        return next((h for n, h in self._typedefs if n == typename), None) \
               or get_message_type_hash(typename)


    def get_topic_info(self):
        """Returns topic and message type metainfo as {(topic, typename, typehash): count}."""
        return dict(self._topics)


    def read_messages(self, topics=None, start_time=None, end_time=None):
        """
        Yields messages from the bag, optionally filtered by topic and timestamp.

        @param   topics      list of topics or a single topic to filter by, if at all
        @param   start_time  earliest timestamp of message to return, as UNIX timestamp
        @param   end_time    latest timestamp of message to return, as UNIX timestamp
        @return              (topic, msg, rclpy.time.Time)
        """
        topics = topics if isinstance(topics, list) else [topics] if topics else []
        for m in self._view.getMessages(topics) if topics else self._view.getMessages():
            if start_time is not None and start_time > m.timestamp.to_sec():
                continue  # for m
            if end_time is not None and end_time < m.timestamp.to_sec():
                continue  # for m

            typename = self._hashdefs[(m.topic, m.md5)]
            msg = self._populate_message(self.get_message_class(typename, m.md5)(), m.data())
            yield m.topic, msg, rospy.Time(m.timestamp.secs, m.timestamp.nsecs)


    def close(self):
        """Closes the bag file."""
        if self._view:
            del self._view
            self._view = None


    @property
    def size(self):
        """Returns current file size."""
        return os.path.getsize(self.filename) if os.path.isfile(self.filename) else None


    def _populate_meta(self):
        """Populates bag metainfo."""
        connections = self._view.connectionsByTopic()
        for topic in self._view.topics():
            for conn in connections.get(topic, ()):
                topickey, typekey = (topic, conn.type, conn.md5sum), (conn.type, conn.md5sum)
                self._topics.setdefault(topickey, 0)
                self._topics[topickey] += conn.message_count
                self._hashdefs[(topic, conn.md5sum)] = conn.type
                self._typedefs[typekey] = conn.message_definition
                subtypedefs = tuple(parse_definition_subtypes(conn.message_definition).items())
                for n, d in subtypedefs:
                    h = calculate_definition_hash(n, d, subtypedefs)
                    self._typedefs.setdefault((n, h), d)


    def _populate_message(self, msg, embagval):
        """Returns the ROS1 message populated from a corresponding embag.RosValue."""
        for name, typename in get_message_fields(msg).items():
            v, scalarname = embagval.get(name), scalar(typename)
            if typename in ROS_BUILTIN_TYPES:      # Single built-in type
                msgv = getattr(embagval, name)
            elif scalarname in ROS_BUILTIN_TYPES:  # List of built-in types
                msgv = list(v)
            elif typename in ROS_TIME_TYPES:              # Single temporal type
                msgv = ROS_TIME_TYPES_MAP[typename](*self._parse_time(v))
            elif scalarname in ROS_TIME_TYPES:            # List of temporal types
                cls = ROS_TIME_TYPES_MAP[scalarname]
                msgv = [cls(*self._parse_time(x)) for x in v]
            elif typename == scalarname:                  # Single subtype
                msgv = self._populate_message(self.get_message_class(typename)(), v)
            else:                                         # List of subtypes
                cls = self.get_message_class(scalarname)
                msgv = [self._populate_message(cls(), x) for x in v]
            setattr(msg, name, msgv)
        return msg


    def _parse_time(self, embagval):
        """Returns (seconds, nanoseconds) from embag.RosValue representing time or duration."""
        m = re.search(r"(\d+)s (\d+)ns", str(embagval))
        return int(m.group(1)), int(m.group(2))



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
                    ConsolePrinter.error("Unable to register with master. Will keep trying.")
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


def create_bag_reader(filename, reindex):
    """
    Returns EmbagReader if embag available else rosbag.Bag.

    Supplemented with get_message_class(), get_message_definition(),
    get_message_type_hash(), and get_topic_info().

    @param   reindex  reindex unindexed bag, making a backup copy if indexed format
    """
    if False and embag:  # @todo enable when embag fixes its memory leak
        return EmbagReader(filename)
    return BagReader(filename, skip_index=True, allow_unindexed=reindex)


def create_bag_writer(filename):
    """Returns a rosbag.Bag."""
    mode = "a" if os.path.isfile(filename) and os.path.getsize(filename) else "w"
    return rosbag.Bag(filename, mode)


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


def create_subscriber(topic, cls_or_typename, handler, queue_size):
    """Returns a rospy.Subscriber. Local message packages are not strictly required."""
    cls = cls_or_typename
    if isinstance(cls, str): cls = get_message_class(cls)
    if cls is None and isinstance(cls_or_typename, str):
        return create_anymsg_subscriber(topic, cls_or_typename, handler, queue_size)
    return rospy.Subscriber(topic, cls, handler, queue_size=queue_size)


def create_anymsg_subscriber(topic, typename, handler, queue_size):
    """
    Returns a rospy.Subscriber not requiring local message packages.

    Subscribes as AnyMsg, creates message class dynamically from connection info,
    and deserializes message before providing to handler.
    """
    def myhandler(msg):
        if msg._connection_header["type"] != typename:
            return
        if typename not in TYPECLASSES:
            typedef = msg._connection_header["message_definition"]
            for name, cls in genpy.dynamic.generate_dynamic(typename, typedef).items():
                TYPECLASSES.setdefault(name, cls)
        handler(TYPECLASSES[typename]().deserialize(msg._buff))

    return rospy.Subscriber(topic, rospy.AnyMsg, myhandler, queue_size=queue_size)


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
    if isinstance(val, (rospy.Time, rospy.Duration)):  # Empty __slots__
        names = genpy.TVal.__slots__
    return collections.OrderedDict(zip(names, getattr(val, "_slot_types", [])))


def get_message_type(msg):
    """Returns ROS1 message type name, like "std_msgs/Header"."""
    return msg._type


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

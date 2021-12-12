# -*- coding: utf-8 -*-
"""
ROS1 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    12.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.ros1
import collections
import io
import os
import re
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
        super(BagReader, self).__init__(*args, **kwargs)
        self.__types    = {}  # {typename: message type class}
        self.__hashdefs = {}  # {message type definition MD5 hash: typename}
        self.__typedefs = {}  # {typename: type definition text}


    def get_message_definition(self, msg_or_type):
        """Returns ROS1 message type definition full text from bag, including subtype definitions."""
        typename = get_message_type(msg_or_type) if is_ros_message(msg_or_type) else msg_or_type
        self._ensure_type(typename)
        return self.__typedefs.get(typename)


    def get_message_class(self, typename):
        """
        Returns rospy message class for typename, or None if unknown type.

        Generates class dynamically if not already generated.
        """
        self._ensure_type(typename)
        if typename not in self.__types and typename in self.__typedefs:
            for n, t in genpy.dynamic.generate_dynamic(typename, self.__typedefs[typename]).items():
                self.__types[n] = t
        return self.__types.get(typename) or get_message_class(typename)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS1 message type MD5 hash."""
        typename = get_message_type(msg_or_type) if is_ros_message(msg_or_type) else msg_or_type
        self._ensure_type(typename)
        md5 = next((h for h, t in self.__hashdefs.items() if t == typename), None)
        return md5 or get_message_type_hash(msg_or_type)


    def _ensure_type(self, typename):
        """Loads type information if not loaded."""
        if not self.__typedefs:
            for c in self._connections.values():
                self.__typedefs[c.datatype] = c.msg_def
                self.__hashdefs[c.md5sum] = c.datatype
        if typename not in self.__typedefs:
            for typedef in list(self.__typedefs.values()):
                subdefs = tuple(parse_definition_subtypes(typedef).items())
                self.__typedefs.update(subdefs)
                for subtype, subdef in subdefs:
                    md5 = calculate_definition_hash(subtype, subdef, subdefs)
                    self.__hashdefs[md5] = subtype
                if typename in subdefs:
                    break  # for typedef
            self.__typedefs.setdefault(typename, "")



class EmbagReader(object):
    """embag reader interface, partially mimicking rosbag.Bag."""


    def __init__(self, filename):
        self._topics   = {}  # {topic: [typename, ]}
        self._counts   = {}  # {(topic, typename): message count}
        self._types    = {}  # {typename: message type class}
        self._hashdefs = {}  # {message type definition MD5 hash: typename}
        self._typedefs = {}  # {typename: type definition text}
        self._view = embag.View(filename)

        ## Bagfile path
        self.filename = filename

        self._configure()


    def get_message_count(self):
        """Returns the number of messages in the bag."""
        return sum(self._counts.values())


    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp."""
        return self._view.getStartTime().to_sec()


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp."""
        return self._view.getEndTime().to_sec()


    def get_type_and_topic_info(self):
        """Returns namedtuple(topics={topicname: namedtuple(msg_type, message_count)})."""
        TopicTuple  = collections.namedtuple("TopicTuple",          ["msg_type", "message_count"])
        ResultTuple = collections.namedtuple("TypesAndTopicsTuple", ["topics"])
        topicmap = {}
        for topic, typenames in self._topics.items():
            for typename in typenames:
                topickey = (topic, typename)
                topicmap[topic] = TopicTuple(typename, self._counts.get(topickey, 0))
        return ResultTuple(topics=topicmap)


    def get_message_class(self, typename):
        """
        Returns rospy message class for typename, or None if unknown type.

        Generates class dynamically if not already generated.
        """
        if typename not in self._types and typename in self._typedefs:
            for n, t in genpy.dynamic.generate_dynamic(typename, self._typedefs[typename]).items():
                self._types[n] = t
        return self._types.get(typename) or get_message_class(typename)


    def get_message_definition(self, msg_or_type):
        """Returns ROS1 message type definition full text, including subtype definitions."""
        typename = get_message_type(msg_or_type) if is_ros_message(msg_or_type) else msg_or_type
        return self._typedefs.get(typename) or get_message_definition(typename)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS1 message type MD5 hash."""
        typename = get_message_type(msg_or_type) if is_ros_message(msg_or_type) else msg_or_type
        md5 = next((h for h, t in self._hashdefs.items() if t == typename), None)
        return md5 or get_message_type_hash(msg_or_type)


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

            typename = self._hashdefs[m.md5]
            msg = self._populate_message(self.get_message_class(typename)(), m.data())
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


    def _configure(self):
        """Populates bag metainfo."""
        connections = self._view.connectionsByTopic()
        for topic in self._view.topics():
            for conn in connections.get(topic, ()):
                typename, topickey = conn.type, (topic, conn.type)
                self._topics.setdefault(topic, []).append(typename)
                self._counts[topickey] = self._counts.get(topickey, 0) + conn.message_count
                self._hashdefs[conn.md5sum] = typename
                self._typedefs[typename] = conn.message_definition
                for n, d in parse_definition_subtypes(conn.message_definition).items():
                    self._typedefs.setdefault(n, d)


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


def create_bag_reader(filename):
    """
    Returns EmbagReader if embag available else rosbag.Bag.

    Supplemented with get_message_class(), get_message_definition(), and get_message_type_hash().
    """
    if False and embag:  # @todo enable when embag fixes its memory leak
        return EmbagReader(filename)
    return BagReader(filename, skip_index=True)


def create_bag_writer(filename):
    """Returns a rosbag.Bag."""
    mode = "a" if os.path.isfile(filename) and os.path.getsize(filename) else "w"
    return rosbag.Bag(filename, mode)


def create_publisher(topic, cls_or_typename, queue_size):
    """Returns a rospy.Publisher."""
    cls = cls_or_typename
    if isinstance(cls, str): cls = get_message_class(cls)
    return rospy.Publisher(topic, cls, queue_size=queue_size)


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

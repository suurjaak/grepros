# -*- coding: utf-8 -*-
"""
ROS1 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    14.11.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.ros1
import collections
import io
import os
import time

import genpy
import rosbag
import roslib
import rospy

from . common import ConsolePrinter, MatchMarkers
from . rosapi import parse_definition_subtypes


## Bagfile extensions to seek
BAG_EXTENSIONS  = (".bag", ".bag.active")

## Bagfile extensions to skip
SKIP_EXTENSIONS = (".bag.orig.active", )

## ROS1 time/duration types
ROS_TIME_TYPES = ["time", "duration"]

## Seconds between checking whether ROS master is available.
SLEEP_INTERVAL = 0.5

## rospy.MasterProxy instance
master = None


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
        rospy.signal_shutdown()


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
    """Returns a rosbag.Bag, supplemented with get_message_definition()."""
    DEFINITIONS = {}
    def get_message_definition(msg_or_type):
        """Returns ROS1 message type definition full text from bag, including subtype definitions."""
        typename = get_message_type(msg_or_type) if is_ros_message(msg_or_type) else msg_or_type
        if not DEFINITIONS:
            for c in bag._connections.values():
                DEFINITIONS[c.datatype] = c.msg_def
        if typename not in DEFINITIONS:
            for typedef in DEFINITIONS.values():
                subdefs = parse_definition_subtypes(typedef)
                DEFINITIONS.update(subdefs)
                if typename in subdefs:
                    break  # for typedef
            DEFINITIONS.setdefault(typename, "")
        return DEFINITIONS.get(typename)

    bag = rosbag.Bag(filename, skip_index=True)
    bag.get_message_definition = get_message_definition
    return bag


def create_bag_writer(filename):
    """Returns a rosbag.Bag."""
    mode = "a" if os.path.isfile(filename) and os.path.getsize(filename) else "w"
    return rosbag.Bag(filename, mode)


def create_publisher(topic, cls, queue_size):
    """Returns a rospy.Publisher."""
    return rospy.Publisher(topic, cls, queue_size=queue_size)


def create_subscriber(topic, cls, handler, queue_size):
    """Returns a rospy.Subscriber."""
    return rospy.Subscriber(topic, cls, handler, queue_size=queue_size)


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


def make_duration(secs=0, nsecs=0):
    """Returns a ROS1 duration, as rospy.Duration."""
    return rospy.Duration(secs=secs, nsecs=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS1 time, as rospy.Time."""
    return rospy.Time(secs=secs, nsecs=nsecs)


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

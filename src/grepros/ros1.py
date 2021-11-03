# -*- coding: utf-8 -*-
"""
ROS1 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    03.11.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.ros1
import collections
import os
import time

import genpy
import rosbag
import roslib
import rospy

from . common import ConsolePrinter, MatchMarkers


## Bagfile extensions to seek
BAG_EXTENSIONS  = (".bag", ".bag.active")

## Bagfile extensions to skip
SKIP_EXTENSIONS = (".bag.orig.active", )

## Seconds between checking whether ROS master is available.
SLEEP_INTERVAL = 0.5

## rospy.MasterProxy instance
master = None


def init_node(name):
    """
    Initializes a ROS1 node if not already initialized.

    Blocks until ROS master available.
    """
    if not validate():
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


def validate():
    """Returns whether ROS1 environment is set, prints error if not."""
    missing = [k for k in ("ROS_DISTRO", "ROS_MASTER_URI", "ROS_ROOT",
               "ROS_PACKAGE_PATH", "ROS_VERSION") if not os.getenv(k)]
    if missing:
        ConsolePrinter.error("ROS environment not sourced: missing %s.",
                             ", ".join(sorted(missing)))
    return not missing


def create_bag_reader(filename):
    """Returns a rosbag.Bag."""
    return rosbag.Bag(filename, skip_index=True)


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
    v = "%s" % value
    if not isinstance(msg, genpy.TVal) or name not in LENS:
        return v

    EXTRA = sum(v.count(x) * len(x) for x in (MatchMarkers.START, MatchMarkers.END))
    return ("%%%ds" % (LENS[name] + EXTRA)) % v  # Default %10s/%9s for secs/nsecs


def get_message_class(typename):
    """Returns ROS1 message class."""
    return roslib.message.get_message_class(typename)


def get_message_fields(val):
    """Returns OrderedDict({field name: field type name}) if RO1 message, else {}."""
    names = getattr(val, "__slots__", [])
    if isinstance(val, (rospy.Time, rospy.Duration)):  # Empty __slots__
        names = genpy.TVal.__slots__
    return collections.OrderedDict(zip(names, getattr(val, "_slot_types", [])))


def get_rostime():
    """Returns current ROS time, as rospy.Time."""
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


def is_ros_message(val):
    """Returns whether value is a ROS message or a special like ROS time/duration."""
    return isinstance(val, (genpy.Message, genpy.TVal))


def make_duration(secs=0, nsecs=0):
    """Returns a ROS duration, as rospy.Duration."""
    return rospy.Duration(secs=secs, nsecs=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS time, as rospy.Time."""
    return rospy.Time(secs=secs, nsecs=nsecs)


def set_message_value(obj, name, value):
    """Sets message or object attribute value."""
    setattr(obj, name, value)


def to_sec(val):
    """Returns value in seconds if value is ROS1 time/duration, else value."""
    return val.to_sec() if isinstance(val, genpy.TVal) else val

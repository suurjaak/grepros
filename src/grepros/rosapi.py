# -*- coding: utf-8 -*-
## @namespace grepros.rosapi
"""
ROS interface, shared facade for ROS1 and ROS2.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    03.11.2021
------------------------------------------------------------------------------
"""
import os

#from . common import ConsolePrinter  # Imported later
#from . import ros1, ros2  # Imported conditionally


## Node base name for connecting to ROS (will be anonymized).
NODE_NAME = "grepros"

## Bagfile extensions to seek, including leading dot, populated after init
BAG_EXTENSIONS  = ()

## Bagfile extensions to skip, including leading dot, populated after init
SKIP_EXTENSIONS = ()

## All built-in numeric basic types in ROS
ROS_NUMERIC_TYPES = ["byte", "char", "int8", "int16", "int32", "int64", "uint8",
                     "uint16", "uint32", "uint64", "float32", "float64", "bool"]

## All built-in basic types in ROS
ROS_BUILTIN_TYPES = ROS_NUMERIC_TYPES + ["string"]

## Module grepros.ros1 or grepros.ros2
realapi = None


def init_node(name=None):
    """
    Initializes a ROS1 or ROS2 node if not already initialized.

    In ROS1, blocks until ROS master available.
    """
    validate() and realapi.init_node(name or NODE_NAME)


def validate():
    """Returns whether ROS environment is set, prints error if not."""
    global realapi, BAG_EXTENSIONS, SKIP_EXTENSIONS
    if realapi:
        return True

    success, version = False, os.getenv("ROS_VERSION")
    if "1" == version:
        from . import ros1
        realapi = ros1
        success = realapi.validate()
    elif "2" == version:
        from . import ros2
        realapi = ros2
        success = realapi.validate()
    elif not version:
        from . common import ConsolePrinter
        ConsolePrinter.error("ROS environment not sourced: missing ROS_VERSION.")
    else:
        from . common import ConsolePrinter
        ConsolePrinter.error("ROS environment not supported: unknown ROS_VERSION %r.", version)
    if success:
        BAG_EXTENSIONS, SKIP_EXTENSIONS = realapi.BAG_EXTENSIONS, realapi.SKIP_EXTENSIONS
    return success


def create_bag_reader(filename):
    """
    Returns an object for reading ROS bags.

    Result is rosbag.Bag in ROS1, and something with a conforming API in ROS2.
    """
    return realapi.create_bag_reader(filename)


def create_bag_writer(filename):
    """
    Returns an object for reading ROS bags.

    Result is rosbag.Bag in ROS1, and something with a conforming API in ROS2.
    """
    return realapi.create_bag_writer(filename)


def create_publisher(topic, cls, queue_size):
    """Returns a ROS publisher instance, with .publish() and .unregister()."""
    return create_publisher(topic, cls, queue_size)


def create_subscriber(topic, cls, handler, queue_size):
    """Returns a ROS subscriber instance, with .unregister()."""
    return realapi.create_subscriber(topic, cls, handler, queue_size)
    #.unregister()


def format_message_value(msg, name, value):
    """
    Returns a message attribute value as string.

    Result is at least 10 chars wide if message is a ROS time/duration
    (aligning seconds and nanoseconds).
    """
    return realapi.format_message_value(msg, name, value)


def get_message_class(typename):
    """Returns ROS message class."""
    return realapi.get_message_class(typename)


def get_message_fields(val):
    """Returns {field name: field type name} if ROS message, else {}."""
    return realapi.get_message_fields(val)


def get_rostime():
    """Returns current ROS time."""
    return realapi.get_rostime()


def get_topic_types():
    """
    Returns currently available ROS topics, as [(topicname, typename)].

    Omits topics that the current ROS node itself has published.
    """
    return realapi.get_topic_types()


def is_ros_message(val):
    """Returns whether value is a ROS message or a special like ROS time/duration."""
    return realapi.is_ros_message(val)


def make_duration(secs=0, nsecs=0):
    """Returns a ROS duration."""
    return realapi.make_duration(secs=secs, nsecs=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS time."""
    return realapi.make_time(secs=secs, nsecs=nsecs)


def to_sec(val):
    """Returns value in seconds if value is ROS time/duration, else value."""
    return realapi.to_sec(val)

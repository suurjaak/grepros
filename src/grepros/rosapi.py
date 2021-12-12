# -*- coding: utf-8 -*-
## @namespace grepros.rosapi
"""
ROS interface, shared facade for ROS1 and ROS2.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     01.11.2021
@modified    12.12.2021
------------------------------------------------------------------------------
"""
import collections
import datetime
import decimal
import hashlib
import os
import re
import time

from . common import ConsolePrinter, filter_fields, memoize
#from . import ros1, ros2  # Imported conditionally


## Node base name for connecting to ROS (will be anonymized).
NODE_NAME = "grepros"

## Bagfile extensions to seek, including leading dot, populated after init
BAG_EXTENSIONS  = ()

## Bagfile extensions to skip, including leading dot, populated after init
SKIP_EXTENSIONS = ()

## All built-in numeric types in ROS
ROS_NUMERIC_TYPES = ["byte", "char", "int8", "int16", "int32", "int64", "uint8",
                     "uint16", "uint32", "uint64", "float32", "float64", "bool"]

## All built-in string types in ROS
ROS_STRING_TYPES = ["string", "wstring"]

## All built-in basic types in ROS
ROS_BUILTIN_TYPES = ROS_NUMERIC_TYPES + ROS_STRING_TYPES

## All built-in basic types plus time types in ROS, populated after init
ROS_COMMON_TYPES = []

## Module grepros.ros1 or grepros.ros2
realapi = None


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
    Returns whether ROS environment is set, prints error if not.

    @param   live  whether environment must support launching a ROS node
    """
    global realapi, BAG_EXTENSIONS, SKIP_EXTENSIONS, ROS_COMMON_TYPES
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
        success = realapi.validate(live)
    elif not version:
        ConsolePrinter.error("ROS environment not set: missing ROS_VERSION.")
    else:
        ConsolePrinter.error("ROS environment not supported: unknown ROS_VERSION %r.", version)
    if success:
        BAG_EXTENSIONS, SKIP_EXTENSIONS = realapi.BAG_EXTENSIONS, realapi.SKIP_EXTENSIONS
        ROS_COMMON_TYPES = ROS_BUILTIN_TYPES + realapi.ROS_TIME_TYPES
    return success


@memoize
def calculate_definition_hash(typename, msgdef, extradefs=()):
    """
    Returns MD5 hash for message type definition.

    @param   extradefs  additional subtype definitions as ((typename, msgdef), )
    """
    # "type name (= constvalue)?" or "type name (defaultvalue)?" (ROS2 format)
    FIELD_RGX = re.compile(r"^([a-z][^\s]+)\s+([^\s=]+)(\s*=\s*([^\n]+))?(\s+([^\n]+))?", re.I)
    STR_CONST_RGX = re.compile("^w?string\s+([^\s=#]+)\s*=")
    lines, pkg = [], typename.rsplit("/", 1)[0]
    subtypedefs = dict(extradefs, **parse_definition_subtypes(msgdef))
    extradefs = tuple(subtypedefs.items())

    # First pass: write constants
    for line in msgdef.splitlines():
        if set(line) == set("="):  # Subtype separator
            continue  # for line
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


def create_bag_reader(filename):
    """
    Returns an object for reading ROS bags.

    Result is rosbag.Bag in ROS1, or an object with a partially conforming API 
    if using embag in ROS1, or if using ROS2.
    Supplemented with get_message_class(), get_message_definition() and get_message_type_hash().
    """
    return realapi.create_bag_reader(filename)


def create_bag_writer(filename):
    """
    Returns an object for writing ROS bags.

    Result is rosbag.Bag in ROS1, and an object with a partially conforming API in ROS2.
    """
    return realapi.create_bag_writer(filename)


def create_publisher(topic, cls_or_typename, queue_size):
    """Returns a ROS publisher instance, with .get_num_connections() and .unregister()."""
    return realapi.create_publisher(topic, cls_or_typename, queue_size)


def create_subscriber(topic, cls_or_typename, handler, queue_size):
    """Returns a ROS subscriber instance, with .unregister()."""
    return realapi.create_subscriber(topic, cls_or_typename, handler, queue_size)


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


def get_message_data(msg):
    """Returns ROS message as a serialized binary."""
    return realapi.get_message_data(msg)


def get_message_definition(msg_or_type):
    """Returns ROS message type definition full text, including subtype definitions."""
    return realapi.get_message_definition(msg_or_type)


def get_message_type_hash(msg_or_type):
    """Returns ROS message type MD5 hash."""
    return realapi.get_message_type_hash(msg_or_type)


def get_message_fields(val):
    """Returns OrderedDict({field name: field type name}) if ROS message, else {}."""
    return realapi.get_message_fields(val)


def get_message_type(msg):
    """Returns ROS message type name, like "std_msgs/Header"."""
    return realapi.get_message_type(msg)


def get_message_value(msg, name, typename):
    """Returns object attribute value, with numeric arrays converted to lists."""
    return realapi.get_message_value(msg, name, typename)


def get_rostime():
    """Returns current ROS time."""
    return realapi.get_rostime()


def get_topic_types():
    """
    Returns currently available ROS topics, as [(topicname, typename)].

    Omits topics that the current ROS node itself has published.
    """
    return realapi.get_topic_types()


def is_ros_message(val, ignore_time=False):
    """
    Returns whether value is a ROS message or special like ROS time/duration.

    @param  ignore_time  whether to ignore ROS time/duration types
    """
    return realapi.is_ros_message(val, ignore_time)


def is_ros_time(val):
    """Returns whether value is a ROS2 time/duration."""
    return realapi.is_ros_time(val)


def iter_message_fields(msg, messages_only=False, top=()):
    """
    Yields ((nested, path), value, typename) from ROS message.

    @param  messages_only  whether to yield only values that are ROS messages themselves
                           or lists of ROS messages, else will yield scalar and list values
    """
    fieldmap = realapi.get_message_fields(msg)
    if fieldmap is msg: return
    if messages_only:
        for k, t in fieldmap.items():
            v = realapi.get_message_value(msg, k, t)
            is_sublist = isinstance(v, (list, tuple)) and \
                         realapi.scalar(t) not in ROS_COMMON_TYPES
            if realapi.is_ros_message(v):
                for p2, v2, t2 in iter_message_fields(v, True, top=top + (k, )):
                    yield p2, v2, t2
            if realapi.is_ros_message(v, ignore_time=True) or is_sublist:
                yield top + (k, ), v, t
    else:
        for k, t in fieldmap.items():
            v = realapi.get_message_value(msg, k, t)
            if realapi.is_ros_message(v):
                for p2, v2, t2 in iter_message_fields(v, top=top + (k, )):
                    yield p2, v2, t2
            else:
                yield top + (k, ), v, t


def make_bag_time(stamp, bag):
    """
    Returns timestamp string or datetime instance as ROS time.

    Stamp interpreted as delta from bag start/end time if numeric string with sign prefix.
    """
    if isinstance(stamp, datetime.datetime):
        stamp, shift = time.mktime(stamp.timetuple()) + stamp.microsecond / 1E6, 0
    else:
        stamp, sign = float(stamp), ("+" == stamp[0] if stamp[0] in "+-" else None)
        shift = 0 if sign is None else bag.get_start_time() if sign else bag.get_end_time()
    return make_time(stamp + shift)


def make_live_time(stamp):
    """
    Returns timestamp string or datetime instance as ROS time.

    Stamp interpreted as delta from system time if numeric string with sign prefix.
    """
    if isinstance(stamp, datetime.datetime):
        stamp, shift = time.mktime(stamp.timetuple()) + stamp.microsecond / 1E6, 0
    else:
        stamp, sign = float(stamp), ("+" == stamp[0] if stamp[0] in "+-" else None)
        shift = 0 if sign is None else time.time()
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


def message_to_dict(msg):
    """Returns ROS message as nested Python dictionary."""
    result = {} if realapi.is_ros_message(msg) else msg
    for name, typename in realapi.get_message_fields(msg).items():
        v = realapi.get_message_value(msg, name, typename)
        if realapi.is_ros_time(v):
            v = dict(zip(["secs", "nsecs"], divmod(realapi.to_nsec(v), 10**9)))
        elif realapi.is_ros_message(v):
            v = message_to_dict(v)
        elif isinstance(v, (list, tuple)) and realapi.scalar(typename) not in ROS_BUILTIN_TYPES:
            v = [message_to_dict(x) for x in v]
        result[name] = v
    return result


@memoize
def parse_definition_subtypes(typedef):
    """
    Returns subtype names and type definitions from a full message definition.

    @return  {"pkg/MsgType": "full definition for MsgType including subtypes"}
    """
    result = collections.OrderedDict()  # {subtypename: subtypedef}
    curtype, curlines = "", []
    rgx = re.compile(r"^((=+)|(MSG: (.+)))$")  # Group 2: separator, 4: new type
    for line in typedef.splitlines():
        m = rgx.match(line)
        if m and m.group(2) and curtype:  # Separator line between nested definitions
            result[curtype] = "\n".join(curlines)
            curtype, curlines = "", []
        elif m and m.group(4):  # Start of nested definition "MSG: pkg/MsgType"
            curtype, curlines = m.group(4), []
        elif not m and curtype:  # Nested definition content
            curlines.append(line)
    if curtype:
        result[curtype] = "\n".join(curlines)

    # "type name (= constvalue)?" or "type name (defaultvalue)?" (ROS2 format)
    FIELD_RGX = re.compile(r"^([a-z][^\s]+)\s+([^\s=]+)(\s*=\s*([^\n]+))?(\s+([^\n]+))?", re.I)
    for subtype, subdef in list(result.items()):
        pkg = subtype.rsplit("/", 1)[0]
        for line in subdef.splitlines():
            m = FIELD_RGX.match(line)
            if m and m.group(1):
                scalartype, fulltype = realapi.scalar(m.group(1)), None
                if scalartype not in ROS_COMMON_TYPES:
                    fulltype = scalartype if "/" in scalartype else "std_msgs/Header" \
                               if "Header" == scalartype else "%s/%s" % (pkg, scalartype)
                if fulltype in result:
                    addendum = "%s\nMSG: %s\n%s" % ("=" * 80, fulltype, result[fulltype])
                    result[subtype] = result[subtype].rstrip() + ("\n\n%s\n" % addendum)
    return result


def scalar(typename):
    """
    Returns scalar type from ROS message data type, like "uint8" from uint8-array.

    Returns type unchanged if an ordinary type.
    """
    return realapi.scalar(typename)


def set_message_value(obj, name, value):
    """Sets message or object attribute value."""
    realapi.set_message_value(obj, name, value)


def to_datetime(val):
    """Returns value as datetime.datetime if value is ROS time/duration, else value."""
    sec = realapi.to_sec(val)
    return datetime.datetime.fromtimestamp(sec) if sec is not val else val


def to_decimal(val):
    """Returns value as decimal.Decimal if value is ROS time/duration, else value."""
    if is_ros_time(val):
        return decimal.Decimal("%d.%09d" % (divmod(to_nsec(val), 10**9)))
    return val


def to_nsec(val):
    """Returns value in nanoseconds if value is ROS time/duration, else value."""
    return realapi.to_nsec(val)


def to_sec(val):
    """Returns value in seconds if value is ROS time/duration, else value."""
    return realapi.to_sec(val)

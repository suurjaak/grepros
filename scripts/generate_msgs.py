#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generates random ROS messages and publishes them to live topics.

Usage:

    generate_msgs.py [TYPE [TYPE ...]]
                     [--no-type TYPE [TYPE ...]]
                     [--max-count NUM]
                     [--max-per-topic NUM]
                     [--max-topics NUM]
                     [--topics-per-type NUM]
                     [--interval SECONDS]
                     [--publish-prefix PREFIX]
                     [--publish-suffix SUFFIX]
                     [--no-latch]
                     [--verbose]
                     [--option KEY=VALUE [KEY=VALUE ...]]

Topic names default to "/generate_msgs/type", like "/generate_msgs/std_msgs/Bool".

Supports both ROS1 and ROS2, version detected from environment.

Stand-alone script, requires ROS1 / ROS2 Python libraries.
ROS1 requires ROS master to be running.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     05.02.2022
@modified    09.02.2022
------------------------------------------------------------------------------
"""
import argparse
import collections
import json
import os
import random
import re
import signal
import string
import subprocess
import sys
import threading
import time
import traceback

rospy = rclpy = None
if os.getenv("ROS_VERSION") != "2":
    import genpy
    import roslib.message
    import rospy
else:
    import builtin_interfaces.msg
    import rclpy
    import rclpy.duration
    import rclpy.qos
    import rclpy.time
    import rosidl_runtime_py.utilities

## Configuration for argparse, as {description, epilog, args: [..], groups: {name: [..]}}
ARGUMENTS = {
    "description": "Generates random ROS messages, publishes to live topics.",
    "arguments": [
        dict(args=["TYPES"], nargs="*", metavar="TYPE",
             help="ROS message types to use if not all available,\n"
                  '(supports * wildcards, like "geometry_msgs/*")'),

        dict(args=["-v", "--verbose"],
             dest="VERBOSE", default=False, action="store_true",
             help="print each emitted message"),

        dict(args=["--no-type"],
             dest="SKIP_TYPES", metavar="TYPE", nargs="+", default=[], action="append",
             help="ROS message types to skip (supports * wildcards)"),

        dict(args=["-m", "--max-count"],
             dest="COUNT", metavar="NUM", default=0, type=int,
             help="maximum number of messages to emit"),

        dict(args=["--max-topics"],
             dest="MAX_TOPICS", metavar="NUM", default=0, type=int,
             help="maximum number of topics to emit"),

        dict(args=["--max-per-topic"],
             dest="MAX_PER_TOPIC", metavar="NUM", default=0, type=int,
             help="number of messages to emit in each topic"),

        dict(args=["--topics-per-type"],
             dest="TOPICS_PER_TYPE", metavar="NUM", default=1, type=int,
             help="number of topics to emit per message type (default 1)"),

        dict(args=["--interval"],
             dest="INTERVAL", metavar="SECONDS", default=0.5, type=float,
             help="live publish interval (default 0.5)"),

        dict(args=["--publish-prefix"],
             dest="PUBLISH_PREFIX", metavar="PREFIX", default="generate_msgs",
             help='prefix to prepend to topic name (default "generate_msgs")'),

        dict(args=["--publish-suffix"],
             dest="PUBLISH_SUFFIX", metavar="SUFFIX", default="",
             help='suffix to append to topic name'),

        dict(args=["--no-latch"],
             dest="LATCH", default=True, action="store_false",
             help="do not latch published topics"),

        dict(args=["--option"],  # Replaced with dictionary after parsing
             dest="OPTIONS", metavar="KEY=VALUE", nargs="+", default=[], action="append",
             help="options for generated message attributes, as\n"
                  "  arraylen=MIN,MAX   range / length of primitive arrays\n"
                  "            or NUM   (default 50,100)\n"
                  "  nestedlen=MIN,MAX  range / length of nested message lists\n"
                  "             or NUM  (default 1,2)\n"
                  "  strlen=MIN,MAX     range / length of strings (default 10,50)\n"
                  "          or NUM\n"
                  "  strchars=CHARS     characters to use in strings\n"
                  "                     (default all printables)\n"
                  "  NUMTYPE=MIN,MAX    value range / constant of numeric types\n"
                  "      or CONSTANT    like int8\n"),
    ],
}

## Name used for node
NAME = "generate_msgs"


class rosapi(object):
    """Generic interface for accessing ROS1 / ROS2 API."""

    ## rclpy.Node instance
    NODE = None

    ## All built-in numeric types in ROS
    ROS_NUMERIC_TYPES = ["byte", "char", "int8", "int16", "int32", "int64", "uint8",
                         "uint16", "uint32", "uint64", "float32", "float64", "bool"]

    ## All built-in string types in ROS
    ROS_STRING_TYPES = ["string", "wstring"]

    ## All built-in basic types in ROS
    ROS_BUILTIN_TYPES = ROS_NUMERIC_TYPES + ROS_STRING_TYPES

    ## ROS time/duration types mapped to type names
    ROS_TIME_CLASSES = {rospy.Time: "time", rospy.Duration: "duration",
                        genpy.Time: "time", genpy.Duration: "duration"} if rospy else \
                       {rclpy.time.Time:                 "builtin_interfaces/Time",
                        builtin_interfaces.msg.Time:     "builtin_interfaces/Time",
                        rclpy.duration.Duration:         "builtin_interfaces/Duration",
                        builtin_interfaces.msg.Duration: "builtin_interfaces/Duration"}

    ## Value ranges for ROS integer types, as {typename: (min, max)}
    ROS_INTEGER_RANGES = dict({
        "byte":   (-2** 7, 2** 7 - 1),
        "int8":   (-2** 7, 2** 7 - 1),
        "int16":  (-2**15, 2**15 - 1),
        "int32":  (-2**31, 2**31 - 1),
        "int64":  (-2**63, 2**63 - 1),
        "char":   (0,      2** 8 - 1),
        "uint8":  (0,      2** 8 - 1),
        "uint16": (0,      2**16 - 1),
        "uint32": (0,      2**31 - 1),
        "uint64": (0,      2**64 - 1),
    }, **{
        "byte":   (0,      2** 8 - 1),
        "char":   (-2** 7, 2** 7 - 1),
    } if rclpy else {})  # ROS2 *reverses* byte and char

    ## ROS2 Data Distribution Service types to ROS built-ins
    DDS_TYPES = {"boolean":             "bool",
                 "float":               "float32",
                 "double":              "float64",
                 "octet":               "byte",
                 "short":               "int16",
                 "unsigned short":      "uint16",
                 "long":                "int32",
                 "unsigned long":       "uint32",
                 "long long":           "int64",
                 "unsigned long long":  "uint64"}

    @classmethod
    def get_message_types(cls):
        """Returns a list of available message types, as ["pksg/Msg", ]."""
        cmd = "rosmsg list" if rospy else "ros2 interface list --only-msgs"
        output = subprocess.check_output(cmd, shell=True).decode()
        return sorted(cls.canonical(l.strip()) for l in output.splitlines()
                      if re.match(r"\w+/\w+", l.strip()))

    @classmethod
    def init(cls, launch=False):
        """Initializes ROS, creating and spinning node if specified."""
        if rospy and launch:
            rospy.init_node(NAME)
        if rospy and launch:
            spinner = threading.Thread(target=rospy.spin)
        if rclpy:
            rclpy.init()
        if rclpy and launch:
            cls.NODE = rclpy.create_node(NAME, enable_rosout=False, start_parameter_services=False)
            spinner = threading.Thread(target=rclpy.spin, args=(cls.NODE, ))
        if launch:
            spinner.daemon = True
            spinner.start()

    @classmethod
    def canonical(cls, typename):
        """
        Returns "pkg/Type" for "pkg/msg/Type", standardizes various ROS2 formats.

        Converts DDS types like "octet" to "byte", and "sequence<uint8, 100>" to "uint8[100]".
        """
        is_array, bound, dimension = False, "", ""

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

        if "[" in typename:  # "string<=5[<=10]" or "string<=5[10]"
            dimension = typename[typename.index("[") + 1:typename.index("]")]
            typename, is_array = typename[:typename.index("[")], True

        if "<=" in typename:  # "string<=5"
            typename, bound = typename.split("<=")

        if typename.count("/") > 1:
            typename = "%s/%s" % tuple((x[0], x[-1]) for x in [typename.split("/")])[0]

        suffix = ("<=%s" % bound if bound else "") + ("[%s]" % dimension if is_array else "")
        return cls.DDS_TYPES.get(typename, typename) + suffix

    @classmethod
    def create_publisher(cls, topic, typecls, latch=True, queue_size=10):
        """Returns ROS publisher instance."""
        if rospy:
            return rospy.Publisher(topic, typecls, latch=latch, queue_size=queue_size)

        qos = rclpy.qos.QoSProfile(depth=queue_size)
        if latch: qos.durability = rclpy.qos.DurabilityPolicy.TRANSIENT_LOCAL
        return cls.NODE.create_publisher(typecls, topic, qos)

    @classmethod
    def get_message_class(cls, typename):
        """Returns ROS message class."""
        if rospy:
            return roslib.message.get_message_class(typename)
        return rosidl_runtime_py.utilities.get_message(cls.make_full_typename(typename))

    @classmethod
    def get_message_fields(cls, val):
        """Returns OrderedDict({field name: field type name}) if ROS1 message, else {}."""
        if rospy:
            names = getattr(val, "__slots__", [])
            if isinstance(val, (rospy.Time, rospy.Duration)):  # Empty __slots__
                names = genpy.TVal.__slots__
            return collections.OrderedDict(zip(names, getattr(val, "_slot_types", [])))

        fields = {k: cls.canonical(v) for k, v in val.get_fields_and_field_types().items()}
        return collections.OrderedDict(fields)

    @classmethod
    def is_ros_message(cls, val):
        """Returns whether value is a ROS message or special like time/duration."""
        if rospy:
            return isinstance(val, (genpy.Message, genpy.TVal))
        return rosidl_runtime_py.utilities.is_message(val)

    @classmethod
    def is_ros_time(cls, val):
        """Returns whether value is a ROS time/duration."""
        return isinstance(val, tuple(cls.ROS_TIME_CLASSES))

    @classmethod
    def make_full_typename(cls, typename):
        """Returns "pkg/msg/Type" for "pkg/Type"."""
        if "/msg/" in typename or "/" not in typename:
            return typename
        return "%s/msg/%s" % tuple((x[0], x[-1]) for x in [typename.split("/")])[0]

    @classmethod
    def message_to_yaml(cls, msg):
        """Returns ROS message as YAML string."""
        if rospy:
            return str(msg)
        return rosidl_runtime_py.message_to_yaml(msg)

    @classmethod
    def scalar(cls, typename, bound=False):
        """
        Returns scalar type from ROS message data type, like "uint8" from "uint8[100]".

        Returns type unchanged if already a scalar.

        @param   bound  if True, does not strip string boundaries like "string<=10"
        """
        if "["  in typename: typename = typename[:typename.index("[")]   # int8[?]
        if "<=" in typename and not bound:
            typename = typename[:typename.index("<=")]  # string<=10
        return typename

    @classmethod
    def shutdown(cls):
        rclpy and rclpy.shutdown()



class generator(object):
    """Generates random ROS values and message attributes."""

    ## Attribute generating options
    OPTIONS = {
        "arraylen":   (50, 100),  # Length range for primitive arrays like uint8[]
        "nestedlen":  ( 1,   2),  # Length range for nested message lists like Point[]
        "strlen":     (10,  50),  # Length range for strings
        "strchars":   string.printable.strip() + " ",  # Characters used in string
    }

    @classmethod
    def make_random_value(cls, typename, options=None):
        """
        Returns random value for ROS builtin type.

        @param   options  {numtype like "int8": fixvalue or (minval, maxval),
                           "strlen": fixlen or (minlen, maxlen),
                           "strchars": str} if not using generator defaults
        """
        options = dict(cls.OPTIONS, **options or {})
        ranges = dict(rosapi.ROS_INTEGER_RANGES, **options or {})
        if rosapi.scalar(typename) in rosapi.ROS_STRING_TYPES:  # "string<=10" to "string"
            LEN = int(re.sub(r"\D", "", typename)) if re.search(r"\d", typename) else \
                  options["strlen"]
            if isinstance(LEN, (list, tuple)): LEN = random.randint(*LEN[:2])
            value = "".join(take_sample(options["strchars"], LEN)) if options["strchars"] else ""
        elif typename in ("bool", ):
            value = random.choice(ranges.get("bool", [True, False]))
        elif typename in ("float32", "float64"):
            value = random.random()
            if typename in ranges:
                a, b = (ranges[typename] * 2)[:2]
                value = a if a == b else value * (b - a)  # Constant or from range
        else:
            a, b = (ranges[typename] * 2)[:2]
            value = a if a == b else random.randint(a, b)  # Constant or from range
            if rclpy and typename in ("byte", ):  # ROS2 *requires* byte value to be bytes()
                value = bytes([value])
        return value

    @classmethod
    def populate(cls, msg, options=None):
        """
        Returns ROS message with fields populated with random content.

        @param   options  {"arraylen" or "nestedlen" or "strlen": fixlen or (minlen, maxlen),
                           numtype like "int8": fixvalue or (minval, maxval),
                           "strchars": str} if not using generator defaults
        """
        options = dict(cls.OPTIONS, **options or {})
        for name, typename in rosapi.get_message_fields(msg).items():
            scalartype = rosapi.scalar(typename)
            if typename in rosapi.ROS_BUILTIN_TYPES \
            or "[" not in typename and scalartype in rosapi.ROS_BUILTIN_TYPES:
                value = cls.make_random_value(typename, options)
            elif scalartype in rosapi.ROS_BUILTIN_TYPES:  # List of primitives
                LEN = options["arraylen"] if typename.endswith("[]") else \
                      int(re.sub(r"\D", "", typename[typename.index("["):]))
                if isinstance(LEN, (list, tuple)): LEN = random.randint(*LEN[:2])
                value = [cls.make_random_value(rosapi.scalar(typename, bound=True), options)
                         for _ in range(LEN)]
            elif typename == scalartype:  # Single nested message
                value = cls.populate(getattr(msg, name), options)
            else:  # List of nested messages
                LEN = options["nestedlen"] if typename.endswith("[]") else \
                      int(re.sub(r"\D", "", typename[typename.index("["):]))
                if isinstance(LEN, (list, tuple)): LEN = random.randint(*LEN[:2])
                msgcls = rosapi.get_message_class(scalartype)
                value = [cls.populate(msgcls(), options) for _ in range(LEN)]
            if rosapi.is_ros_time(msg):
                value = abs(value)
            setattr(msg, name, value)
        return msg



def make_argparser():
    """Returns a populated ArgumentParser instance."""
    kws = dict(description=ARGUMENTS["description"], formatter_class=argparse.RawTextHelpFormatter)
    argparser = argparse.ArgumentParser(**kws)
    for arg in map(dict, ARGUMENTS["arguments"]):
        argparser.add_argument(*arg.pop("args"), **arg)
    return argparser


def plural(word, items):
    """Returns "N words" or "1 word"."""
    count = len(items) if isinstance(items, (dict, list, set, tuple)) else items
    return "%s %s%s" % (count, word, "s" if count != 1 else "")


def take_sample(population, k):
    """Returns a list of k randomly chosen elements from population."""
    result, n, k = [], k, min(k, len(population))
    result = random.sample(population, k)
    while len(result) < n:
        result += random.sample(population, min(k, n - len(result)))
    return result


def wildcard_to_regex(text, end=True):
    """
    Returns plain wildcard like "foo*bar" as re.Pattern("foo.*bar", re.I).

    @param   end  whether pattern should match until end (adds $)
    """
    suff = "$" if end else ""
    return re.compile(".*".join(map(re.escape, text.split("*"))) + suff, re.I)


def process_args(args):
    """
    Converts or combines arguments where necessary, returns args.

    @param   args  arguments object like argparse.Namespace
    """
    for k, v in vars(args).items():  # Flatten lists of lists and drop duplicates
        if not isinstance(v, list): continue  # for k, v
        here = set()
        setattr(args, k, [x for xx in v for x in (xx if isinstance(xx, list) else [xx])
                          if not (x in here or here.add(x))])

    # Split and parse keyword options
    opts = dict(generator.OPTIONS, **dict(x.split("=", 1) for x in args.OPTIONS))
    for k, v in list(opts.items()):
        if not k.endswith("len") and k not in rosapi.ROS_NUMERIC_TYPES \
        or not isinstance(v, str):
            continue  # for k, v
        try:
            vv = sorted(json.loads("[%s]" % v))
            ctor = float if k.startswith("float") else bool if "bool" == k else int
            if k.endswith("int64") and sys.version_info < (3, ): ctor = long  # Py2
            vv = [ctor(x) for x in vv]
            if k in rosapi.ROS_INTEGER_RANGES:  # Force into allowed range
                a, b = rosapi.ROS_INTEGER_RANGES[k]
                vv = [max(a, min(b, x)) for x in vv]
            opts[k] = vv[0] if len(vv) < 2 and k.endswith("len") else tuple(vv[:2])
        except Exception:
            sys.exit("Error parsing option %s=%s." % (k, v))
    args.OPTIONS = opts

    return  args


def run(args):
    """Generates messages until Ctrl-C or end condition reached."""
    msgtypes   = rosapi.get_message_types()
    patterns   = [wildcard_to_regex(x) for x in args.TYPES]
    nopatterns = [wildcard_to_regex(x) for x in args.SKIP_TYPES]
    msgtypes   = [x for x in msgtypes if not patterns or any(p.match(x) for p in patterns)]
    availables = [x for x in msgtypes if not nopatterns or not any(p.match(x) for p in nopatterns)]
    if not availables:
        print("No message types %s." %
              ("match" if args.TYPES or args.SKIP_TYPES else "available"))
        sys.exit(1)

    def choose_topic(typename):
        """Returns new or existing ROS topic name for message type."""
        existing = [n for n, t in topiccounts if t == typename]
        if len(existing) < args.TOPICS_PER_TYPE:
            prefix = "/" + args.PUBLISH_PREFIX.strip("/")
            prefix += "/" if len(prefix) > 1 else ""
            suffix = "/topic%s" % (len(existing) + 1) if args.TOPICS_PER_TYPE > 1 else ""
            suffix += args.PUBLISH_SUFFIX
            return "%s%s%s" % (prefix, typename, suffix)
        return random.choice(existing)

    def choose_type():
        """Returns a random ROS message type name."""
        if availables and (not args.MAX_TOPICS
        or len(topiccounts) / (args.TOPICS_PER_TYPE or 1) < args.MAX_TOPICS):
            return availables.pop(random.randint(0, len(availables) - 1))
        candidates = [t for (_, t), c in topiccounts.items() if len(c) < args.MAX_PER_TOPIC] \
                     if args.MAX_PER_TOPIC else list(typecounts)
        return random.choice(candidates) if candidates else None

    def is_finished():
        """Returns whether generating is complete."""
        done = count >= args.COUNT if args.COUNT else False
        if not done and args.MAX_PER_TOPIC:
            done = all(len(x) >= args.MAX_PER_TOPIC for x in topiccounts.values())
        return done

    count = 0   # Total number of messages emitted
    pubs  = {}  # {(topic, typename): ROS publisher instance}
    typecounts  = collections.Counter()  # {typename: messages emitted}
    topiccounts = collections.Counter()  # {(topic, typename): messages emitted}
    print("Message types available: %s." % len(msgtypes))
    print("Generating a random message each %s seconds." % args.INTERVAL)
    rosapi.init(launch=True)
    signal.signal(signal.SIGINT, lambda *_, **__: sys.exit())  # Break ROS1 spin on Ctrl-C
    try:
        while not is_finished():
            typename = choose_type()
            if not typename:
                break  # while
            topic    = choose_topic(typename)
            topickey = (topic, typename)
            if topickey not in topiccounts:
                print("Adding topic %s." % topic)

            try:
                cls = rosapi.get_message_class(typename)
                msg = generator.populate(cls(), args.OPTIONS)
                if topickey not in pubs:
                    pubs[topickey] = rosapi.create_publisher(topic, cls, latch=args.LATCH)
            except Exception as e:
                print("Error processing message type %r: %s" % (typename, e))
                continue  # while

            if args.VERBOSE:
                print("-- [%s] Message %s in %s" % (count + 1, topiccounts[topickey] + 1, topic))
                print(rosapi.message_to_yaml(msg))

            pubs[topickey].publish(msg)
            count += 1
            topiccounts[topickey] += 1
            typecounts[typename] += 1

            if count and not count % 100:
                print("Total count: %s in %s." %
                      (plural("message", count), plural("topic", topiccounts)))
            if args.INTERVAL:
                time.sleep(args.INTERVAL)
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception:
        traceback.print_exc()

    print("")
    print("Emitted %s in %s%s." % (plural("message", count), plural("topic", topiccounts),
          (" and %s" % plural("type", typecounts)) if args.TOPICS_PER_TYPE > 1 else ""))

    print("")
    print("Press Ctrl-C to close publishers and exit.")
    try:
        while True: time.sleep(10)
    except KeyboardInterrupt:
        pass
    rosapi.shutdown()
    sys.exit()



if "__main__" == __name__:
    runargs = process_args(make_argparser().parse_args())
    run(runargs)

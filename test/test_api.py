#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: functions of grepros.api as library.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     22.12.2022
@modified    21.02.2024
------------------------------------------------------------------------------
"""
import datetime
import decimal
import logging
import os
import re
import sys

import std_msgs.msg

from grepros import api

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from test import testbase

logger = logging.getLogger()


class TestAPI(testbase.TestBase):
    """Tests general API."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]


    def __init__(self, *args, **kwargs):
        super(TestAPI, self).__init__(*args, **kwargs)
        api.validate()


    def test_messages(self):
        """Tests API functions for dealing with messages."""
        NAME = lambda f: "%s.%s()" % (f.__module__, f.__name__)
        ERR  = lambda f: "Unexpected result from %s." % f.__name__
        logger.info("Testing message API functions.")

        func = api.get_message_class
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertEqual(func("std_msgs/Bool"), std_msgs.msg.Bool, ERR(func))

        func = api.get_message_definition
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertTrue(func("std_msgs/Bool"),            ERR(func))
            self.assertTrue(func(std_msgs.msg.Bool),          ERR(func))
            self.assertTrue(func(std_msgs.msg.Bool()),        ERR(func))

        func = api.get_message_fields
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertEqual(func(std_msgs.msg.Bool),   {"data": "bool"}, ERR(func))
            self.assertEqual(func(std_msgs.msg.Bool()), {"data": "bool"}, ERR(func))
            dct = {"seq": "uint32", "stamp": "time", "frame_id": "string"} if api.ROS1 else \
                  {"stamp": "builtin_interfaces/Time", "frame_id": "string"}
            self.assertEqual(func(std_msgs.msg.Header), dct, ERR(func))
            self.assertEqual(func(None),                {},  ERR(func))

        func = api.get_message_type
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertEqual(func(std_msgs.msg.Bool),   "std_msgs/Bool", ERR(func))
            self.assertEqual(func(std_msgs.msg.Bool()), "std_msgs/Bool", ERR(func))
            self.assertIn(func(type(api.time_message(api.make_time()))),
                          api.ROS_TIME_TYPES, ERR(func))
            self.assertIn(func(api.time_message(api.make_time())),
                          api.ROS_TIME_TYPES, ERR(func))
            self.assertIn(func(type(api.time_message(api.make_duration()))),
                          api.ROS_TIME_TYPES, ERR(func))
            self.assertIn(func(api.time_message(api.make_duration())),
                          api.ROS_TIME_TYPES, ERR(func))

        func = api.get_message_type_hash
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            HASH = "8b94c1b53db61fb6aed406028ad6332a"
            self.assertEqual(func(std_msgs.msg.Bool),   HASH, ERR(func))
            self.assertEqual(func(std_msgs.msg.Bool()), HASH, ERR(func))
            self.assertEqual(func("std_msgs/Bool"),     HASH, ERR(func))
            self.assertTrue(func("std_msgs/Header"),          ERR(func))

        func = api.get_message_value
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            msg1 = std_msgs.msg.Header(frame_id=self.NAME)
            msg2 = std_msgs.msg.UInt8MultiArray(data=b"123")
            timename = next(x for x in api.ROS_TIME_TYPES if "time" in x.lower())
            self.assertEqual(func(msg1, "frame_id", "string"), msg1.frame_id,   ERR(func))
            self.assertEqual(func(msg1, "frame_id"),           msg1.frame_id,   ERR(func))
            self.assertEqual(func(msg1, "stamp", timename),    msg1.stamp,      ERR(func))
            self.assertEqual(func(msg2, "data", "uint8[]"),    list(msg2.data), ERR(func))
            self.assertEqual(func(msg1, "nope", default=123),  123,             ERR(func))
            with self.assertRaises(Exception, msg=ERR(func)):
                func(msg1, "nosuchfield", "whatever")

        func = api.set_message_value
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            msg = std_msgs.msg.Header(frame_id=self.NAME)
            api.set_message_value(msg, "frame_id", str(self))
            self.assertEqual(msg.frame_id, str(self), ERR(func))

        func = api.format_message_value
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            msg = std_msgs.msg.Header(frame_id=self.NAME)
            self.assertEqual(func(msg, "frame_id", msg.frame_id), msg.frame_id, ERR(func))
            for name in api.get_message_fields(msg.stamp):
                received = func(msg.stamp, name, getattr(msg.stamp, name))
                self.assertTrue(re.match(r"\s+%s" % getattr(msg.stamp, name), received), ERR(func))

        func = api.is_ros_message
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            msg = std_msgs.msg.Header(frame_id=self.NAME)
            self.assertTrue(func(msg),       ERR(func))
            self.assertTrue(func(msg.stamp), ERR(func))
            self.assertFalse(func(func),     ERR(func))
            self.assertFalse(func(msg.stamp, ignore_time=True), ERR(func))

        func = api.iter_message_fields
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            msg1 = std_msgs.msg.Header(frame_id=self.NAME)
            msg2 = std_msgs.msg.UInt8MultiArray(data=b"123")

            received = list(func(msg1, scalars=["time"]))
            paths = [".".join(p) for p, _, _ in received]
            self.assertTrue("stamp" in paths and not any(p.startswith("stamp.") for p in paths),
                            ERR(func))

            received = list(func(msg2))
            expected = [(('layout', 'dim'), [], 'std_msgs/MultiArrayDimension[]'),
                        (('layout', 'data_offset'), 0, 'uint32'),
                        (('data',), [49, 50, 51], 'uint8[]')]
            self.assertEqual(received, expected, ERR(func))

            received = list(func(msg2, messages_only=True))
            expected = [(('layout', 'dim'), [], 'std_msgs/MultiArrayDimension[]'),
                        (('layout',), std_msgs.msg.MultiArrayLayout(), 'std_msgs/MultiArrayLayout')]
            self.assertEqual(received, expected, ERR(func))

        func = api.parse_definition_fields
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            msg = std_msgs.msg.Header(frame_id=self.NAME)
            received = func(api.get_message_type(msg), api.get_message_definition(type(msg)))
            self.assertIsInstance(received, dict, ERR(func))
            self.assertEqual(received, api.get_message_fields(msg), ERR(func))

        func = api.parse_definition_subtypes
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            received = func(api.get_message_definition(std_msgs.msg.UInt8MultiArray))
            for typename, typedef in received.items():
                self.assertEqual(typedef.strip(), api.get_message_definition(typename).strip(), ERR(func))
            msg = std_msgs.msg.UInt8MultiArray(data=b"123")
            defs, nesteds = func(api.get_message_definition(type(msg)), nesting=True)
            for typename, typedef in defs.items():
                self.assertEqual(typedef.strip(), api.get_message_definition(typename).strip(), ERR(func))
            for typename in sum([[k] + v for k, v in nesteds.items()], []):
                self.assertIn(typename, defs, ERR(func))

        func = api.dict_to_message
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            dct = {"seq": 3, "stamp": {"secs": 1, "nsecs": 2}, "frame_id": self.NAME} if api.ROS1 \
                  else {"stamp": {"sec": 1, "nanosec": 2}, "frame_id": self.NAME}
            stamp = api.make_time(1, 2)
            msg = std_msgs.msg.Header(frame_id=self.NAME,
                                      stamp=stamp if api.ROS1 else stamp.to_msg())
            if api.ROS1: msg.seq = dct["seq"]
            self.assertEqual(func(dct, std_msgs.msg.Header()), msg, ERR(func))

        func = api.message_to_dict
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            dct = {"seq": 3, "stamp": {"secs": 1, "nsecs": 2}, "frame_id": self.NAME} if api.ROS1 \
                  else {"stamp": {"sec": 1, "nanosec": 2}, "frame_id": self.NAME}
            stamp = api.make_time(1, 2)
            msg = std_msgs.msg.Header(frame_id=self.NAME,
                                      stamp=stamp if api.ROS1 else stamp.to_msg())
            if api.ROS1: msg.seq = dct["seq"]
            self.assertEqual(func(msg), dct, ERR(func))

        func = api.serialize_message
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            expected = b"\x01" if api.ROS1 else b"\x00\x01\x00\x00\x01"
            self.assertEqual(func(std_msgs.msg.Bool(data=True)), expected, ERR(func))

        func = api.deserialize_message
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            binary = b"\x01" if api.ROS1 else b"\x00\x01\x00\x00\x01"
            expected = std_msgs.msg.Bool(data=True)
            self.assertEqual(func(binary, "std_msgs/Bool"),     expected, ERR(func))
            self.assertEqual(func(binary, std_msgs.msg.Bool),   expected, ERR(func))


    def test_typenames(self):
        """Tests API functions for dealing with ROS types."""
        NAME = lambda f: "%s.%s()" % (f.__module__, f.__name__)
        ERR  = lambda f: "Unexpected result from %s." % f.__name__
        logger.info("Testing ROS type API functions.")

        func = api.canonical
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertEqual(func("std_msgs/msg/Bool"),        "std_msgs/Bool",    ERR(func))
            self.assertEqual(func("std_srvs/srv/SetBool"),     "std_srvs/SetBool", ERR(func))
            self.assertEqual(func("pkg/Cls"),                  "pkg/Cls",          ERR(func))
            self.assertEqual(func("Cls"),                      "Cls",              ERR(func))
            self.assertEqual(func("int8[4]", unbounded=True),  "int8[]",           ERR(func))
            if api.ROS2:
                self.assertEqual(func("octet"),                "byte",             ERR(func))
                self.assertEqual(func("sequence<uint8, 100>"), "uint8[100]",       ERR(func))


        func = api.get_type_alias
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            signed, unsigned = ("byte", "char") if api.ROS1 else ("char", "byte")
            self.assertEqual(func("int8"),  signed,   ERR(func))
            self.assertEqual(func("uint8"), unsigned, ERR(func))
            self.assertEqual(func("int16"), None,     ERR(func))

        func = api.get_alias_type
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            signed, unsigned = ("byte", "char") if api.ROS1 else ("char", "byte")
            self.assertEqual(func(signed),   "int8",  ERR(func))
            self.assertEqual(func(unsigned), "uint8", ERR(func))
            self.assertEqual(func("other"),  None,    ERR(func))

        func = api.make_full_typename
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertEqual(func("std_msgs/Bool"),               "std_msgs/msg/Bool", ERR(func))
            self.assertEqual(func("std_msgs/msg/Bool"),           "std_msgs/msg/Bool", ERR(func))
            self.assertEqual(func("std_srvs/SetBool", "srv"),     "std_srvs/srv/SetBool", ERR(func))
            self.assertEqual(func("std_srvs/srv/SetBool", "srv"), "std_srvs/srv/SetBool", ERR(func))
            self.assertEqual(func("%s/Time" % api.ROS_FAMILY),
                             "%s/Time" % api.ROS_FAMILY, ERR(func))
            self.assertEqual(func("%s/Duration" % api.ROS_FAMILY),
                             "%s/Duration" % api.ROS_FAMILY, ERR(func))

        func = api.scalar
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertEqual(func("int8"), "int8", ERR(func))
            self.assertEqual(func("std_msgs/Bool[]"), "std_msgs/Bool", ERR(func))


    def test_temporals(self):
        """Tests API functions for dealing with ROS time/duration values."""
        NAME = lambda f: "%s.%s()" % (f.__module__, f.__name__)
        ERR  = lambda f: "Unexpected result from %s." % f.__name__
        logger.info("Testing temporal API functions.")

        func = api.get_ros_time_category
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for typename in api.ROS_TIME_TYPES:
                expected = "duration" if "duration" in typename.lower() else "time"
                self.assertEqual(func(typename), expected, ERR(func))
            for cls in api.ROS_TIME_CLASSES:
                expected = "duration" if "duration" in cls.__name__.lower() else "time"
                self.assertEqual(func(cls),                         expected, ERR(func))
                self.assertEqual(func(cls()),                       expected, ERR(func))
                self.assertEqual(func(api.get_message_type(cls)),   expected, ERR(func))
            self.assertEqual(func(self), self, ERR(func))

        func = api.time_message
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for cls in api.ROS_TIME_CLASSES:
                v1 = cls()
                category, is_msg = api.get_ros_time_category(cls), api.is_ros_message(v1)
                for to_message in (True, False):
                    v2 = func(v1, to_message=to_message)
                    if is_msg == to_message:
                        expected = type(v1)
                    elif is_msg:  # and not to_message
                        expected = type(api.make_time() if "time" == category else
                                        api.make_duration())
                    else:  # not is_msg and to_message
                        expected = next(api.get_message_class(x) for x in api.ROS_TIME_TYPES
                                        if category in x.lower())
                    self.assertTrue(issubclass(expected, type(v2)), ERR(func))

        func = api.is_ros_time
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            msg = std_msgs.msg.Header(frame_id=self.NAME)
            self.assertTrue(func(type(msg.stamp)), ERR(func))
            self.assertTrue(func(msg.stamp),       ERR(func))
            self.assertFalse(func(msg),            ERR(func))

        func = api.make_duration
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            dur = func(1, 2)
            self.assertIsInstance(dur, tuple(api.ROS_TIME_CLASSES),  ERR(func))
            self.assertTrue(api.is_ros_time(type(dur)), ERR(func))
            self.assertTrue(api.is_ros_time(dur),       ERR(func))

        func = api.make_time
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            self.assertIsInstance(func(1, 2), tuple(api.ROS_TIME_CLASSES),  ERR(func))

        func = api.to_datetime
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            tval, dval = api.make_time(1234), api.make_duration(1234)
            expected = datetime.datetime.fromtimestamp(1234)
            self.assertEqual(func(tval), expected, ERR(func))
            self.assertEqual(func(dval), expected, ERR(func))
            self.assertEqual(func(666),       666, ERR(func))

        func = api.to_decimal
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            tval = api.make_time    (123456789, 987654321)
            dval = api.make_duration(123456789, 987654321)
            expected = decimal.Decimal("123456789.987654321")
            self.assertEqual(func(tval), expected, ERR(func))
            self.assertEqual(func(dval), expected, ERR(func))
            self.assertEqual(func(666),       666, ERR(func))

        func = api.to_duration
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))

        func = api.to_nsec
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            tval = api.make_time    (123456789, 987654321)
            dval = api.make_duration(123456789, 987654321)
            expected = 123456789987654321
            self.assertEqual(func(tval), expected, ERR(func))
            self.assertEqual(func(dval), expected, ERR(func))
            self.assertEqual(func(666),       666, ERR(func))

        func = api.to_sec
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            tval = api.make_time    (1, 123456789)
            dval = api.make_duration(1, 123456789)
            expected = 1.123456789
            self.assertEqual(func(tval), expected, ERR(func))
            self.assertEqual(func(dval), expected, ERR(func))
            self.assertEqual(func(666),       666, ERR(func))

        func = api.to_sec_nsec
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            tval = api.make_time    (123456789, 987654321)
            dval = api.make_duration(123456789, 987654321)
            expected = (123456789, 987654321)
            self.assertEqual(func(tval), expected, ERR(func))
            self.assertEqual(func(dval), expected, ERR(func))
            self.assertEqual(func(666),       666, ERR(func))

        func = api.to_time
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))

            val = datetime.datetime.now()
            tval = func(val)
            self.assertTrue(api.is_ros_time(tval), ERR(func))
            self.assertEqual(api.to_datetime(tval), val, ERR(func))

            val = decimal.Decimal("123456789.987654321")
            tval = func(val)
            self.assertTrue(api.is_ros_time(tval), ERR(func))
            self.assertEqual(api.to_decimal(tval), val, ERR(func))

            val = api.make_duration(123456789, 987654321)
            tval = func(val)
            self.assertTrue(api.is_ros_time(tval), ERR(func))
            self.assertEqual(api.make_duration(*api.to_sec_nsec(tval)), val, ERR(func))

            val = 123321123.456
            tval = func(val)
            self.assertTrue(api.is_ros_time(tval), ERR(func))
            self.assertEqual(api.to_sec(tval), val, ERR(func))

            self.assertEqual(func(None), None, ERR(func))
            self.assertEqual(func(self), self, ERR(func))



if "__main__" == __name__:
    TestAPI.run_rostest()

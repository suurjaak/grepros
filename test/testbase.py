#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base class for ROS tests.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.12.2021
@modified    31.01.2024
------------------------------------------------------------------------------
"""
import contextlib
import logging
import glob
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

if os.getenv("ROS_VERSION") == "1":
    import rosbag
    import rospy
    import rostest
else:
    import rclpy.serialization
    import rclpy.time
    import rosidl_runtime_py.utilities

logger = logging.getLogger()


def init_logging(name):
    """Initializes logging."""
    fmt = "[%%(levelname)s]\t[%%(created).06f] [%s] %%(message)s" % name
    logging.basicConfig(level=logging.DEBUG, format=fmt, stream=sys.stdout)
    logger.setLevel(logging.DEBUG)



class TestBase(unittest.TestCase):
    """Tests grepping from a source to a sink, with prepared test bags."""

    ## Test name used in flow logging
    NAME = ""

    ## Name used in flow logging
    INPUT_LABEL = "ROS bags"

    ## Name used in flow logging
    OUTPUT_LABEL = ""

    ## Suffix for write output file, if any
    OUTPUT_SUFFIX = None

    ## Test bags directory
    DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))

    ## ROS bagfile extension
    BAG_SUFFIX = ".bag" if os.getenv("ROS_VERSION") == "1" else ".db3"

    ## Words searched in bag
    SEARCH_WORDS = ["this"]

    ## Base command for running grepros
    CMD_BASE = ["grepros"] + SEARCH_WORDS + \
               ["--topic",    "/match/this*", "--type",    "std_msgs/*",
                "--no-topic", "/not/this*",   "--no-type", "std_msgs/Bool",
                "--match-wrapper", "--color", "never", "--path", DATA_DIR]

    ## Words expected in matched messages, bag name appended
    EXPECTED_WORDS = ["match_this"]

    ## Words not expected in matched messages, bag name appended
    SKIPPED_WORDS = ["not_this"]

    ## Topics expected in output, bag name appended
    EXPECTED_TOPIC_BASES = ["/match/this/intarray", "/match/this/header"]

    ## Topics not expected in output, bag name appended
    SKIPPED_TOPIC_BASES = ["/not/this/intarray", "/match/this/not"]


    def __init__(self, *args, **kwargs):
        super(TestBase, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._bags    = []    # Bags in DATA_DIR
        self._proc    = None  # subprocess.Popen for grepros
        self._cmd     = None  # Full grepros command
        self._outname = None  # Name of temporary file for write output
        self._outfile = None  # Opened temporary file
        self._node    = None  # rclpy.Node for ROS2

        init_logging(self.NAME)


    def setUp(self):
        """Collects bags in data directory, assembles command."""
        logger.debug("Setting up test.")
        self._bags = sorted(glob.glob(os.path.join(self.DATA_DIR, "*" + self.BAG_SUFFIX)))
        if self.OUTPUT_SUFFIX:
            self._outname = tempfile.NamedTemporaryFile(suffix=self.OUTPUT_SUFFIX).name


    def tearDown(self):
        """Terminates subprocess and deletes temporary output files, if any."""
        logger.debug("Tearing down test.")
        try: self._proc and self._proc.terminate()
        except Exception: pass
        try: self._outfile and self._outfile.close()
        except Exception: pass
        try: self._outname and os.unlink(self._outname)
        except Exception: pass


    def subTest(self, msg=None, **params):
        """Shim for TestCase.subTest in Py2."""
        if callable(getattr(super(TestBase, self), "subTest", None)):  # Py 3.7+
            return super(TestBase, self).subTest(msg, **params)
        if callable(getattr(contextlib, "nested", None)):  # Py 2
            return contextlib.nested()


    def create_publisher(self, topic, cls):
        if os.getenv("ROS_VERSION") == "1":
            return rospy.Publisher(topic, cls, queue_size=10)
        return self._node.create_publisher(cls, topic, 10)


    def init_node(self):
        """Creates ROS1 or ROS2 node."""
        if os.getenv("ROS_VERSION") == "1":
            rospy.init_node(self.NAME)
            logger.addHandler(Ros1LogHandler(self.NAME))
            logger.setLevel(logging.DEBUG)
        else:
            try: rclpy.init()
            except Exception: pass  # Must not be called twice
            self._node = rclpy.create_node(self.NAME)


    def shutdown_node(self):
        """Shuts down ROS2 node, if any."""
        self._node and rclpy.shutdown()


    def run_command(self, communicate=True):
        """Executes test command, returns command output text if communicate."""
        logger.debug("Executing %r.", " ".join(self._cmd))
        self._proc = subprocess.Popen(self._cmd, universal_newlines=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if not communicate: return None
        return self._proc.communicate()[0]


    def spin_once(self, timeout):
        """Spins once if ROS2 else sleeps until timeout."""
        if self._node: rclpy.spin_once(self._node, timeout_sec=timeout)
        else: time.sleep(timeout)


    def verify_bags(self):
        """Asserts that ROS bags were found in data directory."""
        logger.info("Verifying reading from %s and writing to %s.",
                    self.INPUT_LABEL, self.OUTPUT_LABEL)
        self.assertTrue(self._bags, "No bags found in data directory.")


    def verify_topics(self, topics, messages=None):
        """Asserts that topics and message texts were emitted as expected."""
        logger.info("Verifying topics and messages." if messages else "Verifying topics.")
        for bag in self._bags:
            bagname = os.path.splitext(os.path.basename(bag))[0]
            for topicbase in self.EXPECTED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                self.assertIn(topic, topics, "Expected topic not in output.")
                if messages is None: continue  # for topicbase

                value = " ".join(self.EXPECTED_WORDS + [bagname])
                self.assertIn(value, messages, "Expected message value not in output.")

            for topicbase in self.SKIPPED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                self.assertNotIn(topic, topics, "Unexpected topic in output.")
                if messages is None: continue  # for topicbase

                value = " ".join(self.SKIPPED_WORDS + [bagname])
                self.assertNotIn(value, messages, "Unexpected message value in output.")


    @classmethod
    def run_rostest(cls):
        """Runs rostest if ROS1."""
        rostest.rosrun("grepros", cls.NAME, cls)



class Ros1LogHandler(logging.Handler):
    """Logging handler that forwards logging messages to rospy.logwarn."""

    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__name = name

    def emit(self, record):
        """Invokes rospy.logwarn or logerr (only warn or higher gets rostest output)."""
        if "rospy" in record.name or record.levelno >= logging.WARN:
            return  # Skip rospy internal logging, or higher levels logged by rospy

        try: text = record.msg % record.args if record.args else record.msg
        except Exception: text = record.msg
        text = "[%s] %s" % (self.__name, text)
        (rospy.logerr if record.levelno >= logging.ERROR else rospy.logwarn)(text)



class Ros2BagReader():
    """Simple ROS2 bag reader."""

    def __init__(self, filename):
        self._db = sqlite3.connect(filename)
        self._db.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))

    def read_messages(self):
        """Yields messages from the bag."""
        topicmap = {x["id"]: x for x in self._db.execute("SELECT * FROM topics").fetchall()}
        msgtypes = {}  # {full typename: cls}

        for row in self._db.execute("SELECT * FROM messages ORDER BY timestamp"):
            tdata = topicmap[row["topic_id"]]
            topic, typename = tdata["name"], tdata["type"]
            if typename not in msgtypes:
                msgtypes[typename] = rosidl_runtime_py.utilities.get_message(typename)
            msg = rclpy.serialization.deserialize_message(row["data"], msgtypes[typename])
            stamp = rclpy.time.Time(nanoseconds=row["timestamp"])
            yield topic, msg, stamp

    def close(self):
        """Closes the bag file."""
        self._db.close()


BagReader = rosbag.Bag if os.getenv("ROS_VERSION") == "1" else Ros2BagReader

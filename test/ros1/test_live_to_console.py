#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep live topics to console output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.12.2021
@modified    23.12.2021
------------------------------------------------------------------------------
"""
import glob
import os
import subprocess
import time
import unittest

import rosbag
import roslib
import rospy
import rostest

PKG, TEST = "grepros", "test_live_to_console"


class logger(object):
    """Simple logging wrapper to rospy."""

    @staticmethod
    def debug(text, *args): rospy.logwarn(text % args)
    @staticmethod
    def info(text, *args):  rospy.logwarn(text % args)
    @staticmethod
    def warn(text, *args):  rospy.logwarn(text % args)
    @staticmethod
    def error(text, *args): rospy.logerr (text % args)


class TestLiveInputConsoleOutput(unittest.TestCase):
    """
    Tests grepping from live topics and printing matches to console.
    """

    ## Name used in logging
    INPUT_LABEL = "live topics"

    ## Name used in logging
    OUTPUT_LABEL = "console"

    ## Test bags directory
    DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

    ## Words searched in bag
    SEARCH_WORDS = ["this"]

    ## Base command to grepros
    CMD_BASE = ["grepros"] + SEARCH_WORDS + \
               ["--topic",    "/match/this", "--type",    "std_msgs/*",
                "--no-topic", "/not/this",   "--no-type", "std_msgs/Bool",
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
        super(TestLiveInputConsoleOutput, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._bags = []    # Bags in DATA_DIR
        self._proc = None  # subprocess.Popen for grepros
        self._cmd  = None  # Full grepros command

        rospy.init_node(TEST)


    def setUp(self):
        """Collects bags in data directory, assembles command."""
        logger.debug("Setting up test.")
        self._bags = glob.glob(os.path.join(self.DATA_DIR, "*.bag"))
        self._cmd = self.CMD_BASE + ["--live"]


    def tearDown(self):
        """Terminates subprocess, if any."""
        logger.debug("Tearing down test.")
        try: self._proc.terminate()
        except Exception: pass


    def test_grepros(self):
        """Runs grepros on live topics, verifies console output."""
        logger.info("Verifying reading from %s and writing to %s.",
                    self.INPUT_LABEL, self.OUTPUT_LABEL)
        self.assertTrue(self._bags, "No bags found in data directory.")

        logger.debug("Executing %r.", " ".join(self._cmd))
        self._proc = subprocess.Popen(self._cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        logger.info("Opening publishers.")
        pubs = {}  # {topic: rospy.Publisher}
        for bagfile in self._bags:
            bag = rosbag.Bag(bagfile)
            bagtopics = bag.get_type_and_topic_info().topics
            for topic, tdata in bag.get_type_and_topic_info().topics.items():
                if topic not in pubs:
                    logger.info("Opening publisher to %r.", topic)
                    cls = roslib.message.get_message_class(tdata.msg_type)
                    pubs[topic] = rospy.Publisher(topic, cls, queue_size=10)
            bag.close()

        logger.info("Publishing messages to live.")
        time.sleep(2)
        for bagfile in self._bags:
            bag = rosbag.Bag(bagfile)
            for topic, msg, _ in bag.read_messages():
                pubs[topic].publish(msg)
                time.sleep(0.5)
            bag.close()

        self._proc.terminate()
        fulltext = self._proc.communicate()[0].decode()
        self.assertTrue(fulltext, "Command did not print to console.")

        logger.info("Verifying topics and messages.")
        for bag in self._bags:
            bagname = os.path.splitext(os.path.basename(bag))[0]
            for topicbase in self.EXPECTED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                value = " ".join(self.EXPECTED_WORDS + [bagname])
                self.assertIn(topic, fulltext, "Expected topic not in output.")
                self.assertIn(value, fulltext, "Expected message value not in output.")
            for topicbase in self.SKIPPED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                value = " ".join(self.SKIPPED_WORDS + [bagname])
                self.assertNotIn(topic, fulltext, "Unexpected topic in output.")
                self.assertNotIn(value, fulltext, "Unexpected message value in output.")


if "__main__" == __name__:
    rostest.rosrun(PKG, TEST, TestLiveInputConsoleOutput)

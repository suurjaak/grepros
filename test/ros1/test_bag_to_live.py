#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to live topics output.

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
import unittest

import rospy
import rostest

PKG, TEST = "grepros", "test_bag_to_live"


class logger(object):
    """Simple logging wrapper to rospy (only warn or higher gets output from rostest)."""

    @staticmethod
    def debug(text, *args): rospy.logwarn(text % args)
    @staticmethod
    def info(text,  *args): rospy.logwarn(text % args)
    @staticmethod
    def warn(text,  *args): rospy.logwarn(text % args)
    @staticmethod
    def error(text, *args): rospy.logerr (text % args)


class TestBagInputLiveOutput(unittest.TestCase):
    """
    Tests grepping from input bags and publishing matches to live topics.
    """

    ## Name used in logging
    INPUT_LABEL = "ROS bags"

    ## Name used in logging
    OUTPUT_LABEL = "live topics"

    ## Test bags directory
    DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

    ## Words searched in bag
    SEARCH_WORDS = ["this"]

    ## Command for running grepros
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
        super(TestBagInputLiveOutput, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._bags    = []    # Bags in DATA_DIR
        self._proc    = None  # subprocess.Popen for grepros
        self._cmd     = None  # Full grepros command
        self._msgs    = {}    # {topic: [msg, ]}

        rospy.init_node(TEST)


    def setUp(self):
        """Collects bags in data directory, assembles command."""
        logger.debug("Setting up test.")
        self._bags = glob.glob(os.path.join(self.DATA_DIR, "*.bag"))
        self._cmd = self.CMD_BASE + ["--publish", "--no-console-output"]


    def tearDown(self):
        """Terminates subprocess."""
        logger.debug("Tearing down test.")
        try: self._proc.terminate()
        except Exception: pass


    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies live topics output."""
        logger.info("Verifying reading from %s and writing to %s.",
                    self.INPUT_LABEL, self.OUTPUT_LABEL)
        self.assertTrue(self._bags, "No bags found in data directory.")

        logger.debug("Executing %r.", " ".join(self._cmd))
        self._proc = subprocess.Popen(self._cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self._proc.wait()
        fulltext = self._proc.communicate()[0].decode()
        self.assertTrue(fulltext, "Command did not print to console.")

        logger.info("Verifying topics.")
        for bag in self._bags:
            bagname = os.path.splitext(os.path.basename(bag))[0]
            for topicbase in self.EXPECTED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                self.assertIn(topic, fulltext, "Expected topic not in output.")
            for topicbase in self.SKIPPED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                self.assertNotIn(topic, fulltext, "Unexpected topic in output.")



if "__main__" == __name__:
    rostest.rosrun(PKG, TEST, TestBagInputLiveOutput)

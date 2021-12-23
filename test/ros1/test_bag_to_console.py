#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to console output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     22.12.2021
@modified    23.12.2021
------------------------------------------------------------------------------
"""
import logging
import glob
import os
import subprocess
import unittest

import rostest

PKG, TEST = "grepros", "test_bag_to_console"

logger = logging.getLogger(__name__)


class TestBagInputConsoleOutput(unittest.TestCase):
    """
    Tests grepping from input bags and printing matches to console.
    """

    ## Name used in logging
    INPUT_LABEL = "ROS bags"

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
        super(TestBagInputConsoleOutput, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._bags = []    # Bags in DATA_DIR
        self._proc = None  # subprocess.Popen for grepros
        self._cmd  = None  # Full grepros command


    def setUp(self):
        """Collects bags in data directory, assembles command."""
        logger.debug("Setting up test.")
        self._bags = glob.glob(os.path.join(self.DATA_DIR, "*.bag"))
        self._cmd = list(self.CMD_BASE)


    def tearDown(self):
        """Terminates subprocess, if any."""
        logger.debug("Tearing down test.")
        try: self._proc.terminate()
        except Exception: pass


    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies console output."""
        logger.info("Verifying reading from ROS bags and writing to %s.", self.OUTPUT_LABEL)
        self.assertTrue(self._bags, "No bags found in data directory.")

        logger.debug("Executing %r.", " ".join(self._cmd))
        self._proc = subprocess.Popen(self._cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        fulltext = self._proc.communicate()[0].decode()
        self.assertTrue(fulltext, "Command did not print to console.")

        logger.info("Verifying topics and messages.")
        for bag in self._bags:
            filename = os.path.basename(bag)
            bagname = os.path.splitext(filename)[0]
            self.assertIn(filename, fulltext, "Expected bag not in output.")
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
    logging.basicConfig(level=logging.DEBUG,
                        format="[%%(levelname)s]\t[%%(created).06f] [%s] %%(message)s" % TEST)
    rostest.rosrun(PKG, TEST, TestBagInputConsoleOutput)

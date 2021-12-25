#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to live topics output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.12.2021
@modified    24.12.2021
------------------------------------------------------------------------------
"""
import logging
import os
import sys

import rospy
import rostest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from ros1 import testbase

PKG, TEST = "grepros", "test_bag_to_live"

logger = logging.getLogger(PKG)


class TestBagInputLiveOutput(testbase.TestBase):
    """Tests grepping from input bags and publishing matches to live topics."""

    ## Name used in logging
    OUTPUT_LABEL = "live topics"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._msgs = {}  # {topic: [msg, ]}
        rospy.init_node(TEST)

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super().setUp()
        self._cmd = self.CMD_BASE + ["--publish", "--no-console-output"]

    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies live topics output."""
        self.verify_bags()
        fulltext = self.run_command()
        self.assertTrue(fulltext, "Command did not print to console.")
        self.verify_topics(fulltext)


if "__main__" == __name__:
    testbase.init_logging(TEST, testbase.RosLogHandler(TEST))
    rostest.rosrun(PKG, TEST, TestBagInputLiveOutput)

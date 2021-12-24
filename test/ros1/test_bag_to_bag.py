#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to bag output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     22.12.2021
@modified    24.12.2021
------------------------------------------------------------------------------
"""
import logging
import os
import sys

import rosbag
import rostest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from ros1 import testbase

PKG, TEST = "grepros", "test_bag_to_bag"

logger = logging.getLogger(PKG)


class TestBagInputBagOutput(testbase.TestBase):
    """Tests grepping from input bags and writing matches to bag."""

    ## Name used in logging
    OUTPUT_LABEL = "ROS bag"

    ## Suffix for write output file
    OUTPUT_SUFFIX = ".bag"

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super().setUp()
        self._cmd = self.CMD_BASE + ["--no-console-output", "--write", self._outname]

    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies bag output."""
        self.verify_bags()
        self.run_command()
        self.assertTrue(os.path.isfile(self._outname), "Expected output file not written.")

        logger.info("Reading data from written %s.", self.OUTPUT_LABEL)
        messages = {}  # {topic: [msg, ]}
        outfile = self._outfile = rosbag.Bag(self._outname)
        for topic, msg, _ in outfile.read_messages():
            messages.setdefault(topic, []).append(msg)
        outfile.close()

        fulltext = "\n".join(str(m) for mm in messages.values() for m in mm)
        super().verify_topics(messages, fulltext)


if "__main__" == __name__:
    testbase.init_logging(TEST)
    rostest.rosrun(PKG, TEST, TestBagInputBagOutput)

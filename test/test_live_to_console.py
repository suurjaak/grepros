#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep live topics to console output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.12.2021
@modified    03.02.2024
------------------------------------------------------------------------------
"""
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase

logger = logging.getLogger()


class TestLiveInputConsoleOutput(testbase.TestBase):
    """Tests grepping from live topics and printing matches to console."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

    ## Name used in logging
    INPUT_LABEL = "live topics"

    ## Name used in logging
    OUTPUT_LABEL = "console"

    def __init__(self, *args, **kwargs):
        super(TestLiveInputConsoleOutput, self).__init__(*args, **kwargs)
        self.init_node()

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super(TestLiveInputConsoleOutput, self).setUp()
        self._cmd = self.CMD_BASE + ["--live"]

    def tearDown(self):
        """Terminates subprocess and shuts down ROS2 node, if any."""
        super(TestLiveInputConsoleOutput, self).tearDown()
        self.shutdown_node()

    def test_grepros(self):
        """Runs grepros on live topics, verifies console output."""
        self.verify_bags()
        self.run_command(communicate=False)

        logger.info("Opening publishers.")
        pubs = {}  # {topic: ROS publisher}
        for bagfile in self._bags:
            bag = testbase.BagReader(bagfile)
            for topic, msg, _ in bag.read_messages():
                if topic not in pubs:
                    logger.info("Opening publisher to %r.", topic)
                    pubs[topic] = self.create_publisher(topic, type(msg))
                    self.spin_once(0.5)
            bag.close()
        self.spin_once(2)

        logger.info("Publishing messages to live.")
        for bagfile in self._bags:
            bag = testbase.BagReader(bagfile)
            for topic, msg, _ in bag.read_messages():
                pubs[topic].publish(msg)
                self.spin_once(0.5)
            bag.close()
        self.spin_once(2)

        self._proc.terminate()
        fulltext = self._proc.communicate()[0]
        self.assertTrue(fulltext, "Command did not print to console.")
        self.verify_topics(fulltext, fulltext)


if "__main__" == __name__:
    TestLiveInputConsoleOutput.run_rostest()

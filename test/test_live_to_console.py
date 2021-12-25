#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep live topics to console output.

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
import time

import rosbag
import roslib
import rospy
import rostest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from ros1 import testbase

PKG, TEST = "grepros", "test_live_to_console"

logger = logging.getLogger(PKG)


class TestLiveInputConsoleOutput(testbase.TestBase):
    """Tests grepping from live topics and printing matches to console."""

    ## Name used in logging
    INPUT_LABEL = "live topics"

    ## Name used in logging
    OUTPUT_LABEL = "console"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        rospy.init_node(TEST)

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super().setUp()
        self._cmd = self.CMD_BASE + ["--live"]

    def test_grepros(self):
        """Runs grepros on live topics, verifies console output."""
        self.verify_bags()
        self.run_command(communicate=False)

        logger.info("Opening publishers.")
        pubs = {}  # {topic: rospy.Publisher}
        for bagfile in self._bags:
            bag = rosbag.Bag(bagfile)
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
        fulltext = self._proc.communicate()[0]
        self.assertTrue(fulltext, "Command did not print to console.")
        self.verify_topics(fulltext, fulltext)


if "__main__" == __name__:
    testbase.init_logging(TEST, testbase.RosLogHandler(TEST))
    rostest.rosrun(PKG, TEST, TestLiveInputConsoleOutput)

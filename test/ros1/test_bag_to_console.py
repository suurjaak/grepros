#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to console output.

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

import rostest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from ros1 import testbase

PKG, TEST = "grepros", "test_bag_to_console"

logger = logging.getLogger(PKG)


class TestBagInputConsoleOutput(testbase.TestBase):
    """Tests grepping from input bags and printing matches to console."""

    ## Name used in flow logging
    OUTPUT_LABEL = "console"

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super().setUp()
        self._cmd = list(self.CMD_BASE)

    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies console output."""
        self.verify_bags()
        fulltext = self.run_command()
        self.assertTrue(fulltext, "Command did not print to console.")

        for bag in self._bags:
            self.assertIn(os.path.basename(bag), fulltext, "Expected bag not in output.")
        super().verify_topics(fulltext, fulltext)


if "__main__" == __name__:
    testbase.init_logging(TEST)
    rostest.rosrun(PKG, TEST, TestBagInputConsoleOutput)

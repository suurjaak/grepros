#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to console output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     22.12.2021
@modified    25.12.2021
------------------------------------------------------------------------------
"""
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase

logger = logging.getLogger()


class TestBagInputConsoleOutput(testbase.TestBase):
    """Tests grepping from input bags and printing matches to console."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

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
    TestBagInputConsoleOutput.run_rostest()

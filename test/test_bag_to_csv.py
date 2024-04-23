#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to CSV output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     22.12.2021
@modified    03.02.2024
------------------------------------------------------------------------------
"""
import glob
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase

logger = logging.getLogger()


class TestBagInputCsvOutput(testbase.TestBase):
    """Tests grepping from input bags and writing matches to CSV files."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

    ## Name used in logging
    OUTPUT_LABEL = "CSV"

    ## Suffix for write output files
    OUTPUT_SUFFIX = ".csv"

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super(TestBagInputCsvOutput, self).setUp()
        self._cmd = self.CMD_BASE + ["--no-console-output", "--write", self._outname]

    def tearDown(self):
        """Terminates subprocess and deletes temporary output files, if any."""
        super(TestBagInputCsvOutput, self).tearDown()
        for filename in glob.glob("%s*%s" % os.path.splitext(self._outname)):
            try: os.unlink(filename)
            except Exception: pass

    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies CSV output."""
        self.verify_bags()
        self.run_command()

        logger.info("Verifying topics and messages.")
        filebase, fileext = os.path.splitext(self._outname)
        for bag in self._bags:
            bagname = os.path.splitext(os.path.basename(bag))[0]
            fulltext = ""
            for topicbase in self.EXPECTED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                value = " ".join(self.EXPECTED_WORDS + [bagname])

                filename = "%s.%s%s" % (filebase, topic.lstrip("/").replace("/", "__"), fileext)
                self.assertTrue(os.path.isfile(filename),
                                "Expected output file not written: %s" % filename)
                logger.info("Reading data from written %s %s.",
                            self.OUTPUT_LABEL, os.path.basename(filename))
                with open(filename) as f:
                    ftext = f.read()
                    self.assertIn(value, ftext, "Expected message value not in output.")
                    fulltext += "\n\n" + ftext
            for topicbase in self.SKIPPED_TOPIC_BASES:
                topic = topicbase + "/" + bagname
                filename = "%s.%s%s" % (filebase, topic.lstrip("/").replace("/", "__"), fileext)
                self.assertFalse(os.path.isfile(filename),
                                 "Unexpected output file written: %s" % filename)


if "__main__" == __name__:
    TestBagInputCsvOutput.run_rostest()

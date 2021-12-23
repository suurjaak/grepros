#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to HTML output.

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
import html.parser
import os
import subprocess
import tempfile
import unittest

import rostest

PKG, TEST = "grepros", "test_bag_to_html"

logger = logging.getLogger(__name__)


class HtmlToText(html.parser.HTMLParser):
    """Parses HTML to text."""

    def __init__(self):
        super(HtmlToText, self).__init__()
        self.text = ""

    def handle_data(self, data):
        self.text += data


class TestBagInputHtmlOutput(unittest.TestCase):
    """
    Tests grepping from input bags and writing matches to HTML file.
    """

    ## Name used in logging
    INPUT_LABEL = "ROS bags"

    ## Name used in logging
    OUTPUT_LABEL = "HTML"

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
        super(TestBagInputHtmlOutput, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._bags    = []    # Bags in DATA_DIR
        self._proc    = None  # subprocess.Popen for grepros
        self._cmd     = None  # Full grepros command
        self._outname = None  # Temporary file for write output


    def setUp(self):
        """Collects bags in data directory, assembles command."""
        logger.debug("Setting up test.")
        self._bags = glob.glob(os.path.join(self.DATA_DIR, "*.bag"))
        self._outname = tempfile.NamedTemporaryFile(suffix=".html").name
        self._cmd = self.CMD_BASE + ["--no-console-output", "--write", self._outname]


    def tearDown(self):
        """Terminates subprocess and deletes temporary output files, if any."""
        logger.debug("Tearing down test.")
        try: self._proc.terminate()
        except Exception: pass
        try: os.unlink(self._outname)
        except Exception: pass


    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies HTML output."""
        logger.info("Verifying reading from %s and writing to %s.",
                    self.INPUT_LABEL, self.OUTPUT_LABEL)
        self.assertTrue(self._bags, "No bags found in data directory.")

        logger.debug("Executing %r.", " ".join(self._cmd))
        self._proc = subprocess.Popen(self._cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self._proc.wait()

        self.assertTrue(os.path.isfile(self._outname), "Expected output file not written.")
        logger.info("Reading data from written %s.", self.OUTPUT_LABEL)
        with open(self._outname) as f:
            h = HtmlToText()
            h.feed(f.read())
            fulltext = h.text

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
    logging.basicConfig(level=logging.DEBUG,
                        format="[%%(levelname)s]\t[%%(created).06f] [%s] %%(message)s" % TEST)
    rostest.rosrun(PKG, TEST, TestBagInputHtmlOutput)

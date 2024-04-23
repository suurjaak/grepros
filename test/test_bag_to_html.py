#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to HTML output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     22.12.2021
@modified    03.02.2024
------------------------------------------------------------------------------
"""
import logging
import html.parser
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase

logger = logging.getLogger()


class TestBagInputHtmlOutput(testbase.TestBase):
    """Tests grepping from input bags and writing matches to HTML file."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

    ## Name used in logging
    OUTPUT_LABEL = "HTML"

    ## Suffix for write output file
    OUTPUT_SUFFIX = ".html"

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super(TestBagInputHtmlOutput, self).setUp()
        self._cmd = self.CMD_BASE + ["--no-console-output", "--write", self._outname]

    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies HTML output."""
        self.verify_bags()
        self.run_command()
        self.assertTrue(os.path.isfile(self._outname), "Expected output file not written.")

        logger.info("Reading data from written %s.", self.OUTPUT_LABEL)
        texts, parser = [], html.parser.HTMLParser()
        parser.handle_data = texts.append
        with open(self._outname) as f:
            parser.feed(f.read())
        fulltext = "".join(texts)
        self.verify_topics(fulltext, fulltext)


if "__main__" == __name__:
    TestBagInputHtmlOutput.run_rostest()

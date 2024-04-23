#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: grep input bags to SQLite output.

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
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase

logger = logging.getLogger()


class TestBagInputSqliteOutput(testbase.TestBase):
    """Tests grepping from input bags and writing matches to SQLite file."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

    ## Name used in logging
    OUTPUT_LABEL = "SQLite"

    ## Suffix for write output files
    OUTPUT_SUFFIX = ".sqlite"

    def setUp(self):
        """Collects bags in data directory, assembles command."""
        super(TestBagInputSqliteOutput, self).setUp()
        self._cmd = self.CMD_BASE + ["--no-console-output", "--write", self._outname]

    def test_grepros(self):
        """Runs grepros on bags in data directory, verifies SQLite output."""
        self.verify_bags()
        self.run_command()
        self.assertTrue(os.path.isfile(self._outname), "Expected output file not written.")

        logger.info("Reading data from written %s.", self.OUTPUT_LABEL)
        db = self._outfile = sqlite3.connect(self._outname)
        db.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))
        messages = {}  # {topic: [msg, ]}
        for msg in db.execute("SELECT * FROM messages").fetchall():
            messages.setdefault(msg["topic"], []).append(msg)
        db.close()

        fulltext = "\n".join(str(m) for mm in messages.values() for m in mm)
        self.verify_topics(fulltext, fulltext)


if "__main__" == __name__:
    TestBagInputSqliteOutput.run_rostest()

# -*- coding: utf-8 -*-
"""
CSV output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.12.2021
@modified    03.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs.csv
from __future__ import absolute_import
import atexit
import csv
import os
import sys

from .. common import ConsolePrinter, format_bytes, plural, unique_path
from .. import rosapi
from . base import SinkBase


class CsvSink(SinkBase):
    """Writes messages to CSV files, each topic to a separate file."""

    def __init__(self, args):
        """
        @param   args           arguments object like argparse.Namespace
        @param   args.OUTFILE   base name of CSV file to write,
                                will add topic name like "name.__my__topic.csv" for "/my/topic",
                                will add counter like "name.__my__topic.2.csv" if exists
        @param   args.VERBOSE   whether to print debug information
        """
        super(CsvSink, self).__init__(args)
        self._filebase      = args.OUTFILE  # Filename base, will be made unique
        self._files         = {}            # {topic: file()}
        self._writers       = {}            # {topic: csv.writer}
        self._lasttopic     = None          # Last topic emitted
        self._close_printed = False

        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output file."""
        data = [v for _, v in self._iter_fields(msg)]
        metadata = [rosapi.to_sec(stamp), rosapi.to_datetime(stamp), rosapi.get_message_type(msg)]
        self._make_writer(topic, msg).writerow(self._format_row(metadata + data))
        super(CsvSink, self).emit(topic, index, stamp, msg, match)

    def close(self):
        """Closes output file(s), if any."""
        names = {t: f.name for t, f in self._files.items()}
        for t in names:
            self._files.pop(t).close()
        self._writers.clear()
        if not self._close_printed and self._counts:
            self._close_printed = True
            sizes = {t: os.path.getsize(n) for t, n in names.items()}
            ConsolePrinter.debug("Wrote %s in %s to file (%s):",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)),
                                 format_bytes(sum(sizes.values())))
            for topic, name in names.items():
                ConsolePrinter.debug("- %s (%s, %s)", name,
                                    format_bytes(sizes[topic]),
                                    plural("message", self._counts[topic]))
        super(CsvSink, self).close()

    def _make_writer(self, topic, msg):
        """
        Returns a csv.writer for writing topic data.

        File is populated with header if 
        """
        if self._lasttopic and topic != self._lasttopic:
            self._files[self._lasttopic].close()  # Avoid hitting ulimit
        if topic not in self._files or self._files[topic].closed:
            name = self._files[topic].name if topic in self._files else None
            if not name:
                base, ext = os.path.splitext(self._filebase)
                name = unique_path("%s.%s%s" % (base, topic.lstrip("/").replace("/", "__"), ext))
            flags = {"mode": "ab"} if sys.version_info < (3, 0) else {"mode": "a", "newline": ""}
            f = open(name, **flags)
            w = csv.writer(f)
            if topic not in self._files:
                if self._args.VERBOSE:
                    ConsolePrinter.debug("Creating %s.", name)
                header = [topic + "/" + ".".join(map(str, p)) for p, _ in self._iter_fields(msg)]
                metaheader = ["__time", "__datetime", "__type"]
                w.writerow(self._format_row(metaheader + header))
            self._files[topic], self._writers[topic] = f, w
        self._lasttopic = topic
        return self._writers[topic]

    def _format_row(self, data):
        """Returns row suitable for writing to CSV."""
        data = [int(v) if isinstance(v, bool) else v for v in data]
        if sys.version_info < (3, 0):  # Py2, CSV is written in binary mode
            data = [v.encode("utf-8") if isinstance(v, unicode) else v for v in data]
        return data

    def _iter_fields(self, msg, top=()):
        """
        Yields ((nested, path), scalar value) from ROS message.

        Lists are returned as ((nested, path, index), value), e.g. (("data", 0), 666).
        """
        fieldmap = rosapi.get_message_fields(msg)
        for k, t in fieldmap.items() if fieldmap != msg else ():
            v, path = rosapi.get_message_value(msg, k, t), top + (k, )
            is_sublist = isinstance(v, (list, tuple)) and \
                         rosapi.scalar(t) not in rosapi.ROS_BUILTIN_TYPES
            if isinstance(v, (list, tuple)) and not is_sublist:
                for i, lv in enumerate(v):
                    yield path + (i, ), rosapi.to_sec(lv)
            elif is_sublist:
                for i, lmsg in enumerate(v):
                    for lp, lv in self._iter_fields(lmsg, path + (i, )):
                        yield lp, lv
            elif rosapi.is_ros_message(v, ignore_time=True):
                for mp, mv in self._iter_fields(v, path):
                    yield mp, mv
            else:
                yield path, rosapi.to_sec(v)

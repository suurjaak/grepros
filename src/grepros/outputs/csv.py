# -*- coding: utf-8 -*-
"""
CSV output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.12.2021
@modified    12.12.2021
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

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".csv", )

    def __init__(self, args):
        """
        @param   args               arguments object like argparse.Namespace
        @param   args.DUMP_TARGET   base name of CSV file to write,
                                    will add topic name like "name.__my__topic.csv" for "/my/topic",
                                    will add counter like "name.__my__topic.2.csv" if exists
        @param   args.VERBOSE       whether to print debug information
        """
        super(CsvSink, self).__init__(args)
        self._filebase      = args.DUMP_TARGET  # Filename base, will be made unique
        self._files         = {}                # {(topic, typehash): file()}
        self._writers       = {}                # {(topic, typehash): csv.writer}
        self._lasttopickey  = None              # Last (topic, typehash) emitted
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
        names = {k: f.name for k, f in self._files.items()}
        for k in names:
            self._files.pop(k).close()
        self._writers.clear()
        if not self._close_printed and self._counts:
            self._close_printed = True
            sizes = {k: os.path.getsize(n) for k, n in names.items()}
            ConsolePrinter.debug("Wrote %s in %s to file (%s):",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)),
                                 format_bytes(sum(sizes.values())))
            for topickey, name in names.items():
                ConsolePrinter.debug("- %s (%s, %s)", name,
                                    format_bytes(sizes[topickey]),
                                    plural("message", self._counts[topickey]))
        super(CsvSink, self).close()

    def _make_writer(self, topic, msg):
        """
        Returns a csv.writer for writing topic data.

        File is populated with header if 
        """
        topickey = (topic, self.source.get_message_type_hash(msg))
        if self._lasttopickey and topickey != self._lasttopickey:
            self._files[self._lasttopickey].close()  # Avoid hitting ulimit
        if topickey not in self._files or self._files[topickey].closed:
            name = self._files[topickey].name if topickey in self._files else None
            if not name:
                base, ext = os.path.splitext(self._filebase)
                name = unique_path("%s.%s%s" % (base, topic.lstrip("/").replace("/", "__"), ext))
            flags = {"mode": "ab"} if sys.version_info < (3, 0) else {"mode": "a", "newline": ""}
            f = open(name, **flags)
            w = csv.writer(f)
            if topickey not in self._files:
                if self._args.VERBOSE:
                    ConsolePrinter.debug("Creating %s.", name)
                header = [topic + "/" + ".".join(map(str, p)) for p, _ in self._iter_fields(msg)]
                metaheader = ["__time", "__datetime", "__type"]
                w.writerow(self._format_row(metaheader + header))
            self._files[topickey], self._writers[topickey] = f, w
        self._lasttopickey = topickey
        return self._writers[topickey]

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

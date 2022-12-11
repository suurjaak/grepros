# -*- coding: utf-8 -*-
"""
CSV output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.12.2021
@modified    11.12.2022
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.auto.csv
from __future__ import absolute_import
import atexit
import csv
import io
import itertools
import os
import sys

from ... common import ConsolePrinter, ensure_namespace, format_bytes, makedirs, plural, unique_path
from ... import rosapi
from ... outputs import BaseSink


class CsvSink(BaseSink):
    """Writes messages to CSV files, each topic to a separate file."""

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".csv", )

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(META=False, WRITE_OPTIONS={}, VERBOSE=False)

    def __init__(self, args=None, **kwargs):
        """
        @param   args                 arguments as namespace or dictionary, case-insensitive
        @param   args.WRITE           base name of CSV file to write,
                                      will add topic name like "name.__my__topic.csv" for "/my/topic",
                                      will add counter like "name.__my__topic.2.csv" if exists
        @param   args.WRITE_OPTIONS   {"overwrite": whether to overwrite existing files
                                                    (default false)}
        @param   args.VERBOSE         whether to print debug information
        @param   kwargs               any and all arguments as keyword overrides, case-insensitive
        """
        args = ensure_namespace(args, CsvSink.DEFAULT_ARGS, **kwargs)
        super(CsvSink, self).__init__(args)
        self._filebase      = args.WRITE  # Filename base, will be made unique
        self._files         = {}          # {(topic, typename, typehash): file()}
        self._writers       = {}          # {(topic, typename, typehash): CsvWriter}
        self._lasttopickey  = None        # Last (topic, typename, typehash) emitted
        self._overwrite     = (args.WRITE_OPTIONS.get("overwrite") == "true")
        self._close_printed = False

        atexit.register(self.close)

    def emit(self, topic, msg, stamp, match, index):
        """Writes message to output file."""
        data = (v for _, v in self._iter_fields(msg))
        metadata = [rosapi.to_sec(stamp), rosapi.to_datetime(stamp), rosapi.get_message_type(msg)]
        self._make_writer(topic, msg).writerow(itertools.chain(metadata, data))
        super(CsvSink, self).emit(topic, msg, stamp, match, index)

    def validate(self):
        """Returns whether overwrite option is valid."""
        result = True
        if self.args.WRITE_OPTIONS.get("overwrite") not in (None, "true", "false"):
            ConsolePrinter.error("Invalid overwrite option for CSV: %r. "
                                 "Choose one of {true, false}.",
                                 self.args.WRITE_OPTIONS["overwrite"])
            result = False
        return result

    def close(self):
        """Closes output file(s), if any."""
        names = {k: f.name for k, f in self._files.items()}
        for k in names:
            self._files.pop(k).close()
        self._writers.clear()
        self._lasttopickey = None
        if not self._close_printed and self._counts:
            self._close_printed = True
            sizes = {k: os.path.getsize(n) for k, n in names.items()}
            ConsolePrinter.debug("Wrote %s in %s to CSV (%s):",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", self._counts), format_bytes(sum(sizes.values())))
            for topickey, name in names.items():
                ConsolePrinter.debug("- %s (%s, %s)", name,
                                    format_bytes(sizes[topickey]),
                                    plural("message", self._counts[topickey]))
        super(CsvSink, self).close()

    def _make_writer(self, topic, msg):
        """
        Returns a csv.writer for writing topic data.

        File is populated with header if not created during this session.
        """
        topickey = rosapi.TypeMeta.make(msg, topic).topickey
        if not self._lasttopickey:
            makedirs(os.path.dirname(self._filebase))
        if self._lasttopickey and topickey != self._lasttopickey:
            self._files[self._lasttopickey].close()  # Avoid hitting ulimit
        if topickey not in self._files or self._files[topickey].closed:
            name = self._files[topickey].name if topickey in self._files else None
            action = "Creating"  # Or "Overwriting"
            if not name:
                base, ext = os.path.splitext(self._filebase)
                name = "%s.%s%s" % (base, topic.lstrip("/").replace("/", "__"), ext)
                if self._overwrite:
                    if os.path.isfile(name) and os.path.getsize(name): action = "Overwriting"
                    open(name, "w").close()
                else: name = unique_path(name)
            flags = {"mode": "ab"} if sys.version_info < (3, 0) else {"mode": "a", "newline": ""}
            f = open(name, **flags)
            w = CsvWriter(f)
            if topickey not in self._files:
                if self.args.VERBOSE:
                    ConsolePrinter.debug("%s %s.", action, name)
                header = (topic + "/" + ".".join(map(str, p)) for p, _ in self._iter_fields(msg))
                metaheader = ["__time", "__datetime", "__type"]
                w.writerow(itertools.chain(metaheader, header))
            self._files[topickey], self._writers[topickey] = f, w
        self._lasttopickey = topickey
        return self._writers[topickey]

    def _iter_fields(self, msg, top=()):
        """
        Yields ((nested, path), scalar value) from ROS message.

        Lists are returned as ((nested, path, index), value), e.g. (("data", 0), 666).
        """
        fieldmap, identity = rosapi.get_message_fields(msg), lambda x: x
        for k, t in fieldmap.items() if fieldmap != msg else ():
            v, path, baset = rosapi.get_message_value(msg, k, t), top + (k, ), rosapi.scalar(t)
            is_sublist = isinstance(v, (list, tuple)) and baset not in rosapi.ROS_BUILTIN_TYPES
            cast = rosapi.to_sec if baset in rosapi.ROS_TIME_TYPES else identity
            if isinstance(v, (list, tuple)) and not is_sublist:
                for i, lv in enumerate(v):
                    yield path + (i, ), cast(lv)
            elif is_sublist:
                for i, lmsg in enumerate(v):
                    for lp, lv in self._iter_fields(lmsg, path + (i, )):
                        yield lp, lv
            elif rosapi.is_ros_message(v, ignore_time=True):
                for mp, mv in self._iter_fields(v, path):
                    yield mp, mv
            else:
                yield path, cast(v)


class CsvWriter(object):
    """Wraps csv.writer with bool conversion, iterator support, and lesser memory use."""

    def __init__(self, csvfile, dialect="excel", **fmtparams):
        """
        @param   csvfile    file-like object with `write()` method
        @param   dialect    CSV dialect to use, one from `csv.list_dialects()`
        @param   fmtparams  override individual format parameters in dialect
        """
        self._file    = csvfile
        self._buffer  = io.BytesIO() if sys.version_info < (3, 0) else io.StringIO()
        self._writer  = csv.writer(self._buffer, dialect, **dict(fmtparams, lineterminator=""))
        self._dialect = csv.writer(self._buffer, dialect, **fmtparams).dialect
        self._format  = lambda v: int(v) if isinstance(v, bool) else v
        if sys.version_info < (3, 0):  # Py2, CSV is written in binary mode
            self._format = lambda v: int(v) if isinstance(v, bool) else \
                                     v.encode("utf-8") if isinstance(v, unicode) else v

    @property
    def dialect(self):
        """A read-only description of the dialect in use by the writer."""
        return self._dialect

    def writerow(self, row):
        """
        Writes the row to the writer’s file object.

        Fields will be formatted according to the current dialect.

        @param   row  iterable of field values
        @return       return value of the call to the write method of the underlying file object
        """
        def write_columns(cols, inter):
            """Writes columns to file, returns number of bytes written."""
            count = self._file.write(inter) if inter else 0
            self._writer.writerow(cols)                         # Hack: use csv.writer to format
            count += self._file.write(self._buffer.getvalue())  # a slice at a time, as it can get
            self._buffer.seek(0); self._buffer.truncate()       # very memory-hungry for huge rows
            return count

        result, chunk, inter, STEP = 0, [], "", 10000
        for v in row:
            chunk.append(self._format(v))
            if len(chunk) >= STEP:
                result += write_columns(chunk, inter)
                chunk, inter = [], self.dialect.delimiter
        if chunk: result += write_columns(chunk, inter)
        result += self._file.write(self.dialect.lineterminator)
        return result

    def writerows(self, rows):
        """
        Writes the rows to the writer’s file object.

        Fields will be formatted according to the current dialect.

        @param   rows  iterable of iterables of field values
        """
        for row in rows: self.writerow(row)



def init(*_, **__):
    """Adds CSV output format support."""
    from ... import plugins  # Late import to avoid circular
    plugins.add_write_format("csv", CsvSink, "CSV", [
        ("overwrite=true|false",  "overwrite existing files in CSV output\n"
                                  "instead of appending unique counter (default false)")
    ])

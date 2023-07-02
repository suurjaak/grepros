# -*- coding: utf-8 -*-
"""
CSV output plugin.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.12.2021
@modified    02.07.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.auto.csv
from __future__ import absolute_import
import atexit
import csv
import itertools
import os

import six

from ... import api
from ... import common
from ... common import ConsolePrinter, plural
from ... outputs import Sink


class CsvSink(Sink):
    """Writes messages to CSV files, each topic to a separate file."""

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".csv", )

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(EMIT_FIELD=(), META=False, NOEMIT_FIELD=(), WRITE_OPTIONS={}, VERBOSE=False)

    def __init__(self, args=None, **kwargs):
        """
        @param   args                 arguments as namespace or dictionary, case-insensitive;
                                      or a single path as the base name of CSV files to write
        @param   args.emit_field      message fields to emit in output if not all
        @param   args.noemit_field    message fields to skip in output
        @param   args.write           base name of CSV files to write,
                                      will add topic name like "name.__my__topic.csv" for "/my/topic",
                                      will add counter like "name.__my__topic.2.csv" if exists
        @param   args.write_options   {"overwrite": whether to overwrite existing files
                                                    (default false)}
        @param   args.meta            whether to emit metainfo
        @param   args.verbose         whether to emit debug information
        @param   kwargs               any and all arguments as keyword overrides, case-insensitive
        """
        args = {"WRITE": str(args)} if isinstance(args, common.PATH_TYPES) else args
        args = common.ensure_namespace(args, CsvSink.DEFAULT_ARGS, **kwargs)
        super(CsvSink, self).__init__(args)
        self._filebase      = args.WRITE  # Filename base, will be made unique
        self._files         = {}          # {(topic, typename, typehash): file()}
        self._writers       = {}          # {(topic, typename, typehash): CsvWriter}
        self._patterns      = {}          # {key: [(() if any field else ('path', ), re.Pattern), ]}
        self._lasttopickey  = None        # Last (topic, typename, typehash) emitted
        self._overwrite     = (args.WRITE_OPTIONS.get("overwrite") in (True, "true"))
        self._close_printed = False

        for key, vals in [("print", args.EMIT_FIELD), ("noprint", args.NOEMIT_FIELD)]:
            self._patterns[key] = [(tuple(v.split(".")), common.wildcard_to_regex(v)) for v in vals]
        atexit.register(self.close)

    def emit(self, topic, msg, stamp=None, match=None, index=None):
        """Writes message to output file."""
        if not self.validate(): raise Exception("invalid")
        stamp, index = self._ensure_stamp_index(topic, msg, stamp, index)
        data = (v for _, v in self._iter_fields(msg))
        metadata = [api.to_sec(stamp), api.to_datetime(stamp), api.get_message_type(msg)]
        self._make_writer(topic, msg).writerow(itertools.chain(metadata, data))
        super(CsvSink, self).emit(topic, msg, stamp, match, index)

    def validate(self):
        """Returns whether overwrite option is valid and file base is writable."""
        if self.valid is not None: return self.valid
        result = True
        if self.args.WRITE_OPTIONS.get("overwrite") not in (None, True, False, "true", "false"):
            ConsolePrinter.error("Invalid overwrite option for CSV: %r. "
                                 "Choose one of {true, false}.",
                                 self.args.WRITE_OPTIONS["overwrite"])
            result = False
        if not common.verify_io(self.args.WRITE, "w"):
            result = False
        self.valid = result
        return self.valid

    def close(self):
        """Closes output file(s), if any."""
        try:
            names = {k: f.name for k, f in self._files.items()}
            for k in names:
                self._files.pop(k).close()
            self._writers.clear()
            self._lasttopickey = None
        finally:
            if not self._close_printed and self._counts:
                self._close_printed = True
                sizes = {k: None for k in names.values()}
                for k, n in names.items():
                    try: sizes[k] = os.path.getsize(n)
                    except Exception as e: ConsolePrinter.warn("Error getting size of %s: %s", n, e)
                ConsolePrinter.debug("Wrote %s in %s to CSV (%s):",
                                     plural("message", sum(self._counts.values())),
                                     plural("topic", self._counts),
                                     common.format_bytes(sum(filter(bool, sizes.values()))))
                for topickey, name in names.items():
                    ConsolePrinter.debug("- %s (%s, %s)", name,
                                         "error getting size" if sizes[topickey] is None else
                                         common.format_bytes(sizes[topickey]),
                                         plural("message", self._counts[topickey]))
            super(CsvSink, self).close()

    def _make_writer(self, topic, msg):
        """
        Returns a csv.writer for writing topic data.

        File is populated with header if not created during this session.
        """
        topickey = api.TypeMeta.make(msg, topic).topickey
        if not self._lasttopickey:
            common.makedirs(os.path.dirname(self._filebase))
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
                else: name = common.unique_path(name)
            flags = {"mode": "ab"} if six.PY2 else {"mode": "a", "newline": ""}
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
        prints, noprints = self._patterns["print"], self._patterns["noprint"]
        fieldmap, identity = api.get_message_fields(msg), lambda x: x
        fieldmap = api.filter_fields(fieldmap, top, include=prints, exclude=noprints)
        for k, t in fieldmap.items() if fieldmap != msg else ():
            v, path, baset = api.get_message_value(msg, k, t), top + (k, ), api.scalar(t)
            is_sublist = isinstance(v, (list, tuple)) and baset not in api.ROS_BUILTIN_TYPES
            cast = api.to_sec if baset in api.ROS_TIME_TYPES else identity
            if isinstance(v, (list, tuple)) and not is_sublist:
                for i, lv in enumerate(v):
                    yield path + (i, ), cast(lv)
            elif is_sublist:
                for i, lmsg in enumerate(v):
                    for lp, lv in self._iter_fields(lmsg, path + (i, )):
                        yield lp, lv
            elif api.is_ros_message(v, ignore_time=True):
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
        self._buffer  = six.BytesIO() if "b" in csvfile.mode else six.StringIO()
        self._writer  = csv.writer(self._buffer, dialect, **dict(fmtparams, lineterminator=""))
        self._dialect = csv.writer(self._buffer, dialect, **fmtparams).dialect
        self._format  = lambda v: int(v) if isinstance(v, bool) else v
        if six.PY2:  # In Py2, CSV is written in binary mode
            self._format = lambda v: int(v) if isinstance(v, bool) else \
                                     v.encode("utf-8") if isinstance(v, six.text_type) else v

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

        result, chunk, inter, DELIM, STEP = 0, [], "", self.dialect.delimiter, 10000
        if "b" in self._file.mode: DELIM = six.binary_type(self.dialect.delimiter)
        for v in row:
            chunk.append(self._format(v))
            if len(chunk) >= STEP:
                result += write_columns(chunk, inter)
                chunk, inter = [], DELIM
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
    plugins.add_output_label("CSV", ["--emit-field", "--no-emit-field"])


__all__ = ["CsvSink", "CsvWriter", "init"]

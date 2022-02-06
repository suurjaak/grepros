# -*- coding: utf-8 -*-
"""
Parquet output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     14.12.2021
@modified    06.02.2022
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.parquet
import json
import os
import re

try: import pandas
except ImportError: pandas = None
try: import pyarrow
except ImportError: pyarrow = None
try: import pyarrow.parquet
except ImportError: pass

from .. common import ConsolePrinter, format_bytes, makedirs, plural, unique_path
from .. outputs import SinkBase
from .. import rosapi


class ParquetSink(SinkBase):
    """
    Writes messages to Apache Parquet files.
    """

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".parquet", )

    ## Number of dataframes to cache before writing, per type
    CHUNK_SIZE = 100

    ## Mapping from pyarrow type names and aliases to pyarrow type constructors
    ARROW_TYPES = {
        "bool":       pyarrow.bool_,     "bool_":        pyarrow.bool_,
        "float16":    pyarrow.float16,   "float64":      pyarrow.float64,
        "float32":    pyarrow.float32,   "decimal128":   pyarrow.decimal128,
        "int8":       pyarrow.int8,      "uint8":        pyarrow.uint8, 
        "int16":      pyarrow.int16,     "uint16":       pyarrow.uint16,
        "int32":      pyarrow.int32,     "uint32":       pyarrow.uint32,
        "int64":      pyarrow.int64,     "uint64":       pyarrow.uint64,
        "date32":     pyarrow.date32,    "time32":       pyarrow.time32,
        "date64":     pyarrow.date64,    "time64":       pyarrow.time64,
        "timestamp":  pyarrow.timestamp, "duration":     pyarrow.duration,
        "binary":     pyarrow.binary,    "large_binary": pyarrow.large_binary,
        "string":     pyarrow.string,    "large_string": pyarrow.large_string,
        "utf8":       pyarrow.string,    "large_utf8":   pyarrow.large_utf8,
        "list":       pyarrow.list_,     "list_":        pyarrow.list_,
        "large_list": pyarrow.large_list,
        "month_day_nano_interval": pyarrow.month_day_nano_interval,
    } if pyarrow else {}

    ## Mapping from ROS common type names to pyarrow type constructors
    COMMON_TYPES = {
        "byte":    pyarrow.uint8(),  "char":     pyarrow.int8(),    "int8":    pyarrow.int8(),
        "int16":   pyarrow.int16(),  "int32":    pyarrow.int32(),   "int64":   pyarrow.int64(),
        "uint8":   pyarrow.uint8(),  "uint16":   pyarrow.uint16(),  "uint32":  pyarrow.uint32(),
        "uint64":  pyarrow.uint64(), "float32":  pyarrow.float32(), "float64": pyarrow.float64(),
        "bool":    pyarrow.bool_(),  "string":   pyarrow.string(),  "wstring": pyarrow.string(),
        "uint8[]": pyarrow.binary(), "char[]":   pyarrow.binary(),
    } if pyarrow else {}

    ## Fallback pyarrow type if mapped type not found
    DEFAULT_TYPE = pyarrow.string() if pyarrow else None

    ## Default columns for message type tables
    MESSAGE_TYPE_BASECOLS  = [("_topic",      "string"),
                              ("_timestamp",  "time"), ]

    ## Custom arguments for pyarrow.parquet.ParquetWriter
    WRITER_ARGS = {"version": "2.6"}


    def __init__(self, args):
        """
        @param   args                 arguments object like argparse.Namespace
        @param   args.META            whether to print metainfo
        @param   args.WRITE           base name of Parquet files to write
        @param   args.WRITE_OPTIONS   {"writer-*": arguments passed to ParquetWriter,
                                       "overwrite": whether to overwrite existing file
                                                    (default false)}
        @param   args.VERBOSE         whether to print debug information
        """
        super(ParquetSink, self).__init__(args)

        self._filebase       = args.WRITE
        self._overwrite      = (args.WRITE_OPTIONS.get("overwrite") == "true")
        self._filenames      = {}  # {(typename, typehash): Parquet file path}
        self._caches         = {}  # {(typename, typehash): [{data}, ]}
        self._schemas        = {}  # {(typename, typehash): pyarrow.Schema}
        self._writers        = {}  # {(typename, typehash): pyarrow.parquet.ParquetWriter}
        self._extra_basecols = []  # [(name, value)]

        self._configure_ok  = True
        self._close_printed = False

        self._configure()


    def validate(self):
        """
        Returns whether required libraries are available (pandas and pyarrow) and overwrite is valid.
        """
        ok, pandas_ok, pyarrow_ok = self._configure_ok, bool(pandas), bool(pyarrow)
        if self.args.WRITE_OPTIONS.get("overwrite") not in (None, "true", "false"):
            ConsolePrinter.error("Invalid overwrite option for Parquet: %r. "
                                 "Choose one of {true, false}.",
                                 self.args.WRITE_OPTIONS["overwrite"])
            ok = False
        if not pandas_ok:
            ConsolePrinter.error("pandas not available: cannot write Parquet files.")
        if not pyarrow_ok:
            ConsolePrinter.error("PyArrow not available: cannot write Parquet files.")
        return ok and pandas_ok and pyarrow_ok


    def emit(self, topic, index, stamp, msg, match):
        """Writes message to a Parquet file."""
        self._process_type(topic, msg)
        self._process_message(topic, stamp, msg)
        super(ParquetSink, self).emit(topic, index, stamp, msg, match)


    def close(self):
        """Writes out any remaining messages, closes writers, clears structures."""
        for k, vv in list(self._caches.items()):
            vv and self._write_table(k)
        for k in list(self._writers):
            self._writers.pop(k).close()
        if not self._close_printed and self._counts:
            self._close_printed = True
            sizes = {n: os.path.getsize(n) for n in self._filenames.values()}
            ConsolePrinter.debug("Wrote %s in %s to %s (%s):",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", self._counts), plural("Parquet file", sizes),
                                 format_bytes(sum(sizes.values())))
            for (t, h), name in self._filenames.items():
                count = sum(c for (_, t_, h_), c in self._counts.items() if (t, h) == (t_, h_))
                ConsolePrinter.debug("- %s (%s, %s)", name,
                                    format_bytes(sizes[name]), plural("message", count))
        self._caches.clear()
        self._schemas.clear()
        self._filenames.clear()


    def _process_type(self, topic, msg):
        """Prepares Parquet schema and writer if not existing."""
        with rosapi.TypeMeta.make(msg, topic) as m:
            typename, typehash, typekey = (m.typename, m.typehash, m.typekey)
        if (topic, typename, typehash) not in self._counts and self.args.VERBOSE:
            ConsolePrinter.debug("Adding topic %s in Parquet output.", topic)
        if typekey in self._writers: return

        basedir, basename = os.path.split(self._filebase)
        pathname = os.path.join(basedir, re.sub(r"\W", "__", "%s__%s" % (typename, typehash)))
        filename = os.path.join(pathname, basename)
        if not self._overwrite:
            filename = unique_path(filename)

        cols = []
        scalars = set(x for x in self.COMMON_TYPES if "[" not in x)
        for path, value, subtype in rosapi.iter_message_fields(msg, scalars=scalars):
            coltype = self._make_column_type(subtype)
            cols += [(".".join(path), coltype)]
        cols += [(c, self._make_column_type(t, fallback="int64" if "time" == t else None))
                 for c, t in self.MESSAGE_TYPE_BASECOLS]

        if self.args.VERBOSE:
            sz = os.path.isfile(filename) and os.path.getsize(filename)
            action = "Overwriting" if sz and self._overwrite else "Adding"
            ConsolePrinter.debug("%s type %s in Parquet output.", action, typename)
        makedirs(pathname)

        schema = pyarrow.schema(cols)
        writer = pyarrow.parquet.ParquetWriter(filename, schema, **self.WRITER_ARGS)
        self._caches[typekey]    = []
        self._filenames[typekey] = filename
        self._schemas[typekey]   = schema
        self._writers[typekey]   = writer


    def _process_message(self, topic, stamp, msg):
        """
        Converts message to pandas dataframe, adds to cache.

        Writes cache to disk if length reached chunk size.
        """
        data = {}
        typekey = rosapi.TypeMeta.make(msg, topic).typekey
        for p, v, t in rosapi.iter_message_fields(msg, scalars=set(self.COMMON_TYPES)):
            data[".".join(p)] = self._make_column_value(v, t)
        data.update(_topic=topic, _timestamp=self._make_column_value(stamp, "time"))
        data.update(self._extra_basecols)
        self._caches[typekey].append(data)
        if len(self._caches[typekey]) >= self.CHUNK_SIZE:
            self._write_table(typekey)


    def _make_column_type(self, typename, fallback=None):
        """
        Returns pyarrow type for ROS type.

        @param  fallback  fallback typename to use for lookup if typename not found
        """
        scalartype = rosapi.scalar(typename)
        timetype   = rosapi.get_ros_time_category(scalartype)
        coltype    = self.COMMON_TYPES.get(typename)

        if not coltype and "[" not in typename and scalartype in self.COMMON_TYPES:
            coltype = self.COMMON_TYPES[scalartype]  # Bounded type like "string<=10"
        if not coltype and scalartype in self.COMMON_TYPES:
            coltype = pyarrow.list_(self.COMMON_TYPES[scalartype])
        if not coltype and timetype in self.COMMON_TYPES:
            if typename != scalartype:
                coltype = pyarrow.list_(self.COMMON_TYPES[timetype])
            else:
                coltype = self.COMMON_TYPES[timetype]
        if not coltype and fallback:
            coltype = self._make_column_type(fallback)
        if not coltype:
            coltype = self.DEFAULT_TYPE
        return coltype


    def _make_column_value(self, value, typename=None):
        """Returns column value suitable for adding to Parquet file."""
        v = value
        if isinstance(v, (list, tuple)):
            if v and rosapi.is_ros_time(v[0]):
                v = [rosapi.to_nsec(x) for x in v]
            elif rosapi.scalar(typename) not in rosapi.ROS_BUILTIN_TYPES:
                v = str([rosapi.message_to_dict(m) for m in v])
            elif pyarrow.binary() == self.COMMON_TYPES.get(typename):
                v = bytes(bytearray(v))  # Py2/Py3 compatible
            else:
                v = list(v)  # Ensure lists not tuples
        elif rosapi.is_ros_time(v):
            v = rosapi.to_nsec(v)
        elif typename and typename not in rosapi.ROS_BUILTIN_TYPES:
            v = str(rosapi.message_to_dict(v))
        return v


    def _write_table(self, typekey):
        """Writes out cached messages for type."""
        dicts = self._caches[typekey][:]
        del self._caches[typekey][:]
        mapping = {k: [d[k] for d in dicts] for k in dicts[0]}
        table = pyarrow.Table.from_pydict(mapping, self._schemas[typekey])
        self._writers[typekey].write_table(table)


    def _configure(self):
        """Parses args.WRITE_OPTIONS."""
        ok = True
        for k, v in self.args.WRITE_OPTIONS.items():
            if k.startswith("column-"):
                # Parse "column-name=rostype:value"
                try:
                    name, (rostype, value), myok = k[len("column-"):], v.split(":", 1), True
                    if "string" not in rostype:
                        value = json.loads(value)
                    if not name:
                        ok = myok = False
                        ConsolePrinter.error("Invalid name option in %s=%s", k, v)
                    if rostype not in rosapi.ROS_BUILTIN_TYPES:
                        ok = myok = False
                        ConsolePrinter.error("Invalid type option in %s=%s", k, v)
                    if myok:
                        self.MESSAGE_TYPE_BASECOLS.append((name, rostype))
                        self._extra_basecols.append((name, value))
                except Exception as e:
                    ok = False
                    ConsolePrinter.error("Invalid column option in %s=%s: %s", k, v, e)
            elif k.startswith("type-"):
                # Eval pyarrow datatype from value like "float64()" or "list(uint8())"
                if not k[len("type-"):]:
                    ok = False
                    ConsolePrinter.error("Invalid type option in %s=%s: %s", k, v)
                    continue  # for k, v
                try:
                    arrowtype = eval(compile(v, "", "eval"), {"__builtins__": self.ARROW_TYPES})
                    self.COMMON_TYPES[k[len("type-"):]] = arrowtype
                except Exception as e:
                    ok = False
                    ConsolePrinter.error("Invalid type option in %s=%s: %s", k, v, e)
            elif k.startswith("writer-"):
                if not k[len("writer-"):]:
                    ok = False
                    ConsolePrinter.error("Invalid name in %s=%s: %s", k, v)
                    continue  # for k, v
                try: v = json.loads(v)
                except Exception: pass
                self.WRITER_ARGS[k[len("writer-"):]] = v
        self._configure_ok = ok


def init(*_, **__):
    """Adds Parquet output format support."""
    from .. import plugins  # Late import to avoid circular
    plugins.add_write_format("parquet", ParquetSink, "Parquet", [
        ("column-name=rostype:value",  "additional column to add in Parquet output,\n"
                                       "like column-bag_hash=string:26dfba2c"),
        ("overwrite=true|false",       "overwrite existing file in Parquet output\n"
                                       "instead of appending unique counter (default false)"),
        ("type-rostype=arrowtype",     "custom mapping between ROS and pyarrow type\n"
                                       "for Parquet output, like type-time=\"timestamp('ns')\"\n"
                                       "or type-uint8[]=\"list(uint8())\""),
        ("writer-argname=argvalue",    "additional arguments for Parquet output\n"
                                       "given to pyarrow.parquet.ParquetWriter"),
    ])

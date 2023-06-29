# -*- coding: utf-8 -*-
"""
Parquet output plugin.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     14.12.2021
@modified    29.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.parquet
import itertools
import json
import os
import re
import uuid

try: import pandas
except ImportError: pandas = None
try: import pyarrow
except ImportError: pyarrow = None
try: import pyarrow.parquet
except ImportError: pass
import six

from .. import api, common
from .. common import ConsolePrinter
from .. outputs import Sink


class ParquetSink(Sink):
    """Writes messages to Apache Parquet files."""

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
        "int8":    pyarrow.int8(),     "int16":   pyarrow.int16(),    "int32":   pyarrow.int32(),
        "uint8":   pyarrow.uint8(),    "uint16":  pyarrow.uint16(),   "uint32":  pyarrow.uint32(),
        "int64":   pyarrow.int64(),    "uint64":  pyarrow.uint64(),   "bool":    pyarrow.bool_(),
        "string":  pyarrow.string(),   "wstring": pyarrow.string(),   "uint8[]": pyarrow.binary(),
        "float32": pyarrow.float32(),  "float64": pyarrow.float64(),
    } if pyarrow else {}

    ## Fallback pyarrow type if mapped type not found
    DEFAULT_TYPE = pyarrow.string() if pyarrow else None

    ## Default columns for message type tables
    MESSAGE_TYPE_BASECOLS  = [("_topic",      "string"),
                              ("_timestamp",  "time"), ]

    ## Additional default columns for messaga type tables with nesting output
    MESSAGE_TYPE_NESTCOLS  = [("_id",          "string"),
                              ("_parent_type", "string"),
                              ("_parent_id",   "string"), ]

    ## Custom arguments for pyarrow.parquet.ParquetWriter
    WRITER_ARGS = {"version": "2.6"}

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(EMIT_FIELD=(), META=False, NOEMIT_FIELD=(), WRITE_OPTIONS={},
                        VERBOSE=False)


    def __init__(self, args=None, **kwargs):
        """
        @param   args                 arguments as namespace or dictionary, case-insensitive;
                                      or a single path as the base name of Parquet files to write
        @param   args.emit_field      message fields to emit in output if not all
        @param   args.noemit_field    message fields to skip in output
        @param   args.write           base name of Parquet files to write
        @param   args.write_options   ```
                                      {"column": additional columns as {name: (rostype, value)},
                                       "type": {rostype: PyArrow type or typename like "uint8"},
                                       "writer": dictionary of arguments passed to ParquetWriter,
                                       "idgenerator": callable or iterable for producing message IDs
                                                      like uuid.uuid4 or itertools.count();
                                                      nesting uses UUID values by default,
                                       "column-k=rostype:v": one "column"-argument
                                                             in flat string form,
                                       "type-k=v: one "type"-argument in flat string form,
                                       "writer-k=v": one "writer"-argument in flat string form,
                                       "nesting": "array" to recursively insert arrays
                                                  of nested types, or "all" for any nesting,
                                       "overwrite": whether to overwrite existing file
                                                    (default false)}
                                      ```
        @param   args.meta            whether to print metainfo
        @param   args.verbose         whether to print debug information
        @param   kwargs               any and all arguments as keyword overrides, case-insensitive
        """
        args = {"WRITE": str(args)} if isinstance(args, common.PATH_TYPES) else args
        args = common.ensure_namespace(args, ParquetSink.DEFAULT_ARGS, **kwargs)
        super(ParquetSink, self).__init__(args)

        self._filebase       = args.WRITE
        self._overwrite      = (args.WRITE_OPTIONS.get("overwrite") in (True, "true"))
        self._filenames      = {}  # {(typename, typehash): Parquet file path}
        self._caches         = {}  # {(typename, typehash): [{data}, ]}
        self._schemas        = {}  # {(typename, typehash): pyarrow.Schema}
        self._writers        = {}  # {(typename, typehash): pyarrow.parquet.ParquetWriter}
        self._extra_basecols = []  # [(name, rostype)]
        self._extra_basevals = []  # [(name, value)]
        self._patterns       = {}  # {key: [(() if any field else ('path', ), re.Pattern), ]}
        self._nesting        = args.WRITE_OPTIONS.get("nesting")
        self._idgenerator    = iter(lambda: str(uuid.uuid4()), self) if self._nesting else None

        self._close_printed = False


    def validate(self):
        """
        Returns whether required libraries are available (pandas and pyarrow) and overwrite is valid
        and file base is writable.
        """
        if self.valid is not None: return self.valid
        ok, pandas_ok, pyarrow_ok = self._configure(), bool(pandas), bool(pyarrow)
        if self.args.WRITE_OPTIONS.get("overwrite") not in (None, True, False, "true", "false"):
            ConsolePrinter.error("Invalid overwrite option for Parquet: %r. "
                                 "Choose one of {true, false}.",
                                 self.args.WRITE_OPTIONS["overwrite"])
            ok = False
        if self.args.WRITE_OPTIONS.get("nesting") not in (None, "", "array", "all"):
            ConsolePrinter.error("Invalid nesting option for Parquet: %r. "
                                 "Choose one of {array,all}.",
                                 self.args.WRITE_OPTIONS["nesting"])
            ok = False
        if not pandas_ok:
            ConsolePrinter.error("pandas not available: cannot write Parquet files.")
        if not pyarrow_ok:
            ConsolePrinter.error("PyArrow not available: cannot write Parquet files.")
        if not common.verify_io(self.args.WRITE, "w"):
            ok = False
        self.valid = ok and pandas_ok and pyarrow_ok
        return self.valid


    def emit(self, topic, msg, stamp=None, match=None, index=None):
        """Writes message to a Parquet file."""
        if not self.validate(): raise Exception("invalid")
        stamp, index = self._ensure_stamp_index(topic, msg, stamp, index)
        self._process_type(topic, msg)
        self._process_message(topic, index, stamp, msg, match)


    def close(self):
        """Writes out any remaining messages, closes writers, clears structures."""
        try:
            for k, vv in list(self._caches.items()):
                vv and self._write_table(k)
            for k in list(self._writers):
                self._writers.pop(k).close()
        finally:
            if not self._close_printed and self._counts:
                self._close_printed = True
                sizes = {n: None for n in self._filenames.values()}
                for n in self._filenames.values():
                    try: sizes[n] = os.path.getsize(n)
                    except Exception as e: ConsolePrinter.warn("Error getting size of %s: %s", n, e)
                ConsolePrinter.debug("Wrote %s in %s to %s (%s):",
                                     common.plural("message", sum(self._counts.values())),
                                     common.plural("topic", self._counts),
                                     common.plural("Parquet file", sizes),
                                     common.format_bytes(sum(filter(bool, sizes.values()))))
                for (t, h), name in self._filenames.items():
                    count = sum(c for (_, t_, h_), c in self._counts.items() if (t, h) == (t_, h_))
                    ConsolePrinter.debug("- %s (%s, %s)", name,
                                         "error getting size" if sizes[name] is None else
                                         common.format_bytes(sizes[name]),
                                         common.plural("message", count))
            self._caches.clear()
            self._schemas.clear()
            self._filenames.clear()


    def _process_type(self, topic, msg, rootmsg=None):
        """Prepares Parquet schema and writer if not existing."""
        rootmsg = rootmsg or msg
        with api.TypeMeta.make(msg, root=rootmsg) as m:
            typename, typehash, typekey = (m.typename, m.typehash, m.typekey)
        if topic and (topic, typename, typehash) not in self._counts and self.args.VERBOSE:
            ConsolePrinter.debug("Adding topic %s in Parquet output.", topic)
        if typekey in self._writers: return

        basedir, basename = os.path.split(self._filebase)
        pathname = os.path.join(basedir, re.sub(r"\W", "__", "%s__%s" % (typename, typehash)))
        filename = os.path.join(pathname, basename)
        if not self._overwrite:
            filename = common.unique_path(filename)

        cols = []
        scalars = set(x for x in self.COMMON_TYPES if "[" not in x)
        fltrs = dict(include=self._patterns["print"], exclude=self._patterns["noprint"])
        for path, value, subtype in api.iter_message_fields(msg, scalars=scalars, **fltrs):
            coltype = self._make_column_type(subtype)
            cols += [(".".join(path), coltype)]
        MSGCOLS = self.MESSAGE_TYPE_BASECOLS + (self.MESSAGE_TYPE_NESTCOLS if self._nesting else [])
        cols += [(c, self._make_column_type(t, fallback="int64" if "time" == t else None))
                 for c, t in MSGCOLS + self._extra_basecols]

        if self.args.VERBOSE:
            sz = os.path.isfile(filename) and os.path.getsize(filename)
            action = "Overwriting" if sz and self._overwrite else "Adding"
            ConsolePrinter.debug("%s type %s in Parquet output.", action, typename)
        common.makedirs(pathname)

        schema = pyarrow.schema(cols)
        writer = pyarrow.parquet.ParquetWriter(filename, schema, **self.WRITER_ARGS)
        self._caches[typekey]    = []
        self._filenames[typekey] = filename
        self._schemas[typekey]   = schema
        self._writers[typekey]   = writer

        nesteds = api.iter_message_fields(msg, messages_only=True, **fltrs) if self._nesting else ()
        for path, submsgs, subtype in nesteds:
            scalartype = api.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for path
            subtypehash = not submsgs and self.source.get_message_type_hash(scalartype)
            if not isinstance(submsgs, (list, tuple)): submsgs = [submsgs]
            [submsg] = submsgs[:1] or [self.source.get_message_class(scalartype, subtypehash)()]
            self._process_type(None, submsg, rootmsg)


    def _process_message(self, topic, index, stamp, msg, match=None,
                         rootmsg=None, parent_type=None, parent_id=None):
        """
        Converts message to pandas dataframe, adds to cache.

        Writes cache to disk if length reached chunk size.

        If nesting is enabled, processes nested messages for subtypes in message.
        If IDs are used, returns generated ID.
        """
        data, myid, rootmsg = {}, None, (rootmsg or None)
        if self._idgenerator: myid = next(self._idgenerator)
        with api.TypeMeta.make(msg, topic, root=rootmsg) as m:
            typename, typekey = m.typename, m.typekey
        fltrs = dict(include=self._patterns["print"], exclude=self._patterns["noprint"])
        for p, v, t in api.iter_message_fields(msg, scalars=set(self.COMMON_TYPES), **fltrs):
            data[".".join(p)] = self._make_column_value(v, t)
        data.update(_topic=topic, _timestamp=self._make_column_value(stamp, "time"))
        if self._idgenerator: data.update(_id=myid)
        if self._nesting:
            COLS = [k for k, _ in self.MESSAGE_TYPE_NESTCOLS if "parent" in k]
            data.update(zip(COLS, [parent_type, parent_id]))
        data.update(self._extra_basevals)
        self._caches[typekey].append(data)
        super(ParquetSink, self).emit(topic, msg, stamp, match, index)

        subids = {}  # {message field path: [ids]}
        nesteds = api.iter_message_fields(msg, messages_only=True, **fltrs) if self._nesting else ()
        for path, submsgs, subtype in nesteds:
            scalartype = api.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for path
            if isinstance(submsgs, (list, tuple)):
                subids[path] = []
            for submsg in submsgs if isinstance(submsgs, (list, tuple)) else [submsgs]:
                subid = self._process_message(topic, index, stamp, submsg,
                                              rootmsg=rootmsg, parent_type=typename, parent_id=myid)
                if isinstance(submsgs, (list, tuple)):
                    subids[path].append(subid)
        data.update(subids)

        if len(self._caches[typekey]) >= self.CHUNK_SIZE:
            self._write_table(typekey)
        return myid


    def _make_column_type(self, typename, fallback=None):
        """
        Returns pyarrow type for ROS type.

        @param  fallback  fallback typename to use for lookup if typename not found
        """
        noboundtype = api.canonical(typename, unbounded=True)
        scalartype  = api.scalar(typename)
        timetype    = api.get_ros_time_category(scalartype)
        coltype     = self.COMMON_TYPES.get(typename) or self.COMMON_TYPES.get(noboundtype)

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
            noboundtype = api.canonical(typename, unbounded=True)
            if v and api.is_ros_time(v[0]):
                v = [api.to_nsec(x) for x in v]
            elif api.scalar(typename) not in api.ROS_BUILTIN_TYPES:
                v = str([api.message_to_dict(m) for m in v])
            elif pyarrow.binary() in (self.COMMON_TYPES.get(typename),
                                      self.COMMON_TYPES.get(noboundtype)):
                v = bytes(bytearray(v))  # Py2/Py3 compatible
            else:
                v = list(v)  # Ensure lists not tuples
        elif api.is_ros_time(v):
            v = api.to_nsec(v)
        elif typename and typename not in api.ROS_BUILTIN_TYPES:
            v = str(api.message_to_dict(v))
        return v


    def _write_table(self, typekey):
        """Writes out cached messages for type."""
        dicts = self._caches[typekey][:]
        del self._caches[typekey][:]
        mapping = {k: [d[k] for d in dicts] for k in dicts[0]}
        table = pyarrow.Table.from_pydict(mapping, self._schemas[typekey])
        self._writers[typekey].write_table(table)


    def _configure(self):
        """Parses args.WRITE_OPTIONS, returns success."""
        ok = self._configure_ids()

        def process_column(name, rostype, value):  # Parse "column-name=rostype:value"
            v, myok = value, True
            if "string" not in rostype:
                v = json.loads(v)
            if not name:
                myok = False
                ConsolePrinter.error("Invalid name option in %s=%s:%s", name, rostype, v)
            if rostype not in api.ROS_BUILTIN_TYPES:
                myok = False
                ConsolePrinter.error("Invalid type option in %s=%s:%s", name, rostype, v)
            if myok:
                self._extra_basecols.append((name, rostype))
                self._extra_basevals.append((name, value))

        def process_type(rostype, arrowtype):  # Eval pyarrow datatype from value like "float64()"
            if arrowtype not in self.ARROW_TYPES.values():
                arrowtype = eval(compile(arrowtype, "", "eval"), {"__builtins__": self.ARROW_TYPES})
            self.COMMON_TYPES[rostype] = arrowtype

        for key, vals in [("print", self.args.EMIT_FIELD), ("noprint", self.args.NOEMIT_FIELD)]:
            self._patterns[key] = [(tuple(v.split(".")), common.wildcard_to_regex(v)) for v in vals]

        # Populate ROS type aliases like "byte" and "char"
        for rostype in list(self.COMMON_TYPES):
            alias = api.get_type_alias(rostype)
            if alias:
                self.COMMON_TYPES[alias] = self.COMMON_TYPES[rostype]
            if alias and rostype + "[]" in self.COMMON_TYPES:
                self.COMMON_TYPES[alias + "[]"] = self.COMMON_TYPES[rostype + "[]"]

        for k, v in self.args.WRITE_OPTIONS.items():
            if "column" == k and v and isinstance(v, dict):
                for name, (rostype, value) in v.items():
                    if not process_column(name, rostype, value): ok = False
            elif "type" == k and v and isinstance(v, dict):
                for name, value in v.items():
                    if not process_type(name, value): ok = False
            elif "writer" == k and v and isinstance(v, dict):
                self.WRITER_ARGS.update(v)
            elif isinstance(k, str) and "-" in k:
                category, name = k.split("-", 1)
                if category not in ("column", "type", "writer"):
                    ConsolePrinter.warn("Unknown %r option in %s=%s", category, k, v)
                    continue  # for k, v
                try:
                    if not name: raise Exception("empty name")
                    if "column" == category:    # column-name=rostype:value
                        if not process_column(name, *v.split(":", 1)): ok = False
                    elif "type" == category:    # type-rostype=arrowtype
                        process_type(name, v)
                    elif "writer" == category:  # writer-argname=argvalue
                        try: v = json.loads(v)
                        except Exception: pass
                        self.WRITER_ARGS[name] = v
                except Exception as e:
                    ok = False
                    ConsolePrinter.error("Invalid %s option in %s=%s: %s", category, k, v, e)
        return ok


    def _configure_ids(self):
        """Configures ID generator from args.WRITE_OPTIONS, returns success."""
        ok = True

        k, v = "idgenerator", self.args.WRITE_OPTIONS.get("idgenerator")
        if k in self.args.WRITE_OPTIONS:
            val, ex, ns = v, None, dict(self.ARROW_TYPES, itertools=itertools, uuid=uuid)
            for root in v.split(".", 1)[:1]:
                try: ns[root] = common.import_item(root)  # Provide root module
                except Exception: pass
            try: common.import_item(re.sub(r"\(.+", "", v))  # Ensure nested imports
            except Exception: pass
            try: val = eval(compile(v, "", "eval"), ns)
            except Exception as e: ok, ex = False, e
            if isinstance(val, (six.binary_type, six.text_type)): ok = False

            if ok:
                try: self._idgenerator = iter(val)
                except Exception as e:
                    try: self._idgenerator = iter(val, self)  # (callable=val, sentinel=self)
                    except Exception as e: ok, ex = False, e
            if not ok:
                ConsolePrinter.error("Invalid value in %s=%s%s", k, v, (": %s" % ex if ex else ""))
            elif not self._nesting:
                self.MESSAGE_TYPE_BASECOLS.append(("_id", "string"))

            if ok and self._idgenerator:  # Detect given ID column type
                fval, typename, generator = next(self._idgenerator), None, self._idgenerator
                if api.is_ros_time(fval):
                    typename = "time" if "time" in str(type(fval)).lower() else "duration"
                elif isinstance(fval, six.integer_types):
                    typename = "int64"
                elif isinstance(fval, float):
                    typename = "float64"
                elif not isinstance(fval, str):  # Cast whatever it is to string
                    fval, self._idgenerator = str(fval), (str(x) for x in generator)
                if typename:
                    repl = lambda n, t: (n, typename) if "_id" in n else (n, t)
                    self.MESSAGE_TYPE_BASECOLS = [repl(*x) for x in self.MESSAGE_TYPE_BASECOLS]
                    self.MESSAGE_TYPE_NESTCOLS = [repl(*x) for x in self.MESSAGE_TYPE_NESTCOLS]
                self._idgenerator = itertools.chain([fval], self._idgenerator)
        return ok



def init(*_, **__):
    """Adds Parquet output format support. Raises ImportWarning if libraries not available."""
    if not pandas or not pyarrow:
        ConsolePrinter.error("pandas or PyArrow not available: cannot write Parquet files.")
        raise ImportWarning()
    from .. import plugins  # Late import to avoid circular
    plugins.add_write_format("parquet", ParquetSink, "Parquet", [
        ("column-NAME=ROSTYPE:VALUE",  "additional column to add in Parquet output,\n"
                                       "like column-bag_hash=string:26dfba2c"),
        ("idgenerator=CALLABLE",       "callable or iterable for producing message IDs \n"
                                       "in Parquet output, like 'uuid.uuid4' or 'itertools.count()';\n"
                                       "nesting uses UUID values by default"),
        ("nesting=array|all",          "create tables for nested message types\n"
                                       "in Parquet output,\n"
                                       'only for arrays if "array" \n'
                                       "else for any nested types\n"
                                       "(array fields in parent will be populated \n"
                                       " with foreign keys instead of messages as JSON)"),
        ("overwrite=true|false",       "overwrite existing file in Parquet output\n"
                                       "instead of appending unique counter (default false)"),
        ("type-ROSTYPE=ARROWTYPE",     "custom mapping between ROS and pyarrow type\n"
                                       "for Parquet output, like type-time=\"timestamp('ns')\"\n"
                                       "or type-uint8[]=\"list(uint8())\""),
        ("writer-ARGNAME=ARGVALUE",    "additional arguments for Parquet output\n"
                                       "given to pyarrow.parquet.ParquetWriter"),
    ])
    plugins.add_output_label("Parquet", ["--emit-field", "--no-emit-field"])


__all__ = ["ParquetSink", "init"]

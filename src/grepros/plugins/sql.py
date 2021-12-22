# -*- coding: utf-8 -*-
"""
SQL schema output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     20.12.2021
@modified    22.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.sql
import atexit
import datetime
import os
import re
import sys

import yaml

from .. import rosapi
from .. common import ConsolePrinter, ellipsize, format_bytes, makedirs, \
                      merge_dicts, plural, quote, unique_path
from .. outputs import SinkBase



class SqlSink(SinkBase):
    """
    Writes SQL schema file for message type tables and topic views.
    """

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".sql", )

    ## Supported SQL dialects and options
    DIALECTS = {

        None: {
            # CREATE TABLE template, args: table, cols, type, hash, package, class
            "table_template":       "CREATE TABLE IF NOT EXISTS {table} ({cols});",
            # CREATE VIEW template, args: view, cols, table, topic, type, hash, package, class
            "view_template":        """
CREATE VIEW IF NOT EXISTS {view} AS
SELECT {cols}
FROM {table}
WHERE _topic = {topic};""",
            "types":                  {},        # Mapping between ROS and SQL common types
            "defaulttype":          None,        # Fallback SQL type if no mapped type for ROS type
            "arraytype_template":   "{type}[]",  # Array type template, args: type
            "maxlen_entity":           0,        # Maximum table/view name length, 0 disables
            "maxlen_column":           0,        # Maximum column name length, 0 disables
            "invalid_char_regex":   None,        # Regex for matching invalid characters in name
            "invalid_char_repl":    "__",        # Replacement for invalid characters in name
        },

        "sqlite": {},

        "postgres": {
            "view_template": """
CREATE OR REPLACE VIEW {view} AS
SELECT {cols}
FROM {table}
WHERE _topic = {topic};""",
            "types": {
                "byte":    "SMALLINT", " char":    "SMALLINT", "int8":    "SMALLINT",
                "int16":   "SMALLINT", "int32":    "INTEGER",  "int64":   "BIGINT",
                "uint8":   "SMALLINT", "uint16":   "INTEGER",  "uint32":  "BIGINT",
                "uint64":  "BIGINT",   "float32":  "REAL",     "float64": "DOUBLE PRECISION",
                "bool":    "BOOLEAN",  "string":   "TEXT",     "wstring": "TEXT",
                "uint8[]": "BYTEA",    "char[]":   "BYTEA",
            },
            "defaulttype":    "JSONB",
            "maxlen_entity":  63,
            "maxlen_column":  63,
        },

        "clickhouse": {
            "table_template":      "CREATE TABLE IF NOT EXISTS {table} ({cols}) ENGINE = ENGINE;",
            "types": {
                "byte":    "UInt8",  "char":     "Int8",    "int8":    "Int8",
                "int16":   "Int16",  "int32":    "Int32",   "int64":   "Int64",
                "uint8":   "UInt8",  "uint16":   "UInt16",  "uint32":  "UInt32",
                "uint64":  "UInt64", "float32":  "Float32", "float64": "Float64",
                "bool":    "UInt8",  "string":   "String",  "wstring": "String",
                "uint8[]": "String", "char[]":   "String",
            },
            "defaulttype":         "String",
            "arraytype_template":  "Array({type})",
        },
    }

    ## Default SQL dialect used if dialect not specified
    DEFAULT_DIALECT = "sqlite"

    ## Default columns for message type tables
    MESSAGE_TYPE_BASECOLS  = [("_topic",      "string"),
                              ("_timestamp",  "time"), ]


    def __init__(self, args):
        """
        @param   args                arguments object like argparse.Namespace
        @param   args.DUMP_OPTIONS   {"dialect": SQL dialect if not default,
                                      "nesting": true|false to created nested type tables}
        """
        super(SqlSink, self).__init__(args)

        self._dialect       = args.DUMP_OPTIONS.get("dialect", self.DEFAULT_DIALECT)
        self._filename      = None   # Unique output filename
        self._file          = None   # Open file() object
        self._batch         = None   # Current source batch
        self._types         = {}     # {(typename, typehash): "CREATE TABLE .."}
        self._nested_types  = {}     # {(typename, typehash): "CREATE TABLE .."}
        self._topics        = {}     # {(topic, typename, typehash): "CREATE VIEW .."}
        self._metas         = []     # [source batch metainfo string, ]
        self._close_printed = False

        # Whether to create tables for nested message types,
        # "array" if to do this only for arrays of nested types, or
        # "all" for any nested type, including those fully flattened into parent fields.
        self._nesting       = args.DUMP_OPTIONS.get("nesting")

        atexit.register(self.close)


    def validate(self):
        """
        Returns whether "dialect" and "nesting" parameters contain supported values.
        """
        ok = True
        if self._args.DUMP_OPTIONS.get("dialect-file"):
            filename = self._args.DUMP_OPTIONS["dialect-file"]
            try:
                with open(filename) as f:
                    dialects = yaml.safe_load(f.read())
                if any(not isinstance(v, dict) for v in dialects.values()):
                    raise Exception("Each dialect must be a dictionary.") 
                merge_dicts(self.DIALECTS, dialects)
            except Exception as e:
                ok = False
                ConsolePrinter.error("Error reading SQL dialect file %r: %s", filename, e)
        if "dialect" in self._args.DUMP_OPTIONS \
        and self._args.DUMP_OPTIONS["dialect"] not in tuple(filter(bool, self.DIALECTS)):
            ok = False
            ConsolePrinter.error("Unknown dialect for SQL: %r. "
                                 "Choose one of {%s}.",
                                 self._args.DUMP_OPTIONS["dialect"],
                                 "|".join(sorted(filter(bool, self.DIALECTS))))
        if self._args.DUMP_OPTIONS.get("nesting") not in (None, "array", "all"):
            ConsolePrinter.error("Invalid nesting option for SQL: %r. "
                                 "Choose one of {array,all}.",
                                 self._args.DUMP_OPTIONS["nesting"])
            ok = False
        return ok


    def emit(self, topic, index, stamp, msg, match):
        """Writes out message type CREATE TABLE statements to SQL schema file."""
        batch = self.source.get_batch()
        if not self._metas or batch != self._batch:
            self._batch = batch
            self._metas.append(self.source.format_meta())
        self._ensure_open()
        self._process_type(msg)
        self._process_topic(topic, msg)


    def close(self):
        """Rewrites out everything to SQL schema file, ensuring all source metas."""
        if self._file:
            self._file.seek(0)
            self._write_header()
            for key in sorted(self._types):
                self._write_entity("table", self._types[key])
            for key in sorted(self._topics):
                self._write_entity("view", self._topics[key])
            self._file.close()
            self._file = None
        if not self._close_printed and self._types:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s and %s to SQL %s (%s).",
                                 plural("message type table",
                                        len(self._types) - len(self._nested_types)),
                                 plural("topic view", self._topics), self._filename,
                                 format_bytes(os.path.getsize(self._filename)))
            if self._nested_types:
                ConsolePrinter.debug("Wrote %s to SQL %s.",
                                     plural("nested message type table", self._nested_types),
                                     self._filename)
        self._types.clear()
        self._nested_types.clear()
        self._topics.clear()
        del self._metas[:]


    def _ensure_open(self):
        """Opens output file if not already open, writes header."""
        if self._file: return

        self._filename = unique_path(self._args.DUMP_TARGET)
        makedirs(os.path.dirname(self._filename))
        self._file = open(self._filename, "wb")
        self._write_header()


    def _process_topic(self, topic, msg):
        """Builds and writes CREATE VIEW statement for topic if not already built."""
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        topickey = (topic, typename, typehash)
        if topickey in self._topics:
            return

        table_name = self._types[(typename, typehash)]["table"]
        view_name  = self._make_entity_name("view",  "%s (%s) (%s)" % (topic, typename, typehash))
        pkgname, clsname = typename.split("/", 1)
        sqlargs = {"view": quote(view_name), "table": quote(table_name, force=True),
                   "topic": repr(topic), "cols": "*", "type": typename, "hash": typehash,
                   "package": pkgname, "class": clsname}
        sql = self._get_dialect_option("view_template").strip().format(**sqlargs)
        self._topics[topickey] = {"topic": topic, "type": typename, "md5": typehash,
                                  "sql": sql, "table": table_name, "view": view_name}
        self._write_entity("view", self._topics[topickey])


    def _process_type(self, msg):
        """
        Builds and writes CREATE TABLE statement for message type if not already built.

        Builds statements recursively for nested types if configured.

        @return   built SQL, or None if already built
        """
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        typekey  = (typename, typehash)
        if typekey in self._types:
            return None

        cols = []
        scalars = set(x for x in self._get_dialect_option("types") if "[" not in x)
        for path, value, subtype in rosapi.iter_message_fields(msg, scalars=scalars):
            coltype = self._make_column_type(subtype)
            cols += [(".".join(path), coltype)]
        cols += [(c, self._make_column_type(t)) for c, t in self.MESSAGE_TYPE_BASECOLS]
        cols = list(zip(self._make_column_names([c for c, _ in cols]), [t for _, t in cols]))

        namewidth = 2 + max(len(n) for n, _ in cols)
        coldefs = ["%s  %s" % (quote(n).ljust(namewidth), t) for n, t in cols]
        table_name = self._make_entity_name("table", "%s (%s)" % (typename, typehash))
        pkgname, clsname = typename.split("/", 1)
        sqlargs = {"table": quote(table_name), "cols": "\n  %s\n" % ",\n  ".join(coldefs),
                   "type": typename, "hash": typehash, "package": pkgname, "class": clsname}
        sql = self._get_dialect_option("table_template").strip().format(**sqlargs)
        self._types[typekey] = {"type": typename, "md5": typehash,
                                "msgdef": self.source.get_message_definition(msg),
                                "table": table_name, "sql": sql}
        self._write_entity("table", self._types[typekey])
        if self._nesting: self._process_nested(msg)
        return sql


    def _process_nested(self, msg):
        """Builds anr writes CREATE TABLE statements for nested types."""
        nesteds = rosapi.iter_message_fields(msg, messages_only=True) if self._nesting else ()
        for path, submsgs, subtype in nesteds:
            scalartype = rosapi.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for path
            subtypehash = self.source.get_message_type_hash(scalartype)
            subtypekey = (scalartype, subtypehash)
            if subtypekey in self._types:
                continue  # for path

            if not isinstance(submsgs, (list, tuple)): submsgs = [submsgs]
            for submsg in submsgs[:1] or [self.source.get_message_class(scalartype, subtypehash)()]:
                subsql = self._process_type(submsg)
                if subsql: self._nested_types[subtypekey] = subsql


    def _make_entity_name(self, category, name):
        """Returns valid unique name for table/view."""
        existing = set(sum(([x["table"], x.get("view")]
                            for dct in (self._topics, self._types)
                            for x in dct.values()), []))
        return self._make_name("entity", name, existing)


    def _make_column_names(self, col_names):
        """Returns valid unique names for table columns."""
        result = []
        for name in col_names:
            result.append(self._make_name("column", name, result))
        return list(result)


    def _make_name(self, category, name, existing):
        """
        Returns a valid unique name for table/view/column.

        Replaces invalid characters and constrains length.
        """
        MAXLEN_ARG   = "maxlen_column" if "column" == category else "maxlen_entity"
        MAXLEN       = self._get_dialect_option(MAXLEN_ARG)
        INVALID_RGX  = self._get_dialect_option("invalid_char_regex")
        INVALID_REPL = self._get_dialect_option("invalid_char_repl")
        if not MAXLEN and not INVALID_RGX: return name

        name1 = re.sub(INVALID_RGX, INVALID_REPL, name) if INVALID_RGX else name
        name2 = ellipsize(name1, MAXLEN)
        counter = 2
        while name2 in existing:
            suffix = " (%s)" % counter
            name2 = ellipsize(name1, MAXLEN - len(suffix)) + suffix
            counter += 1
        return name2


    def _make_column_type(self, typename):
        """Returns column type for SQL."""
        TYPES         = self._get_dialect_option("types")
        ARRAYTEMPLATE = self._get_dialect_option("arraytype_template")
        DEFAULTTYPE   = self._get_dialect_option("defaulttype")

        coltype    = TYPES.get(typename)
        scalartype = rosapi.get_ros_time_category(rosapi.scalar(typename))
        timetype   = rosapi.get_ros_time_category(scalartype)
        if not coltype and scalartype in TYPES:
            coltype = ARRAYTEMPLATE.format(type=TYPES[scalartype])
        if not coltype and timetype in TYPES:
            if typename != scalartype:
                coltype = ARRAYTEMPLATE.format(type=TYPES[timetype])
            else:
                coltype = TYPES[timetype]
        if not coltype:
            coltype = DEFAULTTYPE or quote(typename)
        return coltype


    def _get_dialect_option(self, option):
        """Returns option for current SQL dialect, falling back to default dialect."""
        return self.DIALECTS[self._dialect].get(option, self.DIALECTS[None].get(option))


    def _write_header(self):
        """Writes header to current file."""
        args = {
            "dialect":  self._dialect,
            "args":      " ".join(sys.argv[1:]),
            "source":   "\n\n".join("-- Source:\n" + 
                                    "\n".join("-- " + x for x in s.strip().splitlines())
                                    for s in self._metas),
            "dt":       datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        self._file.write((
            "-- SQL dialect: {dialect}.\n"
            "-- Written by grepros on {dt}.\n"
            "-- Command: grepros {args}.\n"
            "\n{source}\n\n"
        ).format(**args).encode("utf-8"))


    def _write_entity(self, category, item):
        """Writes table or view SQL statement to file."""
        self._file.write(b"\n")
        if "table" == category:
            self._file.write(("-- Message type %(type)s (%(md5)s)\n" % item).encode("utf-8"))
            self._file.write(("-- %s\n" % "\n-- ".join(item["msgdef"].splitlines())).encode("utf-8"))
        else:
            self._file.write(('-- Topic "%(topic)s": %(type)s (%(md5)s)\n' % item).encode("utf-8"))
        self._file.write(("%s\n\n" % item["sql"]).encode("utf-8"))



def init(*_, **__):
    """Adds SQL schema output format support."""
    from .. import plugins  # Late import to avoid circular
    plugins.add_write_format("sql", SqlSink, "SQL", [
        ("dialect=" + "|".join(sorted(filter(bool, SqlSink.DIALECTS))),
                                     "use specified SQL dialect in SQL output\n"
                                     '(default "%s")' % SqlSink.DEFAULT_DIALECT),
        ("dialect-file=path/to/dialects.yaml",
                                     "load additional SQL dialects for SQL output\n"
                                     "from a YAML or JSON file"),
        ("nesting=array|all",        "create tables for nested message types\n"
                                     "in SQL output,\n"
                                     'only for arrays if "array" \n'
                                     "else for any nested types"),
    ])

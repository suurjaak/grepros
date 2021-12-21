# -*- coding: utf-8 -*-
"""
SQL schema output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     20.12.2021
@modified    21.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.sql
import atexit
import collections
import datetime
import os
import re
import sys

from .. import rosapi
from .. common import ConsolePrinter, ellipsize, format_bytes, makedirs, plural, quote, unique_path
from .. outputs import SinkBase


## Strips leading and trailing whitespace from each line in string.
LINESTRIP = lambda v: re.sub("(^ +)|( +$)", "", v, flags=re.M)


class SqlSink(SinkBase):
    """
    Writes SQL schema file for message type tables and topic views.
    """

    ## Supported SQL dialects and options
    DIALECTS = {
        "default": {
            "array":   "%s[]",  # Array type template, given type name argument
            "types":   {},
            "maxlen":  0,
            "table":   "CREATE TABLE IF NOT EXISTS %(table)s (%(cols)s);",
            "view": """CREATE VIEW IF NOT EXISTS %(view)s AS
                       SELECT %(cols)s
                       FROM %(table)s
                       WHERE _topic = %(topic)s;""",
        },
        "sqlite": {},
        "postgres": {
            "maxlen":  63,
            "types": {
                "byte":    "SMALLINT", " char":    "SMALLINT", "int8":    "SMALLINT",
                "int16":   "SMALLINT", "int32":    "INTEGER",  "int64":   "BIGINT",
                "uint8":   "SMALLINT", "uint16":   "INTEGER",  "uint32":  "BIGINT",
                "uint64":  "BIGINT",   "float32":  "REAL",     "float64": "DOUBLE PRECISION",
                "bool":    "BOOLEAN",  "string":   "TEXT",     "wstring": "TEXT",
                "uint8[]": "BYTEA",    "char[]":   "BYTEA",
                "time":    "NUMERIC",  "duration": "NUMERIC",
                "builtin_interfaces/Time":         "NUMERIC",
                "builtin_interfaces/Duration":     "NUMERIC",
            },
            "defaulttype": "JSONB",
            "view": """CREATE OR REPLACE VIEW %(view)s AS
                       SELECT %(cols)s
                       FROM %(table)s
                       WHERE _topic = %(topic)s;""",
        },
        "clickhouse": {
            "array":   "Array(%s)",  # Array type template, given type name argument
            "table":   "CREATE TABLE IF NOT EXISTS %(table)s (%(cols)s) ENGINE = ENGINE;",
            "types": {
                "byte":    "UInt8",  "char":     "Int8",    "int8":    "Int8",
                "int16":   "Int16",  "int32":    "Int32",   "int64":   "Int64",
                "uint8":   "UInt8",  "uint16":   "UInt16",  "uint32":  "UInt32",
                "uint64":  "UInt64", "float32":  "Float32", "float64": "Float64",
                "bool":    "UInt8",  "string":   "String",  "wstring": "String",
                "uint8[]": "String", "char[]":   "String",
                "time":                          "DateTime64(9)",
                "duration":                      "DateTime64(9)",
                "builtin_interfaces/Time":       "DateTime64(9)",
                "builtin_interfaces/Duration":   "DateTime64(9)",
            },
            "defaulttype": "String",
        },
    }


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

        self._filebase      = args.DUMP_TARGET
        self._dialect       = args.DUMP_OPTIONS.get("dialect", "default")
        self._filename      = None   # Unique output filename
        self._file          = None   # Open file() object
        self._batch         = None   # Current source batch
        self._types         = {}     # {(typename, typehash): "CREATE TABLE .."}
        self._nested_types  = {}     # {(typename, typehash): "CREATE TABLE .."}
        self._topics        = {}     # {(topic, typename, typehash): "CREATE VIEW .."}
        self._metas         = []     # [source metainfo string, ]
        self._close_printed = False

        # Whether to create tables and rows for nested message types,
        # "array" if to do this only for arrays of nested types, or
        # "all" for any nested type, including those fully flattened into parent fields.
        self._nesting       = args.DUMP_OPTIONS.get("nesting")

        atexit.register(self.close)


    def validate(self):
        """
        Returns whether "dialect" and "nesting" parameters contain supported values.
        """
        ok = True
        if self._args.DUMP_OPTIONS.get("dialect") not in tuple(self.DIALECTS) + (None, ):
            ok = False
            ConsolePrinter.error("Unknown dialect for SQL: %r."
                                 "Choose one of {%s}.",
                                 self._args.DUMP_OPTIONS["dialect"],
                                 "|".join(sorted(self.DIALECTS)))
        if self._args.DUMP_OPTIONS.get("nesting") not in (None, "array", "all"):
            ConsolePrinter.error("Invalid nesting option for SQL: %r. "
                                 "Choose one of {array,all}.",
                                 self._args.DUMP_OPTIONS["nesting"]) # @todo või ehk global "nesting" ?
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
        """Rewrites out everything to SQL schema file."""
        if self._file:
            self._file.seek(0)
            self._write_header()
            for key in sorted(self._types):
                self._write_item("table", self._types[key])
            for key in sorted(self._topics):
                self._write_item("view", self._topics[key])
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


    def _get_dialect_option(self, option):
        """Returns option for current SQL dialect, falling back to default dialect."""
        return self.DIALECTS[self._dialect].get(option, self.DIALECTS["default"].get(option))


    def _ensure_open(self):
        """Opens output file if not already open, writes header."""
        if self._file: return

        self._filename = unique_path(self._filebase)
        makedirs(os.path.dirname(self._filename))
        self._file = open(self._filename, "wb")
        self._write_header()


    def _process_topic(self, topic, msg):
        """Builds CREATE VIEW statement for topic if not already built."""
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        topickey = (topic, typename, typehash)
        if topickey in self._topics:
            return

        table_name = self._make_name("%s (%s)" % (typename, typehash))
        view_name  = self._make_name("%s (%s) (%s)" % (topic, typename, typehash))
        sqlargs = {"view": quote(view_name),
                   "table": quote(table_name, force=True),
                   "topic": repr(topic), "cols": "*", }
        template = self._get_dialect_option("view")
        sql = LINESTRIP(template) % sqlargs
        self._topics[topickey] = {"topic": topic, "type": typename, "md5": typehash,
                                  "sql": sql, "table": table_name, "view": view_name}


    def _process_type(self, msg):
        """
        Builds CREATE TABLE statement for message type if not already built.

        Builds statements recursively for nested types if configured.

        @return   built SQL, or None if already built
        """
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        typekey  = (typename, typehash)
        if typekey in self._types:
            return None

        cols = []
        for path, value, subtype in rosapi.iter_message_fields(msg):
            coltype = self._make_column_type(subtype)
            cols += [(".".join(path), coltype)]
        cols += [(c, self._make_column_type(t)) for c, t in self.MESSAGE_TYPE_BASECOLS]
        cols = list(zip(self._make_column_names([c for c, _ in cols]), [t for _, t in cols]))

        namewidth = 2 + max(len(n) for n, _ in cols)
        coldefs = ["%s  %s" % (quote(n).ljust(namewidth), t) for n, t in cols]
        table_name = self._make_name("%s (%s)" % (typename, typehash))
        sqlargs = {"table": quote(table_name), "cols": "\n  %s\n" % ",\n  ".join(coldefs), }
        template = self._get_dialect_option("table")
        sql = LINESTRIP(template) % sqlargs
        self._types[typekey] = {"type": typename, "md5": typehash,
                                "msgdef": self.source.get_message_definition(msg),
                                "table": table_name, "sql": sql}

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
        return sql


    def _make_name(self, name):
        """Returns valid unique name for table/view."""
        maxlen = self._get_dialect_option("maxlen")
        if not maxlen: return name

        result = ellipsize(name, maxlen)
        existing = set(sum(([x["table"], x.get("view")]
                            for dct in (self._topics, self._types) for x in dct.values()), []))
        counter = 2
        while result in existing:
            suffix = " (%s)" % counter
            result = ellipsize(name, maxlen - len(suffix)) + suffix
            counter += 1
        return result


    def _make_column_names(self, col_names):
        """Returns valid unique names for table columns."""
        maxlen = self._get_dialect_option("maxlen")
        if not maxlen: return list(col_names)

        result = collections.OrderedDict()  # {sql name: original name}
        for name in col_names:
            name_in_sql = ellipsize(name, maxlen)
            counter = 2
            while name_in_sql in result:
                suffix = " (%s)" % counter
                name_in_sql = ellipsize(name, maxlen - len(suffix)) + suffix
                counter += 1
            result[name_in_sql] = name
        return list(result)


    def _make_column_type(self, typename):
        """Returns column type for SQL."""
        TYPES = self._get_dialect_option("types")
        ARRAYTEMPLATE = self._get_dialect_option("array")
        DEFAULTTYPE = self._get_dialect_option("defaulttype")

        coltype = TYPES.get(typename)
        scalartype = rosapi.scalar(typename)
        if not coltype and scalartype in TYPES:
            coltype = ARRAYTEMPLATE % TYPES[scalartype]
        if not coltype:
            coltype = DEFAULTTYPE or quote(typename)
        return coltype


    def _make_column_value(self, value, typename=None):
        """Returns column value suitable for inserting to database."""
        v, scalartype = value, typename and rosapi.scalar(typename)
        if isinstance(v, (list, tuple)) and typename and scalartype not in rosapi.ROS_BUILTIN_TYPES:
            if self._nesting: v = []
            else: v = [rosapi.message_to_dict(x) for x in v]
        return v


    def _write_header(self):
        """Writes header to current file."""
        args = {
            "dialect":  self._dialect or "default",
            "args":      " ".join(sys.argv[1:]),
            "source":   "\n\n".join("-- Source:\n" + "\n".join("-- " + x for x in s.strip().splitlines())
                                    for s in self._metas),
            "dt":       datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        self._file.write((
            "-- SQL dialect: {dialect}.\n"
            "-- Written by grepros on {dt}.\n"
            "-- Command: grepros {args}.\n"
            "\n{source}\n\n"
        ).format(**args).encode("utf-8"))


    def _write_item(self, category, item):
        """Writes table or view SQL statement to file."""
        self._file.write(b"\n")
        if "table" == category:
            self._file.write(("-- Message type %(type)s (%(md5)s)\n" % item).encode("utf-8"))
            self._file.write(("-- %s\n" % "\n-- ".join(item["msgdef"].splitlines())).encode("utf-8"))
        else:
            self._file.write(('-- Topic "%(topic)s": %(type)s (%(md5)s)\n' % item).encode("utf-8"))
        self._file.write(("%s\n\n" % item["sql"]).encode("utf-8"))


def init(*_, **__):
    """Adds SQL format support."""
    from .. import plugins  # Late import to avoid circular
    plugins.add_write_format("sql", SqlSink, "SQL", [
        ("dialect=" + "|".join(sorted(SqlSink.DIALECTS)),
                                     "use specified SQL dialect in SQL output"),
        ("nesting=array|all",        "create tables for nested message types\n"
                                     "in SQL output,\n"
                                     'only for arrays if "array" \n'
                                     "else for any nested types"),
    ])

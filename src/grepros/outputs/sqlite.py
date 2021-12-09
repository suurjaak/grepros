# -*- coding: utf-8 -*-
"""
SQLite output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.12.2021
@modified    09.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs.sqlite
import atexit
import collections
import json
import os
import re
import sqlite3

from .. common import ConsolePrinter, format_bytes, plural, quote
from .. import rosapi
from . base import SinkBase, TextSinkMixin


class SqliteSink(SinkBase, TextSinkMixin):
    """
    Writes messages to an SQLite database.

    Output will have:
    - table "messages", with all messages as YAML and serialized binary
    - table "types", with message definitions
    - table "topics", with topic information

    plus:
    - table "pkg/MsgType" for each message type, with detailed fields,
      and JSON fields for arrays of nested subtypes,
      with foreign keys if nesting else subtype values as JSON dictionaries;
      plus underscore-prefixed fields for metadata, like `_topic` as the topic name.
      If not nesting, only topic message type tables are created.
    - view "/topic/full/name" for each topic,
      selecting from the message type table

    """

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".sqlite", ".sqlite3")

    ## SQL statements for populating database base schema
    BASE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS messages (
      id           INTEGER   PRIMARY KEY,
      topic_id     INTEGER   NOT NULL,
      timestamp    INTEGER   NOT NULL,
      data         BLOB      NOT NULL,

      topic        TEXT      NOT NULL,
      type         TEXT      NOT NULL,
      dt           TIMESTAMP NOT NULL,
      yaml         TEXT      NOT NULL
    );

    CREATE TABLE IF NOT EXISTS types (
      id            INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      type          TEXT    NOT NULL,
      definition    TEXT    NOT NULL,
      md5           TEXT    NOT NULL,
      table_name    TEXT    NOT NULL,
      nested_tables JSON
    );

    CREATE TABLE IF NOT EXISTS topics (
      id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name                 TEXT    NOT NULL,
      type                 TEXT    NOT NULL,
      serialization_format TEXT    DEFAULT "cdr",
      offered_qos_profiles TEXT    DEFAULT "",

      md5                  TEXT NOT NULL,
      count                INTEGER NOT NULL DEFAULT 0,
      dt_first             TIMESTAMP,
      dt_last              TIMESTAMP,
      timestamp_first      INTEGER,
      timestamp_last       INTEGER
    );

    CREATE INDEX timestamp_idx ON messages (timestamp ASC);
    """

    ## SQL statement for inserting messages
    INSERT_MESSAGE = """
    INSERT INTO messages (topic_id, timestamp, data, topic, type, dt, yaml)
    VALUES (:topic_id, :timestamp, :data, :topic, :type, :dt, :yaml)
    """

    ## SQL statement for inserting topics
    INSERT_TOPIC = """
    INSERT INTO topics (name, type, md5, table_name)
    VALUES (:name, :type, :md5, :table_name)
    """

    ## SQL statement for updating topics with latest message
    UPDATE_TOPIC = """
    UPDATE topics SET count = count + 1,
    dt_first = MIN(COALESCE(dt_first, :dt), :dt),
    dt_last  = MAX(COALESCE(dt_last,  :dt), :dt),
    timestamp_first = MIN(COALESCE(timestamp_first, :timestamp), :timestamp),
    timestamp_last  = MAX(COALESCE(timestamp_last,  :timestamp), :timestamp)
    WHERE name = :name AND type = :type
    """

    ## SQL statement for updating view name in topic
    UPDATE_TOPIC_VIEW = """
    UPDATE topics SET view_name = %(view_name)s
    WHERE id = %(id)s
    """

    ## SQL statement for inserting types
    INSERT_TYPE = """
    INSERT INTO types (type, definition, md5, table_name)
    VALUES (:type, :definition, :md5, :table_name)
    """

    ## SQL statement for creating a view for topic
    CREATE_TOPIC_VIEW = """
    DROP VIEW IF EXISTS %(name)s;

    CREATE VIEW %(name)s AS
    SELECT %(cols)s
    FROM %(type)s
    WHERE _topic = %(topic)s;
    """

    ## SQL statement for creating a table for type
    CREATE_TYPE_TABLE = """
    DROP TABLE IF EXISTS %(name)s;

    CREATE TABLE %(name)s (%(cols)s);
    """

    ## SQL statement template for inserting message types
    INSERT_MESSAGE_TYPE = """
    INSERT INTO %(type)s (_topic, _dt, _timestamp, _parent_type, _parent_id%(cols)s)
    VALUES (:_topic, :_dt, :_timestamp, :_parent_type, :_parent_id%(vals)s)
    """

    ## Default topic-related columns for pkg/MsgType tables
    MESSAGE_TYPE_TOPICCOLS = [("_topic",       "TEXT"),
                              ("_topic_id",    "BIGINT"), ]
    ## Default columns for pkg/MsgType tables
    MESSAGE_TYPE_BASECOLS  = [("_dt",          "TIMESTAMP"),
                              ("_timestamp",   "INTEGER"),
                              ("_id",          "INTEGER NOT NULL "
                                               "PRIMARY KEY AUTOINCREMENT"), ]
    ## Additional default columns for pkg/MsgType tables with nesting output
    MESSAGE_TYPE_NESTCOLS  = [("_parent_type", "TEXT"),
                              ("_parent_id",   "INTEGER"), ]


    def __init__(self, args):
        """
        @param   args               arguments object like argparse.Namespace
        @param   args.META          whether to print metainfo
        @param   args.DUMP_TARGET   name of SQLite file to write,
                                    will be appended to if exists
        @param   args.DUMP_OPTIONS  {"nesting": "array" to recursively insert arrays
                                                of nested types, or "all" for any nesting)}
        @param   args.WRAP_WIDTH    character width to wrap message YAML output at
        @param   args.VERBOSE       whether to print debug information
        """
        args = TextSinkMixin.make_full_yaml_args(args)

        super(SqliteSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)

        self._filename      = args.DUMP_TARGET
        self._db            = None   # sqlite3.Connection
        self._close_printed = False

        # Whether to create tables and rows for nested message types,
        # "array" if to do this only for arrays of nested types, or
        # "all" for any nested type, including those fully flattened into parent fields.
        # In parent, nested arrays are inserted as foreign keys instead of formatted values.
        self._nesting = args.DUMP_OPTIONS.get("nesting")

        self._topics   = {}  # {(topic, typehash): {topics-row}}
        self._types    = {}  # {(typename, typehash): {types-row}}
        self._checkeds = {}  # {topickey/typehash: whether existence checks are done}
        self._schema   = collections.defaultdict(dict)  # {(typename, typehash): {cols}}

        self._format_repls.update({k: "" for k in self._format_repls})  # Override TextSinkMixin
        atexit.register(self.close)


    def validate(self):
        """Returns whether args.DUMP_OPTIONS["nesting"] has valid value, if any."""
        if self._args.DUMP_OPTIONS.get("nesting") not in (None, "", "array", "all"):
            ConsolePrinter.error("Invalid nesting-option for SQLite: %r. "
                                 "Choose one of {array,all}.",
                                 self._args.DUMP_OPTIONS["nesting"])
            return False
        return True


    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output file."""
        if not self._db:
            self._init_db()
        self._process_topic(topic, msg)
        self._process_message(topic, msg, stamp)
        super(SqliteSink, self).emit(topic, index, stamp, msg, match)


    def close(self):
        """Closes output file, if any."""
        if self._db:
            self._db.close()
            self._db = None
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), self._filename,
                                 format_bytes(os.path.getsize(self._filename)))



    def _init_db(self):
        """Opens the database file and populates schema if not already existing."""
        for t in (dict, list, tuple): sqlite3.register_adapter(t, json.dumps)
        for attr in (getattr(self, k) for k in dir(self)):
            isinstance(attr, dict) and attr.clear()
        self._close_printed = False

        if self._args.VERBOSE:
            sz = os.path.exists(self._filename) and os.path.getsize(self._filename)
            ConsolePrinter.debug("%s %s%s.", "Adding to" if sz else "Creating", self._filename,
                                 (" (%s)" % format_bytes(sz)) if sz else "")
        self._db = sqlite3.connect(self._filename, isolation_level=None, check_same_thread=False)
        self._db.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))
        self._db.executescript(self.BASE_SCHEMA)
        self._load_schema()


    def _load_schema(self):
        """Populates instance attributes with schema metainfo."""
        for row in self._db.execute("SELECT * FROM topics"):
            topickey = (row["name"], row["md5"])
            self._topics[topickey] = row

        for row in self._db.execute("SELECT * FROM types"):
            typekey = (row["type"], row["md5"])
            self._types[typekey] = row

        for row in self._db.execute("SELECT name FROM sqlite_master "
                                    "WHERE type = 'table' AND name LIKE '%/%'"):
            cols = self._db.execute("PRAGMA table_info(%s)" % quote(row["name"])).fetchall()
            cols = [c for c in cols if not c["name"].startswith("_")]
            typerow = next(x for x in self._types.values() if x["table_name"] == row["name"])
            typekey = (typerow["type"], typerow["md5"])
            self._schema[typekey] = collections.OrderedDict([(c["name"], c) for c in cols])


    def _process_topic(self, topic, msg):
        """Inserts topic and message rows and tables and views if not already existing."""
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        topickey = (topic, typehash)
        if topickey in self._checkeds:
            return

        is_new = topickey not in self._topics
        if is_new:
            table_name = self._make_name("table", typename, typehash)
            targs = dict(name=topic, type=typename, md5=typehash, table_name=table_name)
            if self._args.VERBOSE:
                ConsolePrinter.debug("Adding topic %s.", topic)
            targs["id"] = self._db.execute(self.INSERT_TOPIC, targs).lastrowid
            self._topics[topickey] = targs

        self._process_type(msg)

        if is_new:
            BASECOLS = [c for c, _ in self.MESSAGE_TYPE_BASECOLS]
            view_name = self._make_name("view", topic, typehash)
            cols = [c for c in self._schema[(typename, typehash)]
                    if not c.startswith("_") or c in BASECOLS]
            vargs = dict(name=quote(view_name), cols=", ".join(map(quote, cols)),
                         type=quote(self._topics[topickey]["table_name"]), topic=repr(topic))
            sql = self.CREATE_TOPIC_VIEW % vargs
            self._db.executescript(sql)

            self._topics[topickey]["view_name"] = view_name
            self._db.execute(self.UPDATE_TOPIC_VIEW, self._topics[topickey])
        self._checkeds[topickey] = True


    def _process_type(self, msg):
        """Inserts type rows and creates pkg/MsgType tables if not already existing."""
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        typekey  = (typename, typehash)
        if typehash in self._checkeds:
            return

        if typekey not in self._types:
            msgdef = self.source.get_message_definition(typename)
            table_name = self._make_name("table", typename, typehash)
            targs = dict(type=typename, definition=msgdef,
                         md5=typehash, table_name=table_name)
            if self._args.VERBOSE:
                ConsolePrinter.debug("Adding type %s.", typename)
            myid = self._db.execute(self.INSERT_TYPE, targs).lastrowid
            self._types[typename]["id"] = myid

        if typekey not in self._schema:
            table_name = self._types[typekey]["table_name"]
            cols = []
            for path, value, subtype in rosapi.iter_message_fields(msg):
                suffix = "[]" if isinstance(value, (list, tuple)) else ""
                cols += [(".".join(path), quote(rosapi.scalar(subtype) + suffix))]
            cols += self.MESSAGE_TYPE_TOPICCOLS + self.MESSAGE_TYPE_BASECOLS
            if self._nesting: cols += self.MESSAGE_TYPE_NESTCOLS
            coldefs = ["%s %s" % (quote(n), t) for n, t in cols]
            sql = self.CREATE_TYPE_TABLE % dict(name=quote(table_name), cols=", ".join(coldefs))
            self._db.execute(sql)
            self._schema[typekey] = collections.OrderedDict(cols)

        nested_tables = self._types[typename].get("nested_tables") or {}
        nesteds = rosapi.iter_message_fields(msg, messages_only=True) if self._nesting else ()
        for path, submsgs, subtype in nesteds:
            scalartype = rosapi.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for path
            subtypehash = self.source.get_message_type_hash(subtype)
            nested_tables[".".join(path)] = self._make_name("table", scalartype, subtypehash)
            if not isinstance(submsgs, (list, tuple)): submsgs = [submsgs]
            for submsg in submsgs[:1] or [rosapi.get_message_class(scalartype)()]:
                self._process_type(submsg)
        if nested_tables:
            self._db.execute("UPDATE types SET nested_tables = ? WHERE id = ?",
                             [nested_tables, self._types[typename]["id"]])
            self._types[typename]["nested_tables"] = nested_tables
        self._checkeds[typehash] = True


    def _process_message(self, topic, msg, stamp):
        """Inserts message to messages-table, and to pkg/MsgType tables."""
        typename = rosapi.get_message_type(msg)
        topic_id = self._topics[(topic, typename)]["id"]
        margs = dict(dt=rosapi.to_datetime(stamp), timestamp=rosapi.to_nsec(stamp),
                     topic=topic, name=topic, topic_id=topic_id, type=typename,
                     yaml=self.format_message(msg), data=rosapi.get_message_data(msg))
        self._db.execute(self.INSERT_MESSAGE, margs)
        self._db.execute(self.UPDATE_TOPIC,   margs)
        self._populate_type(topic, typename, msg, stamp)


    def _populate_type(self, topic, typename, msg, stamp,
                       root_typehash=None, parent_type=None, parent_id=None):
        """
        Inserts pkg/MsgType row for message.

        If nesting is enabled, inserts sub-rows for subtypes in message,
        and returns inserted ID.
        """
        typehash   = self.source.get_message_type_hash(msg)
        root_typehash = root_typehash or typehash
        topic_id   = self._topics[(topic, root_typehash)]["id"]
        table_name = self._types[(typename, typehash)]["table_name"]

        args = dict(_topic=topic, _topic_id=topic_id, _dt=rosapi.to_datetime(stamp),
                    _timestamp=rosapi.to_nsec(stamp),
                    _parent_type=parent_type, _parent_id=parent_id)
        cols, vals = [], []
        for p, v, t in rosapi.iter_message_fields(msg):
            if isinstance(v, (list, tuple)) and rosapi.scalar(t) not in rosapi.ROS_BUILTIN_TYPES:
                if self._nesting: continue  # for p, v, t
                else: v = [rosapi.message_to_dict(x) for x in v]
            cols += [".".join(p)]
            vals += [re.sub(r"\W", "_", cols[-1])]
            args[vals[-1]] = v
        inter = ", " if cols else ""
        fargs = dict(type=quote(table_name), cols=inter + ", ".join(map(quote, cols)),
                     vals=inter + ", ".join(":" + c for c in vals))
        sql = self.INSERT_MESSAGE_TYPE % fargs
        myid = args["_id"] = self._db.execute(sql, args).lastrowid

        subids = {}  # {message field path: [ids]}
        nesteds = rosapi.iter_message_fields(msg, messages_only=True) if self._nesting else ()
        for subpath, submsgs, subtype in nesteds:
            scalartype = rosapi.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for subpath
            if isinstance(submsgs, (list, tuple)):
                subids[subpath] = []
            for submsg in submsgs if isinstance(submsgs, (list, tuple)) else [submsgs]:
                subid = self._populate_type(topic, scalartype, submsg, stamp,
                                            root_typehash, typename, myid)
                if isinstance(submsgs, (list, tuple)):
                    subids[subpath].append(subid)
        if subids:
            sql = "UPDATE %s SET " % quote(typename)
            for i, (path, ids) in enumerate(subids.items()):
                sql += "%s%s = :%s" % (", " if i else "", quote(".".join(path)), "_".join(path))
                args["_".join(path)] = ids
            sql += " WHERE _id = :_id"
            self._db.execute(sql, args)
        return myid


    def _make_name(self, category, name, typehash):
        """Returns valid unique name for table/view."""
        result = name
        if result in set(sum(([x["table_name"], x.get("view_name")]
                              for x in self._topics.values() if x["md5"] != typehash), [])):
            result = "%s (%s)" % (result, typehash)
        return result

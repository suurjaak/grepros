# -*- coding: utf-8 -*-
"""
Shared functionality for database sinks.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     11.12.2021
@modified    28.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.auto.dbbase
import atexit
import collections

from ... import api
from ... common import PATH_TYPES, ConsolePrinter, ensure_namespace, plural
from ... outputs import Sink
from . sqlbase import SqlMixin, quote


class BaseDataSink(Sink, SqlMixin):
    """
    Base class for writing messages to a database.

    Output will have:
    - table "topics", with topic and message type names
    - table "types", with message type definitions

    plus:
    - table "pkg/MsgType" for each topic message type, with detailed fields,
      BYTEA fields for uint8[], array fields for scalar list attributes,
      and JSONB fields for lists of ROS messages;
      with foreign keys if nesting else subtype values as JSON dictionaries;
      plus underscore-prefixed fields for metadata, like `_topic` as the topic name.
      If not nesting, only topic message type tables are created.
    - view "/full/topic/name" for each topic, selecting from the message type table

    If a message type table already exists but for a type with a different MD5 hash,
    the new table will have its MD5 hash appended to end, as "pkg/MsgType (hash)".
    """

    ## Database engine name, overridden in subclasses
    ENGINE = None

    ## Number of emits between commits; 0 is autocommit
    COMMIT_INTERVAL = 1000

    ## Default topic-related columns for pkg/MsgType tables
    MESSAGE_TYPE_TOPICCOLS = [("_topic",       "TEXT"),
                              ("_topic_id",    "INTEGER"), ]
    ## Default columns for pkg/MsgType tables
    MESSAGE_TYPE_BASECOLS  = [("_dt",          "TIMESTAMP"),
                              ("_timestamp",   "INTEGER"),
                              ("_id",          "INTEGER NOT NULL "
                                               "PRIMARY KEY AUTOINCREMENT"), ]
    ## Additional default columns for pkg/MsgType tables with nesting output
    MESSAGE_TYPE_NESTCOLS  = [("_parent_type", "TEXT"),
                              ("_parent_id",   "INTEGER"), ]

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(META=False, WRITE_OPTIONS={}, VERBOSE=False)


    def __init__(self, args=None, **kwargs):
        """
        @param   args                 arguments as namespace or dictionary, case-insensitive;
                                      or a single item as the database connection string
        @param   args.write           database connection string
        @param   args.write_options   ```
                                      {"commit-interval": transaction size (0 is autocommit),
                                      "nesting": "array" to recursively insert arrays
                                                  of nested types, or "all" for any nesting)}
                                      ```
        @param   args.meta            whether to emit metainfo
        @param   args.verbose         whether to emit debug information
        @param   kwargs               any and all arguments as keyword overrides, case-insensitive
        """
        args = {"WRITE": str(args)} if isinstance(args, PATH_TYPES) else args
        args = ensure_namespace(args, BaseDataSink.DEFAULT_ARGS, **kwargs)
        super(BaseDataSink, self).__init__(args)
        SqlMixin.__init__(self, args)

        ## Database connection
        self.db = None

        self._cursor        = None   # Database cursor or connection
        self._dialect       = self.ENGINE.lower()  # Override SqlMixin._dialect
        self._close_printed = False

        # Whether to create tables and rows for nested message types,
        # "array" if to do this only for arrays of nested types, or
        # "all" for any nested type, including those fully flattened into parent fields.
        # In parent, nested arrays are inserted as foreign keys instead of formatted values.
        self._nesting = args.WRITE_OPTIONS.get("nesting")

        self._checkeds      = {}  # {topickey/typekey: whether existence checks are done}
        self._sql_queue     = {}  # {SQL: [(args), ]}
        self._nested_counts = {}  # {(typename, typehash): count}

        atexit.register(self.close)


    def validate(self):
        """
        Returns whether args.write_options has valid values, if any.

        Checks parameters "commit-interval" and "nesting".
        """
        ok, sqlconfig_ok = True, SqlMixin._validate_dialect_file(self)
        if "commit-interval" in self.args.WRITE_OPTIONS:
            try: ok = int(self.args.WRITE_OPTIONS["commit-interval"]) >= 0
            except Exception: ok = False
            if not ok:
                ConsolePrinter.error("Invalid commit-interval option for %s: %r.",
                                     self.ENGINE, self.args.WRITE_OPTIONS["commit-interval"])
        if self.args.WRITE_OPTIONS.get("nesting") not in (None, False, "", "array", "all"):
            ConsolePrinter.error("Invalid nesting option for %s: %r. "
                                 "Choose one of {array,all}.",
                                 self.ENGINE, self.args.WRITE_OPTIONS["nesting"])
            ok = False
        return ok and sqlconfig_ok


    def emit(self, topic, msg, stamp=None, match=None, index=None):
        """Writes message to database."""
        if not self.validate(): raise Exception("invalid")
        if not self.db:
            self._init_db()
        stamp, index = self._ensure_stamp_index(topic, msg, stamp, index)
        self._process_type(msg)
        self._process_topic(topic, msg)
        self._process_message(topic, msg, stamp)
        super(BaseDataSink, self).emit(topic, msg, stamp, match, index)


    def close(self):
        """Closes database connection, if any."""
        try:
            if self.db:
                for sql in list(self._sql_queue):
                    self._executemany(sql, self._sql_queue.pop(sql))
                self.db.commit()
                self._cursor.close()
                self._cursor = None
                self.db.close()
                self.db = None
        finally:
            if not self._close_printed and self._counts:
                self._close_printed = True
                target = self._make_db_label()
                ConsolePrinter.debug("Wrote %s in %s to %s database %s.",
                                     plural("message", sum(self._counts.values())),
                                     plural("topic", self._counts), self.ENGINE, target)
                if self._nested_counts:
                    ConsolePrinter.debug("Wrote %s in %s to %s database %s.",
                        plural("nested message", sum(self._nested_counts.values())),
                        plural("nested message type", self._nested_counts),
                        self.ENGINE, target
                    )
            self._checkeds.clear()
            self._nested_counts.clear()
            SqlMixin.close(self)
            super(BaseDataSink, self).close()


    def _init_db(self):
        """Opens database connection, and populates schema if not already existing."""
        baseattrs = dir(Sink())
        for attr in (getattr(self, k, None) for k in dir(self)
                     if not k.isupper() and k not in baseattrs):
            isinstance(attr, dict) and attr.clear()
        self._close_printed = False

        if "commit-interval" in self.args.WRITE_OPTIONS:
            self.COMMIT_INTERVAL = int(self.args.WRITE_OPTIONS["commit-interval"])
        self.db = self._connect()
        self._cursor = self._make_cursor()
        self._executescript(self._get_dialect_option("base_schema"))
        self.db.commit()
        self._load_schema()
        TYPECOLS = self.MESSAGE_TYPE_TOPICCOLS + self.MESSAGE_TYPE_BASECOLS
        if self._nesting: TYPECOLS += self.MESSAGE_TYPE_NESTCOLS
        self._ensure_columns(TYPECOLS)


    def _load_schema(self):
        """Populates instance attributes with schema metainfo."""
        self._cursor.execute("SELECT * FROM topics")
        for row in self._cursor.fetchall():
            topickey = (row["name"], row["md5"])
            self._topics[topickey] = row

        self._cursor.execute("SELECT * FROM types")
        for row in self._cursor.fetchall():
            typekey = (row["type"], row["md5"])
            self._types[typekey] = row


    def _process_topic(self, topic, msg):
        """
        Inserts topics-row and creates view `/topic/name` if not already existing.

        Also creates types-row and pkg/MsgType table for this message if not existing.
        If nesting enabled, creates types recursively.
        """
        topickey = api.TypeMeta.make(msg, topic).topickey
        if topickey in self._checkeds:
            return

        if topickey not in self._topics:
            exclude_cols = list(self.MESSAGE_TYPE_TOPICCOLS)
            if self._nesting: exclude_cols += self.MESSAGE_TYPE_NESTCOLS
            tdata = self._make_topic_data(topic, msg, exclude_cols)
            self._topics[topickey] = tdata
            self._executescript(tdata["sql"])

            sql, args = self._make_topic_insert_sql(topic, msg)
            if self.args.VERBOSE:
                ConsolePrinter.debug("Adding topic %s in %s output.", topic, self.ENGINE)
            self._topics[topickey]["id"] = self._execute_insert(sql, args)

            if self.COMMIT_INTERVAL: self.db.commit()
        self._checkeds[topickey] = True


    def _process_type(self, msg, rootmsg=None):
        """
        Creates types-row and pkg/MsgType table if not already existing.

        @return   created types-row, or None if already existed
        """
        rootmsg = rootmsg or msg
        with api.TypeMeta.make(msg, root=rootmsg) as m:
            typename, typekey = (m.typename, m.typekey)
        if typekey in self._checkeds:
            return None

        if typekey not in self._types:
            if self.args.VERBOSE:
                ConsolePrinter.debug("Adding type %s in %s output.", typename, self.ENGINE)
            extra_cols = self.MESSAGE_TYPE_TOPICCOLS + self.MESSAGE_TYPE_BASECOLS
            if self._nesting: extra_cols += self.MESSAGE_TYPE_NESTCOLS
            tdata = self._make_type_data(msg, extra_cols)
            self._schema[typekey] = collections.OrderedDict(tdata.pop("cols"))
            self._types[typekey] = tdata

            self._executescript(tdata["sql"])
            sql, args = self._make_type_insert_sql(msg)
            tdata["id"] = self._execute_insert(sql, args)


        nested_tables = self._types[typekey].get("nested_tables") or {}
        nesteds = api.iter_message_fields(msg, messages_only=True) if self._nesting else ()
        for path, submsgs, subtype in nesteds:
            scalartype = api.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for path

            subtypehash = not submsgs and self.source.get_message_type_hash(scalartype)
            if not isinstance(submsgs, (list, tuple)): submsgs = [submsgs]
            [submsg] = submsgs[:1] or [self.source.get_message_class(scalartype, subtypehash)()]
            subdata = self._process_type(submsg, rootmsg)
            if subdata: nested_tables[".".join(path)] = subdata["table_name"]
        if nested_tables:
            self._types[typekey]["nested_tables"] = nested_tables
            sets, where = {"nested_tables": nested_tables}, {"id": self._types[typekey]["id"]}
            sql, args = self._make_update_sql("types", sets, where)
            self._cursor.execute(sql, args)
        self._checkeds[typekey] = True
        return self._types[typekey]


    def _process_message(self, topic, msg, stamp):
        """
        Inserts pkg/MsgType row for this message.

        Inserts sub-rows for subtypes in message if nesting enabled.
        Commits transaction if interval due.
        """
        self._populate_type(topic, msg, stamp)
        if self.COMMIT_INTERVAL:
            do_commit = sum(len(v) for v in self._sql_queue.values()) >= self.COMMIT_INTERVAL
            for sql in list(self._sql_queue) if do_commit else ():
                self._executemany(sql, self._sql_queue.pop(sql))
            do_commit and self.db.commit()


    def _populate_type(self, topic, msg, stamp,
                       rootmsg=None, parent_type=None, parent_id=None):
        """
        Inserts pkg/MsgType row for message.

        If nesting is enabled, inserts sub-rows for subtypes in message,
        and returns inserted ID.
        """
        rootmsg = rootmsg or msg
        with api.TypeMeta.make(msg, root=rootmsg) as m:
            typename, typekey = m.typename, m.typekey
        with api.TypeMeta.make(rootmsg) as m:
            topic_id = self._topics[m.topickey]["id"]
        table_name = self._types[typekey]["table_name"]

        myid = self._get_next_id(table_name) if self._nesting else None
        coldefs = self.MESSAGE_TYPE_TOPICCOLS + self.MESSAGE_TYPE_BASECOLS[:-1]
        colvals = [topic, topic_id, api.to_datetime(stamp), api.to_nsec(stamp)]
        if self._nesting:
            coldefs += self.MESSAGE_TYPE_BASECOLS[-1:] + self.MESSAGE_TYPE_NESTCOLS
            colvals += [myid, parent_type, parent_id]
        extra_cols = list(zip([c for c, _ in coldefs], colvals))
        sql, args = self._make_message_insert_sql(topic, msg, extra_cols)
        self._ensure_execute(sql, args)
        if parent_type: self._nested_counts[typekey] = self._nested_counts.get(typekey, 0) + 1

        subids = {}  # {message field path: [ids]}
        nesteds = api.iter_message_fields(msg, messages_only=True) if self._nesting else ()
        for subpath, submsgs, subtype in nesteds:
            scalartype = api.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for subpath
            if isinstance(submsgs, (list, tuple)):
                subids[subpath] = []
            for submsg in submsgs if isinstance(submsgs, (list, tuple)) else [submsgs]:
                subid = self._populate_type(topic, submsg, stamp, rootmsg, typename, myid)
                if isinstance(submsgs, (list, tuple)):
                    subids[subpath].append(subid)
        if subids:
            sets, where = {".".join(p): subids[p] for p in subids}, {"_id": myid}
            sql, args = self._make_update_sql(table_name, sets, where)
            self._ensure_execute(sql, args)
        return myid


    def _ensure_columns(self, cols):
        """Adds specified columns to any type tables lacking them."""
        sqls = []
        for typekey, typecols in self._schema.items():
            table_name = self._types[typekey]["table_name"]
            for c, t in ((c, t) for c, t in cols if c not in typecols):
                sql = "ALTER TABLE %s ADD COLUMN %s %s;" % (quote(table_name), c, t)
                sqls.append(sql)
                typecols[c] = t
        if sqls:
            self._executescript("\n".join(sqls))
            self.db.commit()


    def _ensure_execute(self, sql, args):
        """Executes SQL if in autocommit mode, else caches arguments for batch execution."""
        args = tuple(args) if isinstance(args, list) else args
        if self.COMMIT_INTERVAL:
            self._sql_queue.setdefault(sql, []).append(args)
        else:
            self._cursor.execute(sql, args)


    def _connect(self):
        """Returns new database connection."""
        raise NotImplementedError()


    def _execute_insert(self, sql, args):
        """Executes INSERT statement, returns inserted ID."""
        raise NotImplementedError()


    def _executemany(self, sql, argses):
        """Executes SQL with all args sequences."""
        raise NotImplementedError()


    def _executescript(self, sql):
        """Executes SQL with one or more statements."""
        raise NotImplementedError()


    def _get_next_id(self, table):
        """Returns next ID value for table."""
        raise NotImplementedError()


    def _make_cursor(self):
        """Returns new database cursor."""
        return self.db.cursor()


    def _make_db_label(self):
        """Returns formatted label for database."""
        return self.args.WRITE


__all__ = ["BaseDataSink"]

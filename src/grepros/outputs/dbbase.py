# -*- coding: utf-8 -*-
"""
Shared functionality for database sinks.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     11.12.2021
@modified    12.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs.dbbase
import atexit
import collections

from .. import common, rosapi
from .. common import ConsolePrinter, ellipsize, plural
from . base import SinkBase

quote = lambda s, force=True: common.quote(s, force)


class DataSinkBase(SinkBase):
    """
    Writes messages to a database.

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

    ## Database engine name
    ENGINE = None

    ## Placeholder for positional arguments in SQL statement
    POSARG = "%s"

    ## Max table/column name length in DB engine
    MAX_NAME_LEN = 0

    ## Number of emits between commits; 0 is autocommit
    COMMIT_INTERVAL = 1000

    ## SQL statements for populating database base schema
    BASE_SCHEMA = ""

    ## SQL statement for inserting topics
    INSERT_TOPIC = ""

    ## SQL statement for inserting types
    INSERT_TYPE = ""

    ## SQL statement for inserting metas for renames
    INSERT_META = ""

    ## SQL statement for updating view name in topic
    UPDATE_TOPIC_VIEW = ""

    ## SQL statement for creating a table for type
    CREATE_TYPE_TABLE = ""

    ## SQL statement for creating a view for topic
    CREATE_TOPIC_VIEW = """
    DROP VIEW IF EXISTS %(name)s;

    CREATE VIEW %(name)s AS
    SELECT %(cols)s
    FROM %(type)s
    WHERE _topic = %(topic)s;
    """

    ## SQL statement for selecting metainfo on pkg/MsgType table columns
    SELECT_TYPE_COLUMNS = ""

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


    def __init__(self, args):
        """
        @param   args                arguments object like argparse.Namespace
        @param   args.DUMP_TARGET    database connection string
        @param   args.DUMP_OPTIONS   {"commit-interval": transaction size (0 is autocommit),
                                      "nesting": "array" to recursively insert arrays
                                                 of nested types, or "all" for any nesting)}
        @param   args.META           whether to print metainfo
        @param   args.VERBOSE        whether to print debug information
        """
        super(DataSinkBase, self).__init__(args)

        self._db            = None   # Database connection
        self._cursor        = None   # Database cursor or connection
        self._close_printed = False

        # Whether to create tables and rows for nested message types,
        # "array" if to do this only for arrays of nested types, or
        # "all" for any nested type, including those fully flattened into parent fields.
        # In parent, nested arrays are inserted as foreign keys instead of formatted values.
        self._nesting = args.DUMP_OPTIONS.get("nesting")

        self._topics        = {}  # {(topic, typehash): {topics-row}}
        self._types         = {}  # {(typename, typehash): {types-row}}
        self._metas         = {}  # {(type, parent): {name: name in db}}
        self._checkeds      = {}  # {topickey/typehash: whether existence checks are done}
        self._sql_cache     = {}  # {table: "INSERT INTO table VALUES (%s, ..)"}
        self._sql_queue     = {}  # {SQL: [(args), ]}
        self._nested_counts = {}  # {typehash: count}
        self._id_queue  = collections.defaultdict(collections.deque)  # {table name: [next ID, ]}
        self._schema    = collections.defaultdict(dict)  # {(typename, typehash): {cols}}

        atexit.register(self.close)


    def validate(self):
        """
        Returns whether args.DUMP_OPTIONS has valid values, if any.

        Checks parameters "commit-interval" and "nesting".
        """
        config_ok = True
        if "commit-interval" in self._args.DUMP_OPTIONS:
            try: config_ok = int(self._args.DUMP_OPTIONS["commit-interval"]) >= 0
            except Exception: config_ok = False
            if not config_ok:
                ConsolePrinter.error("Invalid commit interval for %s: %r.",
                                     self.ENGINE, self._args.DUMP_OPTIONS["commit-interval"])
        if self._args.DUMP_OPTIONS.get("nesting") not in (None, "", "array", "all"):
            ConsolePrinter.error("Invalid nesting-option for %s: %r. "
                                 "Choose one of {array,all}.",
                                 self.ENGINE, self._args.DUMP_OPTIONS["nesting"])
            config_ok = False
        return config_ok


    def emit(self, topic, index, stamp, msg, match):
        """Writes message to database."""
        if not self._db:
            self._init_db()
        self._process_topic(topic, msg)
        self._process_message(topic, msg, stamp)
        super(DataSinkBase, self).emit(topic, index, stamp, msg, match)


    def close(self):
        """Closes database connection, if any."""
        if self._db:
            for sql in list(self._sql_queue):
                self._executemany(sql, self._sql_queue.pop(sql))
            self._db.commit()
            self._cursor.close()
            self._cursor = None
            self._db.close()
            self._db = None
        if not self._close_printed and self._counts:
            self._close_printed = True
            target = self._make_db_label()
            ConsolePrinter.debug("Wrote %s in %s to %s database %s.",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), self.ENGINE, target)
            if self._nested_counts:
                ConsolePrinter.debug("Wrote %s in %s.",
                                     plural("nested message", sum(self._nested_counts.values())),
                                     plural("nested message type", self._nested_counts))
        super(DataSinkBase, self).close()


    def _init_db(self):
        """Opens database connection, and populates schema if not already existing."""
        baseattrs = dir(SinkBase(None))
        for attr in (getattr(self, k, None) for k in dir(self) if k not in baseattrs):
            isinstance(attr, dict) and attr.clear()
        self._close_printed = False

        if "commit-interval" in self._args.DUMP_OPTIONS:
            self.COMMIT_INTERVAL = int(self._args.DUMP_OPTIONS["commit-interval"])
        self._db = self._connect()
        self._cursor = self._make_cursor()
        self._executescript(self.BASE_SCHEMA)
        self._db.commit()
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

        try:
            self._cursor.execute("SELECT * FROM meta")
            for row in self._cursor.fetchall():
                metakey = (row["type"], row["parent"])
                self._metas.setdefault(metakey, {})[row["name"]] = row["name_in_db"]
        except Exception: pass


    def _process_topic(self, topic, msg):
        """
        Inserts topics-row and creates view `/topic/name` if not already existing.

        Also creates types-row and pkg/MsgType table for this message if not existing.
        If nesting enabled, creates types recursively.
        """
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
            targs["id"] = self._execute_insert(self.INSERT_TOPIC, targs)
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
            self._executescript(sql)

            self._topics[topickey]["view_name"] = view_name
            self._cursor.execute(self.UPDATE_TOPIC_VIEW, self._topics[topickey])
            if self.COMMIT_INTERVAL: self._db.commit()
        self._checkeds[topickey] = True


    def _process_type(self, msg):
        """Creates types-row and pkg/MsgType table if not already existing."""
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
            self._cursor.execute(self.INSERT_TYPE, targs)
            targs["id"] = self._execute_insert(self.INSERT_TYPE, targs)
            self._types[typekey] = targs

        if typekey not in self._schema:
            table_name = self._types[typekey]["table_name"]
            cols = []
            for path, value, subtype in rosapi.iter_message_fields(msg):
                coltype = self._make_column_type(subtype, value)
                cols += [(".".join(path), coltype)]
            cols = list(zip(self._make_column_names([c for c, _ in cols], table_name),
                            [t for _, t in cols]))
            cols += self.MESSAGE_TYPE_TOPICCOLS + self.MESSAGE_TYPE_BASECOLS
            if self._nesting: cols += self.MESSAGE_TYPE_NESTCOLS
            coldefs = ["%s %s" % (quote(n), quote(t, force=False)) for n, t in cols]
            sql = self.CREATE_TYPE_TABLE % dict(name=quote(table_name), cols=", ".join(coldefs))
            self._executescript(sql)
            self._schema[typekey] = collections.OrderedDict(cols)

        nested_tables = self._types[typekey].get("nested_tables") or {}
        nesteds = rosapi.iter_message_fields(msg, messages_only=True) if self._nesting else ()
        for path, submsgs, subtype in nesteds:
            scalartype = rosapi.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for path
            subtypehash = self.source.get_message_type_hash(scalartype)
            nested_tables[".".join(path)] = self._make_name("table", scalartype, subtypehash)
            if not isinstance(submsgs, (list, tuple)): submsgs = [submsgs]
            for submsg in submsgs[:1] or [self.source.get_message_class(scalartype)()]:
                self._process_type(submsg)
        if nested_tables:
            sql = "UPDATE types SET nested_tables = %s WHERE id = %s" % (self.POSARG, self.POSARG)
            self._cursor.execute(sql, (nested_tables, self._types[typekey]["id"]))
            self._types[typekey]["nested_tables"] = nested_tables
        self._checkeds[typehash] = True


    def _process_message(self, topic, msg, stamp):
        """
        Inserts pkg/MsgType row for this message.

        Inserts sub-rows for subtypes in message if nesting enabled.
        Commits transaction if interval due.
        """
        typename = rosapi.get_message_type(msg)
        self._populate_type(topic, typename, msg, stamp)
        if self.COMMIT_INTERVAL:
            do_commit = sum(len(v) for v in self._sql_queue.values()) >= self.COMMIT_INTERVAL
            for sql in list(self._sql_queue) if do_commit else ():
                self._executemany(sql, self._sql_queue.pop(sql))
            do_commit and self._db.commit()


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
        sql, cols, args = self._sql_cache.get(table_name), [], []

        for p, v, t in rosapi.iter_message_fields(msg):
            if not sql: cols.append(".".join(p))
            args.append(self._make_column_value(v, t))
        myargs = [topic, topic_id, rosapi.to_datetime(stamp), rosapi.to_nsec(stamp)]
        myid = self._get_next_id(table_name) if self._nesting else None
        if self._nesting: myargs += [myid, parent_type, parent_id]
        args = tuple(args + myargs)

        if not sql:
            cols += [c for c, _ in self.MESSAGE_TYPE_TOPICCOLS + self.MESSAGE_TYPE_BASECOLS[:-1]]
            if self._nesting:
                cols += [c for c, _ in self.MESSAGE_TYPE_BASECOLS[-1:] + self.MESSAGE_TYPE_NESTCOLS]
            sql = "INSERT INTO %s (%s) VALUES (%s)" % \
                  (quote(table_name), ", ".join(map(quote, cols)), ", ".join([self.POSARG] * len(args)))
            self._sql_cache[table_name] = sql
        self._ensure_execute(sql, args)
        if parent_type: self._nested_counts[typehash] = self._nested_counts.get(typehash, 0) + 1

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
            args = [self._make_column_value(x) for x in subids.values()] + [myid]
            sets = ["%s = %s" % (quote(".".join(p)), self.POSARG) for p in subids]
            sql  = "UPDATE %s SET %s WHERE _id = %s" % \
                   (quote(table_name), ", ".join(sets), self.POSARG)
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
            self._db.commit()


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
        return self._db.cursor()


    def _make_db_label(self):
        """Returns formatted label for database."""
        return self._args.DUMP_TARGET


    def _make_name(self, category, name, typehash):
        """Returns valid unique name for table/view, inserting meta-row if necessary."""
        result = ellipsize(name, self.MAX_NAME_LEN)
        existing = set(sum(([x["table_name"], x.get("view_name")]
                            for x in self._topics.values() if x["md5"] != typehash), []))
        suffix = " (%s)" % typehash
        if result in existing:
            result = ellipsize(name, self.MAX_NAME_LEN - len(suffix)) if self.MAX_NAME_LEN else name
            result = result + suffix
        counter = 2
        while result in existing:
            suffix = " (%s) (%s)" % (typehash, counter)
            result = ellipsize(name, self.MAX_NAME_LEN - len(suffix)) + suffix
            counter += 1
        if result != name and self.INSERT_META:
            meta = {"type": category, "parent": None, "name": name, "name_in_db": result}
            self._cursor.execute(self.INSERT_META, meta)
            self._metas.setdefault((category, None), {})[name] = result
        return result


    def _make_column_names(self, col_names, table_name):
        """Returns valid unique names for table columns, inserting meta-rows if necessary."""
        if not self.INSERT_META: return list(col_names)

        result = collections.OrderedDict()  # {dbname: original name}
        metas, METABASE = [], {"type": "column", "parent": table_name}
        for name in col_names:
            name_in_db = ellipsize(name, self.MAX_NAME_LEN)
            counter = 2
            while name_in_db in result:
                suffix = " (%s)" % counter
                name_in_db = ellipsize(name, self.MAX_NAME_LEN - len(suffix)) + suffix
                counter += 1
            if name_in_db != name:
                metas.append(dict(METABASE, **{"name": name, "name_in_db": name_in_db}))
            result[name_in_db] = name
        for meta in metas:
            self._cursor.execute(self.INSERT_META, meta)
            self._metas.setdefault(("column", table_name), {})[meta["name"]] = meta["name_in_db"]
        return list(result)


    def _make_column_type(self, typename, value):
        """Returns column database type."""
        suffix = "[]" if isinstance(value, (list, tuple)) else ""
        return rosapi.scalar(typename) + suffix


    def _make_column_value(self, value, typename=None):
        """Returns column value suitable for inserting to database."""
        v, scalartype = value, typename and rosapi.scalar(typename)
        if isinstance(v, (list, tuple)) and typename and scalartype not in rosapi.ROS_BUILTIN_TYPES:
            if self._nesting: v = []
            else: v = [rosapi.message_to_dict(x) for x in v]
        return v

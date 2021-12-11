# -*- coding: utf-8 -*-
"""
Sink plugin for dumping messages to a Postgres database.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     02.12.2021
@modified    11.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs.postgres
import atexit
import collections
import json

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.extras
    import psycopg2.pool
except ImportError:
    psycopg2 = None

from .. import common, rosapi
from .. common import ConsolePrinter, plural
from . base import SinkBase

quote = lambda s: common.quote(s, force=True)


class PostgresSink(SinkBase):
    """
    Writes messages to a Postgres database.

    Output will have:
    - table "topics", with topic and message type names
    - table "types", with message type definitions
    - table "meta", with mappings between original names and names shortened for Postgres

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

    ## Max table/column name length in Postgres
    MAX_NAME_LEN = 63

    ## Number of emits between commits; 0 is autocommit
    COMMIT_INTERVAL = 1000

    ## Sequence length per table to reserve for inserted message IDs
    ID_SEQUENCE_STEP = 100

    ## Mapping from ROS common types to Postgres types
    COMMON_TYPES = {
        "byte": "SMALLINT", "char": "SMALLINT", "int8": "SMALLINT",
        "int16": "SMALLINT", "int32": "INTEGER", "int64": "BIGINT",
        "uint8": "SMALLINT", "uint16": "INTEGER", "uint32": "BIGINT",
        "uint64": "BIGINT", "float32": "REAL", "float64": "DOUBLE PRECISION",
        "bool": "BOOLEAN", "string": "TEXT", "wstring": "TEXT",
        "uint8[]": "BYTEA", "char[]": "BYTEA",
    }


    ## SQL statements for populating database base schema
    BASE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS meta (
      id         BIGSERIAL PRIMARY KEY,
      type       TEXT NOT NULL,
      parent     TEXT NULL,
      name       TEXT NOT NULL,
      pg_name    TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS topics (
      id                   BIGSERIAL PRIMARY KEY,
      name                 TEXT NOT NULL,
      type                 TEXT NOT NULL,
      md5                  TEXT NOT NULL,
      table_name           TEXT NOT NULL,
      view_name            TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS types (
      id                   BIGSERIAL PRIMARY KEY,
      type                 TEXT NOT NULL,
      definition           TEXT NOT NULL,
      md5                  TEXT NOT NULL,
      table_name           TEXT NOT NULL
    );
    """

    ## SQL statement for inserting topics
    INSERT_TOPIC = """
    INSERT INTO topics (name, type, md5, table_name, view_name)
    VALUES (%(name)s, %(type)s, %(md5)s, %(table_name)s, '')
    RETURNING id
    """

    ## SQL statement for inserting types
    INSERT_TYPE = """
    INSERT INTO types (type, definition, md5, table_name)
    VALUES (%(type)s, %(definition)s, %(md5)s, %(table_name)s)
    RETURNING id
    """

    ## SQL statement for inserting metas for renames
    INSERT_META = """
    INSERT INTO meta (type, parent, name, pg_name)
    VALUES (%(type)s, %(parent)s, %(name)s, %(pg_name)s)
    """

    ## SQL statement for updating view name in topic
    UPDATE_TOPIC = """
    UPDATE topics SET view_name = %(view_name)s
    WHERE id = %(id)s
    """

    ## SQL statement for creating a table for type
    CREATE_TYPE_TABLE = """
    DROP TABLE IF EXISTS %(name)s CASCADE;

    CREATE TABLE %(name)s (%(cols)s);
    """

    ## SQL statement for creating a view for topic
    CREATE_TOPIC_VIEW = """
    DROP VIEW IF EXISTS %(name)s;

    CREATE VIEW %(name)s AS
    SELECT %(cols)s
    FROM %(type)s
    WHERE _topic = %(topic)s;
    """

    ## SQL statement for selecting metainfo on pkg/MsgType table columns
    SELECT_TYPE_COLUMNS = """
    SELECT c.table_name, c.column_name, c.data_type
    FROM   information_schema.columns c INNER JOIN information_schema.tables t
    ON     t.table_name = c.table_name
    WHERE  c.table_schema = current_schema() AND t.table_type = 'BASE TABLE' AND
           c.table_name LIKE '%/%'
    ORDER BY c.table_name, CAST(c.dtd_identifier AS INTEGER)
    """

    ## Default topic-related columns for pkg/MsgType tables
    MESSAGE_TYPE_TOPICCOLS = [("_topic",       "TEXT"),
                              ("_topic_id",    "BIGINT"), ]
    ## Default columns for pkg/MsgType tables
    MESSAGE_TYPE_BASECOLS  = [("_dt",          "TIMESTAMP"),
                              ("_timestamp",   "NUMERIC"),
                              ("_id",          "BIGSERIAL PRIMARY KEY"), ]
    ## Additional default columns for pkg/MsgType tables with nesting output
    MESSAGE_TYPE_NESTCOLS  = [("_parent_type", "TEXT"),
                              ("_parent_id",   "BIGINT"), ]


    def __init__(self, args):
        """
        @param   args                arguments object like argparse.Namespace
        @param   args.DUMP_TARGET    Postgres connection string postgresql://user@host/db
        @param   args.DUMP_OPTIONS   {"commit-interval": transaction size (0 is autocommit),
                                      "nesting": "array" to recursively insert arrays
                                                 of nested types, or "all" for any nesting)}
        @param   args.META           whether to print metainfo
        @param   args.VERBOSE        whether to print debug information
        """
        super(PostgresSink, self).__init__(args)

        self._db            = None   # psycopg2.connection
        self._cursor        = None   # psycopg2.cursor
        self._close_printed = False

        # Whether to create tables and rows for nested message types,
        # "array" if to do this only for arrays of nested types, or
        # "all" for any nested type, including those fully flattened into parent fields.
        # In parent, nested arrays are inserted as foreign keys instead of formatted values.
        self._nesting = args.DUMP_OPTIONS.get("nesting")

        self._topics        = {}  # {(topic, typehash): {topics-row}}
        self._types         = {}  # {(typename, typehash): {types-row}}
        self._metas         = {}  # {(type, parent): {name: pg_name}}
        self._checkeds      = {}  # {topickey/typehash: whether existence checks are done}
        self._sql_cache     = {}  # {table: "INSERT INTO table VALUES (%s, ..)"}
        self._sql_queue     = {}  # {SQL: [(args), ]}
        self._nested_counts = {}  # {typehash: count}
        self._id_queue  = collections.defaultdict(collections.deque)  # {table name: [next ID, ]}
        self._schema    = collections.defaultdict(dict)  # {(typename, typehash): {cols}}

        atexit.register(self.close)


    def validate(self):
        """
        Returns whether Postgres driver is available,
        and "commit-interval" and "nesting" in args.DUMP_OPTIONS have valid value, if any.
        """
        driver_ok, config_ok = bool(psycopg2), True
        if "commit-interval" in self._args.DUMP_OPTIONS:
            try: config_ok = int(self._args.DUMP_OPTIONS["commit-interval"]) >= 0
            except Exception: config_ok = False
            if not config_ok:
                ConsolePrinter.error("Invalid commit interval for Postgres: %r.",
                                     self._args.DUMP_OPTIONS["commit-interval"])
        if self._args.DUMP_OPTIONS.get("nesting") not in (None, "", "array", "all"):
            ConsolePrinter.error("Invalid nesting-option for Postgres: %r. "
                                 "Choose one of {array,all}.",
                                 self._args.DUMP_OPTIONS["nesting"])
            config_ok = False
        if not driver_ok:
            ConsolePrinter.error("psycopg2 not available: cannot write to Postgres.")
        return driver_ok and config_ok


    def emit(self, topic, index, stamp, msg, match):
        """Writes message to database."""
        if not self._db:
            self._init_db()
        self._process_topic(topic, msg)
        self._process_message(topic, msg, stamp)
        super(PostgresSink, self).emit(topic, index, stamp, msg, match)


    def close(self):
        """Closes Postgres connection, if any."""
        if self._db:
            for sql in list(self._sql_queue):
                psycopg2.extras.execute_batch(self._cursor, sql, self._sql_queue.pop(sql))
            self._db.commit()
            self._cursor.close()
            self._cursor = None
            self._db.close()
            self._db = None
            self._id_queue.clear()
        if not self._close_printed and self._counts:
            self._close_printed = True
            target = self._args.DUMP_TARGET
            if not target.startswith("postgresql://"): target = repr(target)
            ConsolePrinter.debug("Wrote %s in %s to Postgres database %s.",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), target)
            if self._nested_counts:
                ConsolePrinter.debug("Wrote %s in %s.",
                                     plural("message", sum(self._nested_counts.values())),
                                     plural("nested message type", self._nested_counts))
        super(PostgresSink, self).close()


    def _init_db(self):
        """Opens the database file, and populates schema if not already existing."""
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
        for attr in (getattr(self, k, None) for k in dir(self) if not k.startswith("__")):
            isinstance(attr, dict) and attr.clear()
        self._close_printed = False
                
        if "commit-interval" in self._args.DUMP_OPTIONS:
            self.COMMIT_INTERVAL = int(self._args.DUMP_OPTIONS["commit-interval"])
        self._db = psycopg2.connect(self._args.DUMP_TARGET,
                                    cursor_factory=psycopg2.extras.RealDictCursor)
        self._db.autocommit = not self.COMMIT_INTERVAL
        cursor = self._cursor = self._db.cursor()
        cursor.execute(self.BASE_SCHEMA)
        self._db.commit()
        self._load_schema()
        self._nesting and self._ensure_columns(self.MESSAGE_TYPE_NESTCOLS)


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

        self._cursor.execute("SELECT * FROM meta")
        for row in self._cursor.fetchall():
            metakey = (row["type"], row["parent"])
            self._metas.setdefault(metakey, {})[row["name"]] = row["pg_name"]

        self._cursor.execute(self.SELECT_TYPE_COLUMNS)
        for row in self._cursor.fetchall():
            typerow = next(x for x in self._types.values() if x["table_name"] == row["table_name"])
            typekey = (typerow["type"], typerow["md5"])
            self._schema.setdefault(typekey, collections.OrderedDict())
            self._schema[typekey][row["column_name"]] = row["data_type"]


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
            self._cursor.execute(self.INSERT_TOPIC, targs)
            targs["id"] = self._cursor.fetchone()["id"]
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
            self._cursor.execute(sql)

            self._topics[topickey]["view_name"] = view_name
            self._cursor.execute(self.UPDATE_TOPIC, self._topics[topickey])
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
            targs["id"] = self._cursor.fetchone()["id"]
            self._types[typekey] = targs

        if typekey not in self._schema:
            table_name = self._types[typekey]["table_name"]
            cols = []
            for path, value, subtype in rosapi.iter_message_fields(msg):
                coltype = self.COMMON_TYPES.get(subtype)
                scalartype = rosapi.scalar(subtype)
                if not coltype and scalartype in self.COMMON_TYPES:
                    coltype = self.COMMON_TYPES.get(scalartype) + "[]"
                if not coltype:
                    coltype = "JSONB"
                cols += [(".".join(path), coltype)]
            cols = list(zip(self._make_column_names([c for c, _ in cols], table_name),
                            [t for _, t in cols]))
            cols += self.MESSAGE_TYPE_TOPICCOLS + self.MESSAGE_TYPE_BASECOLS
            if self._nesting: cols += self.MESSAGE_TYPE_NESTCOLS
            coldefs = ["%s %s" % (quote(n), t) for n, t in cols]
            sql = self.CREATE_TYPE_TABLE % dict(name=quote(table_name), cols=", ".join(coldefs))
            self._cursor.execute(sql)
            self._schema[typekey] = collections.OrderedDict(cols)

        nesteds = rosapi.iter_message_fields(msg, messages_only=True) if self._nesting else ()
        for path, submsgs, subtype in nesteds:
            scalartype = rosapi.scalar(subtype)
            if subtype == scalartype and "all" != self._nesting:
                continue  # for path
            if not isinstance(submsgs, (list, tuple)): submsgs = [submsgs]
            for submsg in submsgs[:1] or [rosapi.get_message_class(scalartype)()]:
                self._process_type(submsg)
        self._checkeds[typehash] = True


    def _process_message(self, topic, msg, stamp):
        """
        Inserts pkg/MsgType row for this message, and sub-rows for subtypes in message
        if nesting enabled; commits transaction if interval due.
        """
        typename = rosapi.get_message_type(msg)
        self._populate_type(topic, typename, msg, stamp)
        if self.COMMIT_INTERVAL:
            do_commit = sum(len(v) for v in self._sql_queue.values()) >= self.COMMIT_INTERVAL
            for sql in list(self._sql_queue) if do_commit else ():
                psycopg2.extras.execute_batch(self._cursor, sql, self._sql_queue.pop(sql))
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
        sql, args = self._sql_cache.get(table_name), []

        for p, v, t in rosapi.iter_message_fields(msg):
            scalart = rosapi.scalar(t)
            if isinstance(v, (list, tuple)):
                if v and rosapi.is_ros_time(v[0]):
                    v = [rosapi.to_decimal(x) for x in v]
                elif scalart not in rosapi.ROS_BUILTIN_TYPES:
                    if self._nesting: v = None
                    else:
                        v = [rosapi.message_to_dict(m) for m in v]
                        v = psycopg2.extras.Json(v, json.dumps)
                elif "BYTEA" == self.COMMON_TYPES.get(t):
                    v = psycopg2.Binary(bytes(bytearray(v)))  # Py2/Py3 compatible
                else:
                    v = list(v)  # Values for psycopg2 cannot be tuples
            elif rosapi.is_ros_time(v):
                v = rosapi.to_decimal(v)
            elif t not in rosapi.ROS_BUILTIN_TYPES:
                v = psycopg2.extras.Json(rosapi.message_to_dict(v), json.dumps)
            args.append(v)
        myargs = [topic, topic_id, rosapi.to_datetime(stamp), rosapi.to_decimal(stamp)]
        myid = self._get_next_id(table_name) if self._nesting else None
        if self._nesting: myargs += [myid, parent_type, parent_id]
        args = tuple(args + myargs)

        if not sql:
            sql = "INSERT INTO %s VALUES (%s)" % (quote(table_name), ", ".join(["%s"] * len(args)))
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
            args = [psycopg2.extras.Json(x, json.dumps) for x in subids.values()] + [myid]
            sets = ["%s = %%s" % quote(".".join(p)) for p in subids]
            sql  = "UPDATE %s SET %s WHERE _id = %%s" % (quote(table_name), ", ".join(sets))
            self._ensure_execute(sql, args)
        return myid


    def _ensure_columns(self, cols):
        """Adds specified columns to any type tables lacking them."""
        altered = False
        for typekey, typecols in self._schema.items():
            missing = [(c, t) for c, t in cols if c not in typecols]
            if not missing: continue  # for typekey
            table_name = self._types[typekey]["table_name"]
            actions = ", ".join("ADD COLUMN %s %s" % ct for ct in missing)
            sql = "ALTER TABLE %s %s" % (quote(table_name), actions)
            self._cursor.execute(sql)
            typecols.update(missing)
            altered = True
        altered and self._db.commit()


    def _ensure_execute(self, sql, args):
        """Executes SQL if in autocommit mode, else caches arguments for batch execution."""
        if self.COMMIT_INTERVAL:
            self._sql_queue.setdefault(sql, []).append(args)
        else:
            self._cursor.execute(sql, args)


    def _get_next_id(self, table):
        """Returns next cached ID value, re-populating empty cache from sequence."""
        if not self._id_queue.get(table):
            sql = "SELECT nextval('%s') AS id" % quote("%s__id_seq" % table)
            for _ in range(self.ID_SEQUENCE_STEP):
                self._cursor.execute(sql)
                self._id_queue[table].append(self._cursor.fetchone()["id"])
        return self._id_queue[table].popleft()


    def _make_name(self, category, name, typehash):
        """Returns valid unique name for table/view, inserting meta-row if necessary."""
        result = name
        if len(result) > self.MAX_NAME_LEN:
            result = result[:self.MAX_NAME_LEN - 2] + ".."
        if result in set(sum(([x["table_name"], x.get("view_name")]
                              for x in self._topics.values() if x["md5"] != typehash), [])):
            result = name[:self.MAX_NAME_LEN - len(typehash) - 3]
            if len(result) + len(typehash) > self.MAX_NAME_LEN:
                result = "%s.." % (name[:self.MAX_NAME_LEN - len(typehash) - 5])
            result = "%s (%s)" % (result, typehash)
        if result != name:
            meta = {"type": category, "parent": None, "name": name, "pg_name": result}
            self._cursor.execute(self.INSERT_META, meta)
            self._metas.setdefault((category, None), {})[name] = result
        return result


    def _make_column_names(self, col_names, table_name):
        """Returns valid unique names for table columns, inserting meta-rows if necessary."""
        result = collections.OrderedDict()  # {pg_name: original name}
        metas, METABASE = [], {"type": "column", "parent": table_name}
        for col_name in col_names:
            pg_name = col_name
            if len(pg_name) > self.MAX_NAME_LEN:
                pg_name = col_name[:self.MAX_NAME_LEN - 2] + ".."
            counter = 2
            while pg_name in result:
                pg_name = "%s.. (%s)" % (col_name[:self.MAX_NAME_LEN - len(str(counter)) - 5], counter)
                counter += 1
            if pg_name != col_name:
                metas.append(dict(METABASE, **{"name": col_name, "pg_name": pg_name}))
            result[pg_name] = col_name
        for meta in metas:
            self._cursor.execute(self.INSERT_META, meta)
            self._metas.setdefault(("column", table_name), {})[meta["name"]] = meta["pg_name"]
        return list(result)


    @classmethod
    def autodetect(cls, dump_target):
        """Returns true if dump_target is recognizable as a Postgres connection string."""
        return (dump_target or "").startswith("postgresql://")

# -*- coding: utf-8 -*-
"""
Sink plugin for dumping messages to a Postgres database.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     02.12.2021
@modified    04.12.2021
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
    - table "meta", with mappings between original names and names shortened for Postgres
    - table "topics", with topic message type definitions

    plus:
    - table "pkg/MsgType" for each topic message type, with detailed fields,
      BYTEA fields for uint8[], array fields for scalar list attributes,
      and JSONB fields for lists of ROS messages;
      plus `_topic` as the topic name, `_topic_id` as link to `topics.id`
      and `_timestamp`

    If a message type table already exists but for a type with a different MD5 hash,
    the new table will have its MD5 hash appended to end, as "pkg/MsgType (hash)".
    """

    ## Max table/column name length in Postgres
    MAX_NAME_LEN = 63

    ## Number of emits between commits; 0 is autocommit
    COMMIT_INTERVAL = 100

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
      definition           TEXT NOT NULL,
      md5                  TEXT NOT NULL,
      table_name           TEXT NOT NULL,
      view_name            TEXT NOT NULL
    );
    """

    ## SQL statement for inserting topics
    INSERT_TOPIC = """
    INSERT INTO topics (name, type, definition, md5, table_name, view_name)
    VALUES (%(name)s, %(type)s, %(definition)s, %(md5)s, %(table_name)s, '')
    RETURNING id
    """

    ## SQL statement for inserting types
    INSERT_META = """
    INSERT INTO meta (type, parent, name, pg_name)
    VALUES (%(type)s, %(parent)s, %(name)s, %(pg_name)s)
    """

    ## SQL statement for updating view name in topic
    UPDATE_TOPIC = """
    UPDATE topics SET view_name = %(view_name)s
    WHERE id = %(id)s
    """

    ## SQL statement for creating a view for topic
    CREATE_TOPIC_VIEW = """
    DROP VIEW IF EXISTS %(name)s;

    CREATE VIEW %(name)s AS
    SELECT %(cols)s
    FROM %(type)s
    WHERE _topic = %(topic)s;
    """

    ## Default columns for pkg/MsgType tables
    MESSAGE_TYPE_BASECOLS = [("_topic",      "TEXT"),
                             ("_topic_id",   "BIGINT"),
                             ("_timestamp",  "NUMERIC"),
                             ("_id",         "BIGSERIAL PRIMARY KEY"), ]


    def __init__(self, args):
        """
        @param   args               arguments object like argparse.Namespace
        @param   args.DUMP_TARGET   Postgres connection string postgresql://user@host/db
        @param   args.META          whether to print metainfo
        @param   args.VERBOSE       whether to print debug information
        """
        super(PostgresSink, self).__init__(args)

        self._db            = None   # psycopg2.connection
        self._cursor        = None   # psycopg2.cursor
        self._close_printed = False

        self._topics    = {}  # {(topic, typehash): {topics-row}}
        self._metas     = {}  # {(type, parent): {name: pg_name}}
        self._sql_cache = {}  # {table: "INSERT INTO table VALUES (%s, ..)"}
        self._sql_queue = {}  # {SQL: [(args), ]}
        self._schema = collections.defaultdict(dict)  # {(typename, typehash): {cols}}

        atexit.register(self.close)


    def validate(self):
        """Returns whether Postgres driver is available."""
        if not psycopg2:
            ConsolePrinter.error("psycopg2 not available: cannot write to Postgres.")
        return bool(psycopg2)


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
            self._db.close()
            self._db = None
            self._cursor = None
        if not self._close_printed and self._counts:
            self._close_printed = True
            target = self._args.DUMP_TARGET
            if not target.startswith("postgresql://"): target = repr(target)
            ConsolePrinter.debug("Wrote %s in %s to Postgres database %s.",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), target)


    def _init_db(self):
        """Opens the database file, and populates schema if not already existing."""
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
        self._db = psycopg2.connect(self._args.DUMP_TARGET,
                                    cursor_factory=psycopg2.extras.RealDictCursor)
        self._db.autocommit = not self.COMMIT_INTERVAL
        cursor = self._cursor = self._db.cursor()
        cursor.execute(self.BASE_SCHEMA)

        cursor.execute("SELECT * FROM topics")
        for row in cursor.fetchall():
            topickey = (row["name"], row["md5"])
            self._topics[topickey] = row
        cursor.execute("SELECT * FROM meta")
        for row in cursor.fetchall():
            metakey = (row["type"], row["parent"])
            self._metas.setdefault(metakey, {})[row["name"]] = row["pg_name"]

        cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = 'public' "
                       "ORDER BY table_name, CAST(dtd_identifier AS INTEGER)")
        for row in cursor.fetchall():
            if "/" not in row["table_name"]:
                continue  # for row
            topic = next(x for x in self._topics.values() if x["table_name"] == row["table_name"])
            schemakey = (topic["type"], topic["md5"])
            self._schema.setdefault(schemakey, collections.OrderedDict())
            self._schema[schemakey][row["column_name"]] = row["data_type"]


    def _process_topic(self, topic, msg):
        """Inserts topics-row and creates view `/topic/name` if not already existing."""
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        topickey = (topic, typehash)
        is_new = topickey not in self._topics
        if is_new:
            msgdef = self.source.get_message_definition(typename)
            table_name = self._make_name("table", typename, typehash)
            targs = dict(name=topic, type=typename, definition=msgdef,
                         md5=typehash, table_name=table_name)
            if self._args.VERBOSE:
                ConsolePrinter.debug("Adding topic %s.", topic)
            self._cursor.execute(self.INSERT_TOPIC, targs)
            targs["id"] = self._cursor.fetchone()["id"]
            self._topics[topickey] = targs

        self._process_type(topic, msg)

        if is_new:
            view_name = self._make_name("view", topic, typehash)
            cols = [c for c in self._schema[(typename, typehash)]
                    if c not in ("_topic", "_topic_id")]
            vargs = dict(name=quote(view_name), cols=", ".join(map(quote, cols)),
                         type=quote(self._topics[topickey]["table_name"]), topic=repr(topic))
            sql = self.CREATE_TOPIC_VIEW % vargs
            self._cursor.execute(sql)

            self._topics[topickey]["view_name"] = view_name
            self._cursor.execute(self.UPDATE_TOPIC, self._topics[topickey])
            if self.COMMIT_INTERVAL: self._db.commit()


    def _process_type(self, topic, msg):
        """Creates pkg/MsgType table if not already existing."""
        typename = rosapi.get_message_type(msg)
        typehash = self.source.get_message_type_hash(msg)
        typekey  = (typename, typehash)
        if typekey not in self._schema:
            table_name = self._topics[(topic, typehash)]["table_name"]
            cols = []
            for path, value, subtype in self._iter_fields(msg):
                coltype = self.COMMON_TYPES.get(subtype)
                scalartype = rosapi.scalar(subtype)
                if not coltype and scalartype in self.COMMON_TYPES:
                    coltype = self.COMMON_TYPES.get(scalartype) + "[]"
                if not coltype:
                    coltype = "JSONB"
                cols += [(".".join(path), coltype)]
            cols = list(zip(self._make_column_names([c for c, _ in cols], table_name),
                            [t for _, t in cols])) + self.MESSAGE_TYPE_BASECOLS
            coldefs = ["%s %s" % (quote(n), t) for n, t in cols]
            sql = "CREATE TABLE %s (%s)" % (quote(table_name), ", ".join(coldefs))
            self._cursor.execute(sql)
            self._schema[typekey] = collections.OrderedDict(cols)


    def _process_message(self, topic, msg, stamp):
        """Inserts message to pkg/MsgType table."""
        typehash   = self.source.get_message_type_hash(msg)
        topic_id   = self._topics[(topic, typehash)]["id"]
        table_name = self._topics[(topic, typehash)]["table_name"]
        sql, args = self._sql_cache.get(table_name), []

        for p, v, t in self._iter_fields(msg):
            scalart = rosapi.scalar(t)
            if isinstance(v, (list, tuple)):
                if v and rosapi.is_ros_time(v[0]):
                    v = [rosapi.to_decimal(x) for x in v]
                elif scalart not in rosapi.ROS_BUILTIN_TYPES:
                    v = psycopg2.extras.Json([rosapi.message_to_dict(m) for m in v], json.dumps)
                elif "BYTEA" == self.COMMON_TYPES.get(t):
                    v = psycopg2.Binary(bytes(bytearray(v)))  
                else:
                    v = list(v)  # psycopg2 does not handle tuples
            elif rosapi.is_ros_time(v):
                v = rosapi.to_decimal(v)
            elif t not in rosapi.ROS_BUILTIN_TYPES:
                v = psycopg2.extras.Json(rosapi.message_to_dict(v), json.dumps)
            args.append(v)
        args = tuple(args + [topic, topic_id, rosapi.to_decimal(stamp)])

        if not sql:
            sql = "INSERT INTO %s VALUES (%s)" % (quote(table_name), ", ".join(["%s"] * len(args)))
            self._sql_cache[table_name] = sql
        if self.COMMIT_INTERVAL:
            self._sql_queue.setdefault(sql, []).append(args)
            do_commit = sum(len(v) for v in self._sql_queue.values()) > self.COMMIT_INTERVAL
            for sql in list(self._sql_queue) if do_commit else ():
                psycopg2.extras.execute_batch(self._cursor, sql, self._sql_queue.pop(sql))
            do_commit and self._db.commit()
        else:
            self._cursor.execute(sql, args)


    def _make_name(self, category, name, typehash):
        """Returns valid unique name for table/view, inserting meta-row if necessary."""
        result = name
        if len(result) > self.MAX_NAME_LEN:
            result = result[:self.MAX_NAME_LEN - 2] + ".."
        if result in set(sum(([x["table_name"], x["view_name"]]
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


    def _iter_fields(self, msg, top=()):
        """
        Yields ((nested, path), value, typename) from ROS message.

        @param  messages_only  whether to yield only values that are ROS messages themselves
                               or lists of ROS messages, else will yield scalar and list values
        """
        fieldmap = rosapi.get_message_fields(msg)
        for k, t in fieldmap.items() if fieldmap != msg else ():
            v, path = rosapi.get_message_value(msg, k, t), top + (k, )
            if not rosapi.is_ros_message(v, ignore_time=False):
                yield path, v, t
            else:
                for p2, v2, t2 in self._iter_fields(v, top=path):
                    yield p2, v2, t2

    @classmethod
    def autodetect(cls, dump_target):
        """Returns true if dump_target is recognizable as a Postgres connection string."""
        return (dump_target or "").startswith("postgresql://")

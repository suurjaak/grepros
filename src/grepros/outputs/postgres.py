# -*- coding: utf-8 -*-
"""
Sink plugin for dumping messages to a Postgres database.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     02.12.2021
@modified    12.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs.postgres
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
from .. common import ConsolePrinter
from . dbbase import DataSinkBase

quote = lambda s: common.quote(s, force=True)


class PostgresSink(DataSinkBase):
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
      name_in_db TEXT NOT NULL
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
      table_name           TEXT NOT NULL,
      nested_tables        JSON
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
    INSERT INTO meta (type, parent, name, name_in_db)
    VALUES (%(type)s, %(parent)s, %(name)s, %(name_in_db)s)
    """

    ## SQL statement for updating view name in topic
    UPDATE_TOPIC_VIEW = """
    UPDATE topics SET view_name = %(view_name)s
    WHERE id = %(id)s
    """

    ## SQL statement for creating a table for type
    CREATE_TYPE_TABLE = """
    DROP TABLE IF EXISTS %(name)s CASCADE;

    CREATE TABLE %(name)s (%(cols)s);
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
        self._id_queue = collections.defaultdict(collections.deque)  # {table name: [next ID, ]}


    def validate(self):
        """
        Returns whether Postgres driver is available,
        and "commit-interval" and "nesting" in args.DUMP_OPTIONS have valid value, if any.
        """
        driver_ok, config_ok = bool(psycopg2), super(PostgresSink, self).validate()
        if not driver_ok:
            ConsolePrinter.error("psycopg2 not available: cannot write to Postgres.")
        return driver_ok and config_ok


    def _init_db(self):
        """Opens the database file, and populates schema if not already existing."""
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
        psycopg2.extras.register_default_jsonb(globally=True, loads=json.loads)
        super(PostgresSink, self)._init_db()


    def _load_schema(self):
        """Populates instance attributes with schema metainfo."""
        super(PostgresSink, self)._load_schema()
        self._cursor.execute(self.SELECT_TYPE_COLUMNS)
        for row in self._cursor.fetchall():
            typerow = next(x for x in self._types.values() if x["table_name"] == row["table_name"])
            typekey = (typerow["type"], typerow["md5"])
            self._schema.setdefault(typekey, collections.OrderedDict())
            self._schema[typekey][row["column_name"]] = row["data_type"]


    def _connect(self):
        """Returns new database connection."""
        return psycopg2.connect(self._args.DUMP_TARGET,
                                cursor_factory=psycopg2.extras.RealDictCursor)


    def _execute_insert(self, sql, args):
        """Executes INSERT statement, returns inserted ID."""
        self._cursor.execute(sql, args)
        return self._cursor.fetchone()["id"]


    def _executemany(self, sql, argses):
        """Executes SQL with all args sequences."""
        psycopg2.extras.execute_batch(self._cursor, sql, argses)


    def _executescript(self, sql):
        """Executes SQL with one or more statements."""
        self._cursor.execute(sql)


    def _get_next_id(self, table):
        """Returns next cached ID value, re-populating empty cache from sequence."""
        if not self._id_queue.get(table):
            sql = "SELECT nextval('%s') AS id" % quote("%s__id_seq" % table)
            for _ in range(self.ID_SEQUENCE_STEP):
                self._cursor.execute(sql)
                self._id_queue[table].append(self._cursor.fetchone()["id"])
        return self._id_queue[table].popleft()


    def _make_column_type(self, typename, value):
        """Returns column database type."""
        coltype = self.COMMON_TYPES.get(typename)
        scalartype = rosapi.scalar(typename)
        if not coltype and scalartype in self.COMMON_TYPES:
            coltype = self.COMMON_TYPES.get(scalartype) + "[]"
        if not coltype:
            coltype = "JSONB"
        return coltype


    def _make_column_value(self, value, typename=None):
        """Returns column value suitable for inserting to database."""
        v = value
        if not typename:
            v = psycopg2.extras.Json(v, json.dumps)
        elif isinstance(v, (list, tuple)):
            if v and rosapi.is_ros_time(v[0]):
                v = [rosapi.to_decimal(x) for x in v]
            elif rosapi.scalar(typename) not in rosapi.ROS_BUILTIN_TYPES:
                if self._nesting: v = None
                else: v = psycopg2.extras.Json([rosapi.message_to_dict(m) for m in v], json.dumps)
            elif "BYTEA" == self.COMMON_TYPES.get(typename):
                v = psycopg2.Binary(bytes(bytearray(v)))  # Py2/Py3 compatible
            else:
                v = list(v)  # Values for psycopg2 cannot be tuples
        elif rosapi.is_ros_time(v):
            v = rosapi.to_decimal(v)
        elif typename and typename not in rosapi.ROS_BUILTIN_TYPES:
            v = psycopg2.extras.Json(rosapi.message_to_dict(v), json.dumps)


    def _make_db_label(self):
        """Returns formatted label for database."""
        target = self._args.DUMP_TARGET
        if not target.startswith("postgresql://"): target = repr(target)
        return target


    @classmethod
    def autodetect(cls, dump_target):
        """Returns true if dump_target is recognizable as a Postgres connection string."""
        return (dump_target or "").startswith("postgresql://")

# -*- coding: utf-8 -*-
"""
Postgres output plugin.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     02.12.2021
@modified    28.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.auto.postgres
import collections
import json

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.extras
except ImportError:
    psycopg2 = None

from ... import api
from ... common import ConsolePrinter
from . dbbase import BaseDataSink, quote


class PostgresSink(BaseDataSink):
    """
    Writes messages to a Postgres database.

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
    ENGINE = "Postgres"

    ## Sequence length per table to reserve for inserted message IDs
    ID_SEQUENCE_STEP = 100

    ## SQL statement for selecting metainfo on pkg/MsgType table columns
    SELECT_TYPE_COLUMNS = """
    SELECT c.table_name, c.column_name, c.data_type
    FROM   information_schema.columns c INNER JOIN information_schema.tables t
    ON     t.table_name = c.table_name
    WHERE  c.table_schema = current_schema() AND t.table_type = 'BASE TABLE' AND
           c.table_name LIKE '%/%'
    ORDER BY c.table_name, CAST(c.dtd_identifier AS INTEGER)
    """

    ## Default topic-related columns for pkg/MsgType tables, as [(name, SQL type)]
    MESSAGE_TYPE_TOPICCOLS = [("_topic",       "TEXT"),
                              ("_topic_id",    "BIGINT"), ]
    ## Default columns for pkg/MsgType tables
    MESSAGE_TYPE_BASECOLS  = [("_dt",          "TIMESTAMP"),
                              ("_timestamp",   "NUMERIC"),
                              ("_id",          "BIGSERIAL PRIMARY KEY"), ]
    ## Additional default columns for pkg/MsgType tables with nesting output
    MESSAGE_TYPE_NESTCOLS  = [("_parent_type", "TEXT"),
                              ("_parent_id",   "BIGINT"), ]


    def __init__(self, args=None, **kwargs):
        """
        @param   args                 arguments as namespace or dictionary, case-insensitive;
                                      or a single item as the database connection string
        @param   args.write           Postgres connection string like "postgresql://user@host/db"
        @param   args.write_options   ```
                                      {"commit-interval": transaction size (0 is autocommit),
                                       "nesting": "array" to recursively insert arrays
                                                  of nested types, or "all" for any nesting)}
                                      ```
        @param   args.meta            whether to emit metainfo
        @param   args.verbose         whether to emit debug information
        @param   kwargs               any and all arguments as keyword overrides, case-insensitive
        """
        super(PostgresSink, self).__init__(args, **kwargs)
        self._id_queue = collections.defaultdict(collections.deque)  # {table name: [next ID, ]}


    def validate(self):
        """
        Returns whether Postgres driver is available,
        and "commit-interval" and "nesting" in args.write_options have valid value, if any,
        and database is connectable.
        """
        if self.valid is not None: return self.valid
        db_ok, driver_ok, config_ok = False, bool(psycopg2), super(PostgresSink, self).validate()
        if not driver_ok:
            ConsolePrinter.error("psycopg2 not available: cannot write to Postgres.")
        else:
            try:
                with self._connect(): db_ok = True
            except Exception as e:
                ConsolePrinter.error("Error connecting Postgres: %s", e)
        self.valid = db_ok and driver_ok and config_ok
        return self.valid


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
            typerow = next((x for x in self._types.values()
                            if x["table_name"] == row["table_name"]), None)
            if not typerow: continue  # for row
            typekey = (typerow["type"], typerow["md5"])
            self._schema.setdefault(typekey, collections.OrderedDict())
            self._schema[typekey][row["column_name"]] = row["data_type"]


    def _connect(self):
        """Returns new database connection."""
        factory = psycopg2.extras.RealDictCursor
        return psycopg2.connect(self.args.WRITE, cursor_factory=factory)


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
            MAXLEN = self._get_dialect_option("maxlen_entity")
            seqbase, seqsuffix = table, "_%s_seq" % self.MESSAGE_TYPE_BASECOLS[-1][0]
            if MAXLEN: seqbase = seqbase[:MAXLEN - len(seqsuffix)]
            sql = "SELECT nextval('%s') AS id" % quote(seqbase + seqsuffix)
            for _ in range(self.ID_SEQUENCE_STEP):
                self._cursor.execute(sql)
                self._id_queue[table].append(self._cursor.fetchone()["id"])
        return self._id_queue[table].popleft()


    def _make_column_value(self, value, typename=None):
        """Returns column value suitable for inserting to database."""
        TYPES = self._get_dialect_option("types")
        plaintype = api.scalar(typename)  # "string<=10" -> "string"
        v = value
        # Common in JSON but disallowed in Postgres
        replace = {float("inf"): None, float("-inf"): None, float("nan"): None}
        if not typename:
            v = psycopg2.extras.Json(v, json.dumps)
        elif isinstance(v, (list, tuple)):
            scalartype = api.scalar(typename)
            if scalartype in api.ROS_TIME_TYPES:
                v = [self._convert_time_value(x, scalartype) for x in v]
            elif scalartype not in api.ROS_BUILTIN_TYPES:
                if self._nesting: v = None
                else: v = psycopg2.extras.Json([api.message_to_dict(m, replace)
                                                for m in v], json.dumps)
            elif "BYTEA" in (TYPES.get(typename),
                             TYPES.get(api.canonical(typename, unbounded=True))):
                v = psycopg2.Binary(bytes(bytearray(v)))  # Py2/Py3 compatible
            else:
                v = list(self._convert_column_value(v, typename))  # Ensure not-tuple for psycopg2
        elif api.is_ros_time(v):
            v = self._convert_time_value(v, typename)
        elif plaintype not in api.ROS_BUILTIN_TYPES:
            v = psycopg2.extras.Json(api.message_to_dict(v, replace), json.dumps)
        else:
            v = self._convert_column_value(v, plaintype)
        return v


    def _make_db_label(self):
        """Returns formatted label for database."""
        target = self.args.WRITE
        if not target.startswith(("postgres://", "postgresql://")): target = repr(target)
        return target


    @classmethod
    def autodetect(cls, target):
        """Returns true if target is recognizable as a Postgres connection string."""
        return (target or "").startswith(("postgres://", "postgresql://"))



def init(*_, **__):
    """Adds Postgres output format support."""
    from ... import plugins  # Late import to avoid circular
    plugins.add_write_format("postgres", PostgresSink, "Postgres", [
        ("commit-interval=NUM",  "transaction size for Postgres output\n"
                                 "(default 1000, 0 is autocommit)"),
        ("dialect-file=path/to/dialects.yaml",
                                 "load additional SQL dialect options\n"
                                 "for Postgres output\n"
                                 "from a YAML or JSON file"),
        ("nesting=array|all",    "create tables for nested message types\n"
                                 "in Postgres output,\n"
                                 'only for arrays if "array" \n'
                                 "else for any nested types\n"
                                 "(array fields in parent will be populated \n"
                                 " with foreign keys instead of messages as JSON)"),
    ])


__all__ = ["PostgresSink", "init"]

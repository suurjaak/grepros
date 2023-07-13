# -*- coding: utf-8 -*-
"""
Base class for producing SQL for topics and messages.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.01.2022
@modified    28.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.auto.sqlbase
import json
import re

import yaml

from ... import api
from ... common import ConsolePrinter, ellipsize, ensure_namespace, import_item, merge_dicts



class SqlMixin(object):
    """
    Base class for producing SQL for topics and messages.

    Can load additional SQL dialects or additional options for existing dialects
    from a YAML/JSON file.
    """

    ## Default SQL dialect used if dialect not specified
    DEFAULT_DIALECT = "sqlite"

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(META=False, WRITE_OPTIONS={}, MATCH_WRAPPER=None, VERBOSE=False)


    def __init__(self, args=None, **kwargs):
        """
        @param   args                 arguments as namespace or dictionary, case-insensitive
        @param   args.write_options   ```
                                      {"dialect": SQL dialect if not default,
                                       "nesting": true|false to created nested type tables}
                                      ```
        @param   kwargs               any and all arguments as keyword overrides, case-insensitive
        """
        self._args      = ensure_namespace(args, SqlMixin.DEFAULT_ARGS, **kwargs)
        self._topics    = {}  # {(topic, typename, typehash): {name, table_name, view_name, sql, ..}}
        self._types     = {}  # {(typename, typehash): {type, table_name, sql, ..}}
        self._schema    = {}  # {(typename, typehash): {cols}}
        self._sql_cache = {}  # {table: "INSERT INTO table VALUES (%s, ..)"}
        self._dialect   = args.WRITE_OPTIONS.get("dialect", self.DEFAULT_DIALECT)
        self._nesting   = args.WRITE_OPTIONS.get("nesting")


    def validate(self):
        """
        Returns whether arguments are valid.

        Verifies that "dialect-file" is valid and "dialect" contains supported value, if any.
        """
        return all([self._validate_dialect_file(), self._validate_dialect()])


    def _validate_dialect_file(self):
        """Returns whether "dialect-file" is valid in args.WRITE_OPTIONS."""
        ok = True
        if self._args.WRITE_OPTIONS.get("dialect-file"):
            filename = self._args.WRITE_OPTIONS["dialect-file"]
            try:
                with open(filename) as f:
                    dialects = yaml.safe_load(f.read())
                if any(not isinstance(v, dict) for v in dialects.values()):
                    raise Exception("Each dialect must be a dictionary.")
                for opts in dialects.values():
                    for k, v in list(opts.get("adapters", {}).items()):
                        try: opts["adapters"][k] = import_item(v)
                        except ImportError:
                            ok = False
                            ConsolePrinter.error("Error loading adapter %r for %r "
                                                 "in SQL dialect file %r.", v, k, filename)
                merge_dicts(self.DIALECTS, dialects)
            except Exception as e:
                ok = False
                ConsolePrinter.error("Error reading SQL dialect file %r: %s", filename, e)

        # Populate ROS type aliases like "byte" and "char"
        for opts in self.DIALECTS.values() if ok else ():
            for rostype in list(opts.get("types", {})):
                alias = api.get_type_alias(rostype)
                if alias:
                    opts["types"][alias] = opts["types"][rostype]
                if alias and rostype + "[]" in opts["types"]:
                    opts["types"][alias + "[]"] = opts["types"][rostype + "[]"]

        return ok


    def _validate_dialect(self):
        """Returns whether "dialect" is valid in args.WRITE_OPTIONS."""
        ok = True
        if "dialect" in self._args.WRITE_OPTIONS \
        and self._args.WRITE_OPTIONS["dialect"] not in tuple(filter(bool, self.DIALECTS)):
            ok = False
            ConsolePrinter.error("Unknown dialect for SQL: %r. "
                                 "Choose one of {%s}.",
                                 self._args.WRITE_OPTIONS["dialect"],
                                 "|".join(sorted(filter(bool, self.DIALECTS))))
        return ok


    def close(self):
        """Clears data structures."""
        self._topics.clear()
        self._types.clear()
        self._schema.clear()
        self._sql_cache.clear()


    def _make_topic_data(self, topic, msg, exclude_cols=()):
        """
        Returns full data dictionary for topic, including view name and SQL.

        @param   exclude_cols  list of column names to exclude from view SELECT, if any
        @return                {"name": topic name, "type": message type name as "pkg/Cls",
                                "table_name": message type table name, "view_name": topic view name,
                                "md5": message type definition MD5 hash, "sql": "CREATE VIEW .."}
        """
        with api.TypeMeta.make(msg, topic) as m:
            typename, typehash, typekey = (m.typename, m.typehash, m.typekey)

        table_name = self._types[typekey]["table_name"]
        pkgname, clsname = typename.split("/", 1)
        nameargs = {"topic": topic, "type": typename, "hash": typehash,
                    "package": pkgname, "class": clsname}
        view_name = self._make_entity_name("view", nameargs)

        sqlargs = dict(nameargs, view=quote(view_name), table=quote(table_name, force=True),
                       topic=repr(topic), cols="*")
        if exclude_cols:
            exclude_cols = [x[0] if isinstance(x, (list, tuple)) else x for x in exclude_cols]
            select_cols = [c for c in self._schema[typekey] if c not in exclude_cols]
            sqlargs["cols"] = ", ".join(quote(c) for c in select_cols)
        sql = self._get_dialect_option("view_template").strip().format(**sqlargs)

        return {"name": topic, "type": typename, "md5": typehash,
                "sql": sql, "table_name": table_name, "view_name": view_name}


    def _make_type_data(self, msg, extra_cols=(), rootmsg=None):
        """
        Returns full data dictionary for message type, including table name and SQL.

        @param   rootmsg     top message this message is nested under, if any
        @param   extra_cols  additional table columns, as [(column name, column def)]
        @return              {"type": message type name as "pkg/Cls",
                              "table_name": message type table name,
                              "definition": message type definition,
                              "cols": [(column name, column type)],
                              "md5": message type definition MD5 hash, "sql": "CREATE TABLE .."}
        """
        rootmsg = rootmsg or msg
        with api.TypeMeta.make(msg, root=rootmsg) as m:
            typename, typehash = (m.typename, m.typehash)

        cols = []
        scalars = set(x for x in self._get_dialect_option("types") if x == api.scalar(x))
        for path, value, subtype in api.iter_message_fields(msg, scalars=scalars):
            coltype = self._make_column_type(subtype)
            cols += [(".".join(path), coltype)]
        cols.extend(extra_cols or [])
        cols = list(zip(self._make_column_names([c for c, _ in cols]), [t for _, t in cols]))
        namewidth = 2 + max(len(n) for n, _ in cols)
        coldefs = ["%s  %s" % (quote(n).ljust(namewidth), t) for n, t in cols]

        pkgname, clsname = typename.split("/", 1)
        nameargs = {"type": typename, "hash": typehash, "package": pkgname, "class": clsname}
        table_name = self._make_entity_name("table", nameargs)

        sqlargs = dict(nameargs, table=quote(table_name), cols="\n  %s\n" % ",\n  ".join(coldefs))
        sql = self._get_dialect_option("table_template").strip().format(**sqlargs)
        return {"type": typename, "md5": typehash,
                "definition": api.TypeMeta.make(msg).definition,
                "table_name": table_name, "cols": cols, "sql": sql}


    def _make_topic_insert_sql(self, topic, msg):
        """Returns ("INSERT ..", [args]) for inserting into topics-table."""
        POSARG = self._get_dialect_option("posarg")
        topickey = api.TypeMeta.make(msg, topic).topickey
        tdata = self._topics[topickey]

        sql  = self._get_dialect_option("insert_topic").strip().replace("%s", POSARG)
        args = [tdata[k] for k in ("name", "type", "md5", "table_name", "view_name")]
        return sql, args


    def _make_type_insert_sql(self, msg):
        """Returns ("INSERT ..", [args]) for inserting into types-table."""
        POSARG = self._get_dialect_option("posarg")
        typekey = api.TypeMeta.make(msg).typekey
        tdata = self._types[typekey]

        sql  = self._get_dialect_option("insert_type").strip().replace("%s", POSARG)
        args = [tdata[k] for k in ("type", "definition", "md5", "table_name")]
        return sql, args


    def _make_message_insert_sql(self, topic, msg, extra_cols=()):
        """
        Returns ("INSERT ..", [args]) for inserting into message type table.

        @param   extra_cols  list of additional table columns, as [(name, value)]
        """
        typekey = api.TypeMeta.make(msg, topic).typekey
        table_name = self._types[typekey]["table_name"]
        sql, cols, args = self._sql_cache.get(table_name), [], []

        scalars = set(x for x in self._get_dialect_option("types") if x == api.scalar(x))
        for p, v, t in api.iter_message_fields(msg, scalars=scalars):
            if not sql: cols.append(".".join(p))
            args.append(self._make_column_value(v, t))
        args = tuple(args) + tuple(v for _, v in extra_cols)

        if not sql:
            POSARG = self._get_dialect_option("posarg")
            if extra_cols: cols.extend(c for c, _ in extra_cols)
            sql = "INSERT INTO %s (%s) VALUES (%s)" % \
                  (quote(table_name), ", ".join(map(quote, cols)),
                   ", ".join([POSARG] * len(args)))
            self._sql_cache[table_name] = sql

        return sql, args


    def _make_update_sql(self, table, values, where=()):
        """Returns ("UPDATE ..", [args])."""
        POSARG = self._get_dialect_option("posarg")
        sql, args, sets, filters = "UPDATE %s SET " % quote(table), [], [], []
        for lst, vals in [(sets, values), (filters, where)]:
            for k, v in vals.items() if isinstance(vals, dict) else vals:
                lst.append("%s = %s" % (quote(k), POSARG))
                args.append(self._make_column_value(v))
        sql += ", ".join(sets) + (" WHERE " if filters else "") + " AND ".join(filters)
        return sql, args


    def _make_entity_name(self, category, args):
        """
        Returns valid unique name for table/view.

        @param   args  format arguments for table/view name template
        """
        name = self._get_dialect_option("%s_name_template" % category).format(**args)
        existing = set(sum(([x["table_name"], x.get("view_name")]
                            for dct in (self._topics, self._types)
                            for x in dct.values()), []))
        return self._make_name("entity", name, existing)


    def _make_name(self, category, name, existing=()):
        """
        Returns a valid unique name for table/view/column.

        Replaces invalid characters and constrains length.
        If name already exists, appends counter like " (2)".
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


    def _make_column_names(self, col_names):
        """Returns valid unique names for table columns."""
        result = []
        for name in col_names:
            result.append(self._make_name("column", name, result))
        return list(result)


    def _make_column_value(self, value, typename=None):
        """Returns column value suitable for inserting to database."""
        if not typename: return value

        v = value
        if isinstance(v, (list, tuple)):
            scalartype = api.scalar(typename)
            if scalartype in api.ROS_TIME_TYPES:
                v = [self._convert_time(x) for x in v]
            elif scalartype not in api.ROS_BUILTIN_TYPES:
                if self._nesting: v = []
                else: v = [api.message_to_dict(x) for x in v]
            else:
                v = self._convert_column_value(v, typename)
        elif api.is_ros_time(v):
            v = self._convert_time_value(v, typename)
        elif typename not in api.ROS_BUILTIN_TYPES:
            v = json.dumps(api.message_to_dict(v))
        else:
            v = self._convert_column_value(v, typename)
        return v


    def _make_column_type(self, typename, fallback=None):
        """
        Returns column type for SQL.

        @param  fallback  fallback typename to use for lookup if no mapping for typename
        """
        TYPES         = self._get_dialect_option("types")
        ARRAYTEMPLATE = self._get_dialect_option("arraytype_template")
        DEFAULTTYPE   = self._get_dialect_option("defaulttype")

        scalartype = api.scalar(typename)
        timetype   = api.get_ros_time_category(scalartype)
        coltype    = TYPES.get(typename) or TYPES.get(api.canonical(typename, unbounded=True))

        if not coltype and scalartype in TYPES:
            coltype = ARRAYTEMPLATE.format(type=TYPES[scalartype])
        if not coltype and timetype in TYPES:
            if typename != scalartype:
                coltype = ARRAYTEMPLATE.format(type=TYPES[timetype])
            else:
                coltype = TYPES[timetype]
        if not coltype and fallback:
            coltype = self._make_column_type(fallback)
        if not coltype:
            coltype = DEFAULTTYPE or quote(typename)
        return coltype


    def _convert_column_value(self, value, typename):
        """Returns ROS value converted to dialect value."""
        ADAPTERS = self._get_dialect_option("adapters")
        if not ADAPTERS: return value

        adapter, iterate = ADAPTERS.get(typename), False
        if not adapter and isinstance(value, (list, tuple)):
            adapter, iterate = ADAPTERS.get(api.scalar(typename)), True
        if adapter:
            value = [adapter(x) for x in value] if iterate else adapter(value)
        return value


    def _convert_time_value(self, value, typename):
        """Returns ROS time/duration value converted to dialect value."""
        adapter = self._get_dialect_option("adapters").get(typename)
        if adapter:
            try: is_int = issubclass(adapter, int)
            except Exception: is_int = False
            v = api.to_sec(value) if is_int else "%d.%09d" % api.to_sec_nsec(value)
            result = adapter(v)
        else:
            result = api.to_decimal(value)
        return result


    def _get_dialect_option(self, option):
        """Returns option for current SQL dialect, falling back to default dialect."""
        return self.DIALECTS[self._dialect].get(option, self.DIALECTS[None].get(option))


    ## Supported SQL dialects and options
    DIALECTS = {

        None: {
            # CREATE TABLE template, args: table, cols, type, hash, package, class
            "table_template":       "CREATE TABLE IF NOT EXISTS {table} ({cols});",
            # CREATE VIEW template, args: view, cols, table, topic, type, hash, package, class
            "view_template":        """
DROP VIEW IF EXISTS {view};

CREATE VIEW {view} AS
SELECT {cols}
FROM {table}
WHERE _topic = {topic};""",
            "table_name_template":  "{type}",    # args: type, hash, package, class
            "view_name_template":   "{topic}",   # args: topic, type, hash, package, class
            "types":                {},          # Mapping between ROS and SQL common types
            "adapters":             {},          # Mapping between ROS types and callable converters
            "defaulttype":          None,        # Fallback SQL type if no mapping for ROS type
            "arraytype_template":   "{type}[]",  # Array type template, args: type
            "maxlen_entity":        0,           # Maximum table/view name length, 0 disables
            "maxlen_column":        0,           # Maximum column name length, 0 disables
            "invalid_char_regex":   None,        # Regex for matching invalid characters in name
            "invalid_char_repl":    "__",        # Replacement for invalid characters in name

            "insert_topic": """
INSERT INTO topics (name, type, md5, table_name, view_name)
VALUES (%s, %s, %s, %s, %s);""",
            "insert_type": """
INSERT INTO types (type, definition, md5, table_name)
VALUES (%s, %s, %s, %s);""",
            "posarg":       "%s",
        },

        "sqlite": {
            "posarg":       "?",
            "base_schema": """
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

CREATE TABLE IF NOT EXISTS topics (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  name                 TEXT    NOT NULL,
  type                 TEXT    NOT NULL,
  serialization_format TEXT    DEFAULT "cdr",
  offered_qos_profiles TEXT    DEFAULT "",

  md5                  TEXT    NOT NULL,
  table_name           TEXT    NOT NULL,
  view_name            TEXT
);

CREATE TABLE IF NOT EXISTS types (
  id            INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type          TEXT    NOT NULL,
  definition    TEXT    NOT NULL,
  md5           TEXT    NOT NULL,
  table_name    TEXT    NOT NULL,
  nested_tables JSON
);

CREATE INDEX IF NOT EXISTS timestamp_idx ON messages (timestamp ASC);

PRAGMA journal_mode = WAL;
""",
            "insert_message": """
INSERT INTO messages (topic_id, timestamp, data, topic, type, dt, yaml)
VALUES (:topic_id, :timestamp, :data, :topic, :type, :dt, :yaml)
""",
        },

        "postgres": {
            "types": {
                "int8":    "SMALLINT",  "int16":   "SMALLINT",  "int32":   "INTEGER",
                "uint8":   "SMALLINT",  "uint16":  "INTEGER",   "uint32":  "BIGINT",
                "int64":   "BIGINT",    "uint64":  "BIGINT",    "bool":    "BOOLEAN",
                "string":  "TEXT",      "wstring": "TEXT",      "uint8[]": "BYTEA",
                "float32": "REAL",      "float64": "DOUBLE PRECISION",
            },
            "defaulttype":    "JSONB",
            "maxlen_entity":  63,
            "maxlen_column":  63,

            "insert_topic": """
INSERT INTO topics (name, type, md5, table_name, view_name)
VALUES (%s, %s, %s, %s, %s)
RETURNING id;""",
            "insert_type": """
INSERT INTO types (type, definition, md5, table_name)
VALUES (%s, %s, %s, %s)
RETURNING id;""",
            "base_schema": """
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
);""",
        },

        "clickhouse": {
            "table_template":      "CREATE TABLE IF NOT EXISTS {table} ({cols}) ENGINE = ENGINE;",
            "types": {
                "int8":    "Int8",     "int16":   "Int16",    "int32":   "Int32",
                "uint8":   "UInt8",    "uint16":  "UInt16",   "uint32":  "UInt32",
                "int64":   "Int64",    "uint64":  "UInt64",   "bool":    "UInt8",
                "float32": "Float32",  "float64": "Float64",  "uint8[]": "String",
                "string":  "String",   "wstring": "String",
            },
            "defaulttype":         "String",
            "arraytype_template":  "Array({type})",
        },
    }


    ## Words that need quoting if in name context, like table name.
    ## Combined from reserved words for Postgres, SQLite, MSSQL, Oracle et al.
    KEYWORDS = [
        "A", "ABORT", "ABS", "ABSOLUTE", "ACCESS", "ACTION", "ADA", "ADD", "ADMIN", "AFTER",
        "AGGREGATE", "ALIAS", "ALL", "ALLOCATE", "ALSO", "ALTER", "ALWAYS", "ANALYSE", "ANALYZE",
        "AND", "ANY", "ARE", "ARRAY", "AS", "ASC", "ASENSITIVE", "ASSERTION", "ASSIGNMENT",
        "ASYMMETRIC", "AT", "ATOMIC", "ATTACH", "ATTRIBUTE", "ATTRIBUTES", "AUDIT",
        "AUTHORIZATION", "AUTOINCREMENT", "AUTO_INCREMENT", "AVG", "AVG_ROW_LENGTH", "BACKUP",
        "BACKWARD", "BEFORE", "BEGIN", "BERNOULLI", "BETWEEN", "BIGINT", "BINARY", "BIT", "BITVAR",
        "BIT_LENGTH", "BLOB", "BOOL", "BOOLEAN", "BOTH", "BREADTH", "BREAK", "BROWSE", "BULK",
        "BY", "C", "CACHE", "CALL", "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CAST",
        "CATALOG", "CATALOG_NAME", "CEIL", "CEILING", "CHAIN", "CHANGE", "CHAR", "CHARACTER",
        "CHARACTERISTICS", "CHARACTERS", "CHARACTER_LENGTH", "CHARACTER_SET_CATALOG",
        "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CHAR_LENGTH", "CHECK", "CHECKED",
        "CHECKPOINT", "CHECKSUM", "CLASS", "CLASS_ORIGIN", "CLOB", "CLOSE", "CLUSTER", "CLUSTERED",
        "COALESCE", "COBOL", "COLLATE", "COLLATION", "COLLATION_CATALOG", "COLLATION_NAME",
        "COLLATION_SCHEMA", "COLLECT", "COLUMN", "COLUMNS", "COLUMN_NAME", "COMMAND_FUNCTION",
        "COMMAND_FUNCTION_CODE", "COMMENT", "COMMIT", "COMMITTED", "COMPLETION", "COMPRESS",
        "COMPUTE", "CONDITION", "CONDITION_NUMBER", "CONNECT", "CONNECTION", "CONNECTION_NAME",
        "CONSTRAINT", "CONSTRAINTS", "CONSTRAINT_CATALOG", "CONSTRAINT_NAME", "CONSTRAINT_SCHEMA",
        "CONSTRUCTOR", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERSION", "CONVERT", "COPY",
        "CORR", "CORRESPONDING", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CREATEDB",
        "CREATEROLE", "CREATEUSER", "CROSS", "CSV", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_DATE",
        "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME",
        "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR",
        "CURSOR_NAME", "CYCLE", "DATA", "DATABASE", "DATABASES", "DATE", "DATETIME",
        "DATETIME_INTERVAL_CODE", "DATETIME_INTERVAL_PRECISION", "DAY", "DAYOFMONTH", "DAYOFWEEK",
        "DAYOFYEAR", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DBCC",
        "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFAULTS", "DEFERRABLE", "DEFERRED",
        "DEFINED", "DEFINER", "DEGREE", "DELAYED", "DELAY_KEY_WRITE", "DELETE", "DELIMITER",
        "DELIMITERS", "DENSE_RANK", "DENY", "DEPTH", "DEREF", "DERIVED", "DESC", "DESCRIBE",
        "DESCRIPTOR", "DESTROY", "DESTRUCTOR", "DETACH", "DETERMINISTIC", "DIAGNOSTICS",
        "DICTIONARY", "DISABLE", "DISCONNECT", "DISK", "DISPATCH", "DISTINCT", "DISTINCTROW",
        "DISTRIBUTED", "DIV", "DO", "DOMAIN", "DOUBLE", "DROP", "DUAL", "DUMMY", "DUMP", "DYNAMIC",
        "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH", "ELEMENT", "ELSE", "ELSEIF", "ENABLE",
        "ENCLOSED", "ENCODING", "ENCRYPTED", "END", "END-EXEC", "ENUM", "EQUALS", "ERRLVL",
        "ESCAPE", "ESCAPED", "EVERY", "EXCEPT", "EXCEPTION", "EXCLUDE", "EXCLUDING", "EXCLUSIVE",
        "EXEC", "EXECUTE", "EXISTING", "EXISTS", "EXIT", "EXP", "EXPLAIN", "EXTERNAL", "EXTRACT",
        "FALSE", "FETCH", "FIELDS", "FILE", "FILLFACTOR", "FILTER", "FINAL", "FIRST", "FLOAT",
        "FLOAT4", "FLOAT8", "FLOOR", "FLUSH", "FOLLOWING", "FOR", "FORCE", "FOREIGN", "FORTRAN",
        "FORWARD", "FOUND", "FREE", "FREETEXT", "FREETEXTTABLE", "FREEZE", "FROM", "FULL",
        "FULLTEXT", "FUNCTION", "FUSION", "G", "GENERAL", "GENERATED", "GET", "GLOBAL", "GO",
        "GOTO", "GRANT", "GRANTED", "GRANTS", "GREATEST", "GROUP", "GROUPING", "HANDLER", "HAVING",
        "HEADER", "HEAP", "HIERARCHY", "HIGH_PRIORITY", "HOLD", "HOLDLOCK", "HOST", "HOSTS",
        "HOUR", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IDENTIFIED", "IDENTITY",
        "IDENTITYCOL", "IDENTITY_INSERT", "IF", "IGNORE", "ILIKE", "IMMEDIATE", "IMMUTABLE",
        "IMPLEMENTATION", "IMPLICIT", "IN", "INCLUDE", "INCLUDING", "INCREMENT", "INDEX",
        "INDICATOR", "INFILE", "INFIX", "INHERIT", "INHERITS", "INITIAL", "INITIALIZE",
        "INITIALLY", "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSERT_ID", "INSTANCE",
        "INSTANTIABLE", "INSTEAD", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER",
        "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "INVOKER", "IS", "ISAM", "ISNULL",
        "ISOLATION", "ITERATE", "JOIN", "K", "KEY", "KEYS", "KEY_MEMBER", "KEY_TYPE", "KILL",
        "LANCOMPILER", "LANGUAGE", "LARGE", "LAST", "LAST_INSERT_ID", "LATERAL", "LEAD", "LEADING",
        "LEAST", "LEAVE", "LEFT", "LENGTH", "LESS", "LEVEL", "LIKE", "LIMIT", "LINENO", "LINES",
        "LISTEN", "LN", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATION", "LOCATOR",
        "LOCK", "LOGIN", "LOGS", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOWER", "LOW_PRIORITY",
        "M", "MAP", "MATCH", "MATCHED", "MAX", "MAXEXTENTS", "MAXVALUE", "MAX_ROWS", "MEDIUMBLOB",
        "MEDIUMINT", "MEDIUMTEXT", "MEMBER", "MERGE", "MESSAGE_LENGTH", "MESSAGE_OCTET_LENGTH",
        "MESSAGE_TEXT", "METHOD", "MIDDLEINT", "MIN", "MINUS", "MINUTE", "MINUTE_MICROSECOND",
        "MINUTE_SECOND", "MINVALUE", "MIN_ROWS", "MLSLABEL", "MOD", "MODE", "MODIFIES", "MODIFY",
        "MODULE", "MONTH", "MONTHNAME", "MORE", "MOVE", "MULTISET", "MUMPS", "MYISAM", "NAME",
        "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NESTING", "NEW", "NEXT", "NO",
        "NOAUDIT", "NOCHECK", "NOCOMPRESS", "NOCREATEDB", "NOCREATEROLE", "NOCREATEUSER",
        "NOINHERIT", "NOLOGIN", "NONCLUSTERED", "NONE", "NORMALIZE", "NORMALIZED", "NOSUPERUSER",
        "NOT", "NOTHING", "NOTIFY", "NOTNULL", "NOWAIT", "NO_WRITE_TO_BINLOG", "NULL", "NULLABLE",
        "NULLIF", "NULLS", "NUMBER", "NUMERIC", "OBJECT", "OCTETS", "OCTET_LENGTH", "OF", "OFF",
        "OFFLINE", "OFFSET", "OFFSETS", "OIDS", "OLD", "ON", "ONLINE", "ONLY", "OPEN",
        "OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPERATION", "OPERATOR",
        "OPTIMIZE", "OPTION", "OPTIONALLY", "OPTIONS", "OR", "ORDER", "ORDERING", "ORDINALITY",
        "OTHERS", "OUT", "OUTER", "OUTFILE", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING",
        "OWNER", "PACK_KEYS", "PAD", "PARAMETER", "PARAMETERS", "PARAMETER_MODE", "PARAMETER_NAME",
        "PARAMETER_ORDINAL_POSITION", "PARAMETER_SPECIFIC_CATALOG", "PARAMETER_SPECIFIC_NAME",
        "PARAMETER_SPECIFIC_SCHEMA", "PARTIAL", "PARTITION", "PASCAL", "PASSWORD", "PATH",
        "PCTFREE", "PERCENT", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "PLACING",
        "PLAN", "PLI", "POSITION", "POSTFIX", "POWER", "PRAGMA", "PRECEDING", "PRECISION",
        "PREFIX", "PREORDER", "PREPARE", "PREPARED", "PRESERVE", "PRIMARY", "PRINT", "PRIOR",
        "PRIVILEGES", "PROC", "PROCEDURAL", "PROCEDURE", "PROCESS", "PROCESSLIST", "PUBLIC",
        "PURGE", "QUOTE", "RAID0", "RAISE", "RAISERROR", "RANGE", "RANK", "RAW", "READ", "READS",
        "READTEXT", "REAL", "RECHECK", "RECONFIGURE", "RECURSIVE", "REF", "REFERENCES",
        "REFERENCING", "REGEXP", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",
        "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "REINDEX", "RELATIVE",
        "RELEASE", "RELOAD", "RENAME", "REPEAT", "REPEATABLE", "REPLACE", "REPLICATION", "REQUIRE",
        "RESET", "RESIGNAL", "RESOURCE", "RESTART", "RESTORE", "RESTRICT", "RESULT", "RETURN",
        "RETURNED_CARDINALITY", "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH", "RETURNED_SQLSTATE",
        "RETURNS", "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE",
        "ROUTINE_CATALOG", "ROUTINE_NAME", "ROUTINE_SCHEMA", "ROW", "ROWCOUNT", "ROWGUIDCOL",
        "ROWID", "ROWNUM", "ROWS", "ROW_COUNT", "ROW_NUMBER", "RULE", "SAVE", "SAVEPOINT", "SCALE",
        "SCHEMA", "SCHEMAS", "SCHEMA_NAME", "SCOPE", "SCOPE_CATALOG", "SCOPE_NAME", "SCOPE_SCHEMA",
        "SCROLL", "SEARCH", "SECOND", "SECOND_MICROSECOND", "SECTION", "SECURITY", "SELECT",
        "SELF", "SENSITIVE", "SEPARATOR", "SEQUENCE", "SERIALIZABLE", "SERVER_NAME", "SESSION",
        "SESSION_USER", "SET", "SETOF", "SETS", "SETUSER", "SHARE", "SHOW", "SHUTDOWN", "SIGNAL",
        "SIMILAR", "SIMPLE", "SIZE", "SMALLINT", "SOME", "SONAME", "SOURCE", "SPACE", "SPATIAL",
        "SPECIFIC", "SPECIFICTYPE", "SPECIFIC_NAME", "SQL", "SQLCA", "SQLCODE", "SQLERROR",
        "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_BIG_SELECTS",
        "SQL_BIG_TABLES", "SQL_CALC_FOUND_ROWS", "SQL_LOG_OFF", "SQL_LOG_UPDATE",
        "SQL_LOW_PRIORITY_UPDATES", "SQL_SELECT_LIMIT", "SQL_SMALL_RESULT", "SQL_WARNINGS", "SQRT",
        "SSL", "STABLE", "START", "STARTING", "STATE", "STATEMENT", "STATIC", "STATISTICS",
        "STATUS", "STDDEV_POP", "STDDEV_SAMP", "STDIN", "STDOUT", "STORAGE", "STRAIGHT_JOIN",
        "STRICT", "STRING", "STRUCTURE", "STYLE", "SUBCLASS_ORIGIN", "SUBLIST", "SUBMULTISET",
        "SUBSTRING", "SUCCESSFUL", "SUM", "SUPERUSER", "SYMMETRIC", "SYNONYM", "SYSDATE", "SYSID",
        "SYSTEM", "SYSTEM_USER", "TABLE", "TABLES", "TABLESAMPLE", "TABLESPACE", "TABLE_NAME",
        "TEMP", "TEMPLATE", "TEMPORARY", "TERMINATE", "TERMINATED", "TEXT", "TEXTSIZE", "THAN",
        "THEN", "TIES", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYBLOB",
        "TINYINT", "TINYTEXT", "TO", "TOAST", "TOP", "TOP_LEVEL_COUNT", "TRAILING", "TRAN",
        "TRANSACTION", "TRANSACTIONS_COMMITTED", "TRANSACTIONS_ROLLED_BACK", "TRANSACTION_ACTIVE",
        "TRANSFORM", "TRANSFORMS", "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER",
        "TRIGGER_CATALOG", "TRIGGER_NAME", "TRIGGER_SCHEMA", "TRIM", "TRUE", "TRUNCATE", "TRUSTED",
        "TSEQUAL", "TYPE", "UESCAPE", "UID", "UNBOUNDED", "UNCOMMITTED", "UNDER", "UNDO",
        "UNENCRYPTED", "UNION", "UNIQUE", "UNKNOWN", "UNLISTEN", "UNLOCK", "UNNAMED", "UNNEST",
        "UNSIGNED", "UNTIL", "UPDATE", "UPDATETEXT", "UPPER", "USAGE", "USE", "USER",
        "USER_DEFINED_TYPE_CATALOG", "USER_DEFINED_TYPE_CODE", "USER_DEFINED_TYPE_NAME",
        "USER_DEFINED_TYPE_SCHEMA", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VACUUM",
        "VALID", "VALIDATE", "VALIDATOR", "VALUE", "VALUES", "VARBINARY", "VARCHAR", "VARCHAR2",
        "VARCHARACTER", "VARIABLE", "VARIABLES", "VARYING", "VAR_POP", "VAR_SAMP", "VERBOSE",
        "VIEW", "VOLATILE", "WAITFOR", "WHEN", "WHENEVER", "WHERE", "WHILE", "WIDTH_BUCKET",
        "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRITE", "WRITETEXT", "X509", "XOR", "YEAR",
        "YEAR_MONTH", "ZEROFILL", "ZONE",
    ]


def quote(name, force=False):
    """
    Returns name in quotes and proper-escaped for SQL queries.

    @param   force  quote even if name does not need quoting (starts with a letter,
                    contains only alphanumerics, and is not a reserved keyword)
    """
    result = name
    if force or result.upper() in SqlMixin.KEYWORDS \
    or re.search(r"(^[\W\d])|(?=\W)", result, re.U):
        result = '"%s"' % result.replace('"', '""')
    return result


__all__ = ["SqlMixin", "quote"]

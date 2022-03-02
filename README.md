grepros
=======

grep for ROS bag files and live topics.

Searches through ROS messages and matches any message field value by regular
expression patterns or plain text, regardless of field type.
Can also look for specific values in specific message fields only.

By default, matches are printed to console. Additionally, matches can be written
to a bagfile or HTML/CSV/Parquet/Postgres/SQL/SQLite, or published to live topics.

Supports both ROS1 and ROS2. ROS environment variables need to be set, at least `ROS_VERSION`.

In ROS1, messages can be grepped even if Python packages for message types are not installed.
Using ROS1 live topics requires ROS master to be running.

Using ROS2 requires Python packages for message types to be available in path.

Supports loading custom plugins, mainly for additional output formats.

[![Screenshot](https://raw.githubusercontent.com/suurjaak/grepros/media/th_screen.png)](https://raw.githubusercontent.com/suurjaak/grepros/media/screen.png)


- [Example usage](#example-usage)
- [Installation](#installation)
  - [Using pip](#using-pip)
  - [Using apt](#using-apt)
  - [Using catkin](#using-catkin)
  - [Using colcon](#using-colcon)
- [Inputs](#inputs)
  - [bag](#bag)
  - [live](#live)
- [Outputs](#outputs)
  - [console](#console)
  - [bag](#bag)
  - [csv](#csv)
  - [html](#html)
  - [postgres](#postgres)
  - [sqlite](#sqlite)
  - [live](#live)
  - [console / html message formatting](#console--html-message-formatting)
- [Matching and filtering](#matching-and-filtering)
  - [Limits](#limits)
  - [Filtering](#filtering)
  - [Conditions](#conditions)
- [Plugins](#plugins)
  - [embag](#embag)
  - [parquet](#parquet)
  - [sql](#sql)
- [SQL dialects](#sql-dialects)
- [All command-line arguments](#all-command-line-arguments)
- [Dependencies](#dependencies)
- [Attribution](#attribution)
- [License](#license)


Example usage
-------------

Search for "my text" in all bags under current directory and subdirectories:

    grepros -r "my text"

Print 30 lines of the first message from each live ROS topic:

    grepros --max-per-topic 1 --lines-per-message 30 --live

Find first message containing "future" (case-insensitive) in my.bag:

    grepros future -I --max-count 1 --name my.bag

Find 10 messages, from geometry_msgs package, in "map" frame,
from bags in current directory, reindexing any unindexed bags:

    grepros frame_id=map --type geometry_msgs/* --max-count 10 --reindex-if-unindexed

Pipe all diagnostics messages with "CPU usage" from live ROS topics to my.bag:

    grepros "CPU usage" --type *DiagnosticArray --no-console-output --write my.bag

Find messages with field "key" containing "0xA002",
in topics ending with "diagnostics", in bags under "/tmp":

    grepros key=0xA002 --topic *diagnostics --path /tmp

Find diagnostics_msgs messages in bags in current directory,
containing "navigation" in fields "name" or "message",
print only header stamp and values:

    grepros --type diagnostic_msgs/* --select-field name message \
            --print-field header.stamp status.values -- navigation

Print first message from each lidar topic on host 1.2.3.4:

    ROS_MASTER_URI=http://1.2.3.4::11311 \
    grepros --live --topic *lidar* --max-per-topic 1

Export all bag messages to SQLite and Postgres, print only export progress:

    grepros -n my.bag --write my.bag.sqlite --no-console-output --no-verbose --progress

    grepros -n my.bag --write postgresql://user@host/dbname \
            --no-console-output --no-verbose --progress


Patterns use Python regular expression syntax, message matches if all match.
'*' wildcards use simple globbing as zero or more characters,
target matches if any value matches.

Note that some expressions may need to be quoted to avoid shell auto-unescaping
or auto-expanding them, e.g. `linear.x=2.?5` should be given as `"linear.x=2\.?5"`.

Care must also be taken with unquoted wildcards, as they will auto-expanded by shell
if they happen to match paths on disk.


Installation
------------

### Using pip

    pip install grepros

This will add the `grepros` command to path.

Requires ROS Python packages
(ROS1: rospy, roslib, rosbag, genpy;
 ROS2: rclpy, rosidl_parser, rosidl_runtime_py).

If you don't want to install the ROS1 stack, and are only interested
in using bag files, not grepping from or publishing to live topics,
minimal ROS1 Python packages can also be installed separately with:

    pip install rospy rosbag roslib roslz4 \
    --extra-index-url https://rospypi.github.io/simple/


### Using apt

If ROS apt repository has been added to system:

  sudo apt install ros-noetic-grepros  # ROS1

  sudo apt install ros-foxy-grepros    # ROS2

This will add the `grepros` command to the global ROS1 / ROS2 environment.


### Using catkin

In a ROS1 workspace, under the source directory:

    git clone https://github.com/suurjaak/grepros.git
    cd grepros
    catkin build --this

This will add the `grepros` command to the local ROS1 workspace path.


### Using colcon

In a ROS2 workspace, at the workspace root:

    git clone https://github.com/suurjaak/grepros.git src/grepros
    colcon build --packages-select grepros

This will add the `grepros` command to the local ROS2 workspace path.


Inputs
------

Input is either from one or more ROS bag files (default), or from live ROS topics.

### bag

Read messages from ROS bag files, by default all in current directory.

Recurse into subdirectories when looking for bagfiles:

    -r
    --recursive

Read specific filenames (supports * wildcards):

    --n        /tmp/*.bag
    --filename my.bag 2021-11-*.bag

Scan specific paths instead of current directory (supports * wildcards):

    -p     /home/bags/2021-11-*
    --path my/dir

Reindex unindexed ROS1 bags before processing
(warning: creates backup copies of files, into same directory as file):

    --reindex-if-unindexed
    --reindex-if-unindexed --progress

Decompress archived ROS bags before processing
(`.zst` `.zstd` extensions, requires `zstandard` Python package)
(warning: unpacks archived file to disk, into same directory as file):

    --decompress
    --decompress --progress

Order bag messages first by topic or type, and only then by time:

    --order-bag-by topic
    --order-bag-by type


### live

    --live

Read messages from live ROS topics instead of bagfiles.

Requires `ROS_MASTER_URI` and `ROS_ROOT` to be set in environment if ROS1.

Set custom queue size for subscribing (default 10):

    --queue-size-in 100

Use ROS time instead of system time for incoming message timestamps:

    --ros-time-in


Outputs
-------

There can be any number of outputs: printing to console (default),
publishing to live ROS topics, or writing to file or database.

### console

Default output is to console, in ANSI colors, mimicking `grep` output.

Disable printing messages to console:

    --no-console-output

Manage color output:

    --color always  (default)
    --color auto    (auto-detect terminal support)
    --color never   (disable colors)

Note that when paging color output with `more` or `less`, the pager needs to
accept raw control characters (`more -f` or `less -R`).


### bag

    --write path/to/my.bag [format=bag] [overwrite=true|false]

Write messages to a ROS bag file, the custom `.bag` format in ROS1
or the `.db3` SQLite database format in ROS2. If the bagfile already exists, 
it is appended to, unless specified to overwrite.

Specifying `format=bag` is not required
if the filename ends with `.bag` in ROS1 or `.db3` in ROS2.


### csv

    --write path/to/my.csv [format=csv] [overwrite=true|false]

Write messages to CSV files, each topic to a separate file, named
`path/to/my.full__topic__name.csv` for `/full/topic/name`.

Output mimicks CSVs compatible with PlotJuggler, all messages values flattened
to a single list, with header fields like `/topic/field.subfield.listsubfield.0.data.1`.

If a file already exists, a unique counter is appended to the name of the new file,
e.g. `my.full__topic__name.2.csv`, unless specified to overwrite.

Specifying `format=csv` is not required if the filename ends with `.csv`.


### html

    --write path/to/my.html [format=html] [overwrite=true|false]

Write messages to an HTML file, with a linked table of contents,
message timeline, message type definitions, and a topically traversable message list.

[![Screenshot](https://raw.githubusercontent.com/suurjaak/grepros/media/th_screen_html.png)](https://raw.githubusercontent.com/suurjaak/grepros/media/screen_html.png)

Note: resulting file may be large, and take a long time to open in browser. 

If the file already exists, a unique counter is appended to the name of the new file,
e.g. `my.2.html`, unless specified to overwrite.

Specifying `format=html` is not required if the filename ends with `.htm` or `.html`.

A custom template file can be specified, in [step](https://github.com/dotpy/step) syntax:

    --write path/to/my.html template=/my/html.template


### postgres

    --write postgresql://username@host/dbname [format=postgres]

Write messages to a Postgres database, with tables `pkg/MsgType` for each ROS message type,
and views `/full/topic/name` for each topic. 
Plus table `topics` with a list of topics, `types` with message types and definitions,
and `meta` with table/view/column name changes from shortenings and conflicts, if any
(Postgres name length is limited to 63 characters).

ROS primitive types are inserted as Postgres data types (time/duration types as NUMERIC),
uint8[] arrays as BYTEA, other primitive arrays as ARRAY, and arrays of subtypes as JSONB.

If the database already exists, it is appended to. If there are conflicting names
(same package and name but different message type definition),
table/view name becomes "name (MD5 hash of type definition)".

Specifying `format=postgres` is not required if the parameter uses the
Postgres URI scheme `postgresql://`.

Requires [psycopg2](https://pypi.org/project/psycopg2).

Parameter `--write` can also use the Postgres keyword=value format,
e.g. `"host=localhost port=5432 dbname=mydb username=postgres connect_timeout=10"`.

Standard Postgres environment variables are also supported (PGPASSWORD et al).

[![Screenshot](https://raw.githubusercontent.com/suurjaak/grepros/media/th_screen_postgres.png)](https://raw.githubusercontent.com/suurjaak/grepros/media/screen_postgres.png)

A custom transaction size can be specified (default is 1000; 0 is autocommit):

    --write postgresql://username@host/dbname commit-interval=NUM

Updates to Postgres SQL dialect can be loaded from a YAML or JSON file:

    --write postgresql://username@host/dbname dialect-file=path/to/dialects.yaml

More on [SQL dialects](#sql-dialects).


#### Nested messages

Nested message types can be recursively populated to separate tables, linked
to parent messages via foreign keys.

To recursively populate nested array fields:

    --write postgresql://username@host/dbname nesting=array

E.g. for `diagnostic_msgs/DiagnosticArray`, this would populate the following tables:

```sql
CREATE TABLE "diagnostic_msgs/DiagnosticArray" (
  "header.seq"          BIGINT,
  "header.stamp.secs"   INTEGER,
  "header.stamp.nsecs"  INTEGER,
  "header.frame_id"     TEXT,
  status                JSONB,       -- [_id from "diagnostic_msgs/DiagnosticStatus", ]
  _topic                TEXT,
  _timestamp            NUMERIC,
  _id                   BIGSERIAL,
  _parent_type          TEXT,
  _parent_id            BIGINT
);

CREATE TABLE "diagnostic_msgs/DiagnosticStatus" (
  level                 SMALLINT,
  name                  TEXT,
  message               TEXT,
  hardware_id           TEXT,
  "values"              JSONB,       -- [_id from "diagnostic_msgs/KeyValue", ]
  _topic                TEXT,        -- _topic from "diagnostic_msgs/DiagnosticArray"
  _timestamp            NUMERIC,     -- _timestamp from "diagnostic_msgs/DiagnosticArray"
  _id                   BIGSERIAL,
  _parent_type          TEXT,        -- "diagnostic_msgs/DiagnosticArray"
  _parent_id            BIGINT       -- _id from "diagnostic_msgs/DiagnosticArray"
);

CREATE TABLE "diagnostic_msgs/KeyValue" (
  "key"                 TEXT,
  value                 TEXT,
  _topic                TEXT,        -- _topic from "diagnostic_msgs/DiagnosticStatus"
  _timestamp            NUMERIC,     -- _timestamp from "diagnostic_msgs/DiagnosticStatus"
  _id                   BIGSERIAL,
  _parent_type          TEXT,        -- "diagnostic_msgs/DiagnosticStatus"
  _parent_id            BIGINT       -- _id from "diagnostic_msgs/DiagnosticStatus"
);
```

Without nesting, array field values are inserted as JSON with full subtype content.

To recursively populate all nested message types:

    --write postgresql://username@host/dbname nesting=all

E.g. for `diagnostic_msgs/DiagnosticArray`, this would, in addition to the above, populate:

```sql
CREATE TABLE "std_msgs/Header" (
  seq                   BIGINT,
  "stamp.secs"          INTEGER,
  "stamp.nsecs"         INTEGER,
  frame_id              TEXT,
  _topic                TEXT,        -- _topic from "diagnostic_msgs/DiagnosticArray"
  _timestamp            NUMERIC,     -- _timestamp from "diagnostic_msgs/DiagnosticArray"
  _id                   BIGSERIAL,
  _parent_type          TEXT,       -- "diagnostic_msgs/DiagnosticArray"
  _parent_id            BIGINT      -- _id from "diagnostic_msgs/DiagnosticArray"
);
```

### sqlite

    --write path/to/my.sqlite [format=sqlite] [overwrite=true|false]

Write an SQLite database with tables `pkg/MsgType` for each ROS message type
and nested type, and views `/full/topic/name` for each topic. 
If the database already exists, it is appended to, unless specified to overwrite.

Output is compatible with ROS2 `.db3` bagfiles, supplemented with
full message YAMLs, and message type definition texts. Note that a database
dumped from a ROS1 source will most probably not be usable as a ROS2 bag,
due to breaking changes in ROS2 standard built-in types and message types.

Specifying `format=sqlite` is not required
if the filename ends with `.sqlite` or `.sqlite3`.

[![Screenshot](https://raw.githubusercontent.com/suurjaak/grepros/media/th_screen_sqlite.png)](https://raw.githubusercontent.com/suurjaak/grepros/media/screen_sqlite.png)

A custom transaction size can be specified (default is 1000; 0 is autocommit):

    --write path/to/my.sqlite commit-interval=NUM

By default, table `messages` is populated with full message YAMLs, unless:

    --write path/to/my.sqlite message-yaml=false

Updates to SQLite SQL dialect can be loaded from a YAML or JSON file:

    --write path/to/my.sqlite dialect-file=path/to/dialects.yaml

More on [SQL dialects](#sql-dialects).


#### Nested messages

Nested message types can be recursively populated to separate tables, linked
to parent messages via foreign keys.

To recursively populate nested array fields:

    --write path/to/my.sqlite nesting=array

E.g. for `diagnostic_msgs/DiagnosticArray`, this would populate the following tables:

```sql
CREATE TABLE "diagnostic_msgs/DiagnosticArray" (
  "header.seq"          INTEGER,
  "header.stamp.secs"   INTEGER,
  "header.stamp.nsecs"  INTEGER,
  "header.frame_id"     TEXT,
  -- [_id from "diagnostic_msgs/DiagnosticStatus", ]
  status                "DIAGNOSTIC_MSGS/DIAGNOSTICSTATUS[]",
  _topic                TEXT,
  _timestamp            INTEGER,
  _id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  _parent_type          TEXT,
  _parent_id            INTEGER
);

CREATE TABLE "diagnostic_msgs/DiagnosticStatus" (
  level                 SMALLINT,
  name                  TEXT,
  message               TEXT,
  hardware_id           TEXT,
  -- [_id from "diagnostic_msgs/KeyValue", ]
  "values"              "DIAGNOSTIC_MSGS/KEYVALUE[]",
  _topic                TEXT,        -- _topic from "diagnostic_msgs/DiagnosticArray"
  _timestamp            INTEGER,     -- _timestamp from "diagnostic_msgs/DiagnosticArray"
  _id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  _parent_type          TEXT,        -- "diagnostic_msgs/DiagnosticArray"
  _parent_id            INTEGER      -- _id from "diagnostic_msgs/DiagnosticArray"
);

CREATE TABLE "diagnostic_msgs/KeyValue" (
  "key"                 TEXT,
  value                 TEXT,
  _topic                TEXT,        -- _topic from "diagnostic_msgs/DiagnosticStatus"
  _timestamp            INTEGER,     -- _timestamp from "diagnostic_msgs/DiagnosticStatus"
  _id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  _parent_type          TEXT,        -- "diagnostic_msgs/DiagnosticStatus"
  _parent_id            INTEGER      -- _id from "diagnostic_msgs/DiagnosticStatus"
);
```

Without nesting, array field values are inserted as JSON with full subtype content.

To recursively populate all nested message types:

    --write path/to/my.sqlite nesting=all

E.g. for `diagnostic_msgs/DiagnosticArray`, this would, in addition to the above, populate:

```sql
CREATE TABLE "std_msgs/Header" (
  seq                   UINT32,
  "stamp.secs"          INT32,
  "stamp.nsecs"         INT32,
  frame_id              TEXT,
  _topic                STRING,      -- _topic from "diagnostic_msgs/DiagnosticArray"
  _timestamp            INTEGER,     -- _timestamp from "diagnostic_msgs/DiagnosticArray"
  _id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  _parent_type          TEXT,       -- "diagnostic_msgs/DiagnosticArray"
  _parent_id            INTEGER     -- _id from "diagnostic_msgs/DiagnosticArray"
);
```


### live

    --publish

Publish messages to live ROS topics. Topic prefix and suffix can be changed, 
or topic name set to one specific name:

    --publish-prefix  /myroot
    --publish-suffix  /myend
    --publish-fixname /my/singular/name

One of the above arguments needs to be specified if publishing to live ROS topics
while grepping from live ROS topics, to avoid endless loops.

Set custom queue size for publishers (default 10):

    --queue-size-out 100


### console / html message formatting

Set maximum number of lines to output per message:

    --lines-per-message 5

Set maximum number of lines to output per message field:

    --lines-per-field 2

Start message output from, or stop output at, message line number:

    --start-line  2   # (1-based if positive
    --end-line   -2   # (count back from total if negative)

Output only the fields where patterns find a match:

    --matched-fields-only

Output only matched fields and specified number of lines around match:

    --lines-around-match 5

Output only specific message fields (supports nested.paths and * wildcards):

    --print-field *data

Skip outputting specific message fields (supports nested.paths and * wildcards):

    --no-print-field header.stamp

Wrap matches in custom texts:

    --match-wrapper @@@
    --match-wrapper "<<<<" ">>>>"

Set custom width for wrapping message YAML printed to console (auto-detected from terminal by default):

    --wrap-width 120


Matching and filtering
----------------------

Any number of patterns can be specified, message matches if all patterns find a match.
If no patterns are given, any message matches.

Match messages containing any of the words:

    cpu memory speed

Match messages where `frame_id` contains "world":

    frame_id=world

Match messages where `header.frame_id` is present:

    header.frame_id=.*

Match as plaintext, not Python regular expression patterns:

    -F
    --fixed-strings

Select non-matching messages instead:

    -v
    --invert-match

Use case-sensitive matching in patterns (default is insensitive):

    -I
    --no-ignore-case


### Limits

Stop after matching a specified number of messages (per each file if bag input):

    -m          100
    --max-count 100

Scan only a specified number of topics (per each file if bag input):

    --max-topics 10

Emit a specified number of matches per topic (per each file if bag input):

    --max-per-topic 20

Emit every Nth match in topic:

    --every-nth-match 10  # (skips 10 matches in topic after each match emitted)


### Filtering

Scan specific topics only (supports * wildcards):

    -t      *lidar* *ins*
    --topic /robot/sensors/*

Skip specific topics (supports * wildcards):

    -nt        *lidar* *ins*
    --no-topic /robot/sensors/*

Scan specific message types only (supports * wildcards):

    -d     *Twist*
    --type sensor_msgs/*

Skip specific message types from scanning (supports * wildcards):

    -nd       *Twist*
    --no-type sensor_msgs/*

Set specific message fields to scan (supports nested.paths and * wildcards):

    -sf            twist.linear
    --select-field *data

Skip specific message fields in scan (supports nested.paths and * wildcards):

    -ns               twist.linear
    --no-select-field *data

Only emit matches that are unique in topic,
taking `--select-field` and `--no-select-field` into account (per each file if bag input):

    --unique-only

Start scanning from a specific timestamp:

    -t0          2021-11     # (using partial ISO datetime)
    --start-time 1636900000  # (using UNIX timestamp)
    --start-time +100        # (seconds from bag start time, or from script startup time if live input)
    --start-time -100        # (seconds from bag end time, or script startup time if live input)

Stop scanning at a specific timestamp:

    -t1        2021-11     # (using partial ISO datetime)
    --end-time 1636900000  # (using UNIX timestamp)
    --end-time +100        # (seconds from bag start time, or from script startup time if live input)
    --end-time -100        # (seconds from bag end time, or from script startup time if live input)

Start scanning from a specific message index in topic:

    -n0           -100  # (counts back from topic total message count in bag)
    --start-index   10  # (1-based index)

Stop scanning at a specific message index in topic:

    -n1         -100  # (counts back from topic total message count in bag)
    --end-index   10  # (1-based index)

Scan every Nth message in topic:

    --every-nth-message 10  # (skips 10 messages in topic with each step)

Scan messages in topic with timestamps at least N seconds apart:

    --every-nth-interval 5  # (samples topic messages no more often than every 5 seconds)


## Conditions

    --condition "PYTHON EXPRESSION"

Specify one or more Python expressions that must evaluate as true to search
encountered messages. Expressions can access topics, by name or * wildcard,
and refer to message fields directly.

    # (Match while last message in '/robot/enabled' has data=true)
    --condition "<topic /robot/enabled>.data"

    # (Match if at least 10 messages have been encountered in /robot/alerts)
    --condition "len(<topic /robot/alerts>) > 10"

    # (Match if last two messages in /robot/mode have equal .value)
    --condition "<topic /robot/mode>[-2].value == <topic /robot/mode>[-1].value"

    # (Match while control is enabled and robot is moving straight and level)
    --condition "<topic */control_enable>.data and <topic */cmd_vel>.linear.x > 0" \
                "and <topic */cmd_vel>.angular.z < 0.02"

Condition namespace:

- `msg`:                    current message from data source
- `topic`:                  full name of current message topic
- `<topic /my/topic>`:      topic by full name or * wildcard
- `len(<topic ..>)`:        number of messages encountered in topic
- `bool(<topic ..>)`:       has any message been encountered in topic
- `<topic ..>.xyz`:         attribute `xyz` of last message in topic
- `<topic ..>[index]`:      topic message at position
                            (from first encountered if index >= 0, last encountered if < 0)
- `<topic ..>[index].xyz`:  attribute `xyz` of topic message at position

Condition is automatically false if trying to access attributes of a message not yet received.


Plugins
-------

    --plugin some.python.module some.other.module.Class

Load one or more Python modules or classes as plugins.
Supported (but not required) plugin interface methods:

- `init(args)`: invoked at startup with command-line arguments
- `load(category, args)`: invoked with category "search" or "source" or "sink",
                          using returned value if not None

Plugins are free to modify `grepros` internals, like adding command-line arguments
to `grepros.main.ARGUMENTS` or adding sink types to `grepros.outputs.MultiSink`.

Convenience methods:

- `plugins.add_write_format(name, cls, label=None, options=())`:
   adds an output plugin to defaults
- `plugins.get_argument(name)`: returns a command-line argument dictionary, or None

Specifying `--plugin someplugin` and `--help` will include plugin options in printed help.

Built-in plugins:

### embag

    --plugin grepros.plugins.embag

Use the [embag](https://github.com/embarktrucks/embag) library for reading ROS1 bags.

Significantly faster, but library tends to be unstable.


### parquet

    --plugin grepros.plugins.parquet --write path/to/my.parquet[format=parquet] \
             [column-name=rostype:value] [overwrite=true|false] [type-rostype=arrowtype] \
             [writer-argname=argvalue]

Write messages to Apache Parquet files (columnar storage format, version 2.6),
each message type to a separate file, named `path/to/package__MessageType__typehash/my.parquet`
for `package/MessageType` (typehash is message type definition MD5 hashsum).
Adds fields `_topic string()` and `_timestamp timestamp("ns")` to each type.

If a file already exists, a unique counter is appended to the name of the new file,
e.g. `package__MessageType__typehash/my.2.parquet`, unless specified to overwrite.

Specifying `format=parquet` is not required if the filename ends with `.parquet`.

Requires [pandas](https://pypi.org/project/pandas) and [pyarrow](https://pypi.org/project/pyarrow).

Supports adding supplementary columns with fixed values to Parquet files:

    --write path/to/my.parquet column-bag_hash=string:26dfba2c

Supports custom mapping between ROS and pyarrow types with `type-rostype=arrowtype`:

    --write path/to/my.parquet type-time="timestamp('ns')"
    --write path/to/my.parquet type-uint8[]="list(uint8())"

Time/duration types are flattened into separate integer columns `secs` and `nsecs`,
unless they are mapped to pyarrow types explicitly, like:

    --write path/to/my.parquet type-time="timestamp('ns')" type-duration="duration('ns')"

Supports additional arguments given to [pyarrow.parquet.ParquetWriter](
https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html), as:

    --write path/to/my.parquet writer-argname=argvalue

For example, specifying no compression:

    --write path/to/my.parquet writer-compression=null

The value is interpreted as JSON if possible, e.g. `writer-use_dictionary=false`.


### sql

    --plugin grepros.plugins.sql --write path/to/my.sql [format=sql] [overwrite=true|false]

Write SQL schema to output file, CREATE TABLE for each message type
and CREATE VIEW for each topic.

If the file already exists, a unique counter is appended to the name of the new file,
e.g. `my.2.sql`, unless specified to overwrite.

Specifying `format=sql` is not required if the filename ends with `.sql`.

To create tables for nested array message type fields:

    --write path/to/my.sql nesting=array

To create tables for all nested message types:

    --write path/to/my.sql nesting=all

A specific SQL dialect can be specified:

    --write path/to/my.sql dialect=clickhouse|postgres|sqlite

Additional dialects, or updates for existing dialects, can be loaded from a YAML or JSON file:

    --write path/to/my.sql dialect=mydialect dialect-file=path/to/dialects.yaml


SQL dialects
------------

Postgres, SQLite and SQL outputs support loading additional options for SQL dialect.

Dialect file format:

```yaml
dialectname:
  table_template:       CREATE TABLE template; args: table, cols, type, hash, package, class
  view_template:        CREATE VIEW template; args: view, cols, table, topic, type, hash, package, class
  table_name_template:  message type table name template; args: type, hash, package, class
  view_name_template:   topic view name template; args: topic, type, hash, package, class
  types:                Mapping between ROS and SQL common types for table columns,
                        e.g. {"uint8": "SMALLINT", "uint8[]": "BYTEA", ..}
  adapters:             Mapping between ROS types and callable converters for table columns,
                        e.g. {"time": "decimal.Decimal"}
  defaulttype:          Fallback SQL type if no mapped type for ROS type;
                        if no mapped and no default type, column type will be ROS type as-is
  arraytype_template:   Array type template; args: type
  maxlen_entity:        Maximum table/view name length, 0 disables
  maxlen_column:        Maximum column name length, 0 disables
  invalid_char_regex:   Regex for matching invalid characters in name, if any
  invalid_char_repl:    Replacement for invalid characters in name
```

Template parameters like `table_name_template` use Python `str.format()` keyword syntax,
e.g. `{"table_name_template": "{type}", "view_name_template": "{topic}"}`.

Time/duration types are flattened into separate integer columns `secs` and `nsecs`,
unless the dialect maps them to SQL types explicitly, e.g. `{"time": "BIGINT"}`.

Any dialect options not specified in the given dialect or built-in dialects,
will be taken from the default dialect configuration:

```yaml
  table_template:       'CREATE TABLE IF NOT EXISTS {table} ({cols});'
  view_template:        'DROP VIEW IF EXISTS {view};
                         CREATE VIEW {view} AS
                         SELECT {cols}
                         FROM {table}
                         WHERE _topic = {topic};'
  table_name_template:  '{type}',
  view_name_template:   '{topic}',
  types:                {}
  defaulttype:          null
  arraytype_template:   '{type}[]'
  maxlen_entity:        0
  maxlen_column:        0
  invalid_char_regex:   null
  invalid_char_repl:    '__'
```



All command-line arguments
--------------------------

```
positional arguments:
  PATTERN               pattern(s) to find in message field values,
                        all messages match if not given,
                        can specify message field as NAME=PATTERN
                        (supports nested.paths and * wildcards)

optional arguments:
  -h, --help            show this help message and exit
  -F, --fixed-strings   PATTERNs are ordinary strings, not regular expressions
  -I, --no-ignore-case  use case-sensitive matching in PATTERNs
  -v, --invert-match    select non-matching messages
  --version             display version information and exit
  --live                read messages from live ROS topics instead of bagfiles
  --publish             publish matched messages to live ROS topics
  --write TARGET [format=bag|csv|html|postgres|sqlite] [KEY=VALUE ...]
                        write matched messages to specified output,
                        format is autodetected from TARGET if not specified.
                        Bag or database will be appended to if it already exists.
                        Keyword arguments are given to output writer:
                          commit-interval=NUM      transaction size for Postgres/SQLite output
                                                   (default 1000, 0 is autocommit)
                          message-yaml=true|false  whether to populate table field messages.yaml
                                                   in SQLite output (default true)
                          nesting=array|all        create tables for nested message types
                                                   in Postgres/SQL/SQLite output,
                                                   only for arrays if "array" 
                                                   else for any nested types
                                                   (array fields in parent will be populated 
                                                    with foreign keys instead of messages as JSON)
                          overwrite=true|false     overwrite existing file in bag/CSV/HTML/SQLite output
                                                   instead of appending to if bag or database
                                                   or appending unique counter to file name
                                                   (default false)
                          template=/my/path.tpl    custom template to use for HTML output
  --plugin PLUGIN [PLUGIN ...]
                        load a Python module or class as plugin
                        (built-in plugins: grepros.plugins.embag, 
                         grepros.plugins.parquet, grepros.plugins.sql)

Filtering:
  -t TOPIC [TOPIC ...], --topic TOPIC [TOPIC ...]
                        ROS topics to scan if not all (supports * wildcards)
  -nt TOPIC [TOPIC ...], --no-topic TOPIC [TOPIC ...]
                        ROS topics to skip (supports * wildcards)
  -d TYPE [TYPE ...], --type TYPE [TYPE ...]
                        ROS message types to scan if not all (supports * wildcards)
  -nd TYPE [TYPE ...], --no-type TYPE [TYPE ...]
                        ROS message types to skip (supports * wildcards)
  --condition CONDITION [CONDITION ...]
                        extra conditions to require for matching messages,
                        as ordinary Python expressions, can refer to last messages
                        in topics as <topic /my/topic>; topic name can contain wildcards.
                        E.g. --condition "<topic /robot/enabled>.data" matches
                        messages only while last message in '/robot/enabled' has data=true.
  -t0 TIME, --start-time TIME
                        earliest timestamp of messages to scan
                        as relative seconds if signed,
                        or epoch timestamp or ISO datetime
                        (for bag input, relative to bag start time
                        if positive or end time if negative,
                        for live input relative to system time,
                        datetime may be partial like 2021-10-14T12)
  -t1 TIME, --end-time TIME
                        latest timestamp of messages to scan
                        as relative seconds if signed,
                        or epoch timestamp or ISO datetime
                        (for bag input, relative to bag start time
                        if positive or end time if negative,
                        for live input relative to system time,
                        datetime may be partial like 2021-10-14T12)
  -n0 INDEX, --start-index INDEX
                        message index within topic to start from
                        (1-based if positive, counts back from bag total if negative)
  -n1 INDEX, --end-index INDEX
                        message index within topic to stop at
                        (1-based if positive, counts back from bag total if negative)
  --every-nth-message NUM
                        scan every Nth message within topic
  --every-nth-interval SECONDS
                        scan messages within topic at least N seconds apart
  --every-nth-match NUM
                        emit every Nth match in topic
  -sf [FIELD [FIELD ...]], --select-field [FIELD [FIELD ...]]
                        message fields to use in matching if not all
                        (supports nested.paths and * wildcards)
  -ns [FIELD [FIELD ...]], --no-select-field [FIELD [FIELD ...]]
                        message fields to skip in matching
                        (supports nested.paths and * wildcards)
  -m NUM, --max-count NUM
                        number of matched messages to emit (per each file if bag input)
  --max-per-topic NUM   number of matched messages to emit from each topic
                        (per each file if bag input)
  --max-topics NUM      number of topics to emit matches from (per each file if bag input)
  --unique-only         only emit matches that are unique in topic,
                        taking --select-field and --no-select-field into account
                        (per each file if bag input)

Output control:
  -B NUM, --before-context NUM
                        emit NUM messages of leading context before match
  -A NUM, --after-context NUM
                        emit NUM messages of trailing context after match
  -C NUM, --context NUM
                        emit NUM messages of leading and trailing context
                        around match
  -pf [FIELD [FIELD ...]], --print-field [FIELD [FIELD ...]]
                        message fields to print in console output if not all
                        (supports nested.paths and * wildcards)
  -np [FIELD [FIELD ...]], --no-print-field [FIELD [FIELD ...]]
                        message fields to skip in console output
                        (supports nested.paths and * wildcards)
  -mo, --matched-fields-only
                        print only the fields where PATTERNs find a match
  -la NUM, --lines-around-match NUM
                        print only matched fields and NUM message lines
                        around match
  -lf NUM, --lines-per-field NUM
                        maximum number of lines to print per field
  -l0 NUM, --start-line NUM
                        message line number to start printing from
                        (1-based if positive, counts back from total if negative)
  -l1 NUM, --end-line NUM
                        message line number to stop printing at
                        (1-based if positive, counts back from total if negative)
  -lm NUM, --lines-per-message NUM
                        maximum number of lines to print per message
  --match-wrapper [STR [STR ...]]
                        string to wrap around matched values,
                        both sides if one value, start and end if more than one,
                        or no wrapping if zero values
                        (default "**" in colorless output)
  --wrap-width NUM      character width to wrap message YAML output at,
                        0 disables (defaults to detected terminal width)
  --color {auto,always,never}
                        use color output in console (default "always")
  --no-meta             do not print source and message metainfo to console
  --no-filename         do not print bag filename prefix on each console message line
  --no-console-output   do not print matches to console
  --progress            show progress bar when not printing matches to console
  --verbose             print status messages during console output
                        for publishing and writing
  --no-verbose          do not print status messages during console output
                        for publishing and writing

Bag input control:
  -n [FILE [FILE ...]], --filename [FILE [FILE ...]]
                        names of ROS bagfiles to scan if not all in directory
                        (supports * wildcards)
  -p [PATH [PATH ...]], --path [PATH [PATH ...]]
                        paths to scan if not current directory
                        (supports * wildcards)
  -r, --recursive       recurse into subdirectories when looking for bagfiles
  --order-bag-by {topic,type}
                        order bag messages by topic or type first and then by time
  --reindex-if-unindexed
                        reindex unindexed bags (ROS1 only; makes backup copies)
  --decompress          decompress archived bags with recognized extensions (.zst .zstd)

Live topic control:
  --publish-prefix PREFIX
                        prefix to prepend to input topic name on publishing match
  --publish-suffix SUFFIX
                        suffix to append to input topic name on publishing match
  --publish-fixname TOPIC
                        single output topic name to publish all matches to,
                        overrides prefix and suffix
  --queue-size-in SIZE  live ROS topic subscriber queue size (default 10)
  --queue-size-out SIZE
                        output publisher queue size (default 10)
  --ros-time-in         use ROS time instead of system time for incoming message
                        timestamps from subsribed live ROS topics
```


Dependencies
------------

Requires the following 3rd-party Python packages:

- ROS1: rospy, roslib, rosbag, genpy
- ROS2: rclpy, rosidl_parser, rosidl_runtime_py

Optional, for decompressing archived bags:

- zstandard (https://pypi.org/project/zstandard/)


Attribution
-----------

Includes a modified version of step, Simple Template Engine for Python,
(c) 2012, Daniele Mazzocchio, https://github.com/dotpy/step,
released under the MIT license.


License
-------

Copyright (c) by Erki Suurjaak.
Released as free open source software under the BSD License,
see [LICENSE.md](LICENSE.md) for full details.

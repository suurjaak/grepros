grepros
=======

grep for ROS bag files and live topics.

Searches through ROS messages and matches any message field value by regular
expression patterns or plain text, regardless of field type.
Can also look for specific values in specific message fields only.

By default, matches are printed to console. Additionally, matches can be written
to a bagfile or as HTML/CSV/SQLite, or published to live topics.

Supports both ROS1 and ROS2. ROS environment variables need to be set, at least `ROS_VERSION`.

In ROS1, messages can be grepped even if Python packages for message types are not installed.
Using ROS1 live topics requires ROS master to be running.

Using ROS2 requires Python packages for message types to be available in path.


[![Screenshot](https://raw.githubusercontent.com/suurjaak/grepros/media/th_screen.png)](https://raw.githubusercontent.com/suurjaak/grepros/media/screen.png)


- [Example usage](#example-usage)
- [Installation](#installation)
  - [Using pip](#using-pip)
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
  - [sqlite](#sqlite)
  - [live](#live)
  - [console / html message formatting](#console--html-message-formatting)
- [Matching and filtering](#matching-and-filtering)
  - [Limits](#limits)
  - [Filtering](#filtering)
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
from bags in current directory:

    grepros frame_id=map --type geometry_msgs/* --max-count 10

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

Export all bag messages to SQLite, print only export progress:

    grepros -n my.bag --write my.bag.sqlite --no-console-output --progress 2>/dev/null


Patterns use Python regular expression syntax, message matches if all match.
'*' wildcards in other arguments use simple globbing as zero or more characters,
target matches if any value matches.

Note that some expressions may need to be quoted to avoid shell auto-unescaping
or auto-expanding them, e.g. `linear.x=2.?5` should be given as `"linear.x=2\.?5"`.


Installation
------------

### Using pip

    pip install grepros

This will add the `grepros` command to path.

Requires ROS Python packages
(ROS1: rospy, roslib, rosbag, genpy; ROS2: rclpy, rosidl_runtime_py).

If you don't want to install the ROS1 stack, and are only interested
in using bag files, not grepping from or publishing to live topics,
minimal ROS1 Python packages can also be installed separately with:

    pip install rospy rosbag roslib roslz4 \
    --extra-index-url https://rospypi.github.io/simple/


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

    --write my.bag [--write-format bag]

Write messages to a ROS bag file, the custom `.bag` format in ROS1
or the `.db3` SQLite database format in ROS2. If the bagfile already exists, 
it is appended to. 

Specifying `--write-format bag` is not required
if the filename ends with `.bag` in ROS1 or `.db3` in ROS2.


### csv

    --write my.csv [--write-format csv]

Write messages to CSV files, each topic to a separate file, named
`my.full__topic__name.csv` for `/full/topic/name`.

Output mimicks CSVs compatible with PlotJuggler, all messages values flattened
to a single list, with header fields like `/topic/field.subfield.listsubfield.0.data.1`.

Specifying `--write-format csv` is not required if the filename ends with `.csv`.


### html

    --write my.html [--write-format html]

Write messages to an HTML file, with a linked table of contents,
message type definitions, and a topically traversable message list.

[![Screenshot](https://raw.githubusercontent.com/suurjaak/grepros/media/th_screen_html.png)](https://raw.githubusercontent.com/suurjaak/grepros/media/screen_html.png)

Note: resulting file may be large, and take a long time to open in browser. 

Specifying `--write-format html` is not required if the filename ends with `.htm` or `.html`.

A custom template file can be specified, in [step](https://github.com/dotpy/step) syntax:

    --write-format-template /my/html.template


### sqlite

    --write my.sqlite {--write-format sqlite]

Write an SQLite database with tables `pkg/MsgType` for each ROS message type
and nested type, and views `/full/topic/name` for each topic. 
If the database already exists, it is appended to.

Output is fully compatible with ROS2 `.db3` bagfiles, supplemented with
full message YAMLs, and message type definition texts.

Specifying `--write-format sqlite` is not required
if the filename ends with `.sqlite` or `.sqlite3`.

[![Screenshot](https://raw.githubusercontent.com/suurjaak/grepros/media/th_screen_sqlite.png)](https://raw.githubusercontent.com/suurjaak/grepros/media/screen_sqlite.png)


### live

    --publish

Publish messages to live ROS topics. The published topic name will default to
`/grepros/original/name`. Topic prefix and suffix can be changed, 
or topic name set to one specific name:

    --publish-prefix  /myroot
    --publish-suffix  /myend
    --publish-fixname /my/singular/name

Set custom queue size for publishers (default 10):

    --queue-size-out 100


### console / html message formatting

Set maximum number of lines to output per message:

    --lines-per-message 5

Set maximum number of lines to output per message field:

    --lines-per-field 2

Start outputting from, or stop outputting at, message line number:

    --start-line  2   (1-based if positive
    --end-line   -2   (count back from total if negative)

Output only the fields where patterns find a match:

    --matched-fields-only

Output only matched fields and specified number of lines around match:

    --lines-around-match 5

Output only specific message fields (supports nested.paths and * wildcards):

    --print-field *data

Skip outputting specific message fields (supports nested.paths and * wildcards):

    --no-print-field header.stamp

Wrap matches in custom texts:

    --match-wrapper $$$
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

    -t0          2021-11     (using partial ISO datetime)
    --start-time 1636900000  (using UNIX timestamp)
    --start-time +100        (seconds from bag start time, or from script startup time if live input)
    --start-time -100        (seconds from bag end time, or script startup time if live input)

Stop scanning at a specific timestamp:

    -t1        2021-11     (using partial ISO datetime)
    --end-time 1636900000  (using UNIX timestamp)
    --end-time +100        (seconds from bag start time, or from script startup time if live input)
    --end-time -100        (seconds from bag end time, or from script startup time if live input)

Start scanning from a specific message index in topic:

    -n0           -100  (counts back from topic total message count in bag)
    --start-index   10  (1-based index)

Stop scanning at a specific message index in topic:

    -n1         -100  (counts back from topic total message count in bag)
    --end-index   10  (1-based index)


Command-line arguments
----------------------

```
positional arguments:
  PATTERN               pattern(s) to find in message field values,
                        can specify message field as NAME=PATTERN
                        (name may be a nested.path)

optional arguments:
  -h, --help            show this help message and exit
  -F, --fixed-strings   PATTERNs are ordinary strings, not regular expressions
  -I, --no-ignore-case  use case-sensitive matching in PATTERNs
  -v, --invert-match    select non-matching messages
  --version             display version information and exit
  --live                read messages from live ROS topics instead of bagfiles
  --publish             publish matched messages to live ROS topics
  --write OUTFILE       write matched messages to specified output file
  --write-format {bag,csv,html,sqlite}
                        output format, auto-detected from OUTFILE extension if not given,
                        bag or database will be appended to if file already exists

Filtering:
  -t TOPIC [TOPIC ...], --topic TOPIC [TOPIC ...]
                        ROS topics to scan if not all (supports * wildcards)
  -nt TOPIC [TOPIC ...], --no-topic TOPIC [TOPIC ...]
                        ROS topics to skip (supports * wildcards)
  -d TYPE [TYPE ...], --type TYPE [TYPE ...]
                        ROS message types to scan if not all (supports * wildcards)
  -nd TYPE [TYPE ...], --no-type TYPE [TYPE ...]
                        ROS message types to skip (supports * wildcards)
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
  --write-format-template OUTFILE_TEMPLATE
                        path to custom template to use for HTML output
  --color {auto,always,never}
                        use color output in console (default "always")
  --no-meta             do not print source and message metainfo to console
  --no-filename         do not print bag filename prefix on each console message line
  --no-console-output   do not print matches to console
  --progress            show progress bar when not printing matches to console
  --verbose             print status messages during console output
                        for publishing and bag writing

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

Live topic control:
  --publish-prefix PREFIX
                        prefix to prepend to input topic name on publishing match
                        (default "/grepros")
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

grepros
=======

grep for ROS1 bag files and live topics.

Can search through ROS messages and match any message field value by regular
expression patterns or plain text, regardless of field type.
Can also look for specific values in specific message fields only.

By default, matches are printed to console. Additionally, matches can be written
to a bagfile, or published to live topics.

Grepping from or publishing to live topics requires ROS environment to be set
and ROS master to be running.


Example usage
-------------

Search for "my text" in all bags under current directory and subdirectories:

    grepros -r "my text"

Print 30 lines of the first message from each live ROS topic:

    grepros ".*" --max-per-topic 1 --lines-per-message 30 --live

Find first message containing "future" (case-insensitive) in my.bag:

    grepros future -I -m 1 -n my.bag

Find 10 messages, from geometry_msgs package, in "map" frame,
from bags in current directory:

    grepros frame_id=map -d geometry* -m 10

Pipe all diagnostics messages with "CPU usage" from live ROS topics to my.bag:
    
    grepros "CPU usage" -d *DiagnosticArray --no-console-output --write my.bag

Find messages with field "key" containing "0xA002",
in topics ending with "diagnostics", in bags under "/tmp":

    grepros key=0xA002 -t *diagnostics -p /tmp

Find diagnostics_msgs messages in bags in current directory,
containing "navigation" in fields "name" or "message",
print only header stamp and values:

    grepros -d diagnostic_msgs/* -sf name message \
            -pf header.stamp status.values -- navigation

Print first message from each lidar topic on host 1.2.3.4:

    ROS_MASTER_URI=http://1.2.3.4::11311 \
    grepros ".*" --live --topic *lidar* --max-per-topic 1


Patterns use Python regular expression syntax, message matches if all match.
'*' wildcards in other arguments use simple globbing as zero or more characters,
target matches if any value matches.


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
  --live                read messages from live ROS topics instead of bagfiles
  --publish             publish matched messages to live ROS topics
  --write OUTBAG        write matched messages to specified bagfile

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
                        as relative seconds or ISO datetime
                        (for bag input, relative to bag start time
                        if positive or end time if negative,
                        for live input relative to system time,
                        datetime may be partial like 2021-10-14T12)
  -t1 TIME, --end-time TIME
                        latest timestamp of messages to scan
                        as relative seconds or ISO datetime
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
  -ns [FIELD [FIELD ...]], --noselect-field [FIELD [FIELD ...]]
                        message fields to skip in matching
                        (supports nested.paths and * wildcards)
  -m NUM, --max-count NUM
                        number of matched messages to emit (per file if bag input)
  --max-per-topic NUM   number of matched messages to emit from each topic
                        (per file if bag input)
  --max-topics NUM      number of topics to print matches from

Output control:
  -pf [FIELD [FIELD ...]], --print-field [FIELD [FIELD ...]]
                        message fields to print in console output if not all
                        (supports nested.paths and * wildcards)
  -np [FIELD [FIELD ...]], --noprint-field [FIELD [FIELD ...]]
                        message fields to skip in console output
                        (supports nested.paths and * wildcards)
  -B NUM, --before-context NUM
                        emit NUM messages of leading context before match
  -A NUM, --after-context NUM
                        emit NUM messages of trailing context after match
  -C NUM, --context NUM
                        emit NUM messages of leading and trailing context
                        around match
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
                        (default ** in colorless output)
  --color {auto,always,never}
                        use color output in console (default always)
  --no-meta             do not print metainfo to console
  --no-filename         do not print bag filename prefix on each console line
  --no-console-output   do not print matches to console
  --verbose             print status messages during publish or bag output

Bag input control:
  -n [FILE [FILE ...]], --filename [FILE [FILE ...]]
                        names of ROS bagfiles to scan if not all in directory
                        (supports * wildcards)
  -p [PATH [PATH ...]], --path [PATH [PATH ...]]
                        paths to scan if not current directory
                        (supports * wildcards)
  -r, --recursive       recurse into subdirectories when looking for bagfiles

Live topic control:
  --publish-prefix PREFIX
                        prefix to prepend to input topic on publishing match
                        (default /grepros)
  --publish-suffix SUFFIX
                        suffix to append to input topic on publishing match
  --publish-fixname TOPIC
                        single output topic name to publish all matches to,
                        overrides prefix and suffix
  --queue-size-in SIZE  live ROS topic subscriber queue size (default infinite)
  --queue-size-out SIZE
                        output publisher queue size (default 10)
```


Installation
------------

Requires ROS Python packages (rospy, rosmsg, roslib, rosbag, genpy).

### Using pip

    pip install grepros

This will add the `rosgrep` command to your path.


### Using catkin

In your catkin workspace, under the source directory:

    git glone https://github.com/suurjaak/grepros.git
    cd grepros
    catkin build --this


This will add the `rosgrep` command to your local catkin workspace path.

grep4ros
========

grep for rosbags (ROS 1).


Example usage
-------------

Search for "my text" in all bags under current directory and subdirectories:

    grep4ros -r "my text"

Print 30 lines of the first message from each topic in my.bag:

    grep4ros ".*" --messages-per-topic 1 --lines-per-message 30 -n my.bag

Find first message containing "future" (case-insensitive) in my.bag:

    grep4ros future -I -m 1 -n my.bag

Find 10 messages, from geometry_msgs package, in "map" frame,
from bags in current directory:

    grep4ros frame_id=map -d geometry* -m 10

Find messages with field "key" containing "0xA002",
in topics ending with "diagnostics", in bags under "/tmp":

    grep4ros key=0xA002 -t *diagnostics -p /tmp

Find diagnostics_msgs messages in bags in current directory,
containing "navigation" in fields "name" or "message",
print only header stamp and values:

    grep4ros -d diagnostic_msgs/* -sf name message \
             -pf header.stamp status.values -- navigation


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
  -I, --no-ignore-case  use case-sensitive matching in PATTERNS

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
                        (relative to bag start time if positive
                        or end time if negative,
                        datetime may be partial like 2021-10-14T12)
  -t1 TIME, --end-time TIME
                        latest timestamp of messages to scan
                        as relative seconds or ISO datetime
                        (relative to bag start time if positive
                        or end time if negative,
                        datetime may be partial like 2021-10-14T12)
  -n0 INDEX, --start-index INDEX
                        message index within topic to start from
                        (1-based if positive, counts back from total if negative)
  -n1 INDEX, --end-index INDEX
                        message index within topic to stop at
                        (1-based if positive, counts back from total if negative)
  -sf [FIELD [FIELD ...]], --select-field [FIELD [FIELD ...]]
                        message fields to use in scanning if not all
                        (supports nested.paths and * wildcards)
  -ns [FIELD [FIELD ...]], --noselect-field [FIELD [FIELD ...]]
                        message fields to skip in scanning
                        (supports nested.paths and * wildcards)
  -m NUM, --max-count NUM
                        number of matched messages to print from each file
  --max-per-topic NUM   number of matched messages to print from each topic
  --max-topics NUM      number of topics to print matches from

Output control:
  -pf [FIELD [FIELD ...]], --print-field [FIELD [FIELD ...]]
                        message fields to print in output if not all
                        (supports nested.paths and * wildcards)
  -np [FIELD [FIELD ...]], --noprint-field [FIELD [FIELD ...]]
                        message fields to skip in output
                        (supports nested.paths and * wildcards)
  -B NUM, --before-context NUM
                        print NUM messages of leading context before match
  -A NUM, --after-context NUM
                        print NUM messages of trailing context after match
  -C NUM, --context NUM
                        print NUM messages of leading and trailing context
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
                        use color output (default auto)
  --no-meta             do not print metainfo for bags and messages
  --no-filename         do not print bag filename prefix on each line

File selection:
  -n [FILE [FILE ...]], --filename [FILE [FILE ...]]
                        names of ROS bagfiles to scan if not all in directory
                        (supports * wildcards)
  -p [PATH [PATH ...]], --path [PATH [PATH ...]]
                        paths to scan if not current directory
                        (supports * wildcards)
  -r, --recursive       recurse into subdirectories when looking for bagfiles
```

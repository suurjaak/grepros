# -*- coding: utf-8 -*-
"""
Program main interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    29.10.2021
------------------------------------------------------------------------------
"""
import argparse
import atexit
import os
import re
import sys

from . import inputs, outputs, search
from . common import ConsolePrinter, parse_datetime


ARGUMENTS = {
    "description": "Searches through messages in ROS1 bag files or live topics.",
    "epilog":      """
PATTERNs use Python regular expression syntax, message matches if all match.
* wildcards in other arguments use simple globbing as zero or more characters,
target matches if any value matches.
 

Example usage:

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
    grepros -d diagnostic_msgs/* -sf name message \\
            -pf header.stamp status.values -- navigation
    """,

    "arguments": [
        dict(args=["PATTERNS"], nargs="+", metavar="PATTERN",
             help="pattern(s) to find in message field values,\n"
                  "can specify message field as NAME=PATTERN\n"
                  "(name may be a nested.path)"),

        dict(args=["-F", "--fixed-strings"],
             dest="RAW", action="store_true",
             help="PATTERNs are ordinary strings, not regular expressions"),

        dict(args=["-I", "--no-ignore-case"],
             dest="CASE", action="store_true",
             help="use case-sensitive matching in PATTERNs"),

        dict(args=["--live"],
             dest="LIVE", action="store_true",
             help="read messages from live ROS topics instead of bagfiles"),

        dict(args=["--publish"],
             dest="PUBLISH", action="store_true",
             help="publish matched messages to live ROS topics"),

        dict(args=["--write"], dest="OUTBAG", default="",
             help="write matched messages to specified bagfile"),
    ],

    "groups": {"Filtering": [

        dict(args=["-t", "--topic"],
             dest="TOPICS", metavar="TOPIC", nargs="+", default=[],
             help="ROS topics to scan if not all (supports * wildcards)"),

        dict(args=["-nt", "--no-topic"],
             dest="SKIP_TOPICS", metavar="TOPIC", nargs="+", default=[],
             help="ROS topics to skip (supports * wildcards)"),

        dict(args=["-d", "--type"],
             dest="TYPES", metavar="TYPE", nargs="+", default=[],
             help="ROS message types to scan if not all (supports * wildcards)"),

        dict(args=["-nd", "--no-type"],
             dest="SKIP_TYPES", metavar="TYPE", nargs="+", default=[],
             help="ROS message types to skip (supports * wildcards)"),

        dict(args=["-t0", "--start-time"],
             dest="START_TIME", metavar="TIME",
             help="earliest timestamp of messages to scan\n"
                  "as relative seconds or ISO datetime\n"
                  "(for bag input, relative to bag start time\n"
                  "if positive or end time if negative,\n"
                  "for live input relative to system time,\n"
                  "datetime may be partial like 2021-10-14T12)"),

        dict(args=["-t1", "--end-time"],
             dest="END_TIME", metavar="TIME",
             help="latest timestamp of messages to scan\n"
                  "as relative seconds or ISO datetime\n"
                  "(for bag input, relative to bag start time\n"
                  "if positive or end time if negative,\n"
                  "for live input relative to system time,\n"
                  "datetime may be partial like 2021-10-14T12)"),

        dict(args=["-n0", "--start-index"],
             dest="START_INDEX", metavar="INDEX", type=int,
             help="message index within topic to start from\n"
                  "(1-based if positive, counts back from bag total if negative)"),

        dict(args=["-n1", "--end-index"],
             dest="END_INDEX", metavar="INDEX", type=int,
             help="message index within topic to stop at\n"
                  "(1-based if positive, counts back from bag total if negative)"),

        dict(args=["-sf", "--select-field"],
             dest="SELECT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to use in matching if not all\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-ns", "--noselect-field"],
             dest="NOSELECT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to skip in matching\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-m", "--max-count"],
             dest="MAX_MATCHES", metavar="NUM", default=0, type=int,
             help="number of matched messages to emit (per file if bag input)"),

        dict(args=["--max-per-topic"],
             dest="MAX_TOPIC_MATCHES", metavar="NUM", default=0, type=int,
             help="number of matched messages to emit from each topic\n"
                  "(per file if bag input)"),

        dict(args=["--max-topics"],
             dest="MAX_TOPICS", metavar="NUM", default=0, type=int,
             help="number of topics to print matches from"),

    ], "Output control": [

        dict(args=["-pf", "--print-field"],
             dest="PRINT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to print in console output if not all\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-np", "--noprint-field"],
             dest="NOPRINT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to skip in console output\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-B", "--before-context"],
             dest="BEFORE", metavar="NUM", default=0, type=int,
             help="emit NUM messages of leading context before match"),

        dict(args=["-A", "--after-context"],
             dest="AFTER", metavar="NUM", default=0, type=int,
             help="emit NUM messages of trailing context after match"),

        dict(args=["-C", "--context"],
             dest="CONTEXT", metavar="NUM", default=0, type=int,
             help="emit NUM messages of leading and trailing context\n"
                  "around match"),

        dict(args=["-mo", "--matched-fields-only"],
             dest="MATCHED_FIELDS_ONLY", action="store_true",
             help="print only the fields where PATTERNs find a match"),

        dict(args=["-la", "--lines-around-match"],
             metavar="NUM", dest="LINES_AROUND_MATCH", type=int,
             help="print only matched fields and NUM message lines\n"
                  "around match"),

        dict(args=["-lf", "--lines-per-field"],
             metavar="NUM", dest="MAX_FIELD_LINES", type=int,
             help="maximum number of lines to print per field"),

        dict(args=["-l0", "--start-line"],
             metavar="NUM", dest="START_LINE", type=int,
             help="message line number to start printing from\n"
                  "(1-based if positive, counts back from total if negative)"),

        dict(args=["-l1", "--end-line"],
             metavar="NUM", dest="END_LINE", type=int,
             help="message line number to stop printing at\n"
                  "(1-based if positive, counts back from total if negative)"),

        dict(args=["-lm", "--lines-per-message"],
             metavar="NUM", dest="MAX_MESSAGE_LINES", type=int,
             help="maximum number of lines to print per message"),

        dict(args=["--match-wrapper"],
             dest="MATCH_WRAPPER", metavar="STR", nargs="*",
             help="string to wrap around matched values,\n"
                  "both sides if one value, start and end if more than one,\n"
                  "or no wrapping if zero values\n"
                  "(default ** in colorless output)"),

        dict(args=["--color"], dest="COLOR",
             choices=["auto", "always", "never"], default="always",
             help="use color output in console (default always)"),

        dict(args=["--no-meta"], dest="META", action="store_false",
             help="do not print bag and topic and message metainfo to console"),

        dict(args=["--no-filename"], dest="FILENAME", action="store_false",
             help="do not print bag filename prefix on each console message line"),

        dict(args=["--no-console-output"], dest="CONSOLE", action="store_false",
             help="do not print matches to console"),

        dict(args=["--verbose"], dest="VERBOSE", action="store_true",
             help="print status messages during publish or bag output"),

    ], "Bag input control": [

        dict(args=["-n", "--filename"],
             dest="FILES", metavar="FILE", nargs="*", default=[],
             help="names of ROS bagfiles to scan if not all in directory\n"
                  "(supports * wildcards)"),

        dict(args=["-p", "--path"],
             dest="PATHS", metavar="PATH", nargs="*", default=[],
             help="paths to scan if not current directory\n"
                  "(supports * wildcards)"),

        dict(args=["-r", "--recursive"],
             dest="RECURSE", action="store_true",
             help="recurse into subdirectories when looking for bagfiles"),

    ], "Live topic control": [

        dict(args=["--publish-prefix"],
             dest="PUBLISH_PREFIX", metavar="PREFIX", default="/grepros",
             help="prefix to prepend to input topic on publishing match\n"
                  "(default /grepros)"),

        dict(args=["--publish-suffix"],
             dest="PUBLISH_SUFFIX", metavar="SUFFIX", default="",
             help="suffix to append to input topic on publishing match"),

        dict(args=["--publish-fixname"],
             dest="PUBLISH_FIXNAME", metavar="TOPIC", default="",
             help="single output topic name to publish all matches to,\n"
                  "overrides prefix and suffix"),

        dict(args=["--queue-size-in"],
             dest="QUEUE_SIZE_IN", metavar="SIZE", type=int, default=-1,
             help="live ROS topic subscriber queue size (default infinite)"),

        dict(args=["--queue-size-out"],
             dest="QUEUE_SIZE_OUT", metavar="SIZE", type=int, default=10,
             help="output publisher queue size (default 10)"),


    ]},
}


def make_parser():
    kws = dict(description=ARGUMENTS["description"], epilog=ARGUMENTS["epilog"],
               formatter_class=argparse.RawTextHelpFormatter)
    argparser = argparse.ArgumentParser(**kws)
    for arg in map(dict, ARGUMENTS["arguments"]):
        argparser.add_argument(*arg.pop("args"), **arg)
    for group, groupargs in ARGUMENTS.get("groups", {}).items():
        grouper = argparser.add_argument_group(group)
        for arg in map(dict, groupargs):
            grouper.add_argument(*arg.pop("args"), **arg)
    return argparser


def validate_args(args):
    """Validates arguments, prints errors, returns success."""
    try: args.START_TIME = float(args.START_TIME)
    except Exception: pass
    try: args.END_TIME = float(args.END_TIME)
    except Exception: pass
    if isinstance(args.START_TIME, str):
        args.START_TIME = parse_datetime(args.START_TIME)
    if isinstance(args.END_TIME, str):
        args.END_TIME = parse_datetime(args.END_TIME)
    if args.CONTEXT:
        args.BEFORE = args.AFTER = args.CONTEXT

    errors = []
    for v in args.PATTERNS:
        split = v.find("=", 1, -1)  # May be "PATTERN" or "attribute=PATTERN"
        v = v[split + 1:] if split > 0 else v
        try:
            re.compile(re.escape(v) if args.RAW else v)
        except Exception as e:
            errors.append("'%s': %s" % (v, e))
    if errors:
        ConsolePrinter.error("\nInvalid regular expression.")
        for err in errors:
            ConsolePrinter.error("  %s" % err)
    return not errors


def flush_stdout():
    """Writes a linefeed to sdtout if nothing has been printed to it so far."""
    if not ConsolePrinter.PRINTS.get(sys.stdout) and not sys.stdout.isatty():
        try: print()  # Piping cursed output to `more` remains paging if nothing is printed
        except Exception: pass


def run():
    """Parses arguments and runs search."""
    args, _ = make_parser().parse_known_args()
    ConsolePrinter.configure(args)
    atexit.register(flush_stdout)
    if not validate_args(args):
        sys.exit(1)

    cls = inputs.TopicSource if args.LIVE else inputs.BagSource
    searcher, source, sink = search.Searcher(args), cls(args), outputs.MultiSink(args)
    if not sink.sinks:
        ConsolePrinter.error("No output configured.")
        sys.exit(1)
    if not source.validate() or not sink.validate():
        sys.exit(1)

    source.thread_excepthook = lambda e: (ConsolePrinter.error(e), sys.exit(1))
    BREAK_EXS = (KeyboardInterrupt, )
    try: BREAK_EXS += (BrokenPipeError, KeyboardInterrupt, )  # Py3
    except NameError: pass  # Py2

    try: searcher.search(source, sink)
    except BREAK_EXS:
        # Redirect remaining output to devnull to avoid another BrokenPipeError
        try: os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except Exception: pass
        sys.exit()

if "__main__" == __name__:
    run()

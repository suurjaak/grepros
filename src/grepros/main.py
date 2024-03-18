# -*- coding: utf-8 -*-
"""
Program main interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    18.03.2024
------------------------------------------------------------------------------
"""
## @namespace grepros.main
import argparse
import atexit
import os
import random
import signal
import sys
import traceback

from . import __title__, __version__, __version_date__, api, inputs, outputs, search
from . common import ArgumentUtil, ConsolePrinter, MatchMarkers
from . import plugins



## Configuration for argparse, as {description, epilog, args: [..], groups: {name: [..]}}
ARGUMENTS = {
    "description": "Searches through messages in ROS bag files or live topics.",
    "epilog":      """
PATTERNs use Python regular expression syntax, message matches if all match.
* wildcards use simple globbing as zero or more characters,
target matches if any value matches.
 

Example usage:

Search for "my text" in all bags under current directory and subdirectories:
    %(title)s -r "my text"

Print 30 lines of the first message from each live ROS topic:
    %(title)s --max-per-topic 1 --lines-per-message 30 --live

Find first message containing "future" (case-sensitive) in my.bag:
    %(title)s future -I --max-count 1 --name my.bag

Find 10 messages, from geometry_msgs package, in "map" frame,
from bags in current directory, reindexing any unindexed bags:
    %(title)s frame_id=map --type geometry_msgs/* --max-count 10 --reindex-if-unindexed

Pipe all diagnostics messages with "CPU usage" from live ROS topics to my.bag:
    %(title)s "CPU usage" --type *DiagnosticArray --no-console-output --write my.bag

Find messages with field "key" containing "0xA002",
in topics ending with "diagnostics", in bags under "/tmp":
    %(title)s key=0xA002 --topic *diagnostics --path /tmp

Find diagnostics_msgs messages in bags in current directory,
containing "navigation" in fields "name" or "message",
print only header stamp and values:
    %(title)s --type diagnostic_msgs/* --select-field name message \\
            --emit-field header.stamp status.values -- navigation

Print first message from each lidar topic on ROS1 host 1.2.3.4, without highlight:
    ROS_MASTER_URI=http://1.2.3.4::11311 \\
    %(title)s --live --topic *lidar* --max-per-topic 1 --no-highlight

Export all bag messages to SQLite and Postgres, print only export progress:
    %(title)s -n my.bag --write my.bag.sqlite --no-console-output --no-verbose --progress

    %(title)s -n my.bag --write postgresql://user@host/dbname \\
            --no-console-output --no-verbose --progress
    """ % dict(title=__title__),

    "arguments": [
        dict(args=["PATTERN"], nargs="*", default=[],
             help="pattern(s) to find in message field values,\n"
                  "all messages match if not given,\n"
                  "can specify message field as NAME=PATTERN\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-h", "--help"],
             dest="HELP", action="store_true",
             help="show this help message and exit"),

        dict(args=["-F", "--fixed-strings"],
             dest="FIXED_STRING", action="store_true",
             help="PATTERNs are ordinary strings, not regular expressions"),

        dict(args=["-I", "--no-ignore-case"],
             dest="CASE", action="store_true",
             help="use case-sensitive matching in PATTERNs"),

        dict(args=["-v", "--invert-match"],
             dest="INVERT", action="store_true",
             help="select messages not matching PATTERNs"),

        dict(args=["-e", "--expression"],
             dest="EXPRESSION", action="store_true",
             help="PATTERNs are a logical expression\n"
                  "like 'this AND (this2 OR NOT \"skip this\")',\n"
                  "with elements as patterns to find in message fields"),

        dict(args=["--version"],
             dest="VERSION", action="version",
             version="%s: grep for ROS bag files and live topics, v%s (%s)" %
                     (__title__, __version__, __version_date__),
             help="display version information and exit"),

        dict(args=["--live"],
             dest="LIVE", action="store_true",
             help="read messages from live ROS topics instead of bagfiles"),

        dict(args=["--publish"],
             dest="PUBLISH", action="store_true",
             help="publish matched messages to live ROS topics"),

        dict(args=["--write"],
             dest="WRITE", nargs="+", default=[], action="append",
             metavar="TARGET [format=bag] [KEY=VALUE ...]",
             help="write matched messages to specified output,\n"
                  "format is autodetected from TARGET if not specified.\n"
                  "Bag or database will be appended to if it already exists.\n"
                  "Keyword arguments are given to output writer."),

        dict(args=["--write-options"],  # Will be populated from --write by MultiSink
             dest="WRITE_OPTIONS", default=argparse.SUPPRESS, help=argparse.SUPPRESS),

        dict(args=["--plugin"],
             dest="PLUGIN", nargs="+", default=[], action="append",
             help="load a Python module or class as plugin"),

        dict(args=["--stop-on-error"],
             dest="STOP_ON_ERROR", action="store_true",
             help="stop further execution on any error like unknown message type"),
    ],

    "groups": {"Filtering": [

        dict(args=["-t", "--topic"],
             dest="TOPIC", nargs="+", default=[], action="append",
             help="ROS topics to read if not all (supports * wildcards)"),

        dict(args=["-nt", "--no-topic"],
             dest="SKIP_TOPIC", metavar="TOPIC", nargs="+", default=[], action="append",
             help="ROS topics to skip (supports * wildcards)"),

        dict(args=["-d", "--type"],
             dest="TYPE", nargs="+", default=[], action="append",
             help="ROS message types to read if not all (supports * wildcards)"),

        dict(args=["-nd", "--no-type"],
             dest="SKIP_TYPE", metavar="TYPE", nargs="+", default=[], action="append",
             help="ROS message types to skip (supports * wildcards)"),

        dict(args=["--condition"],
             dest="CONDITION", nargs="+", default=[], action="append",
             help="extra conditions to require for matching messages,\n"
                  "as ordinary Python expressions, can refer to last messages\n"
                  "in topics as <topic /my/topic>; topic name can contain wildcards.\n"
                  'E.g. --condition "<topic /robot/enabled>.data" matches\n'
                  "messages only while last message in '/robot/enabled' has data=true."),

        dict(args=["-t0", "--start-time"],
             dest="START_TIME", metavar="TIME",
             help="earliest timestamp of messages to read\n"
                  "as relative seconds if signed,\n"
                  "or epoch timestamp or ISO datetime\n"
                  "(for bag input, relative to bag start time\n"
                  "if positive or end time if negative,\n"
                  "for live input relative to system time,\n"
                  "datetime may be partial like 2021-10-14T12)"),

        dict(args=["-t1", "--end-time"],
             dest="END_TIME", metavar="TIME",
             help="latest timestamp of messages to read\n"
                  "as relative seconds if signed,\n"
                  "or epoch timestamp or ISO datetime\n"
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

        dict(args=["--every-nth-message"],
             dest="NTH_MESSAGE", metavar="NUM", type=int, default=1,
             help="read every Nth message within topic, starting from first"),

        dict(args=["--every-nth-interval"],
             dest="NTH_INTERVAL", metavar="SECONDS", type=float, default=0,
             help="read messages at least N seconds apart within topic"),

        dict(args=["--every-nth-match"],
             dest="NTH_MATCH", metavar="NUM", type=int, default=1,
             help="emit every Nth match in topic, starting from first"),

        dict(args=["-sf", "--select-field"],
             dest="SELECT_FIELD", metavar="FIELD", nargs="+", default=[], action="append",
             help="message fields to use in matching if not all\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-ns", "--no-select-field"],
             dest="NOSELECT_FIELD", metavar="FIELD", nargs="+", default=[], action="append",
             help="message fields to skip in matching\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-m", "--max-count"],
             dest="MAX_COUNT", metavar="NUM", default=0, type=int,
             help="number of matched messages to emit (per each file if bag input)"),

        dict(args=["--max-per-topic"],
             dest="MAX_PER_TOPIC", metavar="NUM", default=0, type=int,
             help="number of matched messages to emit from each topic\n"
                  "(per each file if bag input)"),

        dict(args=["--max-topics"],
             dest="MAX_TOPICS", metavar="NUM", default=0, type=int,
             help="number of topics to emit matches from (per each file if bag input)"),

        dict(args=["--unique-only"],
             dest="UNIQUE", action="store_true",
             help="only emit matches that are unique in topic,\n"
                  "taking --select-field and --no-select-field into account\n"
                  "(per each file if bag input)"),

    ], "Output control": [

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

        dict(args=["-ef", "--emit-field"],
             dest="EMIT_FIELD", metavar="FIELD", nargs="+", default=[], action="append",
             help="message fields to emit in console output if not all\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-nf", "--no-emit-field"],
             dest="NOEMIT_FIELD", metavar="FIELD", nargs="+", default=[], action="append",
             help="message fields to skip in console output\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-mo", "--matched-fields-only"],
             dest="MATCHED_FIELDS_ONLY", action="store_true",
             help="emit only the fields where PATTERNs find a match in console output"),

        dict(args=["-la", "--lines-around-match"],
             dest="LINES_AROUND_MATCH", metavar="NUM", type=int,
             help="emit only matched fields and NUM message lines\n"
                  "around match in console output"),

        dict(args=["-lf", "--lines-per-field"],
             dest="MAX_FIELD_LINES", metavar="NUM", type=int,
             help="maximum number of lines to emit per field in console output"),

        dict(args=["-l0", "--start-line"],
             dest="START_LINE", metavar="NUM", type=int,
             help="message line number to start emitting from in console output\n"
                  "(1-based if positive, counts back from total if negative)"),

        dict(args=["-l1", "--end-line"],
             dest="END_LINE", metavar="NUM", type=int,
             help="message line number to stop emitting at in console output\n"
                  "(1-based if positive, counts back from total if negative)"),

        dict(args=["-lm", "--lines-per-message"],
             dest="MAX_MESSAGE_LINES", metavar="NUM", type=int,
             help="maximum number of lines to emit per message in console output"),

        dict(args=["--match-wrapper"],
             dest="MATCH_WRAPPER", metavar="STR", nargs="*",
             help="string to wrap around matched values in console output,\n"
                  "both sides if one value, start and end if more than one,\n"
                  "or no wrapping if zero values\n"
                  '(default "**" in colorless output)'),

        dict(args=["--wrap-width"],
             dest="WRAP_WIDTH", metavar="NUM", type=int,
             help="character width to wrap message YAML console output at,\n"
                  "0 disables (defaults to detected terminal width)"),

        dict(args=["--color"], dest="COLOR",
             choices=["auto", "always", "never"], default="always",
             help='use color output in console (default "always")'),

        dict(args=["--no-meta"], dest="META", action="store_false",
             help="do not print source and message metainfo to console"),

        dict(args=["--no-filename"], dest="LINE_PREFIX", action="store_false",
             help="do not print bag filename prefix on each console message line"),

        dict(args=["--no-highlight"], dest="HIGHLIGHT", action="store_false",
             help="do not highlight matched values"),

        dict(args=["--no-console-output"], dest="CONSOLE", action="store_false",
             help="do not print matches to console"),

        dict(args=["--progress"], dest="PROGRESS", action="store_true",
             help="show progress bar when not printing matches to console"),

        dict(args=["--verbose"], dest="VERBOSE", action="store_true",
             help="print status messages during console output\n"
                  "for publishing and writing, and error stacktraces"),

        dict(args=["--no-verbose"], dest="SKIP_VERBOSE", action="store_true",
             help="do not print status messages during console output\n"
                  "for publishing and writing"),

    ], "Bag input control": [

        dict(args=["-n", "--filename"],
             dest="FILE", nargs="+", default=[], action="append",
             help="names of ROS bagfiles to read if not all in directory\n"
                  "(supports * wildcards)"),

        dict(args=["-p", "--path"],
             dest="PATH", nargs="+", default=[], action="append",
             help="paths to scan if not current directory\n"
                  "(supports * wildcards)"),

        dict(args=["-r", "--recursive"],
             dest="RECURSE", action="store_true",
             help="recurse into subdirectories when looking for bagfiles"),

        dict(args=["--order-bag-by"],
             dest="ORDERBY", choices=["topic", "type"],
             help="order bag messages by topic or type first and then by time"),

        dict(args=["--decompress"],
             dest="DECOMPRESS", action="store_true",
             help="decompress archived bagfiles with recognized extensions (.zst .zstd)"),

        dict(args=["--reindex-if-unindexed"],
             dest="REINDEX", action="store_true",
             help="reindex unindexed bagfiles (ROS1 only), makes backup copies"),

        dict(args=["--time-scale"],
             dest="TIMESCALE", metavar="FACTOR", nargs="?", type=float, const=1, default=0,
             help="emit messages on original bag timeline from first matched message,\n"
                  "optionally with a speedup or slowdown factor"),

        dict(args=["--time-scale-emission"],
             dest="TIMESCALE_EMISSION", nargs="?", type=int, const=True, default=True,
             help=argparse.SUPPRESS),  # Timeline from first matched message vs first in bag

    ], "Live topic control": [

        dict(args=["--publish-prefix"],
             dest="PUBLISH_PREFIX", metavar="PREFIX", default="",
             help="prefix to prepend to input topic name on publishing match"),

        dict(args=["--publish-suffix"],
             dest="PUBLISH_SUFFIX", metavar="SUFFIX", default="",
             help="suffix to append to input topic name on publishing match"),

        dict(args=["--publish-fixname"],
             dest="PUBLISH_FIXNAME", metavar="TOPIC", default="",
             help="single output topic name to publish all matches to,\n"
                  "overrides prefix and suffix"),

        dict(args=["--queue-size-in"],
             dest="QUEUE_SIZE_IN", metavar="SIZE", type=int, default=10,
             help="live ROS topic subscriber queue size (default 10)"),

        dict(args=["--queue-size-out"],
             dest="QUEUE_SIZE_OUT", metavar="SIZE", type=int, default=10,
             help="output publisher queue size (default 10)"),

        dict(args=["--ros-time-in"],
             dest="ROS_TIME_IN", action="store_true",
             help="use ROS time instead of system time for incoming message\n"
                  "timestamps from subsribed live ROS topics"),

    ]},
}

## List of command-line arguments the program was invoked with
CLI_ARGS = None


def flush_stdout():
    """Writes a linefeed to sdtout if nothing has been printed to it so far."""
    if not ConsolePrinter.PRINTS.get(sys.stdout) and not sys.stdout.isatty():
        try: print()  # Piping cursed output to `more` remains paging if nothing is printed
        except (Exception, KeyboardInterrupt): pass


def preload_plugins(cli_args):
    """Imports and initializes plugins from auto-load folder and from arguments."""
    plugins.add_write_format("bag", outputs.BagSink, "bag", [
        ("overwrite=true|false",   "overwrite existing file\nin bag output\n"
                                   "instead of appending to if bag or database\n"
                                   "or appending unique counter to file name\n"
                                   "(default false)")

    ] + outputs.RolloverSinkMixin.get_write_options("bag"))
    args = None
    if "--plugin" in cli_args:
        args, _ = ArgumentUtil.make_parser(ARGUMENTS).parse_known_args(cli_args)
        args = ArgumentUtil.flatten(args)
    try: plugins.init(args)
    except ImportWarning: sys.exit(1)


def make_thread_excepthook(args, exitcode_dict):
    """Returns thread exception handler: function(text, exc) prints error, stops application."""
    def thread_excepthook(text, exc):
        """Prints error, sets exitcode flag, shuts down ROS node if any, interrupts main thread."""
        ConsolePrinter.error(text)
        if args.VERBOSE: traceback.print_exc()
        exitcode_dict["value"] = 1
        api.shutdown_node()
        os.kill(os.getpid(), signal.SIGINT)
    return thread_excepthook


def run():
    """Parses command-line arguments and runs search."""
    global CLI_ARGS
    CLI_ARGS = sys.argv[1:]
    MatchMarkers.populate("%08x" % random.randint(1, 1E9))
    preload_plugins(CLI_ARGS)
    argparser = ArgumentUtil.make_parser(ARGUMENTS)
    if not CLI_ARGS:
        argparser.print_usage()
        return

    atexit.register(flush_stdout)
    args = argparser.parse_args(CLI_ARGS)
    if args.HELP:
        argparser.print_help()
        return

    BREAK_EXS = (KeyboardInterrupt, )
    try: BREAK_EXS += (BrokenPipeError, )  # Py3
    except NameError: pass  # Py2

    exitcode = {"value": 0}
    source, sink = None, None
    try:
        ConsolePrinter.configure({"always": True, "never": False}.get(args.COLOR))
        args = ArgumentUtil.validate(args, cli=True)

        source = plugins.load("source", args) or \
                 (inputs.LiveSource if args.LIVE else inputs.BagSource)(args)
        if not source.validate():
            sys.exit(1)
        sink = outputs.MultiSink(args)
        sink.sinks.extend(filter(bool, plugins.load("sink", args, collect=True)))
        if not sink.validate():
            sys.exit(1)

        source.thread_excepthook = sink.thread_excepthook = make_thread_excepthook(args, exitcode)
        grepper = plugins.load("scan", args) or search.Scanner(args)
        grepper.work(source, sink)
    except BREAK_EXS:
        try: sink and sink.close()
        except (Exception, KeyboardInterrupt): pass
        try: source and source.close()
        except (Exception, KeyboardInterrupt): pass
        # Redirect remaining output to devnull to avoid another BrokenPipeError
        try: os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except (Exception, KeyboardInterrupt): pass
        sys.exit(exitcode["value"])
    except Exception as e:
        ConsolePrinter.error(e)
        if args.VERBOSE: traceback.print_exc()
    finally:
        sink and sink.close()
        source and source.close()
        try: api.shutdown_node()
        except BREAK_EXS: pass


__all__ = [
    "ARGUMENTS", "CLI_ARGS", "flush_stdout", "make_thread_excepthook", "preload_plugins", "run",
]



if "__main__" == __name__:
    run()

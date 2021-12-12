# -*- coding: utf-8 -*-
"""
Program main interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    12.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.main
import argparse
import atexit
import collections
import os
import re
import sys

from . import __version__, inputs, outputs, search
from . common import ConsolePrinter, Plugins, parse_datetime


## Configuration for argparse, as {description, epilog, args: [..], groups: {name: [..]}}
ARGUMENTS = {
    "description": "Searches through messages in ROS bag files or live topics.",
    "epilog":      """
PATTERNs use Python regular expression syntax, message matches if all match.
* wildcards use simple globbing as zero or more characters,
target matches if any value matches.
 

Example usage:

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
    grepros --type diagnostic_msgs/* --select-field name message \\
            --print-field header.stamp status.values -- navigation

Print first message from each lidar topic on host 1.2.3.4:
    ROS_MASTER_URI=http://1.2.3.4::11311 \\
    grepros --live --topic *lidar* --max-per-topic 1

Export all bag messages to SQLite and Postgres, print only export progress:
    grepros -n my.bag --write my.bag.sqlite --no-console-output --no-verbose --progress

    grepros -n my.bag --write postgresql://user@host/dbname \\
            --no-console-output --no-verbose --progress
    """,

    "arguments": [
        dict(args=["PATTERNS"], nargs="*", metavar="PATTERN",
             help="pattern(s) to find in message field values,\n"
                  "all messages match if not given,\n"
                  "can specify message field as NAME=PATTERN\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-F", "--fixed-strings"],
             dest="RAW", action="store_true",
             help="PATTERNs are ordinary strings, not regular expressions"),

        dict(args=["-I", "--no-ignore-case"],
             dest="CASE", action="store_true",
             help="use case-sensitive matching in PATTERNs"),

        dict(args=["-v", "--invert-match"],
             dest="INVERT", action="store_true",
             help="select non-matching messages"),

        dict(args=["--version"],
             dest="VERSION", action="version",
             version="grepros (grep for ROS bag files and live topics) %s" % __version__,
             help="display version information and exit"),

        dict(args=["--live"],
             dest="LIVE", action="store_true",
             help="read messages from live ROS topics instead of bagfiles"),

        dict(args=["--publish"],
             dest="PUBLISH", action="store_true",
             help="publish matched messages to live ROS topics"),

        dict(args=["--write"], dest="DUMP_TARGET", metavar="TARGET", default="",
             help="write matched messages to specified output file"),

        dict(args=["--write-format"], dest="DUMP_FORMAT",
             choices=["bag", "csv", "html", "postgres", "sqlite"],
             help="output format, auto-detected from TARGET if not given,\n"
                  "bag or database will be appended to if it already exists"),

        dict(args=["--plugin"],
             dest="PLUGINS", metavar="PLUGIN", nargs="+", default=[],
             help="load a Python module or class as plugin"),
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

        dict(args=["--condition"],
             dest="CONDITIONS", metavar="CONDITION", nargs="+", default=[],
             help="extra conditions to require for matching messages,\n"
                  "as ordinary Python expressions, can refer to last messages\n"
                  "in topics as {topic /my/topic}; topic name can contain wildcards.\n"
                  'E.g. --condition "{topic /robot/enabled}.data" matches\n'
                  "messages only while last message in '/robot/enabled' has data=true."),

        dict(args=["-t0", "--start-time"],
             dest="START_TIME", metavar="TIME",
             help="earliest timestamp of messages to scan\n"
                  "as relative seconds if signed,\n"
                  "or epoch timestamp or ISO datetime\n"
                  "(for bag input, relative to bag start time\n"
                  "if positive or end time if negative,\n"
                  "for live input relative to system time,\n"
                  "datetime may be partial like 2021-10-14T12)"),

        dict(args=["-t1", "--end-time"],
             dest="END_TIME", metavar="TIME",
             help="latest timestamp of messages to scan\n"
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

        dict(args=["-sf", "--select-field"],
             dest="SELECT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to use in matching if not all\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-ns", "--no-select-field"],
             dest="NOSELECT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to skip in matching\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-m", "--max-count"],
             dest="MAX_MATCHES", metavar="NUM", default=0, type=int,
             help="number of matched messages to emit (per each file if bag input)"),

        dict(args=["--max-per-topic"],
             dest="MAX_TOPIC_MATCHES", metavar="NUM", default=0, type=int,
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

        dict(args=["-pf", "--print-field"],
             dest="PRINT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to print in console output if not all\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-np", "--no-print-field"],
             dest="NOPRINT_FIELDS", metavar="FIELD", nargs="*", default=[],
             help="message fields to skip in console output\n"
                  "(supports nested.paths and * wildcards)"),

        dict(args=["-mo", "--matched-fields-only"],
             dest="MATCHED_FIELDS_ONLY", action="store_true",
             help="print only the fields where PATTERNs find a match"),

        dict(args=["-la", "--lines-around-match"],
             dest="LINES_AROUND_MATCH", metavar="NUM", type=int,
             help="print only matched fields and NUM message lines\n"
                  "around match"),

        dict(args=["-lf", "--lines-per-field"],
             dest="MAX_FIELD_LINES", metavar="NUM", type=int,
             help="maximum number of lines to print per field"),

        dict(args=["-l0", "--start-line"],
             dest="START_LINE", metavar="NUM", type=int,
             help="message line number to start printing from\n"
                  "(1-based if positive, counts back from total if negative)"),

        dict(args=["-l1", "--end-line"],
             dest="END_LINE", metavar="NUM", type=int,
             help="message line number to stop printing at\n"
                  "(1-based if positive, counts back from total if negative)"),

        dict(args=["-lm", "--lines-per-message"],
             dest="MAX_MESSAGE_LINES", metavar="NUM", type=int,
             help="maximum number of lines to print per message"),

        dict(args=["--match-wrapper"],
             dest="MATCH_WRAPPER", metavar="STR", nargs="*",
             help="string to wrap around matched values,\n"
                  "both sides if one value, start and end if more than one,\n"
                  "or no wrapping if zero values\n"
                  '(default "**" in colorless output)'),

        dict(args=["--wrap-width"],
             dest="WRAP_WIDTH", metavar="NUM", type=int,
             help="character width to wrap message YAML output at,\n"
                  "0 disables (defaults to detected terminal width)"),

        dict(args=["--write-option"], dest="DUMP_OPTIONS", metavar="KEY=VALUE",
             default=[], nargs="*", type=lambda x: (x.split("=", 1)*2)[:2],
             help="write options as key=value pairs, supported flags:\n"
                  "  template=/my/path.tpl - custom template to use for HTML output\n"
                  "  commit-interval=NUM - transaction size for Postgres/SQLite output\n"
                  "                        (default 1000, 0 is autocommit)\n"
                  "  nesting=array|all - create tables for nested message types\n"
                  "                      in Postgres/SQLite output,\n"
                  '                      only for arrays if "array" else for any nested types\n'
                  "                      (array fields in parent will be populated with foreign keys\n"
                  "                       instead of formatted nested values)"),

        dict(args=["--color"], dest="COLOR",
             choices=["auto", "always", "never"], default="always",
             help='use color output in console (default "always")'),

        dict(args=["--no-meta"], dest="META", action="store_false",
             help="do not print source and message metainfo to console"),

        dict(args=["--no-filename"], dest="LINE_PREFIX", action="store_false",
             help="do not print bag filename prefix on each console message line"),

        dict(args=["--no-console-output"], dest="CONSOLE", action="store_false",
             help="do not print matches to console"),

        dict(args=["--progress"], dest="PROGRESS", action="store_true",
             help="show progress bar when not printing matches to console"),

        dict(args=["--verbose"], dest="VERBOSE", action="store_true",
             help="print status messages during console output\n"
                  "for publishing and writing"),

        dict(args=["--no-verbose"], dest="SKIP_VERBOSE", action="store_true",
             help="do not print status messages during console output\n"
                  "for publishing and writing"),

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

        dict(args=["--order-bag-by"],
             dest="ORDERBY", choices=["topic", "type"],
             help="order bag messages by topic or type first and then by time"),

    ], "Live topic control": [

        dict(args=["--publish-prefix"],
             dest="PUBLISH_PREFIX", metavar="PREFIX", default="/grepros",
             help="prefix to prepend to input topic name on publishing match\n"
                  '(default "/grepros")'),

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


def make_parser():
    """Returns a configured ArgumentParser instance."""
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
    """
    Validates arguments, prints errors, returns success.

    @param   args  arguments object like argparse.Namespace
    """
    errors = collections.defaultdict(list)  # {category: [error, ]}
    if args.CONTEXT:
        args.BEFORE = args.AFTER = args.CONTEXT

    # Turn [["key", "value"], ] into a dictionary
    args.DUMP_OPTIONS = dict(args.DUMP_OPTIONS)

    # Default to printing metadata for publish/write if no console output
    args.VERBOSE = False if args.SKIP_VERBOSE else (args.VERBOSE or not args.CONSOLE)

    # Show progress bar only if no console output
    args.PROGRESS = args.PROGRESS and not args.CONSOLE

    # Print filename prefix on each console message line if not single specific file
    args.LINE_PREFIX = args.LINE_PREFIX and (args.RECURSE or len(args.FILES) != 1
                                             or args.PATHS or any("*" in x for x in args.FILES))

    for k, v in vars(args).items():  # Drop duplicates from list values
        if isinstance(v, list):
            setattr(args, k, list(collections.OrderedDict((x, None) for x in v)))

    for n, v in [("START_TIME", args.START_TIME), ("END_TIME", args.END_TIME)]:
        if v is None: continue  # for v, n
        try: v = float(v)
        except Exception: pass
        try: not isinstance(v, float) and setattr(args, n, parse_datetime(v))
        except Exception: errors[""].append("Invalid ISO datetime for %s: %s" % 
                                            (n.lower().replace("_", " "), v))

    for v in args.PATTERNS if not args.RAW else ():
        split = v.find("=", 1, -1)  # May be "PATTERN" or "attribute=PATTERN"
        v = v[split + 1:] if split > 0 else v
        try: re.compile(re.escape(v) if args.RAW else v)
        except Exception as e:
            errors["Invalid regular expression"].append("'%s': %s" % (v, e))

    for v in args.CONDITIONS:
        v = inputs.ConditionMixin.TOPIC_RGX.sub("dummy", v)
        try: compile(v, "", "eval")
        except SyntaxError as e:
            errors["Invalid condition"].append("'%s': %s at %schar %s" % 
                (v, e.msg, "line %s " % e.lineno if e.lineno > 1 else "", e.offset))
        except Exception as e:
            errors["Invalid condition"].append("'%s': %s" % (v, e))

    for err in errors.get("", []):
        ConsolePrinter.error(err)
    for category in filter(bool, errors):
        ConsolePrinter.error(category)
        for err in errors[category]:
            ConsolePrinter.error("  %s" % err)
    return not errors


def flush_stdout():
    """Writes a linefeed to sdtout if nothing has been printed to it so far."""
    if not ConsolePrinter.PRINTS.get(sys.stdout) and not sys.stdout.isatty():
        try: print()  # Piping cursed output to `more` remains paging if nothing is printed
        except (Exception, KeyboardInterrupt): pass


def preload_plugins():
    """Imports and initializes plugins from arguments."""
    if "--plugin" in sys.argv:
        Plugins.configure(make_parser().parse_known_args()[0])


def run():
    """Parses command-line arguments and runs search."""
    preload_plugins()
    argparser = make_parser()
    if len(sys.argv) < 2:
        argparser.print_usage()
        return

    atexit.register(flush_stdout)
    args, _ = argparser.parse_known_args()

    BREAK_EXS = (KeyboardInterrupt, )
    try: BREAK_EXS += (BrokenPipeError, )  # Py3
    except NameError: pass  # Py2

    source, sink = None, None
    try:
        ConsolePrinter.configure({"always": True, "never": False}.get(args.COLOR))
        if not validate_args(args):
            sys.exit(1)

        source = Plugins.load("source", args) or \
                 (inputs.TopicSource if args.LIVE else inputs.BagSource)(args)
        if not source.validate():
            sys.exit(1)
        sink = outputs.MultiSink(args)
        sink.sinks.extend(filter(bool, [Plugins.load("sink", args)]))
        if not sink.validate():
            sys.exit(1)

        thread_excepthook = lambda e: (ConsolePrinter.error(e), sys.exit(1))
        source.thread_excepthook = sink.thread_excepthook = thread_excepthook
        searcher = Plugins.load("search", args) or search.Searcher(args)
        searcher.search(source, sink)
    except BREAK_EXS:
        try: source and source.close()
        except (Exception, KeyboardInterrupt): pass
        try: sink and sink.close()
        except (Exception, KeyboardInterrupt): pass
        # Redirect remaining output to devnull to avoid another BrokenPipeError
        try: os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except (Exception, KeyboardInterrupt): pass
        sys.exit()
    finally:
        sink and sink.close()
        source and source.close()


if "__main__" == __name__:
    run()

# -*- coding: utf-8 -*-
"""
Program main interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    05.07.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.main
import argparse
import atexit
import collections
import logging
import os
import random
import re
import sys

import six

from . import __title__, __version__, __version_date__, api, inputs, outputs, search
from . common import ConsolePrinter, MatchMarkers, parse_datetime
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
    %(title)s frame_id=map --type geometry_msgs/* --max-count 10  --reindex-if-unindexed

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

Print first message from each lidar topic on host 1.2.3.4, without highlight:
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
             help="select messages not matching PATTERN"),

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
             help="read every Nth message within topic"),

        dict(args=["--every-nth-interval"],
             dest="NTH_INTERVAL", metavar="SECONDS", type=int, default=0,
             help="read messages at least N seconds apart within topic"),

        dict(args=["--every-nth-match"],
             dest="NTH_MATCH", metavar="NUM", type=int, default=1,
             help="emit every Nth match in topic"),

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
                  "for publishing and writing"),

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


class HelpFormatter(argparse.RawTextHelpFormatter):
    """RawTextHelpFormatter returning custom metavar for WRITE."""

    def _format_action_invocation(self, action):
        """Returns formatted invocation."""
        if "WRITE" == action.dest:
            return " ".join(action.option_strings + [action.metavar])
        return super(HelpFormatter, self)._format_action_invocation(action)


def make_parser():
    """Returns a configured ArgumentParser instance."""
    kws = dict(description=ARGUMENTS["description"], epilog=ARGUMENTS["epilog"],
               formatter_class=HelpFormatter, add_help=False)
    argparser = argparse.ArgumentParser(**kws)
    for arg in map(dict, ARGUMENTS["arguments"]):
        argparser.add_argument(*arg.pop("args"), **arg)
    for group, groupargs in ARGUMENTS.get("groups", {}).items():
        grouper = argparser.add_argument_group(group)
        for arg in map(dict, groupargs):
            grouper.add_argument(*arg.pop("args"), **arg)
    return argparser


def process_args(args):
    """
    Converts or combines arguments where necessary, returns full args.

    @param   args  arguments object like argparse.Namespace
    """
    for arg in sum(ARGUMENTS.get("groups", {}).values(), ARGUMENTS["arguments"][:]):
        name = arg.get("dest") or arg["args"][0]
        if "version" != arg.get("action") and argparse.SUPPRESS != arg.get("default") \
        and "HELP" != name and not hasattr(args, name):
            value = False if arg.get("store_true") else True if arg.get("store_false") else None
            setattr(args, name, arg.get("default", value))

    if args.CONTEXT:
        args.BEFORE = args.AFTER = args.CONTEXT

    # Default to printing metadata for publish/write if no console output
    args.VERBOSE = False if args.SKIP_VERBOSE else \
                   (args.VERBOSE or not args.CONSOLE and bool(CLI_ARGS))

    # Show progress bar only if no console output
    args.PROGRESS = args.PROGRESS and not args.CONSOLE

    # Print filename prefix on each console message line if not single specific file
    args.LINE_PREFIX = args.LINE_PREFIX and (args.RECURSE or len(args.FILE) != 1
                                             or args.PATH or any("*" in x for x in args.FILE))

    for k, v in vars(args).items():  # Flatten lists of lists and drop duplicates
        if k != "WRITE" and isinstance(v, list):
            here = set()
            setattr(args, k, [x for xx in v for x in (xx if isinstance(xx, list) else [xx])
                              if not (x in here or here.add(x))])

    for n, v in [("START_TIME", args.START_TIME), ("END_TIME", args.END_TIME)]:
        if not isinstance(v, (six.binary_type, six.text_type)): continue  # for v, n
        try: v = float(v)
        except Exception: pass  # If numeric, leave as string for source to process as relative time
        try: not isinstance(v, float) and setattr(args, n, parse_datetime(v))
        except Exception: pass

    return  args


def validate_args(args):
    """
    Validates arguments, prints errors, returns success.

    @param   args  arguments object like argparse.Namespace
    """
    errors = collections.defaultdict(list)  # {category: [error, ]}

    # Validate --write .. key=value
    for opts in args.WRITE:  # List of lists, one for each --write
        erropts = []
        for opt in opts[1:]:
            try: dict([opt.split("=", 1)])
            except Exception: erropts.append(opt)
        if erropts:
            errors[""].append('Invalid KEY=VALUE in "--write %s": %s' %
                              (" ".join(opts), " ".join(erropts)))

    for n, v in [("START_TIME", args.START_TIME), ("END_TIME", args.END_TIME)]:
        if v is None: continue  # for v, n
        try: v = float(v)
        except Exception: pass
        try: not isinstance(v, float) and setattr(args, n, parse_datetime(v))
        except Exception: errors[""].append("Invalid ISO datetime for %s: %s" %
                                            (n.lower().replace("_", " "), v))

    for v in args.PATTERN if not args.FIXED_STRING else ():
        split = v.find("=", 1, -1)  # May be "PATTERN" or "attribute=PATTERN"
        v = v[split + 1:] if split > 0 else v
        try: re.compile(re.escape(v) if args.FIXED_STRING else v)
        except Exception as e:
            errors["Invalid regular expression"].append("'%s': %s" % (v, e))

    for v in args.CONDITION:
        v = inputs.ConditionMixin.TOPIC_RGX.sub("dummy", v)
        try: compile(v, "", "eval")
        except SyntaxError as e:
            errors["Invalid condition"].append("'%s': %s at %schar %s" %
                (v, e.msg, "line %s " % e.lineno if e.lineno > 1 else "", e.offset))
        except Exception as e:
            errors["Invalid condition"].append("'%s': %s" % (v, e))

    for err in errors.get("", []):
        ConsolePrinter.log(logging.ERROR, err)
    for category in filter(bool, errors):
        ConsolePrinter.log(logging.ERROR, category)
        for err in errors[category]:
            ConsolePrinter.log(logging.ERROR, "  %s" % err)
    return not errors


def flush_stdout():
    """Writes a linefeed to sdtout if nothing has been printed to it so far."""
    if not ConsolePrinter.PRINTS.get(sys.stdout) and not sys.stdout.isatty():
        try: print()  # Piping cursed output to `more` remains paging if nothing is printed
        except (Exception, KeyboardInterrupt): pass


def preload_plugins():
    """Imports and initializes plugins from auto-load folder and from arguments."""
    plugins.add_write_format("bag", outputs.BagSink, "bag", [
        ("overwrite=true|false",   "overwrite existing file\nin bag output\n"
                                   "instead of appending to if bag or database\n"
                                   "or appending unique counter to file name\n"
                                   "(default false)")

    ])
    args = make_parser().parse_known_args(CLI_ARGS)[0] if "--plugin" in CLI_ARGS else None
    try: plugins.init(process_args(args) if args else None)
    except ImportWarning: sys.exit(1)


def run():
    """Parses command-line arguments and runs search."""
    global CLI_ARGS
    CLI_ARGS = sys.argv[1:]
    MatchMarkers.populate("%08x" % random.randint(1, 1E9))
    preload_plugins()
    argparser = make_parser()
    if not CLI_ARGS:
        argparser.print_usage()
        return

    atexit.register(flush_stdout)
    args, _ = argparser.parse_known_args(CLI_ARGS)
    if args.HELP:
        argparser.print_help()
        return

    BREAK_EXS = (KeyboardInterrupt, )
    try: BREAK_EXS += (BrokenPipeError, )  # Py3
    except NameError: pass  # Py2

    source, sink = None, None
    try:
        ConsolePrinter.configure({"always": True, "never": False}.get(args.COLOR))
        if not validate_args(process_args(args)):
            sys.exit(1)

        source = plugins.load("source", args) or \
                 (inputs.TopicSource if args.LIVE else inputs.BagSource)(args)
        if not source.validate():
            sys.exit(1)
        sink = outputs.MultiSink(args)
        sink.sinks.extend(filter(bool, plugins.load("sink", args, collect=True)))
        if not sink.validate():
            sys.exit(1)

        thread_excepthook = lambda t, e: (ConsolePrinter.error(t), sys.exit(1))
        source.thread_excepthook = sink.thread_excepthook = thread_excepthook
        grepper = plugins.load("scan", args) or search.Scanner(args)
        grepper.work(source, sink)
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
        api.shutdown_node()


__all__ = [
    "ARGUMENTS", "CLI_ARGS", "HelpFormatter",
    "make_parser", "process_args", "validate_args", "flush_stdout", "preload_plugins", "run",
]



if "__main__" == __name__:
    run()

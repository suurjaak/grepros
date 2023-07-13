# -*- coding: utf-8 -*-
"""
grepros library interface.

Source classes:

- {@link grepros.inputs.AppSource AppSource}: produces messages from iterable or pushed data
- {@link grepros.inputs.BagSource BagSource}: produces messages from ROS bagfiles
- {@link grepros.inputs.TopicSource TopicSource}: produces messages from live ROS topics

Sink classes:

- {@link grepros.outputs.AppSink AppSink}: provides messages to callback function
- {@link grepros.outputs.BagSink BagSink}: writes messages to bagfile
- {@link grepros.outputs.ConsoleSink ConsoleSink}: prints messages to console
- {@link grepros.plugins.auto.csv.CsvSink CsvSink}: writes messages to CSV files, each topic to a separate file
- {@link grepros.plugins.auto.html.HtmlSink HtmlSink}: writes messages to an HTML file
- {@link grepros.plugins.mcap.McapSink McapSink}: writes messages to MCAP file
- {@link grepros.outputs.MultiSink MultiSink}: combines any number of sinks
- {@link grepros.plugins.parquet.ParquetSink ParquetSink}: writes messages to Apache Parquet files
- {@link grepros.plugins.auto.postgres.PostgresSink PostgresSink}: writes messages to a Postgres database
- {@link grepros.plugins.auto.sqlite.SqliteSink SqliteSink}: writes messages to an SQLite database
- {@link grepros.plugins.sql.SqlSink SqlSink}: writes SQL schema file for message type tables and topic views
- {@link grepros.outputs.TopicSink TopicSink}: publishes messages to ROS topics

{@link grepros.api.BaseBag Bag}: generic bag interface.
{@link grepros.search.Scanner Scanner}: ROS message grepper.

Format-specific bag classes:

- {@link grepros.ros1.ROS1Bag ROS1Bag}: ROS1 bag reader and writer in .bag format
- {@link grepros.ros2.ROS2Bag ROS2Bag}: ROS2 bag reader and writer in .db3 SQLite format
- {@link grepros.plugins.embag.EmbagReader EmbagReader}: ROS1 bag reader
  using the <a href="https://github.com/embarktrucks/embag">embag</a> library
- {@link grepros.plugins.mcap.McapBag McapBag}: ROS1/ROS2 bag reader and writer in MCAP format

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     09.12.2022
@modified    29.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.library
from . plugins.auto.csv      import CsvSink
from . plugins.auto.html     import HtmlSink
from . plugins.auto.postgres import PostgresSink
from . plugins.auto.sqlite   import SqliteSink
from . plugins.mcap          import McapBag, McapSink
from . plugins.parquet       import ParquetSink
from . plugins.sql           import SqlSink

from . api  import Bag
from . inputs  import AppSource, BagSource, Source, TopicSource
from . outputs import AppSink, BagSink, ConsoleSink, MultiSink, Sink, TopicSink
from . search  import Scanner
from . import api
from . import common
from . import main
from . import plugins


_inited = False


def grep(args=None, **kwargs):
    """
    Yields matching messages from specified source.

    Initializes grepros if not already initialized.

    Read from bagfiles: `grep(file="2022-10-*.bag", pattern="cpu")`.

    Read from live topics: `grep(live=True, pattern="cpu")`.


    @param   args                     arguments as namespace or dictionary, case-insensitive;
                                      or a single path as the ROS bagfile to read,
                                      or one or more {@link grepros.api.Bag Bag} instances
    @param   kwargs                   any and all arguments as keyword overrides, case-insensitive
    <!--sep-->

    Bag source:
    @param   args.file                names of ROS bagfiles to read if not all in directory
    @param   args.path                paths to scan if not current directory
    @param   args.recurse             recurse into subdirectories when looking for bagfiles
    @param   args.decompress          decompress archived bags to file directory
    @param   args.reindex             make a copy of unindexed bags and reindex them (ROS1 only)
    @param   args.orderby             "topic" or "type" if any to group results by
    @param   args.bag                 one or more {@link grepros.api.Bag Bag} instances
    <!--sep-->

    Live source:
    @param   args.live                whether reading messages from live ROS topics
    @param   args.queue_size_in       subscriber queue size (default 10)
    @param   args.ros_time_in         stamp messages with ROS time instead of wall time
    <!--sep-->

    App source:
    @param   args.app                 whether reading messages from iterable or pushed data;
                                      may contain the iterable itself
    @param   args.iterable            iterable yielding (topic, msg, stamp) or (topic, msg);
                                      yielding `None` signals end of content
    Any source:
    @param   args.topic               ROS topics to read if not all
    @param   args.type                ROS message types to read if not all
    @param   args.skip_topic          ROS topics to skip
    @param   args.skip_type           ROS message types to skip
    @param   args.start_time          earliest timestamp of messages to read
    @param   args.end_time            latest timestamp of messages to read
    @param   args.start_index         message index within topic to start from
    @param   args.end_index           message index within topic to stop at

    @param   args.nth_message         read every Nth message in topic
    @param   args.nth_interval        minimum time interval between messages in topic

    @param   args.select_field        message fields to use in matching if not all
    @param   args.noselect_field      message fields to skip in matching

    @param   args.unique              emit messages that are unique in topic
                                      (select_field and noselect_field apply if specified)
    @param   args.condition           Python expressions that must evaluate as true
                                      for message to be processable, see ConditionMixin
    <!--sep-->

    Search&zwj;:
    @param   args.pattern             pattern(s) to find in message field values
    @param   args.fixed_string        pattern contains ordinary strings, not regular expressions
    @param   args.case                use case-sensitive matching in pattern
    @param   args.invert              select messages not matching pattern

    @param   args.nth_match           emit every Nth match in topic
    @param   args.max_count           number of matched messages to emit (per file if bag input)
    @param   args.max_per_topic       number of matched messages to emit from each topic
    @param   args.max_topics          number of topics to print matches from

    @param   args.before              number of messages of leading context to emit before match
    @param   args.after               number of messages of trailing context to emit after match
    @param   args.context             number of messages of leading and trailing context
                                      to emit around match

    @param   args.highlight           highlight matched values
    @param   args.match_wrapper       string to wrap around matched values,
                                      both sides if one value, start and end if more than one,
                                      or no wrapping if zero values

    @return  {@link grepros.Scanner.GrepMessage GrepMessage} namedtuples
             of (topic, message, timestamp, match, index)
    """
    DEFAULT_ARGS = dict(FILE=[], LIVE=False, APP=False, ITERABLE=None,
                        COLOR="never", HIGHLIGHT=False)

    args0 = args
    is_bag = isinstance(args, Bag) or \
             common.is_iterable(args) and all(isinstance(x, Bag) for x in args)
    args = {"FILE": str(args)} if isinstance(args, common.PATH_TYPES) else \
           {} if is_bag or isinstance(args, Source) else args
    args = common.ensure_namespace(args, DEFAULT_ARGS, **kwargs)
    main.validate_args(main.process_args(args))
    if not _inited: init(args)

    if common.is_iterable(args.APP) and not common.is_iterable(args.ITERABLE):
        args.APP, args.ITERABLE = True, args.APP
    src = args0 if isinstance(args0, Source) else \
          TopicSource(args) if args.LIVE else \
          AppSource(args) if args.APP else \
          BagSource(args0, **vars(args)) if is_bag else BagSource(args)
    src.validate()

    try:
        for x in Scanner(args).find(src): yield x
    finally:
        if not isinstance(args0, (Bag, Source)): src.close()


def source(args=None, **kwargs):
    """
    Convenience for creating a {@link grepros.inputs.Source Source} instance from arguments.

    Initializes grepros if not already initialized.

    @param   args                  arguments as namespace or dictionary, case-insensitive;
                                   or a single path as the ROS bagfile to read
    @param   kwargs                any and all arguments as keyword overrides, case-insensitive
    @param   args.file             one or more names of ROS bagfiles to read from
    @param   args.live             read messages from live ROS topics instead
    @param   args.app              read messages from iterable or pushed data instead;
                                   may contain the iterable itself
    <!--sep-->

    Bag source:
    @param   args.file             names of ROS bagfiles to read if not all in directory
    @param   args.path             paths to scan if not current directory
    @param   args.recurse          recurse into subdirectories when looking for bagfiles
    @param   args.orderby          "topic" or "type" if any to group results by
    @param   args.decompress       decompress archived bags to file directory
    @param   args.reindex          make a copy of unindexed bags and reindex them (ROS1 only)
    @param   args.progress         whether to print progress bar
    <!--sep-->

    Live source:
    @param   args.queue_size_in    subscriber queue size (default 10)
    @param   args.ros_time_in      stamp messages with ROS time instead of wall time
    @param   args.progress         whether to print progress bar
    <!--sep-->

    App source:
    @param   args.iterable         iterable yielding (topic, msg, stamp) or (topic, msg);
                                   yielding `None` signals end of content
    <!--sep-->

    Any source:
    @param   args.topic            ROS topics to read if not all
    @param   args.type             ROS message types to read if not all
    @param   args.skip_topic       ROS topics to skip
    @param   args.skip_type        ROS message types to skip
    @param   args.start_time       earliest timestamp of messages to read
    @param   args.end_time         latest timestamp of messages to read
    @param   args.start_index      message index within topic to start from
    @param   args.end_index        message index within topic to stop at
    @param   args.unique           emit messages that are unique in topic
    @param   args.select_field     message fields to use for uniqueness if not all
    @param   args.noselect_field   message fields to skip for uniqueness
    @param   args.nth_message      read every Nth message in topic
    @param   args.nth_interval     minimum time interval between messages in topic
    @param   args.condition        Python expressions that must evaluate as true
                                   for message to be processable, see ConditionMixin
    """
    DEFAULT_ARGS = dict(FILE=[], LIVE=False, APP=False, ITERABLE=None)
    args = {"FILE": str(args)} if isinstance(args, common.PATH_TYPES) else args
    args = common.ensure_namespace(args, DEFAULT_ARGS, **kwargs)
    if not _inited: init(args)

    if common.is_iterable(args.APP) and not common.is_iterable(args.ITERABLE):
        args.APP, args.ITERABLE = True, args.APP
    result = (TopicSource if args.LIVE else AppSource if args.APP else BagSource)(args)
    result.validate()
    return result


def sink(args=None, **kwargs):
    """
    Convenience for creating a {@link grepros.outputs.Sink Sink} instance from arguments,
    {@link grepros.outputs.MultiSink MultiSink} if several outputs.

    Initializes grepros if not already initialized.

    @param   args                       arguments as namespace or dictionary, case-insensitive;
                                        or a single item as sink target like bag filename
    @param   kwargs                     any and all arguments as keyword overrides, case-insensitive
    @param   args.app                   provide messages to given callback function
    @param   args.console               print matches to console
    @param   args.publish               publish matches to live topics
    @param   args.write                 file or other target like Postgres database to write,
                                        as "target", or ["target", dict(format="format", ..)]
                                        or [[..target1..], [..target2..], ..]
    @param   args.write_options         format-specific options like
                                        {"overwrite": whether to overwrite existing file
                                                      (default false)}
    <!--sep-->

    Console sink:
    @param   args.line_prefix           print source prefix like bag filename on each message line
    @param   args.max_field_lines       maximum number of lines to print per field
    @param   args.start_line            message line number to start output from
    @param   args.end_line              message line number to stop output at
    @param   args.max_message_lines     maximum number of lines to output per message
    @param   args.lines_around_match    number of message lines around matched fields to output
    @param   args.matched_fields_only   output only the fields where match was found
    @param   args.wrap_width            character width to wrap message YAML output at
    @param   args.match_wrapper         string to wrap around matched values,
                                        both sides if one value, start and end if more than one,
                                        or no wrapping if zero values
    <!--sep-->

    Console / HTML sink:
    @param   args.color                 False or "never" for not using colors in replacements
    @param   args.highlight             highlight matched values (default true)
    @param   args.emit_field            message fields to emit if not all
    @param   args.noemit_field          message fields to skip in output
    @param   args.max_field_lines       maximum number of lines to output per field
    @param   args.start_line            message line number to start output from
    @param   args.end_line              message line number to stop output at
    @param   args.max_message_lines     maximum number of lines to output per message
    @param   args.lines_around_match    number of message lines around matched fields to output
    @param   args.matched_fields_only   output only the fields where match was found
    @param   args.wrap_width            character width to wrap message YAML output at
    @param   args.match_wrapper         string to wrap around matched values,
                                        both sides if one value, start and end if more than one,
                                        or no wrapping if zero values
    <!--sep-->

    Topic sink:
    @param   args.queue_size_out        publisher queue size (default 10)
    @param   args.publish_prefix        output topic prefix, prepended to input topic
    @param   args.publish_suffix        output topic suffix, appended to output topic
    @param   args.publish_fixname       single output topic name to publish to,
                                        overrides prefix and suffix if given
    <!--sep-->

    App sink:
    @param   args.emit                  callback(topic, msg, stamp, highlighted msg, index in topic)
                                        if any
    @param   args.metaemit              callback(metadata dict) if any,
                                        invoked before first emit from source batch
    <!--sep-->

    Any sink:
    @param   args.meta                  whether to print metainfo
    @param   args.verbose               whether to print debug information
    """
    DEFAULT_ARGS = dict(CONSOLE=False, PUBLISH=False, WRITE=[], APP=False, EMIT=None, METAEMIT=None)

    result = None
    args = {"WRITE": str(args)} if isinstance(args, common.PATH_TYPES) else args
    args = common.ensure_namespace(args, DEFAULT_ARGS, **kwargs)
    if not _inited: init(args)

    if args.WRITE:
        if isinstance(args.WRITE, common.PATH_TYPES):
            args.WRITE = [[args.WRITE]]  # Nest deeper, single file given
        elif isinstance(args.WRITE, (list, tuple)) and isinstance(args.WRITE[0], common.PATH_TYPES):
            args.WRITE = [args.WRITE]  # Nest deeper, must have been single [target, ..opts]
    if callable(args.APP) and not callable(args.EMIT): args.APP, args.EMIT = True, args.APP

    multisink = MultiSink(args)
    multisink.validate()
    result = multisink.sinks[0] if len(multisink.sinks) == 1 else multisink
    return result


def init(args=None, **kwargs):
    """
    Initializes ROS version bindings, loads all built-in plugins if dependencies available.

    @param   args
    @param   args.plugin   one or more extra plugins to load,
                           as full names or instances of Python module/class
    @param   kwargs        any and all arguments as keyword overrides, case-insensitive
    """
    global _inited
    args = common.ensure_namespace(args, {"PLUGIN": []}, **kwargs)
    if _inited:
        if args: plugins.configure(args)
        return

    common.ConsolePrinter.configure(color=None, apimode=True)
    api.validate()
    plugins.init(args)
    for x in (plugins.mcap, plugins.parquet, plugins.sql):
        try: plugins.configure(PLUGIN=x)
        except Exception: pass
    Bag.READER_CLASSES.add(McapBag)  # Ensure MCAP files at least get recognized,
    Bag.WRITER_CLASSES.add(McapBag)  # even if loading them will fail when dependencies missing
    if args.PLUGIN: plugins.configure(args)
    # Switch message metadata cache to constrain on total number instead of time
    api.TypeMeta.LIFETIME, api.TypeMeta.POPULATION = 0, 100
    _inited = True



__all__ = [
    "AppSink", "AppSource", "Bag", "BagSink", "BagSource", "ConsoleSink", "CsvSink", "HtmlSink",
    "McapBag", "McapSink", "MultiSink", "ParquetSink", "PostgresSink", "Scanner", "Sink", "Source",
    "SqliteSink", "SqlSink", "TopicSink", "TopicSource",
    "grep", "init", "sink", "source",
]

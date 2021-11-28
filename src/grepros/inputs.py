# -*- coding: utf-8 -*-
"""
Input sources for search content.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    28.11.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.inputs
from __future__ import print_function
import copy
import collections
import datetime
import functools
import os
try: import queue  # Py3
except ImportError: import Queue as queue  # Py2
import threading
import time

from . common import ConsolePrinter, ProgressBar, drop_zeros, filter_dict, find_files, \
                     format_bytes, format_stamp, format_timedelta, plural
from . import rosapi


class SourceBase(object):
    """Message producer base class."""

    ## Template for message metainfo line
    MESSAGE_META_TEMPLATE = "{topic} {index} ({type}  {dt}  {stamp})"

    def __init__(self, args):
        """
        @param   args              arguments object like argparse.Namespace
        @param   args.START_TIME   earliest timestamp of messages to scan
        @param   args.END_TIME     latest timestamp of messages to scan
        """
        self._args = copy.deepcopy(args)
        # {topic: ["pkg/MsgType", ]} searched in current source
        self._topics   = collections.defaultdict(list)

        ## outputs.SinkBase instance bound to this source
        self.sink = None
        ## All topics in source, as {(topic, "pkg/MsgType"): total message count or None}
        self.topics = {}
        ## ProgressBar instance, if any
        self.bar = None

    def read(self):
        """Yields messages from source, as (topic, msg, ROS time)."""

    def bind(self, sink):
        """Attaches sink to source"""
        self.sink = sink

    def validate(self):
        """Returns whether source prerequisites are met (like ROS environment for TopicSource)."""
        return True

    def close(self):
        """Shuts down input, closing any files or connections."""
        self.topics.clear()
        self._topics.clear()
        if self.bar:
            self.bar.pulse_pos = None
            self.bar.update(flush=True)
            self.bar, _ = None, self.bar.stop()

    def close_batch(self):
        """Shuts down input batch if any (like bagfile), else all input."""
        self.close()

    def format_meta(self):
        """Returns source metainfo string."""
        return ""

    def format_message_meta(self, topic, index, stamp, msg):
        """Returns message metainfo string."""
        return self.MESSAGE_META_TEMPLATE.format(**self.get_message_meta(topic, index, stamp, msg))

    def get_batch(self):
        """Returns source batch identifier if any (like bagfile name if BagSource)."""

    def get_meta(self):
        """Returns source metainfo data dict."""
        return {}

    def get_message_meta(self, topic, index, stamp, msg):
        """Returns message metainfo data dict."""
        return dict(topic=topic, type=rosapi.get_message_type(msg), index=index,
                    dt=drop_zeros(format_stamp(rosapi.to_sec(stamp)), " "),
                    stamp=drop_zeros(rosapi.to_sec(stamp)),
                    schema=rosapi.get_message_definition(msg))

    def get_message_class(self, typename):
        """Returns message type class."""
        return rosapi.get_message_class(typename)

    def get_message_definition(self, msg_or_type):
        """Returns ROS message type definition full text, including subtype definitions."""
        return rosapi.get_message_definition(msg_or_type)

    def is_processable(self, topic, index, stamp, msg):
        """Returns whether specified message in topic is in acceptable time range."""
        if self._args.START_TIME and stamp < self._args.START_TIME:
            return False
        if self._args.END_TIME and stamp > self._args.END_TIME:
            return False
        return True

    def notify(self, status):
        """Reports match status of last produced message."""

    def thread_excepthook(self, exc):
        """Handles exception, used by background threads."""
        ConsolePrinter.error(exc)


class BagSource(SourceBase):
    """Produces messages from ROS bagfiles."""

    ## Template for message metainfo line
    MESSAGE_META_TEMPLATE = "{topic} {index}/{total} ({type}  {dt}  {stamp})"
    ## Template for bag metainfo header
    META_TEMPLATE         = "\nFile {file} ({size}), {tcount} topics, {mcount:,d} messages\n" \
                            "File period {startdt} - {enddt}\n" \
                            "File span {delta} ({start} - {end})"

    def __init__(self, args):
        """
        @param   args               arguments object like argparse.Namespace
        @param   args.FILES         names of ROS bagfiles to scan if not all in directory
        @param   args.PATHS         paths to scan if not current directory
        @param   args.RECURSE       recurse into subdirectories when looking for bagfiles
        @param   args.TOPICS        ROS topics to scan if not all
        @param   args.TYPES         ROS message types to scan if not all
        @param   args.SKIP_TOPICS   ROS topics to skip
        @param   args.SKIP_TYPES    ROS message types to skip
        @param   args.START_TIME    earliest timestamp of messages to scan
        @param   args.END_TIME      latest timestamp of messages to scan
        @param   args.START_INDEX   message index within topic to start from
        @param   args.END_INDEX     message index within topic to stop at
        @param   args.AFTER         emit NUM messages of trailing context after match
        @param   args.ORDERBY       "topic" or "type" if any to group results by
        @param   args.OUTFILE       output bagfile, to skip in input files
        @param   args.PROGRESS      whether to print progress bar
        """
        super(BagSource, self).__init__(args)
        self._args0     = copy.deepcopy(args)  # Original arguments
        self._status    = None  # Match status of last produced message
        self._sticky    = False  # Scanning a single topic until all after-context emitted
        self._totals_ok = False  # Whether message count totals have been retrieved
        self._running   = False
        self._counts    = collections.Counter()  # {(topic, type): count processed}
        self._bag       = None   # Current bag object instance
        self._filename  = None   # Current bagfile path
        self._meta      = None   # Cached get_meta()

    def read(self):
        """Yields messages from ROS bagfiles, as (topic, msg, ROS time)."""
        self._running = True
        names, paths = self._args.FILES, self._args.PATHS
        exts, skip_exts = rosapi.BAG_EXTENSIONS, rosapi.SKIP_EXTENSIONS
        for filename in find_files(names, paths, exts, skip_exts, recurse=self._args.RECURSE):
            if not self._running or not self._configure(filename) or not self._topics:
                continue  # for filename

            topicsets = [self._topics]
            if "topic" == self._args.ORDERBY:  # Group output by sorted topic names
                topicsets = [{n: tt} for n, tt in sorted(self._topics.items())]
            elif "type" == self._args.ORDERBY:  # Group output by sorted type names
                typetopics = {}
                for n, tt in self._topics.items():
                    for t in tt: typetopics.setdefault(t, []).append(n)
                topicsets = [{n: [t] for n in nn} for t, nn in sorted(typetopics.items())]

            self._init_progress()
            for topics in topicsets:
                for topic, msg, stamp in self._produce(topics):
                    yield topic, msg, stamp
                if not self._running:
                    break  # for topics
        self._running = False

    def validate(self):
        """Returns whether ROS environment is set, prints error if not."""
        return rosapi.validate()

    def close(self):
        """Closes current bag, if any."""
        self._running = False
        self._bag and self._bag.close()
        super(BagSource, self).close()

    def close_batch(self):
        """Closes current bag, if any, and moves reading to the next one, if any."""
        self._bag and self._bag.close()
        self._bag = None

    def format_meta(self):
        """Returns bagfile metainfo string."""
        return self.META_TEMPLATE.format(**self.get_meta())

    def format_message_meta(self, topic, index, stamp, msg):
        """Returns message metainfo string."""
        return self.MESSAGE_META_TEMPLATE.format(**self.get_message_meta(topic, index, stamp, msg))

    def get_batch(self):
        """Returns name of current bagfile."""
        return self._filename

    def get_meta(self):
        """Returns bagfile metainfo data dict."""
        if self._meta is not None:
            return self._meta
        start, end = self._bag.get_start_time(), self._bag.get_end_time()
        self._meta = dict(file=self._filename, size=format_bytes(self._bag.size),
                          mcount=self._bag.get_message_count(), tcount=len(self.topics),
                          start=drop_zeros(start), startdt=drop_zeros(format_stamp(start)),
                          end=drop_zeros(end), enddt=drop_zeros(format_stamp(end)),
                          delta=format_timedelta(datetime.timedelta(seconds=end - start)))
        return self._meta

    def get_message_meta(self, topic, index, stamp, msg):
        """Returns message metainfo data dict."""
        self._ensure_totals()
        msgtype = rosapi.get_message_type(msg)
        return dict(topic=topic, type=msgtype, total=self.topics[(topic, msgtype)],
                    dt=drop_zeros(format_stamp(rosapi.to_sec(stamp)), " "),
                    stamp=drop_zeros(rosapi.to_sec(stamp)), index=index,
                    schema=self.get_message_definition(msg))

    def get_message_class(self, typename):
        """Returns ROS message type class."""
        return self._bag.get_message_class(typename) or \
               rosapi.get_message_class(typename)

    def get_message_definition(self, msg_or_type):
        """Returns ROS message type definition full text, including subtype definitions."""
        return self._bag.get_message_definition(msg_or_type) or \
               rosapi.get_message_definition(msg_or_type)

    def notify(self, status):
        """Reports match status of last produced message."""
        self._status = bool(status)
        if status and not self._totals_ok:
            self._ensure_totals()

    def is_processable(self, topic, index, stamp, msg):
        """Returns whether specified message in topic is in acceptable range."""
        topickey = (topic, rosapi.get_message_type(msg))
        if self._args.START_INDEX:
            self._ensure_totals()
            START = self._args.START_INDEX
            MIN = max(0, START + (self.topics[topickey] if START < 0 else 0))
            if MIN >= index:
                return False
        if self._args.END_INDEX:
            self._ensure_totals()
            END = self._args.END_INDEX
            MAX = END + (self.topics[topickey] if END < 0 else 0)
            if MAX < index:
                return False
        return super(BagSource, self).is_processable(topic, index, stamp, msg)

    def _produce(self, topics, start_time=None):
        """Yields messages from current ROS bagfile, as (topic, msg, ROS time)."""
        counts = collections.Counter()
        for topic, msg, stamp in self._bag.read_messages(list(topics), start_time):
            if not self._running or not self._bag:
                break  # for topic
            typename = rosapi.get_message_type(msg)
            if typename not in topics[topic]:
                continue  # for topic

            topickey = (topic, typename)
            counts[topickey] += 1; self._counts[topickey] += 1
            # Skip messages already processed during sticky
            if not self._sticky and counts[topickey] != self._counts[topickey]:
                continue  # for topic

            self._status = None
            self.bar and self.bar.update(value=sum(self._counts.values()))
            yield topic, msg, stamp

            if self._status and self._args.AFTER and not self._sticky \
            and (len(self._topics) > 1 or len(next(iter(self._topics.values()))) > 1):
                # Stick to one topic until trailing messages have been emitted
                self._sticky = True
                continue_from = stamp + rosapi.make_duration(nsecs=1)
                for entry in self._produce({topic: typename}, continue_from):
                    yield entry
                self._sticky = False

    def _init_progress(self):
        """Initializes progress bar, if any, for current bag."""
        if self._args.PROGRESS and not self.bar:
            self._ensure_totals()
            self.bar = ProgressBar(aftertemplate=" {afterword} ({value:,d}/{max:,d})")
            self.bar.afterword = os.path.basename(self._filename)
            self.bar.max = sum(self.topics[(t, d)] for t, dd in self._topics.items() for d in dd)
            self.bar.update(value=0)

    def _ensure_totals(self):
        """Retrieves total message counts if not retrieved."""
        if not self._totals_ok:  # Must be ros2.Bag
            for k, v in self._bag.get_type_and_topic_info(counts=True).topics.items():
                self.topics[(k, v.msg_type)] = v.message_count
            self._totals_ok = True

    def _configure(self, filename):
        """Opens bag and populates bag-specific argument state, returns success."""
        self._meta      = None
        self._bag       = None
        self._filename  = None
        self._sticky    = False
        self._totals_ok = False
        self._counts.clear()
        self.topics.clear()
        if self._args.OUTFILE and os.path.realpath(self._args.OUTFILE) == os.path.realpath(filename):
            return False
        try:
            bag = rosapi.create_bag_reader(filename)
        except Exception as e:
            ConsolePrinter.error("\nError opening %r: %s", filename, e)
            return False

        self._bag      = bag
        self._filename = filename

        dct = {}  # {topic: [typename, ]}
        for k, v in bag.get_type_and_topic_info().topics.items():
            dct.setdefault(k, []).append(v.msg_type)
            self.topics[(k, v.msg_type)] = v.message_count
        self._totals_ok = not any(v is None for v in self.topics.values())

        dct = filter_dict(dct, self._args.TOPICS, self._args.TYPES)
        dct = filter_dict(dct, self._args.SKIP_TOPICS, self._args.SKIP_TYPES, reverse=True)
        self._topics = dct
        self._meta   = self.get_meta()

        args = self._args = copy.deepcopy(self._args0)
        if args.START_TIME is not None:
            args.START_TIME = rosapi.make_bag_time(args.START_TIME, bag)
        if args.END_TIME is not None:
            args.END_TIME = rosapi.make_bag_time(args.END_TIME, bag)
        return True


class TopicSource(SourceBase):
    """Produces messages from live ROS topics."""

    ## Seconds between refreshing available topics from ROS master.
    MASTER_INTERVAL = 2

    def __init__(self, args):
        """
        @param   args                 arguments object like argparse.Namespace
        @param   args.TOPICS          ROS topics to scan if not all
        @param   args.TYPES           ROS message types to scan if not all
        @param   args.SKIP_TOPICS     ROS topics to skip
        @param   args.SKIP_TYPES      ROS message types to skip
        @param   args.START_TIME      earliest timestamp of messages to scan
        @param   args.END_TIME        latest timestamp of messages to scan
        @param   args.START_INDEX     message index within topic to start from
        @param   args.END_INDEX       message index within topic to stop at
        @param   args.QUEUE_SIZE_IN   subscriber queue size
        @param   args.ROS_TIME_IN     stamp messages with ROS time instead of wall time
        @param   args.PROGRESS        whether to print progress bar
        """
        super(TopicSource, self).__init__(args)
        self._running = False  # Whether is in process of yielding messages from topics
        self._queue   = None   # [(topic, msg, ROS time)]
        self._subs    = {}     # {(topic, type): ROS subscriber}

        self._configure()

    def read(self):
        """Yields messages from subscribed ROS topics, as (topic, msg, ROS time)."""
        if not self._running:
            self._running = True
            self._queue = queue.Queue()
            self.refresh_topics()
            t = threading.Thread(target=self._run_refresh)
            t.daemon = True
            t.start()

        total = 0
        self._init_progress()
        while self._running:
            topic, msg, stamp = self._queue.get()
            total += bool(topic)
            self._update_progress(total, self._running and bool(topic))
            if topic:
                yield topic, msg, stamp
        self._queue = None
        self._running = False

    def bind(self, sink):
        """Attaches sink to source and blocks until connected to ROS live."""
        SourceBase.bind(self, sink)
        rosapi.init_node()

    def validate(self):
        """Returns whether ROS environment is set, prints error if not."""
        return rosapi.validate(live=True)

    def close(self):
        """Shuts down subscribers and stops producing messages."""
        self._running = False
        for k in list(self._subs):
            self._subs.pop(k).unregister()
        self._queue and self._queue.put((None, None, None))  # Wake up iterator
        self._queue = None
        super(TopicSource, self).close()
        rosapi.shutdown_node()

    def get_meta(self):
        """Returns source metainfo data dict."""
        ENV = {k: os.getenv(k) for k in ("ROS_MASTER_URI", "ROS_DOMAIN_ID") if os.getenv(k)}
        return dict(ENV, tcount=len(self.topics))

    def format_meta(self):
        """Returns source metainfo string."""
        metadata = self.get_meta()
        result = "\nROS%s live" % os.getenv("ROS_VERSION")
        if "ROS_MASTER_URI" in metadata:
            result += ", ROS master %s" % metadata["ROS_MASTER_URI"]
        if "ROS_DOMAIN_ID" in metadata:
            result += ", ROS domain ID %s" % metadata["ROS_DOMAIN_ID"]
        result += ", %s initially" % plural("topic", metadata["tcount"])
        return result

    def is_processable(self, topic, index, stamp, msg):
        """Returns whether specified message in topic is in acceptable range."""
        if self._args.START_INDEX:
            if max(0, self._args.START_INDEX) >= index:
                return False
        if self._args.END_INDEX:
            if 0 < self._args.END_INDEX < index:
                return False
        return super(TopicSource, self).is_processable(topic, index, stamp, msg)

    def refresh_topics(self):
        """Refreshes topics and subscriptions from ROS live."""
        for topic, typename in rosapi.get_topic_types():
            topickey = (topic, typename)
            if topickey in self.topics:
                continue  # for topic
            dct = filter_dict({topic: [typename]}, self._args.TOPICS, self._args.TYPES)
            dct = filter_dict(dct, self._args.SKIP_TOPICS, self._args.SKIP_TYPES, reverse=True)
            if dct:
                handler = functools.partial(self._on_message, topic)
                sub = rosapi.create_subscriber(topic, typename, handler,
                                               queue_size=self._args.QUEUE_SIZE_IN)
                self._subs[topickey] = sub
            self.topics[topickey] = None

    def _init_progress(self):
        """Initializes progress bar, if any."""
        if self._args.PROGRESS and not self.bar:
            self.bar = ProgressBar(afterword="ROS%s live" % os.getenv("ROS_VERSION"),
                                   aftertemplate=" {afterword} ({value:,d})", pulse=True)
            self.bar.start()

    def _update_progress(self, count, running=True):
        """Updates progress bar, if any."""
        if self.bar:
            afterword = "ROS%s live, %s" % (os.getenv("ROS_VERSION"), plural("message", count))
            self.bar.afterword, self.bar.max = afterword, count
            if not running:
                self.bar.pause, self.bar.pulse_pos = True, None
            self.bar.update(count)

    def _configure(self):
        """Adjusts start/end time filter values to current time."""
        if self._args.START_TIME is not None:
            self._args.START_TIME = rosapi.make_live_time(self._args.START_TIME)
        if self._args.END_TIME is not None:
            self._args.END_TIME = rosapi.make_live_time(self._args.END_TIME)

    def _run_refresh(self):
        """Periodically refreshes topics and subscriptions from ROS live."""
        time.sleep(self.MASTER_INTERVAL)
        while self._running:
            try: self.refresh_topics()
            except Exception as e: self.thread_excepthook(e)
            time.sleep(self.MASTER_INTERVAL)

    def _on_message(self, topic, msg):
        """Subscription callback handler, queues message for yielding."""
        stamp = rosapi.get_rostime() if self._args.ROS_TIME_IN else \
                rosapi.make_time(time.time())
        self._queue and self._queue.put((topic, msg, stamp))

# -*- coding: utf-8 -*-
"""
Input sources for grep.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS message content.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    23.10.2021
------------------------------------------------------------------------------
"""
import copy
import collections
import datetime

import rosbag
import rospy

from . common import ConsolePrinter, filter_dict, find_files, format_bytes, \
                     format_stamp, format_timedelta, make_bag_time


class SourceBase:
    """Producer base class."""

    def __init__(self, args):
        self._args = copy.deepcopy(args)
        self._patterns = {}    # {key: [re.Pattern, ]}
        self._msgtypes = {}    # {topic: "pkg/MsgType"} in source
        self._topics   = []    # [topics searched in current source]

        self.sink = None

    def __iter__(self):
        """Yields messages from source, as (topic, msg, rospy.Time)."""

    def bind(self, sink):
        """Attaches sink to source"""
        self.sink = sink

    def close(self):
        """Shuts down input, closing any files or connections."""

    def get_message_meta(self, topic, index, stamp, msg):
        """Returns message metainfo dict, for console output."""
        return dict(topic=topic, type=self._msgtypes[topic], stamp=format_stamp(stamp), index=index)

    def is_processable(self, topic, index):
        """Returns whether current message in topic is in acceptable index range."""
        return True

    def notify(self, status):
        """Reports match status of last produced message."""


class BagSource(SourceBase):
    """Produces messages from ROS bagfiles."""

    BAG_EXTENSIONS  = (".bag", ".bag.active")
    SKIP_EXTENSIONS = (".bag.orig.active", )

    def __init__(self, args):
        super().__init__(args)
        self._args0     = copy.deepcopy(args)  # Original arguments
        self._status    = None  # Match status of last produced message
        self._sticky    = False  # Scanning a single topic until all after-context emitted
        self._running   = False
        self._counts    = collections.defaultdict(int)  # {topic: count processed}
        self._msgtypes  = {}  # {topic: typename}
        self._msgtotals = {}  # {topic: total count in bag}

        self.sink     = None   # outputs.SinkBase child instance
        self.bag      = None   # Current rosbag.Bag instance
        self.filename = None   # Current bagfile path

    def __iter__(self):
        """Yields messages from ROS bagfiles, as (topic, msg, rospy.Time)."""
        self._running = True
        files, paths = self._args.FILES, self._args.PATHS
        exts, skip_exts = self.BAG_EXTENSIONS, self.SKIP_EXTENSIONS
        for filename in find_files(files, paths, exts, skip_exts, recurse=self._args.RECURSE):
            if not self._configure(filename) or not self._topics:
                continue  # for filename
            yield from self._produce(self._topics, self._args.START_TIME, self._args.END_TIME)
            if not self._running:
                break  # for filename
        self._running = False

    def _produce(self, topics, start_time=None, end_time=None):
        """Yields messages from current ROS bagfile, as (topic, msg, rospy.Time)."""
        counts = collections.defaultdict(int)
        for topic, msg, stamp in self.bag.read_messages(topics, start_time, end_time):
            if not self._running:
                break  # for topic

            counts[topic], self._counts[topic] = counts[topic] + 1, self._counts[topic] + 1
            # Skip messages already processed during sticky
            if not self._sticky and counts[topic] != self._counts[topic]:
                continue  # for topic

            self._status = None
            yield topic, msg, stamp

            if self._status and self._args.AFTER and not self._sticky and len(self._topics) > 1:
                # Stick to one topic until trailing messages have been emitted
                self._sticky = True
                yield from self._produce([topic], stamp + rospy.Duration(nsecs=1), end_time)
                self._sticky = False

    def close(self):
        """Closes current bag, if any."""
        self._running = False
        self.bag and self.bag.close()

    def get_meta(self):
        """Returns bagfile metainfo dict, for console output."""
        start, end = self.bag.get_start_time(), self.bag.get_end_time()
        return dict(file=self.filename, size=format_bytes(self.bag.size),
                    mcount=self.bag.get_message_count(), tcount=len(self._msgtypes),
                    start=format_stamp(start), end=format_stamp(end),
                    delta=format_timedelta(datetime.timedelta(seconds=end - start)))

    def get_message_meta(self, topic, index, stamp, msg):
        """Returns message metainfo dict, for console output."""
        return dict(topic=topic, type=self._msgtypes[topic], stamp=format_stamp(stamp),
                    index=index, total=self._msgtotals[topic])

    def notify(self, status):
        """Reports match status of last produced message."""
        self._status = bool(status)

    def is_processable(self, topic, index):
        """Returns whether current message in topic is in acceptable index range."""
        if self._args.START_INDEX:
            START = self._args.START_INDEX
            MIN = max(0, START + (self._msgtotals[topic] if START < 0 else 0))
            if MIN >= index:
                return False
        if self._args.END_INDEX:
            END = self._args.END_INDEX
            MAX = END + (self._msgtotals[topic] if END < 0 else 0)
            if MAX < index:
                return False
        return True

    def _configure(self, filename):
        """Opens bag and populates bag-specific argument state, returns success."""
        try:
            bag = rosbag.Bag(filename, skip_index=True)
        except Exception as e:
            ConsolePrinter.error("\nError opening %r: %s", filename, e)
            return False

        self.bag      = bag
        self.filename = filename
        self._sticky  = False
        self._counts.clear()

        topicdata = bag.get_type_and_topic_info().topics
        self._msgtypes  = {k: v.msg_type      for k, v in topicdata.items()}
        self._msgtotals = {k: v.message_count for k, v in topicdata.items()}

        dct = filter_dict(self._msgtypes, self._args.TOPICS, self._args.TYPES)
        dct = filter_dict(dct, self._args.SKIP_TOPICS, self._args.SKIP_TYPES, reverse=True)
        self._topics = list(dct)

        args = self._args = copy.deepcopy(self._args0)
        if args.START_TIME is not None:
            args.START_TIME = make_bag_time(args.START_TIME, bag)
        if args.END_TIME is not None:
            args.END_TIME = make_bag_time(args.END_TIME, bag)
        return True

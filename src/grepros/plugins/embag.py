# -*- coding: utf-8 -*-
"""
ROS1 bag reader plugin using the `embag` library.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     19.11.2021
@modified    26.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.embag
from __future__ import absolute_import
import os

try: import embag
except ImportError: embag = None
try: import genpy
except ImportError: genpy = None

from .. import api
from .. common import PATH_TYPES, ConsolePrinter



class EmbagReader(api.BaseBag):
    """embag reader interface, providing most of rosbag.Bag interface."""

    ## Supported opening modes
    MODES = ("r", )

    ## Whether bag supports reading or writing stream objects, overridden in subclasses
    STREAMABLE = False

    ## ROS1 bag file header magic start bytes
    ROSBAG_MAGIC = b"#ROSBAG"

    def __init__(self, filename, mode="r", **__):
        if not isinstance(filename, PATH_TYPES):
            raise ValueError("invalid filename %r" % type(filename))
        if mode not in self.MODES: raise ValueError("invalid mode %r" % mode)

        self._topics   = {}    # {(topic, typename, typehash): message count}
        self._types    = {}    # {(typename, typehash): message type class}
        self._hashdefs = {}    # {(topic, typehash): typename}
        self._typedefs = {}    # {(typename, typehash): type definition text}
        self._iterer   = None  # Generator from read_messages() for next()
        self._ttinfo   = None  # Cached result for get_type_and_topic_info()
        self._view     = embag.View(filename)
        self._filename = str(filename)

        self._populate_meta()


    def get_message_count(self, topic_filters=None):
        """
        Returns the number of messages in the bag.

        @param   topic_filters  list of topics or a single topic to filter by, if any
        """
        if topic_filters:
            topics = topic_filters
            topics = topics if isinstance(topics, (dict, list, set, tuple)) else [topics]
            return sum(c for (t, _, _), c in self._topics.items() if t in topics)
        return sum(self._topics.values())


    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp, or None if bag empty."""
        if self.closed or not self._topics: return None
        return self._view.getStartTime().to_sec()


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp, or None if bag empty."""
        if self.closed or not self._topics: return None
        return self._view.getEndTime().to_sec()


    def get_message_class(self, typename, typehash=None):
        """
        Returns rospy message class for typename, or None if unknown message type for bag.

        Generates class dynamically if not already generated.

        @param   typehash  message type definition hash, if any
        """
        typekey = (typename, typehash or next((h for n, h in self._types if n == typename), None))
        if typekey not in self._types and typekey in self._typedefs:
            for n, c in genpy.dynamic.generate_dynamic(typename, self._typedefs[typekey]).items():
                self._types[(n, c._md5sum)] = c
        return self._types.get(typekey)


    def get_message_definition(self, msg_or_type):
        """
        Returns ROS1 message type definition full text from bag, including subtype definitions.

        Returns None if unknown message type for bag.
        """
        if api.is_ros_message(msg_or_type):
            return self._typedefs.get((msg_or_type._type, msg_or_type._md5sum))
        typename = msg_or_type
        return next((d for (n, h), d in self._typedefs.items() if n == typename), None)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS1 message type MD5 hash, or None if unknown message type for bag."""
        if api.is_ros_message(msg_or_type): return msg_or_type._md5sum
        typename = msg_or_type
        return next((h for n, h in self._typedefs if n == typename), None)


    def get_topic_info(self, *_, **__):
        """Returns topic and message type metainfo as {(topic, typename, typehash): count}."""
        return dict(self._topics)


    def get_type_and_topic_info(self, topic_filters=None):
        """
        Returns thorough metainfo on topic and message types.

        @param   topic_filters  list of topics or a single topic to filter returned topics-dict by,
                                if any
        @return                 TypesAndTopicsTuple(msg_types, topics) namedtuple,
                                msg_types as dict of {typename: typehash},
                                topics as a dict of {topic: TopicTuple() namedtuple}.
        """
        topics = topic_filters
        topics = topics if isinstance(topics, (list, set, tuple)) else [topics] if topics else []
        if self._ttinfo and (not topics or set(topics) == set(t for t, _, _ in self._topics)):
            return self._ttinfo
        if self.closed: raise ValueError("I/O operation on closed file.")

        msgtypes = {n: h for t, n, h in self._topics}
        topicdict = {}

        def median(vals):
            """Returns median value from given sorted numbers."""
            vlen = len(vals)
            return None if not vlen else vals[vlen // 2] if vlen % 2 else \
                   float(vals[vlen // 2 - 1] + vals[vlen // 2]) / 2

        conns = self._view.connectionsByTopic()  # {topic: [embag.Connection, ]}
        for (t, n, _), c in sorted(self._topics.items()):
            if topics and t not in topics: continue  # for
            mymedian = None
            if c > 1:
                stamps = sorted(m.timestamp.secs + m.timestamp.nsecs / 1E9
                                for m in self._view.getMessages([t]))
                mymedian = median(sorted(s1 - s0 for s1, s0 in zip(stamps[1:], stamps[:-1])))
            freq = 1.0 / mymedian if mymedian else None
            topicdict[t] = self.TopicTuple(n, c, len(conns.get(t, [])), freq)
        if not topics or set(topics) == set(t for t, _, _ in self._topics):
            self._ttinfo = self.TypesAndTopicsTuple(msgtypes, topicdict)
        return self._ttinfo


    def read_messages(self, topics=None, start_time=None, end_time=None, raw=False):
        """
        Yields messages from the bag, optionally filtered by topic and timestamp.

        @param   topics      list of topics or a single topic to filter by, if at all
        @param   start_time  earliest timestamp of message to return, as ROS time or convertible
                             (int/float/duration/datetime/decimal)
        @param   end_time    latest timestamp of message to return, as ROS time or convertible
                             (int/float/duration/datetime/decimal)
        @param   raw         if true, then returned messages are tuples of
                             (typename, bytes, typehash, typeclass)
        @return              BagMessage namedtuples of (topic, message, timestamp as rospy.Time)
        """
        if self.closed: raise ValueError("I/O operation on closed file.")

        topics = topics if isinstance(topics, list) else [topics] if topics else []
        start_time, end_time = (api.to_sec(api.to_time(x)) for x in (start_time, end_time))
        for m in self._view.getMessages(topics) if topics else self._view.getMessages():
            if start_time is not None and start_time > m.timestamp.to_sec():
                continue  # for m
            if end_time is not None and end_time < m.timestamp.to_sec():
                continue  # for m

            typename = self._hashdefs[(m.topic, m.md5)]
            stamp = api.make_time(m.timestamp.secs, m.timestamp.nsecs)
            if raw: msg = (typename, m.data(), m.md5, self.get_message_class(typename, m.md5))
            else: msg = self._populate_message(self.get_message_class(typename, m.md5)(), m.data())
            api.TypeMeta.make(msg, m.topic, self)
            yield self.BagMessage(m.topic, msg, stamp)
            if self.closed: break  # for m


    def open(self):
        """Opens the bag file if not already open."""
        if not self._view: self._view = embag.View(self._filename)


    def close(self):
        """Closes the bag file."""
        if self._view:
            del self._view
            self._view   = None
            self._iterer = None


    @property
    def closed(self):
        """Returns whether file is closed."""
        return not self._view


    @property
    def topics(self):
        """Returns the list of topics in bag, in alphabetic order."""
        return sorted((t for t, _, _ in self._topics), key=str.lower)


    @property
    def filename(self):
        """Returns bag file path."""
        return self._filename


    @property
    def size(self):
        """Returns current file size."""
        return os.path.getsize(self._filename) if os.path.isfile(self._filename) else None


    @property
    def mode(self):
        """Returns file open mode."""
        return "r"


    def __contains__(self, key):
        """Returns whether bag contains given topic."""
        return any(key == t for t, _, _ in self._topics)


    def __next__(self):
        """Retrieves next message from bag as (topic, message, timestamp)."""
        if self.closed: raise ValueError("I/O operation on closed file.")
        if self._iterer is None: self._iterer = self.read_messages()
        return next(self._iterer)


    def _populate_meta(self):
        """Populates bag metainfo."""
        connections = self._view.connectionsByTopic()
        for topic in self._view.topics():
            for conn in connections.get(topic, ()):
                topickey, typekey = (topic, conn.type, conn.md5sum), (conn.type, conn.md5sum)
                self._topics.setdefault(topickey, 0)
                self._topics[topickey] += conn.message_count
                self._hashdefs[(topic, conn.md5sum)] = conn.type
                self._typedefs[typekey] = conn.message_definition
                subtypedefs = api.parse_definition_subtypes(conn.message_definition)
                for n, d in subtypedefs.items():
                    h = api.calculate_definition_hash(n, d, tuple(subtypedefs.items()))
                    self._typedefs.setdefault((n, h), d)


    def _populate_message(self, msg, embagval):
        """Returns the ROS1 message populated from a corresponding embag.RosValue."""
        for name, typename in api.get_message_fields(msg).items():
            v, scalarname = embagval.get(name), api.scalar(typename)
            if typename in api.ROS_BUILTIN_TYPES:      # Single built-in type
                msgv = getattr(embagval, name)
            elif scalarname in api.ROS_BUILTIN_TYPES:  # List of built-in types
                msgv = list(v)
            elif typename in api.ROS_TIME_TYPES:       # Single temporal type
                cls = next(k for k, v in api.ROS_TIME_CLASSES.items() if v == typename)
                msgv = cls(v.secs, v.nsecs)
            elif scalarname in api.ROS_TIME_TYPES:     # List of temporal types
                cls = next(k for k, v in api.ROS_TIME_CLASSES.items() if v == scalarname)
                msgv = [cls(x.secs, x.nsecs) for x in v]
            elif typename == scalarname:                  # Single subtype
                msgv = self._populate_message(self.get_message_class(typename)(), v)
            else:                                         # List of subtypes
                cls = self.get_message_class(scalarname)
                msgv = [self._populate_message(cls(), x) for x in v]
            setattr(msg, name, msgv)
        return msg


    @classmethod
    def autodetect(cls, filename):
        """Returns whether file is readable as ROS1 bag."""
        result = os.path.isfile(filename)
        if result:
            with open(filename, "rb") as f:
                result = (f.read(len(cls.ROSBAG_MAGIC)) == cls.ROSBAG_MAGIC)
        return result


def init(*_, **__):
    """Replaces ROS1 bag reader with EmbagReader. Raises ImportWarning if embag not available."""
    if not embag:
        ConsolePrinter.error("embag not available: cannot read bag files.")
        raise ImportWarning()
    api.Bag.READER_CLASSES.add(EmbagReader)


__all__ = ["EmbagReader", "init"]

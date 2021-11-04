# -*- coding: utf-8 -*-
"""
ROS2 interface.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     02.11.2021
@modified    04.11.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.ros2
import collections
import os
import sqlite3
import threading
import time

import builtin_interfaces.msg
import rclpy
import rclpy.duration
import rclpy.serialization
import rclpy.time
import rosidl_runtime_py.utilities

from . common import ConsolePrinter, MatchMarkers


## Bagfile extensions to seek
BAG_EXTENSIONS  = (".db3")

## Bagfile extensions to skip
SKIP_EXTENSIONS = ()

## rclpy.node.Node instance
node = None



class Bag(object):
    """ROS2 bag interface, partially mimicking rosbag.Bag."""

    ## ROS2 bag SQLite schema
    CREATE_SQL = """
CREATE TABLE IF NOT EXISTS messages (
  id        INTEGER PRIMARY KEY,
  topic_id  INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  data      BLOB    NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
  id                   INTEGER PRIMARY KEY,
  name                 TEXT    NOT NULL,
  type                 TEXT    NOT NULL,
  serialization_format TEXT    NOT NULL,
  offered_qos_profiles TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS timestamp_idx ON messages (timestamp ASC);
    """


    def __init__(self, filename):
        self._db        = None  # sqlite3.Connection instance
        self._topics    = {}    # {name: {id, name, type}}

        ## Bagfile path
        self.filename = filename


    def get_message_count(self):
        """Returns the number of messages in the bag."""
        self._ensure_open()
        if self._has_table("messages"):
            row = self._db.execute("SELECT COUNT(*) AS count FROM messages").fetchone()
            return row["count"]
        return None


    def get_start_time(self):
        """Returns the start time of the bag, as UNIX timestamp."""
        self._ensure_open()
        if self._has_table("messages"):
            row = self._db.execute("SELECT MAX(timestamp) AS val FROM messages").fetchone()
            secs, nsecs = divmod(row["val"], 10**9)
            return secs + nsecs / 1E9
        return None


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp."""
        self._ensure_open()
        if self._has_table("messages"):
            row = self._db.execute("SELECT MIN(timestamp) AS val FROM messages").fetchone()
            secs, nsecs = divmod(row["val"], 10**9)
            return secs + nsecs / 1E9
        return None


    def get_type_and_topic_info(self):
        """Returns namedtuple(topics={topicname: namedtuple(msg_type, message_count)})."""
        self._ensure_open()
        TopicTuple  = collections.namedtuple("TopicTuple",          ["msg_type", "message_count"])
        ResultTuple = collections.namedtuple("TypesAndTopicsTuple", ["topics"])
        topicmap = {}
        if self._has_table("messages") and self._has_table("topics"):
            counts = self._db.execute("SELECT topic_id, COUNT(*) AS count FROM messages "
                                      "GROUP BY topic_id").fetchall()
            countmap = {x["topic_id"]: x["count"] for x in counts}
            for row in self._db.execute("SELECT * FROM topics ORDER BY id").fetchall():
                mytype, mycount =row["type"], countmap.get(row["id"], 0)
                topicmap[row["name"]] = TopicTuple(msg_type=mytype, message_count=mycount)
        return ResultTuple(topics=topicmap)


    def read_messages(self, topics=None, start_time=None, end_time=None):
        """
        Yields messages from the bag, optionally filtered by topic and timestamp.

        @param   topics      list of topics or a single topic to filter by, if at all
        @param   start_time  earliest timestamp of message to return, as UNIX timestamp
        @param   end_time    latest timestamp of message to return, as UNIX timestamp
        @return              (topic, msg, rclpy.time.Time)
        """
        self._ensure_open()
        if not self._has_table("messages"):
            return

        sql = "SELECT id, name, type FROM topics"
        self._topics = {r["name"]: r for r in self._db.execute(sql).fetchall()}

        sql, exprs, args = "SELECT * FROM messages", [], ()
        if topics:
            topics = topics if isinstance(topics, (list, tuple)) else [topics]
            rows   = list(filter(bool, map(self._topics.get, topics)))
            exprs += ["topic_id IN (%s)" % ", ".join("%s" % (x["id"], ) for x in rows)]
        if start_time is not None:
            exprs += ["timestamp >= ?"]
            args  += (start_time, )
        if end_time is not None:
            exprs += ["timestamp <= ?"]
            args  += (end_time, )
        sql += ((" WHERE " + " AND ".join(exprs)) if exprs else "")
        sql += " ORDER BY timestamp"

        topicmap = {v["id"]: v for v in self._topics.values()}
        msgtypes = {}  # {typename: cls}
        for row in self._db.execute(sql, args):
            tdata = topicmap[row["topic_id"]]
            topic, tname = tdata["name"], tdata["type"]
            cls = msgtypes.get(tname) or msgtypes.setdefault(tname, get_message_class(tname))
            msg = rclpy.serialization.deserialize_message(row["data"], cls)
            stamp = rclpy.time.Time(nanoseconds=row["timestamp"])

            yield topic, msg, stamp
            if not self._db:
                break


    def write(self, topic, msg, stamp):
        """
        Writes a message to the bag.

        @param   topic  name of topic
        @param   msg    ROS2 message
        @param   stamp  rclpy.time.Time of message publication
        """
        self._ensure_open(populate=True)

        if not self._topics:
            sql = "SELECT id, name, type FROM topics"
            self._topics = {r["name"]: r for r in self._db.execute(sql).fetchall()}

        cursor = self._db.cursor()
        if topic not in self._topics:
            typename = "%s/msg/%s" % (type(msg).__module__.split(".")[0], type(msg).__name__)
            sql = "INSERT INTO topics (name, type, serialization_format, offered_qos_profiles) " \
                  "VALUES (?, ?, ?, ?)"
            args = (topic, typename, "cdr", "")
            cursor.execute(sql, args)
            tdata = {"id": cursor.lastrowid, "name": topic, "type": typename}
            self._topics[topic] = tdata

        data = rclpy.serialization.serialize_message(msg)
        sql = "INSERT INTO messages (topic_id, timestamp, data) VALUES (?, ?, ?)"
        args = (self._topics[topic]["id"], stamp.nanoseconds, data)
        cursor.execute(sql, args)


    def close(self):
        """Closes the bag file."""
        if self._db:
            self._db.close()
            self._db = None


    @property
    def size(self):
        """Returns current file size."""
        return os.path.getsize(self.filename) if os.path.isfile(self.filename) else None


    def _ensure_open(self, populate=False):
        """Opens bag database if not open, can populate schema if not present."""
        if self._db:
            return
        self._db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES,
                                   isolation_level=None, check_same_thread=False)
        self._db.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))
        if populate:
            self._db.executescript(self.CREATE_SQL)


    def _has_table(self, name):
        """Returns whether specified table exists in database."""
        sql = "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?"
        return bool(self._db.execute(sql, ("table", name)).fetchone())



def init_node(name):
    """Initializes a ROS2 node if not already initialized."""
    global node
    if node or not validate():
        return

    def spin_loop():
        while rclpy.ok():
            rclpy.spin_once(node, timeout_sec=1)

    try: rclpy.init()
    except Exception: pass  # Must not be called twice at runtime
    node_name = "%s_%s_%s" % (name, os.getpid(), int(time.time() * 1000))
    node = rclpy.create_node(node_name)
    spinner = threading.Thread(target=spin_loop)
    spinner.daemon = True
    spinner.start()


def validate():
    """Returns whether ROS2 environment is set, prints error if not."""
    missing = [k for k in ("ROS_DISTRO", "ROS_VERSION") if not os.getenv(k)]
    if missing:
        ConsolePrinter.error("ROS environment not sourced: missing %s.",
                             ", ".join(sorted(missing)))
    return not missing


def create_bag_reader(filename):
    """Returns a ROS2 bag reader with rosbag.Bag-like interface."""
    return Bag(filename)


def create_bag_writer(filename):
    """Returns a ROS2 bag writer with rosbag.Bag-like interface."""
    return Bag(filename)


def create_publisher(topic, cls, queue_size):
    """Returns a ROS publisher instance, with .get_num_connections() and .unregister()."""
    qos = rclpy.qos.QoSProfile(depth=queue_size)
    pub = node.create_publisher(cls, topic, qos)
    pub.get_num_connections = pub.get_subscription_count
    pub.unregister = pub.destroy
    return pub


def create_subscriber(topic, cls, handler, queue_size):
    """Returns an rclpy.Subscriber, with .unregister()."""
    qos = rclpy.qos.QoSProfile(depth=queue_size)
    sub = node.create_subscription(cls, topic, handler, qos)
    sub.unregister = sub.destroy
    return sub


def format_message_value(msg, name, value):
    """
    Returns a message attribute value as string.

    Result is at least 10 chars wide if message is a ROS2 time/duration
    (aligning seconds and nanoseconds).
    """
    LENS = {"secs": 10, "nanosecs": 9}
    TEMPORAL_TYPES = (builtin_interfaces.msg.Time, builtin_interfaces.msg.Duration)
    v = "%s" % (value, )
    if not isinstance(msg, TEMPORAL_TYPES) or name not in LENS:
        return v

    EXTRA = sum(v.count(x) * len(x) for x in (MatchMarkers.START, MatchMarkers.END))
    return ("%%%ds" % (LENS[name] + EXTRA)) % v  # Default %10s/%9s for secs/nanosecs


def get_message_class(typename):
    """Returns ROS2 message class."""
    return rosidl_runtime_py.utilities.get_message(typename)


def get_message_fields(val):
    """Returns OrderedDict({field name: field type name}) if ROS2 message, else {}."""
    if not is_ros_message(val): return val
    return collections.OrderedDict(val.get_fields_and_field_types())


def get_rostime():
    """Returns current ROS2 time, as rclpy.time.Time."""
    return node.get_clock().now()


def get_topic_types():
    """
    Returns currently available ROS2 topics, as [(topicname, typename)].

    Omits topics that the current ROS2 node itself has published.
    """
    result = []
    myname, myns = node.get_name(), node.get_namespace()
    mytypes = dict(node.get_publisher_names_and_types_by_node(myname, myns))
    for t in ("/parameter_events", "/rosout"):
        mytypes.pop(t, None)
    for topic, typenames in node.get_topic_names_and_types():  # [(topicname, [typename, ])]
        if topic not in mytypes and typenames:
            result += [(topic, typenames[0])]
    return result


def is_ros_message(val):
    """Returns whether value is a ROS2 message or a special like ROS2 time/duration."""
    return rosidl_runtime_py.utilities.is_message(val)


def make_duration(secs=0, nsecs=0):
    """Returns an rclpy.duration.Duration."""
    return rclpy.duration.Duration(seconds=secs, nanoseconds=nsecs)


def make_time(secs=0, nsecs=0):
    """Returns a ROS2 time, as rclpy.time.Time."""
    return rclpy.time.Time(seconds=secs, nanoseconds=nsecs)


def set_message_value(obj, name, value):
    """Sets message or object attribute value."""
    if is_ros_message(obj):
        # Bypass setter as it does type checking
        fieldmap = obj.get_fields_and_field_types()
        if name in fieldmap:
            name = obj.__slots__[list(fieldmap).index(name)]
    setattr(obj, name, value)


def to_sec(val):
    """Returns value in seconds if value is ROS2 time/duration, else value."""
    if not isinstance(val, (rclpy.duration.Duration, rclpy.time.Time)):
        return val
    secs, nsecs = divmod(val.nanoseconds, 10**9)
    return secs + nsecs / 1E9

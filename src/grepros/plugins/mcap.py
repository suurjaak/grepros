# -*- coding: utf-8 -*-
"""
MCAP input and output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     14.10.2022
@modified    29.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.mcap
from __future__ import absolute_import
import atexit
import copy
import io
import os
import time
import types

from .. import api

try: import mcap, mcap.reader
except ImportError: mcap = None
if "1" == os.getenv("ROS_VERSION"):
    import genpy.dynamic
    try: import mcap_ros1 as mcap_ros, mcap_ros1.decoder, mcap_ros1.writer
    except ImportError: mcap_ros = None
elif "2" == os.getenv("ROS_VERSION"):
    try: import mcap_ros2 as mcap_ros, mcap_ros2.decoder, mcap_ros2.writer
    except ImportError: mcap_ros = None
else: mcap_ros = None
import yaml

from .. import common
from .. common import ConsolePrinter
from .. outputs import Sink


class McapBag(api.BaseBag):
    """
    MCAP bag interface, providing most of rosbag.Bag interface.

    Bag cannot be appended to, and cannot be read and written at the same time
    (MCAP API limitation).
    """

    ## Supported opening modes
    MODES = ("r", "w")

    ## MCAP file header magic start bytes
    MCAP_MAGIC = b"\x89MCAP\x30\r\n"

    def __init__(self, f, mode="r", **__):
        """
        Opens file and populates metadata.

        @param   f         bag file path, or a stream object
        @param   mode      return reader if "r" or writer if "w"
        """
        if mode not in self.MODES: raise ValueError("invalid mode %r" % mode)
        self._mode           = mode
        self._topics         = {}     # {(topic, typename, typehash): message count}
        self._types          = {}     # {(typename, typehash): message type class}
        self._typedefs       = {}     # {(typename, typehash): type definition text}
        self._schemas        = {}     # {(typename, typehash): mcap.records.Schema}
        self._schematypes    = {}     # {mcap.records.Schema.id: (typename, typehash)}
        self._qoses          = {}     # {(topic, typename): [{qos profile dict}]}
        self._typefields     = {}     # {(typename, typehash): {field name: type name}}
        self._type_subtypes  = {}     # {(typename, typehash): {typename: typehash}}
        self._field_subtypes = {}     # {(typename, typehash): {field name: (typename, typehash)}}
        self._temporal_ctors = {}     # {typename: time/duration constructor}
        self._start_time     = None   # Bag start time, as UNIX timestamp
        self._end_time       = None   # Bag end time, as UNIX timestamp
        self._file           = None   # File handle
        self._reader         = None   # mcap.McapReader
        self._decoder        = None   # mcap_ros.Decoder
        self._writer         = None   # mcap_ros.Writer
        self._iterer         = None   # Generator from read_messages() for next()
        self._ttinfo         = None   # Cached result for get_type_and_topic_info()
        self._opened         = False  # Whether file has been opened at least once
        self._filename       = None   # File path, or None if stream

        if common.is_stream(f):
            if not common.verify_io(f, mode):
                raise io.UnsupportedOperation("read" if "r" == mode else "write")
            self._file, self._filename = f, None
            f.seek(0)
        else:
            if not isinstance(f, common.PATH_TYPES):
                raise ValueError("invalid filename %r" % type(f))
            if "w" == mode: common.makedirs(os.path.dirname(f))
            self._filename = str(f)

        if api.ROS2 and "r" == mode: self._temporal_ctors.update(
            (t, c) for c, t in api.ROS_TIME_CLASSES.items() if api.get_message_type(c) == t
        )

        self.open()


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
        """Returns the start time of the bag, as UNIX timestamp."""
        return self._start_time


    def get_end_time(self):
        """Returns the end time of the bag, as UNIX timestamp."""
        return self._end_time


    def get_message_class(self, typename, typehash=None):
        """
        Returns ROS message class for typename, or None if unknown type.

        @param   typehash  message type definition hash, if any
        """
        typehash = typehash or next((h for t, h in self._typedefs if t == typename), None)
        typekey = (typename, typehash)
        if typekey not in self._types and typekey in self._typedefs:
            if api.ROS2:
                name = typename.split("/")[-1]
                fields = api.parse_definition_fields(typename, self._typedefs[typekey])
                cls = type(name, (types.SimpleNamespace, ), {
                    "__name__": name, "__slots__": list(fields),
                    "__repr__": message_repr, "__str__": message_repr
                })
                self._types[typekey] = self._patch_message_class(cls, typename, typehash)
            else:
                typeclses = genpy.dynamic.generate_dynamic(typename, self._typedefs[typekey])
                self._types[typekey] = typeclses[typename]

        return self._types.get(typekey)


    def get_message_definition(self, msg_or_type):
        """Returns ROS message type definition full text from bag, including subtype definitions."""
        if api.is_ros_message(msg_or_type):
            return self._typedefs.get((api.get_message_type(msg_or_type),
                                       api.get_message_type_hash(msg_or_type)))
        typename = msg_or_type
        return next((d for (n, h), d in self._typedefs.items() if n == typename), None)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS message type MD5 hash."""
        typename = msg_or_type
        if api.is_ros_message(msg_or_type): typename = api.get_message_type(msg_or_type)
        return next((h for n, h in self._typedefs if n == typename), None)


    def get_qoses(self, topic, typename):
        """Returns topic Quality-of-Service profiles as a list of dicts, or None if not available."""
        return self._qoses.get((topic, typename))


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

        topics = topic_filters
        topics = topics if isinstance(topics, (list, set, tuple)) else [topics] if topics else []
        msgtypes = {n: h for t, n, h in self._topics}
        topicdict = {}

        def median(vals):
            """Returns median value from given sorted numbers."""
            vlen = len(vals)
            return None if not vlen else vals[vlen // 2] if vlen % 2 else \
                   float(vals[vlen // 2 - 1] + vals[vlen // 2]) / 2

        for (t, n, _), c in sorted(self._topics.items()):
            if topics and t not in topics: continue  # for
            mymedian = None
            if c > 1 and self._reader:
                stamps = sorted(m.publish_time / 1E9 for _, _, m in self._reader.iter_messages([t]))
                mymedian = median(sorted(s1 - s0 for s1, s0 in zip(stamps[1:], stamps[:-1])))
            freq = 1.0 / mymedian if mymedian else None
            topicdict[t] = self.TopicTuple(n, c, len(self._qoses.get((t, n)) or []), freq)
        result = self.TypesAndTopicsTuple(msgtypes, topicdict)
        if not topics or set(topics) == set(t for t, _, _ in self._topics): self._ttinfo = result
        return result


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
        @return              BagMessage namedtuples of 
                             (topic, message, timestamp as rospy/rclpy.Time)
        """
        if self.closed: raise ValueError("I/O operation on closed file.")
        if "w" == self._mode: raise io.UnsupportedOperation("read")

        topics = topics if isinstance(topics, list) else [topics] if topics else None
        start_ns, end_ns = (api.to_nsec(api.to_time(x)) for x in (start_time, end_time))
        for schema, channel, message in self._reader.iter_messages(topics, start_ns, end_ns):
            if raw:
                typekey = (typename, typehash) = self._schematypes[schema.id]
                if typekey not in self._types:
                    self._types[typekey] = self._make_message_class(schema, message)
                msg = (typename, message.data, typehash, self._types[typekey])
            else: msg = self._decode_message(message, channel, schema)
            api.TypeMeta.make(msg, channel.topic, self)
            yield self.BagMessage(channel.topic, msg, api.make_time(nsecs=message.publish_time))
            if self.closed: break  # for schema


    def write(self, topic, msg, t=None, raw=False, **__):
        """
        Writes out message to MCAP file.

        @param   topic   name of topic
        @param   msg     ROS1 message
        @param   t       message timestamp if not using current ROS time,
                         as ROS time type or convertible (int/float/duration/datetime/decimal)
        @param   raw     if true, `msg` is in raw format, (typename, bytes, typehash, typeclass)
        """
        if self.closed: raise ValueError("I/O operation on closed file.")
        if "r" == self._mode: raise io.UnsupportedOperation("write")

        if raw:
            typename, binary, typehash = msg[:3]
            cls = msg[-1]
            typedef = self._typedefs.get((typename, typehash)) or api.get_message_definition(cls)
            msg = api.deserialize_message(binary, cls)
        else:
            with api.TypeMeta.make(msg, topic) as meta:
                typename, typehash, typedef = meta.typename, meta.typehash, meta.definition
        topickey, typekey = (topic, typename, typehash), (typename, typehash)

        nanosec = (time.time_ns() if hasattr(time, "time_ns") else int(time.time() * 10**9)) \
                  if t is None else api.to_nsec(api.to_time(t))
        if api.ROS2:
            if typekey not in self._schemas:
                fullname = api.make_full_typename(typename)
                schema = self._writer.register_msgdef(fullname, typedef)
                self._schemas[typekey] = schema
            schema, data = self._schemas[typekey], api.message_to_dict(msg)
            self._writer.write_message(topic, schema, data, publish_time=nanosec)
        else:
            self._writer.write_message(topic, msg, publish_time=nanosec)

        sec = nanosec / 1E9
        self._start_time = sec if self._start_time is None else min(sec, self._start_time)
        self._end_time   = sec if self._end_time   is None else min(sec, self._end_time)
        self._topics[topickey] = self._topics.get(topickey, 0) + 1
        self._types.setdefault(typekey, type(msg))
        self._typedefs.setdefault(typekey, typedef)
        self._ttinfo = None


    def open(self):
        """Opens the bag file if not already open."""
        if self._reader is not None or self._writer is not None: return
        if self._opened and "w" == self._mode:
            raise io.UnsupportedOperation("Cannot reopen bag for writing.")

        if self._file is None: self._file = open(self._filename, "%sb" % self._mode)
        self._reader  = mcap.reader.make_reader(self._file) if "r" == self._mode else None
        self._decoder = mcap_ros.decoder.Decoder()          if "r" == self._mode else None
        self._writer  = mcap_ros.writer.Writer(self._file)  if "w" == self._mode else None
        if "r" == self._mode: self._populate_meta()
        self._opened = True


    def close(self):
        """Closes the bag file."""
        if self._file is not None:
            if self._writer: self._writer.finish()
            self._file.close()
            self._file, self._reader, self._writer, self._iterer = None, None, None, None


    @property
    def closed(self):
        """Returns whether file is closed."""
        return self._file is None


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
        if self._filename is None and self._file is not None:
            pos, _  = self._file.tell(), self._file.seek(0, os.SEEK_END)
            size, _ = self._file.tell(), self._file.seek(pos)
            return size
        return os.path.getsize(self._filename) if os.path.isfile(self._filename) else None


    @property
    def mode(self):
        """Returns file open mode."""
        return self._mode


    def __contains__(self, key):
        """Returns whether bag contains given topic."""
        return any(key == t for t, _, _ in self._topics)


    def __next__(self):
        """Retrieves next message from bag as (topic, message, timestamp)."""
        if self.closed: raise ValueError("I/O operation on closed file.")
        if self._iterer is None: self._iterer = self.read_messages()
        return next(self._iterer)


    def _decode_message(self, message, channel, schema):
        """
        Returns ROS message deserialized from binary data.

        @param   message  mcap.records.Message instance
        @param   channel  mcap.records.Channel instance for message
        @param   schema   mcap.records.Schema instance for message type
        """
        cls = self._make_message_class(schema, message, generate=False)
        if api.ROS2 and not issubclass(cls, types.SimpleNamespace):
            msg = api.deserialize_message(message.data, cls)
        else:
            msg = self._decoder.decode(schema=schema, message=message)
            if api.ROS2:  # MCAP ROS2 message classes need monkey-patching with expected API
                msg = self._patch_message(msg, *self._schematypes[schema.id])
                # Register serialized binary, as MCAP does not support serializing its own creations
                api.TypeMeta.make(msg, channel.topic, self, data=message.data)
        typekey = self._schematypes[schema.id]
        if typekey not in self._types: self._types[typekey] = type(msg)
        return msg


    def _make_message_class(self, schema, message, generate=True):
        """
        Returns message type class, generating if not already available.

        @param   schema    mcap.records.Schema instance for message type
        @param   message   mcap.records.Message instance
        @param   generate  generate message class dynamically if not available
        """
        typekey = (typename, typehash) = self._schematypes[schema.id]
        if api.ROS2 and typekey not in self._types:
            try:  # Try loading class from disk for full compatibility
                cls = api.get_message_class(typename)
                clshash = api.get_message_type_hash(cls)
                if typehash == clshash: self._types[typekey] = cls
            except Exception: pass  # ModuleNotFoundError, AttributeError etc
        if typekey not in self._types and generate:
            if api.ROS2:  # MCAP ROS2 message classes need monkey-patching with expected API
                msg = self._decoder.decode(schema=schema, message=message)
                self._types[typekey] = self._patch_message_class(type(msg), typename, typehash)
            else:
                typeclses = genpy.dynamic.generate_dynamic(typename, schema.data.decode())
                self._types[typekey] = typeclses[typename]
        return self._types.get(typekey)


    def _patch_message_class(self, cls, typename, typehash):
        """
        Patches MCAP ROS2 message class with expected attributes and methods, recursively.

        @param   cls       ROS message class as returned from mcap_ros2.decoder
        @param   typename  ROS message type name, like "std_msgs/Bool"
        @param   typehash  ROS message type hash
        @return            patched class
        """
        typekey = (typename, typehash)
        if typekey not in self._typefields:
            fields = api.parse_definition_fields(typename, self._typedefs[typekey])
            self._typefields[typekey] = fields
            self._field_subtypes[typekey] = {n: (s, h) for n, t in fields.items()
                                             for s in [api.scalar(t)]
                                             if s not in api.ROS_BUILTIN_TYPES
                                             for h in [self._type_subtypes[typekey][s]]}

        # mcap_ros2 creates a dynamic class for each message, having __slots__
        # but no other ROS2 API hooks; even the class module is "mcap_ros2.dynamic".
        cls.__module__ = typename.split("/", maxsplit=1)[0]
        cls.SLOT_TYPES = ()  # rosidl_runtime_py.utilities.is_message() checks for presence
        cls._fields_and_field_types = dict(self._typefields[typekey])
        cls.get_fields_and_field_types = message_get_fields_and_field_types
        cls.__copy__     = copy_with_slots  # MCAP message classes lack support for copy
        cls.__deepcopy__ = deepcopy_with_slots

        return cls


    def _patch_message(self, message, typename, typehash):
        """
        Patches MCAP ROS2 message with expected attributes and methods, recursively.

        @param   message   ROS message instance as returned from mcap_ros2.decoder
        @param   typename  ROS message type name, like "std_msgs/Bool"
        @param   typehash  ROS message type hash
        @return            original message patched, or new instance if ROS2 temporal type
        """
        result = message
        # [(field value, (field type name, field type hash), parent, (field name, ?array index))]
        stack = [(message, (typename, typehash), None, ())]
        while stack:
            msg, typekey, parent, path = stack.pop(0)
            mycls, typename = type(msg), typekey[0]

            if typename in self._temporal_ctors:
                # Convert temporal types to ROS2 temporal types for grepros to recognize
                msg2 = self._temporal_ctors[typename](sec=msg.sec, nanosec=msg.nanosec)
                if msg is message: result = msg2                     # Replace input message
                elif len(path) == 1: setattr(parent, path[0], msg2)  # Set scalar field
                else: getattr(parent, path[0])[path[1]] = msg2       # Set array field element
                continue  # while stack

            self._patch_message_class(mycls, *typekey)

            for name, subtypekey in self._field_subtypes[typekey].items():
                v = getattr(msg, name)
                if isinstance(v, list):  # Queue each array element for patching
                    stack.extend((x, subtypekey, msg, (name, i)) for i, x in enumerate(v))
                else:  # Queue scalar field for patching
                    stack.append((v, subtypekey, msg, (name, )))

        return result


    def _populate_meta(self):
        """Populates bag metainfo."""
        summary = self._reader.get_summary()
        self._start_time = summary.statistics.message_start_time / 1E9
        self._end_time   = summary.statistics.message_end_time   / 1E9

        defhashes = {}  # Cached {type definition full text: type hash}
        for cid, channel in summary.channels.items():
            schema = summary.schemas[channel.schema_id]
            topic, typename = channel.topic, api.canonical(schema.name)

            typedef = schema.data.decode("utf-8")  # Full definition including subtype definitions
            subtypedefs, nesting = api.parse_definition_subtypes(typedef, nesting=True)
            typehash = channel.metadata.get("md5sum") or \
                       api.calculate_definition_hash(typename, typedef,
                                                        tuple(subtypedefs.items()))
            topickey, typekey = (topic, typename, typehash), (typename, typehash)

            qoses = None
            if channel.metadata.get("offered_qos_profiles"):
                try: qoses = yaml.safe_load(channel.metadata["offered_qos_profiles"])
                except Exception as e:
                    ConsolePrinter.warn("Error parsing topic QoS profiles from %r: %s.",
                                        channel.metadata["offered_qos_profiles"], e)

            self._topics.setdefault(topickey, 0)
            self._topics[topickey] += summary.statistics.channel_message_counts[cid]
            self._typedefs[typekey] = typedef
            defhashes[typedef] = typehash
            for t, d in subtypedefs.items():  # Populate subtype definitions and hashes
                if d in defhashes: h = defhashes[d]
                else: h = api.calculate_definition_hash(t, d, tuple(subtypedefs.items()))
                self._typedefs.setdefault((t, h), d)
                self._type_subtypes.setdefault(typekey, {})[t] = h
                defhashes[d] = h
            for t, subtypes in nesting.items():  # Populate all nested type references
                h = self._type_subtypes[typekey][t]
                for t2 in subtypes:
                    h2 = self._type_subtypes[typekey][t2]
                    self._type_subtypes.setdefault((t, h), {})[t2] = h2

            if qoses: self._qoses[topickey] = qoses
            self._schemas[typekey] = schema
            self._schematypes[schema.id] = typekey


    @classmethod
    def autodetect(cls, f):
        """Returns whether file is readable as MCAP format."""
        if common.is_stream(f):
            pos, _ = f.tell(), f.seek(0)
            result, _ = (f.read(len(cls.MCAP_MAGIC)) == cls.MCAP_MAGIC), f.seek(pos)
        elif os.path.isfile(f) and os.path.getsize(f):
            with open(f, "rb") as f:
                result = (f.read(len(cls.MCAP_MAGIC)) == cls.MCAP_MAGIC)
        else:
            ext = os.path.splitext(f or "")[-1].lower()
            result = ext in McapSink.FILE_EXTENSIONS
        return result



def copy_with_slots(self):
    """Returns a shallow copy, with slots populated manually."""
    result = self.__class__.__new__(self.__class__)
    for n in self.__slots__:
        setattr(result, n, copy.copy(getattr(self, n)))
    return result


def deepcopy_with_slots(self, memo):
    """Returns a deep copy, with slots populated manually."""
    result = self.__class__.__new__(self.__class__)
    for n in self.__slots__:
        setattr(result, n, copy.deepcopy(getattr(self, n), memo))
    return result


def message_get_fields_and_field_types(self):
    """Returns a map of message field names and types (patch method for MCAP message classes)."""
    return self._fields_and_field_types.copy()


def message_repr(self):
    """Returns a string representation of ROS message."""
    fields = ", ".join("%s=%r" % (n, getattr(self, n)) for n in self.__slots__)
    return "%s(%s)" % (self.__name__, fields)



class McapSink(Sink):
    """Writes messages to MCAP file."""

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".mcap", )

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(META=False, WRITE_OPTIONS={}, VERBOSE=False)


    def __init__(self, args=None, **kwargs):
        """
        @param   args                 arguments as namespace or dictionary, case-insensitive;
                                      or a single path as the file to write
        @param   args.write           base name of MCAP files to write
        @param   args.write_options   {"overwrite": whether to overwrite existing file
                                                    (default false)}
        @param   args.meta            whether to print metainfo
        @param   args.verbose         whether to print debug information
        @param   kwargs               any and all arguments as keyword overrides, case-insensitive
        """
        args = {"WRITE": str(args)} if isinstance(args, common.PATH_TYPES) else args
        args = common.ensure_namespace(args, McapSink.DEFAULT_ARGS, **kwargs)
        super(McapSink, self).__init__(args)

        self._filename      = None  # Output filename
        self._file          = None  # Open file() object
        self._writer        = None  # mcap_ros.writer.Writer object
        self._schemas       = {}    # {(typename, typehash): mcap.records.Schema}
        self._overwrite     = (args.WRITE_OPTIONS.get("overwrite") in (True, "true"))
        self._close_printed = False

        atexit.register(self.close)


    def validate(self):
        """
        Returns whether required libraries are available (mcap, mcap_ros1/mcap_ros2)
        and overwrite is valid and file is writable.
        """
        if self.valid is not None: return self.valid
        ok, mcap_ok, mcap_ros_ok = True, bool(mcap), bool(mcap_ros)
        if self.args.WRITE_OPTIONS.get("overwrite") not in (None, True, False, "true", "false"):
            ConsolePrinter.error("Invalid overwrite option for MCAP: %r. "
                                 "Choose one of {true, false}.",
                                 self.args.WRITE_OPTIONS["overwrite"])
            ok = False
        if not mcap_ok:
            ConsolePrinter.error("mcap not available: cannot work with MCAP files.")
        if not mcap_ros_ok:
            ConsolePrinter.error("mcap_ros%s not available: cannot work with MCAP files.",
                                 api.ROS_VERSION or "")
        if not common.verify_io(self.args.WRITE, "w"):
            ok = False
        self.valid = ok and mcap_ok and mcap_ros_ok
        return self.valid


    def emit(self, topic, msg, stamp=None, match=None, index=None):
        """Writes out message to MCAP file."""
        if not self.validate(): raise Exception("invalid")
        self._ensure_open()
        stamp, index = self._ensure_stamp_index(topic, msg, stamp, index)
        kwargs = dict(publish_time=api.to_nsec(stamp), sequence=index)
        if api.ROS2:
            with api.TypeMeta.make(msg, topic) as m:
                typekey = m.typekey
                if typekey not in self._schemas:
                    fullname = api.make_full_typename(m.typename)
                    self._schemas[typekey] = self._writer.register_msgdef(fullname, m.definition)
            schema, data = self._schemas[typekey], api.message_to_dict(msg)
            self._writer.write_message(topic, schema, data, **kwargs)
        else:
            self._writer.write_message(topic, msg, **kwargs)
        super(McapSink, self).emit(topic, msg, stamp, match, index)


    def close(self):
        """Closes output file if open."""
        try:
            if self._writer:
                self._writer.finish()
                self._file.close()
                self._file, self._writer = None, None
        finally:
            if not self._close_printed and self._counts:
                self._close_printed = True
                try: sz = common.format_bytes(os.path.getsize(self._filename))
                except Exception as e:
                    ConsolePrinter.warn("Error getting size of %s: %s", self._filename, e)
                    sz = "error getting size"
                ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                     common.plural("message", sum(self._counts.values())),
                                     common.plural("topic", self._counts), self._filename, sz)
            super(McapSink, self).close()


    def _ensure_open(self):
        """Opens output file if not already open."""
        if self._file: return

        filename = self.args.WRITE
        if not self._overwrite and os.path.isfile(filename) and os.path.getsize(filename):
            filename = common.unique_path(filename)
            if self.args.VERBOSE:
                ConsolePrinter.debug("Making unique filename %r, as %s does not support "
                                     "appending.", filename, type(self).__name___)
        self._filename = filename
        common.makedirs(os.path.dirname(self._filename))
        if self.args.VERBOSE:
            sz = os.path.exists(self._filename) and os.path.getsize(self._filename)
            action = "Overwriting" if sz and self._overwrite else "Creating"
            ConsolePrinter.debug("%s %s.", action, self._filename)
        self._file = open(self._filename, "wb")
        self._writer = mcap_ros.writer.Writer(self._file)


def init(*_, **__):
    """Adds MCAP support to reading and writing. Raises ImportWarning if libraries not available."""
    if not mcap or not mcap_ros:
        ConsolePrinter.error("mcap libraries not available: cannot work with MCAP files.")
        raise ImportWarning()
    from .. import plugins  # Late import to avoid circular
    plugins.add_write_format("mcap", McapSink, "MCAP", [
        ("overwrite=true|false",  "overwrite existing file in MCAP output\n"
                                  "instead of appending unique counter (default false)"),
    ])
    api.BAG_EXTENSIONS += McapSink.FILE_EXTENSIONS
    api.Bag.READER_CLASSES.add(McapBag)
    api.Bag.WRITER_CLASSES.add(McapBag)


__all__ = ["McapBag", "McapSink", "init"]

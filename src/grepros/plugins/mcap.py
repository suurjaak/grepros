# -*- coding: utf-8 -*-
"""
MCAP input and output.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     14.10.2022
@modified    18.10.2022
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.mcap
from __future__ import absolute_import
import atexit
import copy
import os

try: import mcap, mcap.reader
except ImportError: mcap = None
if "1" == os.getenv("ROS_VERSION"):
    try: import mcap_ros1 as mcap_ros, mcap_ros1.decoder, mcap_ros1.writer
    except ImportError: mcap_ros = None
elif "2" == os.getenv("ROS_VERSION"):
    try: import mcap_ros2 as mcap_ros, mcap_ros2.decoder, mcap_ros2.writer
    except ImportError: mcap_ros = None
else: mcap_ros = None
import yaml

from .. common import ConsolePrinter, format_bytes, makedirs, plural, unique_path
from .. outputs import SinkBase
from .. import rosapi
ros2 = None
if "2" == os.getenv("ROS_VERSION"):
    from .. import ros2


class McapReader(object):
    """MCAP reader interface, partially mimicking rosbag.Bag."""

    ## MCAP file header magic 8 bytes
    MCAP_MAGIC = b"\x89MCAP\x30\r\n"

    def __init__(self, filename, **__):
        """Opens file and populates metadata."""
        self._topics         = {}    # {(topic, typename, typehash): message count}
        self._types          = {}    # {(typename, typehash): message type class}
        self._typedefs       = {}    # {(typename, typehash): type definition text}
        self._schemas        = {}    # {mcap.records.Schema.id: (typename, typehash)}
        self._qoses          = {}    # {(topic, typename): [{qos profile dict}]}
        self._typefields     = {}    # {(typename, typehash): {field name: type name}}
        self._type_subtypes  = {}    # {(typename, typehash): {typename: typehash}}
        self._field_subtypes = {}    # {(typename, typehash): {field name: (typename, typehash)}}
        self._temporal_ctors = {}    # {typename: time/duration constructor}
        self._start_time     = None  # Bag start time, as UNIX timestamp
        self._end_time       = None  # Bag end time, as UNIX timestamp
        self._file           = open(filename, "rb")
        self._reader         = mcap.reader.make_reader(self._file)

        ## Bagfile path
        self.filename = filename

        if ros2: self._temporal_ctors.update(
            (t, c) for c, t in rosapi.ROS_TIME_CLASSES.items() if rosapi.get_message_type(c) == t
        )
        self._populate_meta()


    def get_message_count(self):
        """Returns the number of messages in the bag."""
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
        typehash = typehash or next((t for n, t in self._types if n == typename), None)
        return self._types.get((typename, typehash))


    def get_message_definition(self, msg_or_type):
        """Returns ROS message type definition full text from bag, including subtype definitions."""
        if rosapi.is_ros_message(msg_or_type):
            return self._typedefs.get((rosapi.get_message_type(msg_or_type),
                                       rosapi.get_message_type_hash(msg_or_type)))
        typename = msg_or_type
        return next((d for (n, h), d in self._typedefs.items() if n == typename), None)


    def get_message_type_hash(self, msg_or_type):
        """Returns ROS message type MD5 hash."""
        typename = rosapi.get_message_type(msg_or_type)
        return next((h for n, h in self._typedefs if n == typename), None)


    def get_qoses(self, topic, typename):
        """Returns topic Quality-of-Service profiles as a list of dicts, or None if not available."""
        return self._qoses.get((topic, typename))


    def get_topic_info(self):
        """Returns topic and message type metainfo as {(topic, typename, typehash): count}."""
        return dict(self._topics)


    def read_messages(self, topics=None, start_time=None, end_time=None):
        """
        Yields messages from the bag, optionally filtered by topic and timestamp.

        @param   topics      list of topics or a single topic to filter by, if at all
        @param   start_time  earliest timestamp of message to return, as UNIX timestamp
        @param   end_time    latest timestamp of message to return, as UNIX timestamp
        @return              (topic, msg, rclpy.time.Time)
        """
        topics = topics if isinstance(topics, list) else [topics] if topics else []
        decoder = mcap_ros.decoder.Decoder()
        start_time_ns, end_time_ns = (x and x * 10**9 for x in (start_time, end_time))
        for schema, channel, msg in self._reader.iter_messages(topics, start_time_ns, end_time_ns):
            m = decoder.decode(schema=schema, message=msg)
            if ros2:
                m = self._patch_message(m, *self._schemas[schema.id])
                # Register serialized binary, as MCAP does not support serializing its own creations
                rosapi-TypeMeta.make(m, channel.topic, raw=message.data)
            typekey = self._schemas[schema.id]
            if typekey not in self._types: self._types[typekey] = type(m)
            yield channel.topic, m, rosapi.make_time(nsecs=msg.publish_time)


    def close(self):
        """Closes the bag file."""
        if self._file:
            self._file.close()
            self._file, self._reader = None, None


    @property
    def size(self):
        """Returns current file size."""
        return os.path.getsize(self.filename) if os.path.isfile(self.filename) else None


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

            if typekey not in self._typefields:
                fields = rosapi.parse_definition_fields(typename, self._typedefs[typekey])
                self._typefields[typekey] = fields
                self._field_subtypes[typekey] = {n: (s, h) for n, t in fields.items()
                                                 for s in [rosapi.scalar(t)]
                                                 if s not in rosapi.ROS_BUILTIN_TYPES
                                                 for h in [self._type_subtypes[typekey][s]]}

            # mcap_ros2 creates a dynamic class for each message, having __slots__
            # but no other ROS2 API hooks; even the class module is "mcap_ros2.dynamic".
            mycls.__module__ = typename.split("/", maxsplit=1)[0]
            mycls.SLOT_TYPES = ()  # rosidl_runtime_py.utilities.is_message() checks for presence
            mycls._fields_and_field_types = dict(self._typefields[typekey])
            mycls.get_fields_and_field_types = message_get_fields_and_field_types
            mycls.__copy__     = copy_with_slots  # MCAP message classes lack support for copy
            mycls.__deepcopy__ = deepcopy_with_slots

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
            topic, typename = channel.topic, schema.name
            if ros2: typename = ros2.canonical(typename)

            typedef = schema.data.decode("utf-8")  # Full definition including subtype definitions
            subtypedefs, nesting = rosapi.parse_definition_subtypes(typedef, nesting=True)
            typehash = channel.metadata.get("md5sum") or \
                       rosapi.calculate_definition_hash(typename, typedef,
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
                else: h = rosapi.calculate_definition_hash(t, d, tuple(subtypedefs.items()))
                self._typedefs.setdefault((t, h), d)
                self._type_subtypes.setdefault(typekey, {})[t] = h
                defhashes[d] = h
            for t, subtypes in nesting.items():  # Populate all nested type references
                h = self._type_subtypes[typekey][t]
                for t2 in subtypes:
                    h2 = self._type_subtypes[typekey][t2]
                    self._type_subtypes.setdefault((t, h), {})[t2] = h2

            if qoses: self._qoses[topickey] = qoses
            self._schemas[schema.id] = typekey


    @classmethod
    def autodetect(cls, filename):
        """Returns whether file is readable as MCAP format."""
        result = os.path.isfile(filename)
        if result:
            with open(filename, "rb") as f:
                result = (f.read(len(cls.MCAP_MAGIC)) == cls.MCAP_MAGIC)
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



class McapSink(SinkBase):
    """Writes messages to MCAP files."""

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".mcap", )


    def __init__(self, args):
        """
        @param   args                 arguments object like argparse.Namespace
        @param   args.META            whether to print metainfo
        @param   args.WRITE           base name of MCAP files to write
        @param   args.WRITE_OPTIONS   {"overwrite": whether to overwrite existing file
                                                    (default false)}
        @param   args.VERBOSE         whether to print debug information
        """
        super(McapSink, self).__init__(args)

        self._filename      = None  # Output filename
        self._file          = None  # Open file() object
        self._writer        = None  # mcap_ros.writer.Writer object
        self._schemas       = {}    # {(typename, typehash): mcap.records.Schema}
        self._overwrite     = (args.WRITE_OPTIONS.get("overwrite") == "true")
        self._close_printed = False

        atexit.register(self.close)


    def validate(self):
        """
        Returns whether required libraries are available (mcap, mcap_ros1/mcap_ros2)
        and overwrite is valid.
        """
        ok, mcap_ok, mcap_ros_ok = True, bool(mcap), bool(mcap_ros)
        if self.args.WRITE_OPTIONS.get("overwrite") not in (None, "true", "false"):
            ConsolePrinter.error("Invalid overwrite option for MCAP: %r. "
                                 "Choose one of {true, false}.",
                                 self.args.WRITE_OPTIONS["overwrite"])
            ok = False
        if not mcap_ok:
            ConsolePrinter.error("mcap not available: cannot work with MCAP files.")
        if not mcap_ros_ok:
            ConsolePrinter.error("mcap_ros%s not available: cannot work with MCAP files.",
                                 os.getenv("ROS_VERSION", ""))
        return ok and mcap_ok and mcap_ros_ok


    def emit(self, topic, index, stamp, msg, match):
        """Writes out message to MCAP file."""
        self._ensure_open()
        kwargs = dict(publish_time=rosapi.to_nsec(stamp), sequence=index)
        if ros2:
            with rosapi.TypeMeta.make(msg, topic) as m:
                typekey = m.typekey
                if typekey not in self._schemas:
                    fullname = ros2.make_full_typename(m.typename)
                    self._schemas[typekey] = self._writer.register_msgdef(fullname, m.definition)
            schema, data = self._schemas[typekey], rosapi.message_to_dict(msg)
            self._writer.write_message(topic, schema, data, **kwargs)
        else:
            self._writer.write_message(topic, msg, **kwargs)
        super(McapSink, self).emit(topic, index, stamp, msg, match)


    def close(self):
        """Closes output file if open."""
        if self._writer:
            self._writer.finish()
            self._file.close()
            self._file, self._writer = None, None
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", self._counts), self._filename,
                                 format_bytes(os.path.getsize(self._filename)))
        super(McapSink, self).close()


    def _ensure_open(self):
        """Opens output file if not already open."""
        if self._file: return

        self._filename = self.args.WRITE if self._overwrite else unique_path(self.args.WRITE)
        makedirs(os.path.dirname(self._filename))
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
    rosapi.BAG_EXTENSIONS += McapSink.FILE_EXTENSIONS
    rosapi.Bag.READER_CLASSES.add(McapReader)

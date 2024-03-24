#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: main functions and classes of grepros as library.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     15.12.2022
@modified    24.03.2024
------------------------------------------------------------------------------
"""
import glob
import inspect
import logging
import os
import random
import re
import string
import sys
import tempfile

import std_msgs.msg

import grepros
from grepros import api


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase
from test.testbase import NAME, ERR

logger = logging.getLogger()


class TestLibrary(testbase.TestBase):
    """Tests using grepros main functions and classes as a library."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

    ## Name used in flow logging
    OUTPUT_LABEL = "various sinks"

    ## Suffix for write output file, if any
    OUTPUT_SUFFIX = testbase.TestBase.BAG_SUFFIX


    def __init__(self, *args, **kwargs):
        super(TestLibrary, self).__init__(*args, **kwargs)
        self._outnames = []  # [outfile path, ]


    def setUp(self):
        """Collects and verifies bags in data directory."""
        super(TestLibrary, self).setUp()
        grepros.init()
        self.verify_bags()


    def tearDown(self):
        """Deletes temporary output files, if any."""
        while self._outnames:
            try: os.unlink(self._outnames.pop())
            except Exception: pass
        super(TestLibrary, self).tearDown()


    def test_bag_rw_grep(self):
        """Tests reading and writing bags and matching messages."""
        logger.info("Verifying reading and writing bags, and grepping messages.")
        grep = grepros.Scanner(pattern=self.SEARCH_WORDS,
                               topic="/match/this*", skip_topic="/not/this*",
                               type="std_msgs/*", skip_type="std_msgs/Bool")
        logger.debug("Writing to bag %r.", self._outname)
        with grepros.Bag(self._outname, mode="w") as outbag:
            for bagname in self._bags:
                logger.debug("Reading from bag %r.", bagname)
                with grepros.Bag(bagname) as inbag:
                    for topic, msg, stamp in inbag:
                        if grep.match(topic, msg, stamp):
                            outbag.write(topic, msg, stamp)

        logger.debug("Validating messages written to %r.", self._outname)
        messages = {}  # {topic: [msg, ]}
        outfile = self._outfile = testbase.BagReader(self._outname)
        for topic, msg, _ in outfile.read_messages():
            messages.setdefault(topic, []).append(msg)
        outfile.close()
        fulltext = "\n".join(str(m) for mm in messages.values() for m in mm)
        super(TestLibrary, self).verify_topics(messages, fulltext)


    def test_bag_attributes(self):
        """Tests Bag class general interface."""
        logger.info("Verifying Bag-class attributes.")
        self.verify_bag_attributes(grepros.Bag, self._bags[0], self._outname)
        if ".mcap" in api.BAG_EXTENSIONS:
            outname = self.mkfile(".mcap")
            self.verify_bag_attributes(grepros.McapBag, outname, outname)


    def verify_bag_attributes(self, bagcls, infilename, outfilename):
        """Tests general interface for given bag class."""
        logger.info("Verifying Bag %s attributes.", bagcls)

        bag = bagcls(outfilename, mode="w")
        self.assertEqual(bag.mode, "w", "Unexpected result for Bag.mode.")
        self.assertGreaterEqual(bag.size, 0, "Unexpected result for Bag.size.")
        bag.open()
        self.assertFalse(bag.closed, "Unexpected result for Bag.closed.")
        self.assertEqual(bag.get_message_count(), 0,
                         "Unexpected result for Bag.get_message_count().")
        self.assertEqual(bag.get_start_time(), None,
                         "Unexpected result for Bag.get_start_time().")
        self.assertEqual(bag.get_end_time(), None,
                         "Unexpected result for Bag.get_end_time().")
        self.assertIsInstance(bag.get_topic_info(), dict,
                              "Unexpected result for Bag.get_topic_info().")
        self.assertIsInstance(bag.get_type_and_topic_info(), tuple,
                              "Unexpected result for Bag.get_type_and_topic_info().")
        bag.write("/my/topic", std_msgs.msg.Bool())
        with self.assertRaises(Exception):
            next(bag, None)  # Should raise as mode is "w"

        bag.close()
        self.assertTrue(bag.closed, "Unexpected result for Bag.closed.")

        bag = bagcls(infilename)
        self.assertIsInstance(bag.size, int, "Unexpected result for Bag.size.")
        with bag:
            self.assertFalse(bag.closed, "Unexpected result for Bag.closed.")
            topic, msg, stamp = next(bag)
            self.assertIsInstance(topic, str, "Unexpected result for next(Bag).")
            self.assertTrue(api.is_ros_message(msg), "Unexpected result for next(Bag).")
            self.assertTrue(api.is_ros_time(stamp), "Unexpected result for next(Bag).")
            with self.assertRaises(Exception):
                bag.write(topic, msg, stamp)  # Should raise as mode is "r"

            self.assertIsInstance(len(bag), int, "Unexpected result for len(Bag).")
            self.assertTrue(bool(bag), "Unexpected result for bool(Bag).")
            self.assertIsInstance(bag.size, int, "Unexpected result for Bag.size.")
            self.assertEqual(bag.mode, "r", "Unexpected result for Bag.mode.")

            self.assertTrue(callable(bag.get_message_class),
                            "Unexpected result for Bag.get_message_class.")
            self.assertTrue(callable(bag.get_message_definition),
                            "Unexpected result for Bag.get_message_definition.")
            self.assertTrue(callable(bag.get_message_type_hash),
                            "Unexpected result for Bag.get_message_type_hash.")
            self.assertTrue(callable(bag.get_qoses),
                            "Unexpected result for Bag.get_qoses.")

            self.assertIsInstance(bag.get_message_count(), int,
                                  "Unexpected result for Bag.get_message_count().")
            self.assertIsInstance(bag.get_end_time(), (float, int),
                                  "Unexpected result for Bag.get_start_time().")
            self.assertIsInstance(bag.get_end_time(), (float, int),
                                  "Unexpected result for Bag.get_end_time().")
            self.assertIsInstance(bag.get_topic_info(), dict,
                                  "Unexpected result for Bag.get_topic_info().")
            self.assertIsInstance(bag.get_type_and_topic_info(), tuple,
                                  "Unexpected result for Bag.get_type_and_topic_info().")

        self.assertTrue(bag.closed, "Unexpected result for Bag.closed.")


    def test_bag_parameters(self):
        """Tests parameters to Bag functions."""
        logger.info("Verifying invoking Bag methods with parameters.")
        self.verify_bag_parameters_read(grepros.Bag, self._bags[0])
        self.verify_bag_parameters_write(grepros.Bag, self._outname)
        if ".mcap" in api.BAG_EXTENSIONS:
            outname = self.mkfile(".mcap")
            self.verify_bag_parameters_write(grepros.McapBag, outname)
            self.verify_bag_parameters_read(grepros.McapBag, outname)


    def verify_bag_parameters_read(self, bagcls, filename):
        """Tests parameters to read functions of given Bag class."""
        logger.info("Verifying invoking Bag %r read methods with parameters.", bagcls)

        messages = {}  # {topic: [(message, stamp)]}
        with bagcls(filename) as bag:
            for t, m, s in bag: messages.setdefault(t, []).append((m, s))
        self.assertTrue(messages, "Unexpected result for reading bag contents.")

        bag = bagcls(filename)
        bag.open()

        func = bag.get_message_class
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            typename = "std_msgs/Bool"
            self.assertEqual(func(typename + "unknown"),  None, ERR(func))
            self.assertEqual(func(typename, "wronghash"), None, ERR(func))
            cls = func(typename)
            self.assertTrue(api.is_ros_message(cls), ERR(func))
            self.assertEqual(api.get_message_type(cls), typename, ERR(func))

        func = bag.read_messages
        with self.subTest(NAME(func)):
            # Bag.read_messages(topics=None, start_time=None, end_time=None, raw=False)
            logger.info("Testing %s.", NAME(func))

            logger.debug("Verifying %s.", NAME(func, "topics"))
            topics = random.sample(list(messages), 2)
            mymsgs = {}
            for topic, msg, stamp in func(topics):
                mymsgs.setdefault(topic, []).append((msg, stamp))
            for topic in topics:

                self.assertEqual([(api.message_to_dict(m), t) for m, t in mymsgs[topic]],
                                 [(api.message_to_dict(m), t) for m, t in messages[topic]],
                                 ERR(func, "topics"))

            topic, shift = next(iter(topics)), api.make_duration(1)
            logger.debug("Verifying %s.", NAME(func, "topic", "start_time=.."))
            start_time = messages[topic][-1][-1] + shift
            nomsgs = list(func(topic, start_time=start_time))
            self.assertFalse(nomsgs, ERR(func, "topic", "start_time=.."))
            logger.debug("Verifying %s.", NAME(func, "topic", "end_time=.."))
            end_time = messages[topic][0][-1] - shift
            nomsgs = list(func(topic, end_time=end_time))
            self.assertFalse(nomsgs, ERR(func, "topic", "end_time=.."))

            logger.debug("Verifying %s.", NAME(func, "topic", "..", "raw=True"))
            start_time, end_time = messages[topic][0][-1], messages[topic][-1][-1]
            for _, msg, _ in func(topic, start_time=start_time, end_time=end_time, raw=True):
                bbytes, typeclass = msg[1], msg[-1]
                self.assertIsInstance(bbytes, bytes, ERR(func, "topic", "..", "raw=True"))
                self.assertTrue(inspect.isclass(typeclass), ERR(func, "topic", "..", "raw=True"))

        func = bag.get_message_definition
        with self.subTest(NAME(func)):
            # Bag.get_message_definition(msg_or_type)
            logger.info("Testing %s.", NAME(func))
            msgcls = func("std_msgs/Bool")
            self.assertTrue(msgcls, ERR(func))
            self.assertIsInstance(msgcls, str, ERR(func))
            msgcls = func(std_msgs.msg.Bool)
            self.assertTrue(msgcls, ERR(func))
            self.assertIsInstance(msgcls, str, ERR(func))

        func = bag.get_message_type_hash
        with self.subTest(NAME(func)):
            # Bag.get_message_type_hash(msg_or_type)
            logger.info("Testing %s.", NAME(func))
            typehash = func("std_msgs/Bool")
            self.assertTrue(typehash, ERR(func))
            self.assertIsInstance(typehash, str, ERR(func))
            typehash = func(std_msgs.msg.Bool)
            self.assertTrue(typehash, ERR(func))
            self.assertIsInstance(typehash, str, ERR(func))

        func = bag.get_qoses
        with self.subTest(NAME(func)):
            # Bag.get_qoses(topic, typename)
            logger.info("Testing %s.", NAME(func))
            topic = next(iter(messages))
            typename = api.get_message_type(messages[topic][0][0])
            received = func(topic, typename)
            expected = type(None) if api.ROS1 else (list, type(None))
            self.assertIsInstance(received, expected, ERR(func))

        func = bag.get_type_and_topic_info
        with self.subTest(NAME(func)):
            # Bag.get_type_and_topic_info(topic_filters=None)
            logger.info("Testing %s.", NAME(func))
            topic = next(iter(messages))
            typename = api.get_message_type(messages[topic][0][0])
            msg_types, topics = func(topic_filters=topic)
            self.assertGreaterEqual(len(msg_types), 1, ERR(func, "topic_filters=sometopic"))
            self.assertEqual(len(topics), 1,   ERR(func, "topic_filters=sometopic"))
            self.assertIn(typename, msg_types, ERR(func, "topic_filters=sometopic"))
            self.assertIn(topic,    topics,    ERR(func, "topic_filters=sometopic"))

        func = bag.get_message_count
        with self.subTest(NAME(func)):
            # Bag.get_message_count(topic_filters=None)
            logger.info("Testing %s.", NAME(func))
            for count in (1, 2):
                topics = random.sample(list(messages), count)
                received = func(topic_filters=topics)
                expected = sum(len(messages[t]) for t in topics)
                self.assertEqual(received, expected, ERR(func, "topic_filters=sometopics"))

        bag.close()


        bag = bagcls(filename)
        bag.open()

        func = bag.get_topic_info
        with self.subTest(NAME(func)):
            # Bag.get_topic_info(counts=False)
            logger.info("Testing %s.", NAME(func))
            counts = list(func(counts=True).values())
            self.assertNotIn(None, counts, ERR(func, "counts=True"))


    def verify_bag_parameters_write(self, bagcls, filename):
        """Tests parameters to write functions of given Bag class."""
        logger.info("Verifying invoking Bag %r write methods with parameters.", bagcls)

        messages = {}  # {topic: [message, ]}
        bag = bagcls(filename, "w")
        bag.open()

        func = bag.write
        with self.subTest(NAME(func)):
            # Bag.write(topic, msg, t=None, raw=False)
            logger.info("Testing %s.", NAME(func))
            topicbase, typename = "/my/topic", "std_msgs/Bool"
            typehash, typeclass = api.get_message_type_hash(typename), std_msgs.msg.Bool
            for i in range(5):
                topic, msg = "%s/%s" % (topicbase, i % 2), std_msgs.msg.Bool(data=bool(i % 2))
                bbytes = api.serialize_message(msg)
                bag.write(topic, (typename, bbytes, typehash, typeclass), raw=True)
                messages.setdefault(topic, []).append(msg)

            bag.close()
            bag = bagcls(filename)
            bag.open()
            for topic, msg, stamp in bag:
                self.assertIn(topic, messages, ERR(func, "..", "raw=True"))
                self.assertTrue(any(api.message_to_dict(msg) == api.message_to_dict(m)
                                    for m in messages[topic]), ERR(func, "..", "raw=True"))

        bag.close()


    def test_global_library(self):
        """Tests grepros global functions: init(), grep(), source(), sink()."""
        grepros.init()  # Should not raise if called twice
        self.verify_grep()
        self.verify_sources_sinks()


    def test_rollover(self):
        """Tests rollover settings for sinks."""
        logger.debug("Verifying sink rollover.")
        SINKS = [grepros.BagSink, grepros.HtmlSink, grepros.SqliteSink] + \
                ([grepros.McapSink] if ".mcap" in api.BAG_EXTENSIONS else [])
        TEMPLATE = "test_%Y_%m_%d__%(index)s__%(index)s"
        OPTS = [  # [({..rollover opts..}, (min files, max files or None for undetermined))]
            (dict(rollover_size=2000),    (2, None)),
            (dict(rollover_count=40),     (2, 3)),
            (dict(rollover_duration=40),  (2, 3)),
        ]
        OPT_OVERRIDES = {grepros.McapSink: dict(rollover_size=None)}  # MCAP has 1MB cache

        START = api.to_time(12345)
        for cls in SINKS:
            EXT = api.BAG_EXTENSIONS[0] if cls is grepros.BagSink else cls.FILE_EXTENSIONS[0]
            WRITE, OUTDIR = next((x, os.path.dirname(x)) for x in [self.mkfile(EXT)])
            with self.subTest(NAME(cls)):
                logger.info("Testing %s rollover.", NAME(cls))
                for ropts, output_range in OPTS:
                    if cls in OPT_OVERRIDES and any(k in ropts for k in OPT_OVERRIDES[cls]):
                        ropts.update(OPT_OVERRIDES[cls])
                    if not any(ropts.values()): continue  # for ropts
                    logger.info("Testing %s rollover with %s.", NAME(cls), ropts)
                    SUFF = "__%s__" % "".join(random.sample(string.ascii_lowercase, 5))
                    SUFF += "_".join("%s=%s" % x for x in ropts.items())
                    template = os.path.join(OUTDIR, TEMPLATE + SUFF + EXT)

                    with cls(WRITE, write_options=dict(ropts, rollover_template=template)) as sink:
                        for i in range(100):
                            msg = std_msgs.msg.Bool(data=not i % 2)
                            sink.emit("my/topic%s" % (i % 2), msg, START + api.make_duration(i))

                    outputs = sorted(glob.glob(os.path.join(OUTDIR, "test_*" + SUFF + EXT)))
                    self._outnames.extend(outputs)
                    self.assertGreaterEqual(len(outputs), output_range[0],
                                            "Unexpected output files from %s." % NAME(cls))
                    if output_range[1] is not None:
                        self.assertLessEqual(len(outputs), output_range[1],
                                             "Unexpected output files from %s." % NAME(cls))
                    self.assertFalse(os.path.exists(WRITE), "Unexpected output from %s." % NAME(cls))
                    for name in map(os.path.basename, outputs):
                        self.assertTrue(re.search(r"^test_\d+_\d+_\d+__\d+__\d+", name),
                                        "Unexpected output file from %s." % NAME(cls))


    def verify_grep(self):
        """Tests grepros.grep()."""
        logger.info("Verifying reading bags and grepping messages, via grepros.grep().")
        messages = {}  # {topic: [msg, ]}
        args = dict(pattern=self.SEARCH_WORDS, topic="/match/this*", skip_topic="/not/this*",
                    file=self._bags, type="std_msgs/*", skip_type="std_msgs/Bool")
        for topic, msg, stamp, match, index in grepros.grep(**args):
            self.assertIn("/match/this", topic, ERR(grepros.grep, **args))
            self.assertNotIn("/not/this", topic, ERR(grepros.grep, **args))
            self.assertIn("std_msgs/", api.get_message_type(msg), ERR(grepros.grep, **args))
            self.assertNotIn("/Bool", api.get_message_type(msg), ERR(grepros.grep, **args))
            self.assertTrue(match, ERR(grepros.grep, **args))
            self.assertIsInstance(index, int, ERR(grepros.grep, **args))
            messages.setdefault(topic, []).append(msg)
        fulltext = "\n".join(str(m) for mm in messages.values() for m in mm)
        super(TestLibrary, self).verify_topics(messages, fulltext)


    def verify_sources_sinks(self):
        """Tests general Source and Sink API."""
        FUNC_TESTS = {  # {function: [({..kwargs..}, expected source class), ]}
            grepros.source: [
                (dict(app=True),                      grepros.AppSource),
                (dict(file=self._bags),               grepros.BagSource),
                (dict(live=True),                     grepros.LiveSource),
           ],
           grepros.sink: [
                (dict(app=True),                      grepros.AppSink),
                (dict(app=lambda *_: _),              grepros.AppSink),
                (dict(console=True),                  grepros.ConsoleSink),
                (dict(publish=True),                  grepros.LiveSink),
                (dict(app=True, console=True),        grepros.MultiSink),
                (dict(app=True, publish=True),        grepros.MultiSink),
                (dict(console=True, publish=True),    grepros.MultiSink),
                (dict(write=self._outname),           grepros.BagSink),
                (dict(write=self.mkfile(".csv")),     grepros.CsvSink),
                (dict(write=self.mkfile(".html")),    grepros.HtmlSink),
                (dict(write=self.mkfile(".sql")),     grepros.SqlSink),
                (dict(write=self.mkfile(".sqlite")),  grepros.SqliteSink),
           ],
        }
        if ".mcap" in api.BAG_EXTENSIONS: FUNC_TESTS[grepros.sink].append(
                (dict(write=self.mkfile(".mcap")),    grepros.McapSink),
        )
        if "parquet" in grepros.MultiSink.FORMAT_CLASSES: FUNC_TESTS[grepros.sink].append(
                (dict(write=self.mkfile(".parquet")), grepros.ParquetSink),
        )

        for func, args in FUNC_TESTS.items():
            logger.info("Verifying %s.", NAME(func))
            for kwargs, cls in args:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                with func(**kwargs) as source:
                    self.assertIsInstance(source, cls, ERR(func, **kwargs))


        logger.debug("Verifying grepros.PostgresSink failing for invalid configuration.",)
        with self.assertRaises(Exception, msg="Unexpected success from PostgresSink()."):
            with grepros.PostgresSink("username=nosuchuser dbname=nosuchdb") as sink:
                sink.validate()
        with self.assertRaises(Exception, msg="Unexpected success from PostgresSink()."):
            with grepros.sink("postgresql://nosuchuser/nosuchdb") as sink:
                sink.validate()

        logger.debug("Verifying grepros.AppSource and AppSink.")
        expected, received = [], []
        iterable = [("/my/topic", std_msgs.msg.String(data="my")), None]
        emitter  = lambda *a: received.append(a)
        source = grepros.AppSource(iterable=iterable)
        sink   = grepros.AppSink(emit=emitter)
        for topic, msg, stamp in source:
            sink.emit(topic, msg, stamp)
            expected.append((topic, msg, stamp))
        source.push("/other/topic", std_msgs.msg.String(data="other"))
        source.push("/third/topic", std_msgs.msg.String(data="third"))
        source.push(None)
        for topic, msg, stamp in source:
            sink.emit(topic, msg, stamp)
            expected.append((topic, msg, stamp))
        self.assertEqual(len(received), len(expected), ERR(type(sink)))
        for a, b in zip(received, expected):
            self.assertEqual(a[:2], b[:2], ERR(type(sink)))


    def mkfile(self, suffix):
        """Returns temporary filename with given suffix, deleted in teardown."""
        name = tempfile.NamedTemporaryFile(suffix=suffix).name
        self._outnames.append(name)
        return name


if "__main__" == __name__:
    TestLibrary.run_rostest()

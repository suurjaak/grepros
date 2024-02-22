#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: nth and start/end filters on all message sources.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     15.02.2024
@modified    22.02.2024
------------------------------------------------------------------------------
"""
import collections
import logging
import os
try: import queue  # Py3
except ImportError: import Queue as queue  # Py2
import signal
import sys
import threading
import tempfile
import time

import std_msgs.msg

import grepros
from grepros.api import make_duration, make_time, message_to_dict, to_sec
from grepros.common import ensure_namespace


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase
from test.testbase import NAME

logger = logging.getLogger()


class TestSourceLimits(testbase.TestBase):
    """Tests filters on various sources."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

    ## Total number of messages to generate in each test
    MESSAGES_PER_TEST = 100

    ## Number of messages to provide in topic per Nth time interval
    MESSAGES_IN_INTERVAL = 5

    ## Number of topics to produce messages in
    TOPICS_TOTAL = 4

    ## Keyword argumnents for source classes
    SOURCE_ARGS = [{"nth_interval": 0.1},
                   {"nth_interval": 0.1, "start_index": 2},
                   {"nth_message": 5},
                   {"nth_message": 5, "start_index": 2},
                   {"nth_message": 5, "start_time": "+0.5"},
                   {"nth_message": 5, "start_time": "-0.5"},
                   {"nth_message": 5, "start_time": "+0.5", "end_time": "+5"},
                   {"nth_message": 5, "start_time": "+0.3", "end_time": "-0.2"},
                   {"nth_message": 5, "end_index": MESSAGES_PER_TEST // TOPICS_TOTAL - 3},
                   {"nth_message": 5, "start_index": 2,
                    "end_index": MESSAGES_PER_TEST // TOPICS_TOTAL - 3}]

    ## Keyword arguments for search
    SCANNER_ARGS = [{},
                    {"nth_match": 2}]


    def __init__(self, *args, **kwargs):
        super(TestSourceLimits, self).__init__(*args, **kwargs)
        self._pubs = {}  # {topic: publisher)
        self._outnames = []  # [bagfile path, ]
        grepros.init()
        self.init_node()


    def setUp(self):
        """Opens publishers."""
        super(TestSourceLimits, self).setUp()
        logger.debug("Opening %s publishers.", self.TOPICS_TOTAL)
        for topic in ("/topic%s" % (i + 1) for i in range(self.TOPICS_TOTAL)):
            self._pubs[topic] = self.create_publisher(topic, std_msgs.msg.Header)


    def tearDown(self):
        """Closes publishers and unlinks temporary files, if any."""
        for topic in list(self._pubs):
            try: self._pubs.pop(topic).unregister()
            except Exception: pass
        while self._outnames:
            try: os.unlink(self._outnames.pop())
            except Exception: pass
        super(TestSourceLimits, self).tearDown()


    def test_source_limits(self):
        """Tests reading from sources with limit filters."""
        logger.info("Verifying reading with limit filters.")

        HANDLERS = {grepros.AppSource:   self.verify_appsource_limits,
                    grepros.BagSource:   self.verify_bagsource_limits,
                    grepros.TopicSource: self.verify_topicsource_limits}
        SKIP_ARGS = {grepros.AppSource:   ("start_time", "end_time"),  # Tricky to make stable test
                     grepros.TopicSource: ("start_time", "end_time", "nth_interval")}
        for cls, handler in HANDLERS.items():
            for source_args in self.SOURCE_ARGS:
                if cls in SKIP_ARGS and set(SKIP_ARGS[cls]) & set(source_args):
                    continue  # for source_args
                logger.info("Verifying reading from %s.", NAME(cls, **source_args))
                with self.subTest(NAME(cls, **source_args)):
                    handler(source_args)
                    for scanner_args in self.SCANNER_ARGS:
                        logger.info("Verifying reading from %s via %s.",
                                    NAME(cls, **source_args), NAME(grepros.Scanner, **scanner_args))
                        handler(source_args, scanner_args)


    def verify_appsource_limits(self, source_args, scanner_args=None):
        """Tests reading from AppSource with limit filters."""
        all_args = dict(source_args, **scanner_args or {})
        with grepros.AppSource(source_args) as source:
            expecteds = self.make_producer(all_args, self.make_emitter(source))
            self.verify_results(grepros.AppSource, source, expecteds, source_args, scanner_args)


    def verify_bagsource_limits(self, source_args, scanner_args=None):
        """Tests reading from BagSource with limit filters."""
        outname = tempfile.NamedTemporaryFile(suffix=self.BAG_SUFFIX).name
        self._outnames.append(outname)
        source = grepros.BagSource(source_args, file=outname)
        all_args = dict(source_args, **scanner_args or {})
        expecteds = list(self.make_producer(all_args, self.make_emitter(source)))  # Write bag
        with source:
            self.verify_results(grepros.BagSource, source, expecteds, source_args, scanner_args)


    def verify_topicsource_limits(self, source_args, scanner_args=None):
        """Tests reading from live with limit filters."""
        clify = lambda x: "--%s%s" % ("every-" if x.startswith("nth_") else "", x.replace("_", "-"))

        # Run process that greps live and writes all to bag; verify from bag
        outname = tempfile.NamedTemporaryFile(suffix=self.BAG_SUFFIX).name
        cmd = ["grepros", "--live", "--queue-size-in", str(self.MESSAGES_PER_TEST // 2),
               "--topic", "/topic*", "--write", outname]
        cmd.extend(sum(([clify(k), str(v)] for k, v in source_args.items()), []))
        cmd.extend(sum(([clify(k), str(v)] for k, v in (scanner_args or {}).items()), []))

        self._cmd = cmd
        self._outnames.append(outname)
        self.run_command(communicate=False)
        all_args = dict(source_args, **scanner_args or {})
        expecteds = list(self.make_producer(all_args, self.make_emitter(grepros.TopicSource)))
        time.sleep(1)  # Allow subprocess to receive all messages
        logger.debug("Closing subprocess.")
        os.kill(self._proc.pid, signal.SIGINT)
        time.sleep(1)  # Allow subprocess to finalize bagfile

        with grepros.BagSource(file=outname) as source:
            self.verify_results(grepros.TopicSource, source, expecteds, source_args, scanner_args)


    def verify_results(self, cls, source, expecteds, source_args, scanner_args=None):
        """Verifies messages from source being as expected."""
        NAMETEXT = NAME(cls, **source_args)
        expecteds_provider, receiveds_provider, count_expected = iter(expecteds), source, 0
        if scanner_args is not None:
            NAMETEXT += " via %s" % NAME(grepros.Scanner, **scanner_args)
            if cls is not grepros.TopicSource:  # Applied in subprocess already
                receiveds_provider = grepros.Scanner(source_args, **scanner_args).find(source)

        logger.debug("Checking expected messages vs received messages.")
        for received in receiveds_provider:  # (topic, msg, stamp)
            expected, received = next(expecteds_provider), tuple(received)[:3]
            if isinstance(source, grepros.BagSource):  # Dictify as ROS1 bag makes its own classes
                expected = (expected[0], message_to_dict(expected[1]), expected[2])
                received = (received[0], message_to_dict(received[1]), received[2])
            if cls is grepros.TopicSource:  # Drop timestamps from live as inevitably different
                expected, received = (x[:2] for x in (expected, received))
            self.assertEqual(expected, received, "Unexpected result from %s." % NAMETEXT)
            count_expected += 1
        self.assertTrue(count_expected, "No results from %s." % NAMETEXT)
        self.assertEqual(count_expected + len(list(expecteds_provider)), count_expected,
                         "Unexpected number of result from %s." % NAMETEXT)


    def make_producer(self, kwargs, emit):
        """Runs thread ensuring messages in source, returns generator for expected source output."""

        def producer():
            ARG_DEFAULTS = dict(grepros.Scanner.DEFAULT_ARGS, **grepros.Source.DEFAULT_ARGS)
            args = ensure_namespace(kwargs, ARG_DEFAULTS)
            INTERVAL_MODULUS = self.MESSAGES_IN_INTERVAL - 1
            MESSAGE_INTERVAL = (args.NTH_INTERVAL or 0.1) / INTERVAL_MODULUS / self.TOPICS_TOTAL
            START_INDEX, END_INDEX = (args.START_INDEX or 1), (args.END_INDEX or 0)
            if START_INDEX < 0: START_INDEX += self.MESSAGES_PER_TEST // self.TOPICS_TOTAL
            if END_INDEX   < 0: END_INDEX   += self.MESSAGES_PER_TEST // self.TOPICS_TOTAL
            STAMP0 = make_time(int(time.time()))  # Drop nanos to avoid precision errors with floats
            STAMPN = make_time(to_sec(STAMP0) + (self.MESSAGES_PER_TEST - 1) * MESSAGE_INTERVAL)
            START_TIME, END_TIME = (float(x or 0) for x in (args.START_TIME, args.END_TIME))
            START_TIME, END_TIME = (make_time(x + to_sec(STAMP0 if x > 0 else STAMPN)) if x else 0
                                    for x in (START_TIME, END_TIME))

            logger.debug("Starting to produce test input messages.")
            expected_counts, match_counts = (collections.Counter() for _ in range(2))
            for i in range(self.MESSAGES_PER_TEST):
                index_in_topic, topic_index = (x + 1 for x in divmod(i, self.TOPICS_TOTAL))
                topic = "/topic%s" % topic_index
                msg = std_msgs.msg.Header(frame_id="msg #%s (#%s in %s)" %
                                                   (i + 1, index_in_topic, topic))
                stamp = STAMP0 + make_duration(i * MESSAGE_INTERVAL)
                do_expect = True

                if START_INDEX:
                    do_expect = (index_in_topic >= START_INDEX)
                if END_INDEX and do_expect:
                    do_expect = (index_in_topic <= END_INDEX)
                if START_TIME and do_expect:
                    do_expect = (stamp >= START_TIME)
                if END_TIME and do_expect:
                    do_expect = (stamp <= END_TIME)
                if args.NTH_MESSAGE and do_expect:  # Expect (START, START+NTH, +NTH*2, ..)
                    shift = args.START_INDEX if (args.START_INDEX or 0) > 1 else 0
                    do_expect = not expected_counts[topic] or index_in_topic == START_INDEX or \
                                not (index_in_topic - shift) % args.NTH_MESSAGE
                if args.NTH_INTERVAL and do_expect:  # Expect (START, START+4, +4*2, ..)
                    do_expect = not (index_in_topic - START_INDEX) % INTERVAL_MODULUS
                if args.NTH_MATCH > 1 and do_expect:
                    match_counts.update([topic])
                    do_expect = not (match_counts[topic] - 1) % args.NTH_MATCH

                if do_expect: expecteds.put((topic, msg, stamp)), expected_counts.update([topic])
                emit(topic, msg, stamp)

            emit(None), expecteds.put(None)  # Signal completion
            logger.debug("Finished producing test input messages.")

        def provider():
            while True:
                x = expecteds.get()
                if x is None: break  # while
                yield x

        expecteds = queue.Queue()
        runner = threading.Thread(target=producer)
        runner.daemon = True
        runner.start()
        return provider()


    def make_emitter(self, source_or_cls):
        """Returns function for emitting messages to source."""
        if isinstance(source_or_cls, grepros.AppSource):
            return source_or_cls.push

        if isinstance(source_or_cls, grepros.TopicSource) or source_or_cls is grepros.TopicSource:
            def publish(topic, msg=None, stamp=None):
                if not topic: return
                self._pubs[topic].publish(msg)
                self.spin_once(0.02)
            logger.debug("Waiting for subscriptions.")
            while any(not x.get_num_connections() for x in self._pubs.values()):
                self.spin_once(0.2)
            return publish

        if isinstance(source_or_cls, grepros.BagSource) or source_or_cls is grepros.BagSource:
            def emit(topic, msg=None, stamp=None):
                bag.write(topic, msg, stamp) if topic else bag.close()
            bag = grepros.Bag(source_or_cls.args.FILE[0], mode="w")
            return emit



if "__main__" == __name__:
    TestSourceLimits.run_rostest()

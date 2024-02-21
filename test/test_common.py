#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: functionality of grepros.common.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.02.2024
@modified    21.02.2024
------------------------------------------------------------------------------
"""
from argparse import Namespace
import datetime
import io
import logging
import os
import re
import sys
import tempfile

from grepros import common

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from test import testbase

logger = logging.getLogger()


class TestCommon(testbase.TestBase):
    """Tests common utilities."""

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]


    def test_common(self):
        """Tests common functions."""
        ARGS = lambda *a, **w: ", ".join(filter(bool, [", ".join(map(repr, a)),
                                                       ", ".join("%s=%r" % x for x in w.items())]))
        NAME = lambda f, *a, **w: "%s.%s(%s)" % (f.__module__, f.__name__, ARGS(*a, **w))
        ERR  = lambda f, *a, **w: "Unexpected result from %s." % NAME(f, *a, **w)
        logger.info("Testing common functions.")

        VALS = [(dict(v=12.1),                  str(12.1)),
                (dict(v="3.0"),                 "3"),
                (dict(v="30"),                  "30"),
                (dict(v="0"),                   "0"),
                (dict(v="00"),                  "00"),
                (dict(v="0.000"),               "0"),
                (dict(v="30.0"),                "30"),
                (dict(v="30.00", replace="_"),  "30.__"),
        ]
        func = common.drop_zeros
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        VALS = [(("abc", 2),       ".."),
                (("abc", 3),       "abc"),
                (("a",   0),       "a"),
                (("a",  -1),       "a"),
                (("",   10),       ""),
                (("abc", 2, ""),   "ab"),
                (("abc", 2, "_"),  "a_"),
        ]
        func = common.ellipsize
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for args, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, *args))
                self.assertEqual(func(*args), expected, ERR(func, *args))

        VALS = [
            (dict(val={}),                                         Namespace()),
            (dict(val=None),                                       Namespace()),
            (dict(val=None, a=1),                                  Namespace(A=1)),
            (dict(val=Namespace()),                                Namespace()),
            (dict(val=dict(a=1), defaults=dict(a=2)),              Namespace(A=1)),
            (dict(val=dict(a=1), defaults=dict(a=2), a=3),         Namespace(A=3)),
            (dict(val=dict(a=1, b=2), defaults=dict(a=[], b=[])),  Namespace(A=[1], B=[2])),
            (dict(val=dict(a={"x_y": 0}, b={"_x": 1, "y_": 2, "x_y_z": 3}), dashify=["a", "B"]),
             Namespace(A={"x-y": 0}, B={"_x": 1, "y_": 2, "x-y_z": 3})),
        ]
        func = common.ensure_namespace
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        VALS = [
            (dict(dct={"a": "x", "b": "y"}),                             {"a": "x", "b": "y"}),
            (dict(dct={"a": "x", "b": "y"}, keys=["*"]),                 {"a": "x", "b": "y"}),
            (dict(dct={"a": "x", "b": "y"}, keys=["*a"]),                {"a": "x"}),
            (dict(dct={"a": "x", "b": "y"}, keys="ab"),                  {"a": "x", "b": "y"}),
            (dict(dct={"a": "x", "b": "y"}, values=["x"]),               {"a": "x"}),
            (dict(dct={"a": ["x", "y"]}, keys=["a"], values=["x"]),      {"a": ["x"]}),
            (dict(dct={"a": "x", "b": "y"}, reverse=True),               {"a": "x", "b": "y"}),
            (dict(dct={"a": "x", "b": "z"}, keys=["c"], values=["z"], reverse=True),
                                                                         {"a": "x"}),
            (dict(dct={"a": "x", "b": "y"}, keys=["*"], reverse=True),   {}),
        ]
        func = common.filter_dict
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        PARENT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        VALS = [
            (dict(names=["one*", "two*"], paths=[PARENT], suffixes=[self.BAG_SUFFIX], recurse=True),
             [os.path.join(PARENT, "test", "data", "%s%s" % (x, self.BAG_SUFFIX))
              for x in ("one", "two")]),
            (dict(names=["LI*"], paths=[PARENT, os.path.join(PARENT, "doc")], suffixes=[".md"]),
             [os.path.join(PARENT, "LICENSE.md"), os.path.join(PARENT, "doc", "LIBRARY.md")]),
            (dict(names=["generate*"], paths=[PARENT], suffixes=[".sh"], recurse=True),
             [os.path.join(PARENT, "doc", "generate.sh")]),
            (dict(names=["generate*"], paths=[PARENT], skip_suffixes=[".sh", ".html", ".js"],
                  recurse=True),
             [os.path.join(PARENT, "scripts", "generate_msgs.py")]),
        ]
        func = common.find_files
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                files = list(func(**kwargs))
                self.assertEqual(expected, files, ERR(func, **kwargs))

        VALS = [
            (dict(size=0),                                "0 bytes"),
            (dict(size=1, inter=""),                      "1byte"),
            (dict(size=1.5),                              "1.5 bytes"),
            (dict(size=1.1, precision=0),                 "1 byte"),
            (dict(size=float("nan")),                     ""),
            (dict(size=float("inf")),                     ""),
            (dict(size=2**20),                            "1 MB"),
            (dict(size=2**20, strip=False),               "1.00 MB"),
            (dict(size=2**20, strip=False, precision=4),  "1.0000 MB"),
        ]
        func = common.format_bytes
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        VALS = [(datetime.timedelta(seconds=0),             "0sec"),
                (datetime.timedelta(seconds=60),            "1min"),
                (datetime.timedelta(seconds=90),            "1min 30sec"),
                (datetime.timedelta(seconds=3900),          "1h 5min"),
                (datetime.timedelta(seconds=24*3601),       "1d 24sec"),
                (datetime.timedelta(seconds=1000*24*3600),  "1000d"),
        ]
        func = common.format_timedelta
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for arg, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, arg))
                self.assertEqual(func(arg), expected, ERR(func, arg))

        obj = Namespace()
        VALS = [(Namespace,    "argparse.Namespace"),
                (obj,          "argparse.Namespace<0x%x>" % id(obj)),
                (dict.update,  "dict.update"),
                (common,       "grepros.common"),
        ]
        func = common.get_name
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for arg, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, arg))
                self.assertEqual(func(arg), expected, ERR(func, arg))

        VALS = [((self.test_common, "nope"),             False),
                ((self.test_common, "self"),             True),
                ((datetime.timedelta.__init__, "days"),  True),
                ((lambda x: x, "x"),                     True),
                ((lambda *x: x, "x"),                    False),
                ((lambda **x: x, "y"),                   True),
        ]
        func = common.has_arg
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for args, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, *args))
                self.assertEqual(func(*args), expected, ERR(func, *args))

        VALS = [("os",                           os),
                ("grepros.common",               common),
                ("datetime.timedelta",           datetime.timedelta),
                ("datetime.datetime.isoformat",  datetime.datetime.isoformat),
                ("grepros.common.TextWrapper",   common.TextWrapper),
        ]
        func = common.import_item
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for arg, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, arg))
                self.assertEqual(func(arg), expected, ERR(func, arg))

        VALS = [([],        True),
                ((),        True),
                ({},        True),
                ("",        True),
                (range(5),  True),
                (5,         False),
                (self,      False),
        ]
        func = common.is_iterable
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for arg, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, arg))
                self.assertEqual(func(arg), expected, ERR(func, arg))

        VALS = [(None,          False),
                ("",            False),
                (io.BytesIO(),  True),
        ]
        func = common.is_stream
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for arg, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, arg))
                self.assertEqual(func(arg), expected, ERR(func, arg))

        VALS = [self.DATA_DIR,
                os.path.join(tempfile.gettempdir(), "a", "b", "c"),
        ]
        func = common.makedirs
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for arg in VALS:
                logger.debug("Verifying %s.", NAME(func, arg))
                func(arg)
                self.assertTrue(os.path.exists(arg), ERR(func, arg))

        VALS = [(1,       True),
                ("ab",    True),
                ((1, 2),  True),
                ([1, 2],  False),
                ({},      False),
        ]
        func = common.memoize
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            mylist = []
            wrappedfunc = func(mylist.append)
            self.assertTrue(callable(wrappedfunc), ERR(func, arg))
            for arg, cached in VALS:
                wrappedfunc(arg)
                wrappedfunc(arg)
                self.assertEqual(1 if cached else 2, mylist.count(arg), ERR(func, arg))

        VALS = [(({}, {"b": 1}),                      {"b": 1}),
                (({"a": 1}, {"b": 2}),                {"a": 1, "b": 2}),
                (({"a": 1}, {"a": {}}),               {"a": {}}),
                (({"a": {"b": 2}}, {"a": {"c": 3}}),  {"a": {"b": 2, "c": 3}}),
        ]
        func = common.merge_dicts
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for args, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, *args))
                self.assertEqual(func(*args), expected, ERR(func, *args))

        VALS = [(dict(spans=[(0, 0), (1, 1)]),                    [(0, 0), (1, 1)]),
                (dict(spans=[(0, 0), (1, 1)], join_blanks=True),  [(0, 1)]),
                (dict(spans=[(2, 4), (4, 8), (0, 1), (2, 3)], ),  [(0, 1), (2, 8)]),
        ]
        func = common.merge_spans
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        VALS = [("2000",                     datetime.datetime(2000,  1,  1,  0,  0,  0,      0)),
                ("200012",                   datetime.datetime(2000, 12,  1,  0,  0,  0,      0)),
                ("2000-12-20",               datetime.datetime(2000, 12, 20,  0,  0,  0,      0)),
                ("2000111213",               datetime.datetime(2000, 11, 12, 13,  0,  0,      0)),
                ("20001112112233",           datetime.datetime(2000, 11, 12, 11, 22, 33,      0)),
                ("200011121122334",          datetime.datetime(2000, 11, 12, 11, 22, 33,      4)),
                ("20001112112233400",        datetime.datetime(2000, 11, 12, 11, 22, 33,    400)),
                ("20001112112233400000000",  datetime.datetime(2000, 11, 12, 11, 22, 33, 400000)),
        ]
        func = common.parse_datetime
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for arg, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, arg))
                self.assertEqual(func(arg), expected, ERR(func, arg))

        VALS = [(dict(value="12.3"),  12),
                (dict(value=b"1024K", suffixes=dict(zip("KMGT", [2**10, 2**20, 2**30, 2**40]))),
                 2**20),
                (dict(value="1024.5K", suffixes=dict(zip("KMGT", [2**10, 2**20, 2**30, 2**40]))),
                 2**20 + 512),
        ]
        func = common.parse_number
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        VALS = [(dict(text="foo*.bar"),             re.compile("foo.*\\.bar", re.I)),
                (dict(text="*foo*"),                re.compile(".*foo.*", re.I)),
                (dict(text="*foo*", end=True),      re.compile(".*foo.*$", re.I)),
                (dict(text="$foo$", wildcard="$"),  re.compile(".*foo.*", re.I)),
                (dict(text="foo*/bar", sep="/"),    re.compile("foo.*/bar", re.I)),
                (dict(text="foo*\\bar", sep="\\"),  re.compile("foo.*\\\\bar", re.I)),
                (dict(text="a.b.c", intify=True),   re.compile(r"a(\.\d+)?\.b(\.\d+)?\.c", re.I)),
        ]
        func = common.path_to_regex
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        VALS = [(dict(word="day"),                                "days"),
                (dict(word="DAY"),                                "DAYS"),
                (dict(word="DAy"),                                "DAys"),
                (dict(word="tax"),                                "taxes"),
                (dict(word="elegy"),                              "elegies"),
                (dict(word="ELEGY"),                              "ELEGIES"),
                (dict(word="pass"),                               "passes"),
                (dict(word="day", items=0),                       "0 days"),
                (dict(word="day", items=[]),                      "0 days"),
                (dict(word="day", items=1),                       "1 day"),
                (dict(word="day", items=range(5)),                "5 days"),
                (dict(word="day", items=0, numbers=False),        "days"),
                (dict(word="day", items=1, numbers=False),        "day"),
                (dict(word="day", items=5, numbers=False),        "days"),
                (dict(word="day", items=1, single="a"),           "a day"),
                (dict(word="day", items=1000),                    "1,000 days"),
                (dict(word="day", items=1000, sep=""),            "1000 days"),
                (dict(word="day", items=100, pref="~"),           "~100 days"),
                (dict(word="day", items=100, suf="+"),            "100+ days"),
                (dict(word="day", items=100, pref="~", suf="+"),  "~100+ days"),
        ]
        func = common.plural
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        PKG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        VALS = [(dict(pathname=os.path.join(self.DATA_DIR, "one" + self.BAG_SUFFIX)),
                 os.path.join(self.DATA_DIR, "one.2" + self.BAG_SUFFIX)),
                (dict(pathname=os.path.join(PKG_DIR, "resource", "grepros")),
                 os.path.join(PKG_DIR, "resource", "grepros.2")),
                (dict(pathname=os.path.join(PKG_DIR, "resource", "grepros"), empty_ok=True),
                 os.path.join(PKG_DIR, "resource", "grepros")),
        ]
        func = common.unique_path
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))

        VALS = [((io.BytesIO(), "r"),                                          True),
                ((io.BytesIO(), "w"),                                          True),
                ((io.BytesIO(), "a"),                                          True),
                ((io.StringIO(), "r"),                                         False),
                ((tempfile.TemporaryFile(), "r"),                              True),
                ((os.path.join(self.DATA_DIR, "one" + self.BAG_SUFFIX), "r"),  True),
        ]
        func = common.verify_io
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for args, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, *args))
                self.assertEqual(func(*args), expected, ERR(func, *args))

        VALS = [(dict(text="foo*bar"),      re.compile("foo.*bar", re.I)),
                (dict(text="*a*b*"),        re.compile(".*a.*b.*", re.I)),
                (dict(text="a", end=True),  re.compile("a$", re.I)),
                (dict(text="", end=True),   re.compile("$", re.I)),
        ]
        func = common.wildcard_to_regex
        with self.subTest(NAME(func)):
            logger.info("Testing %s.", NAME(func))
            for kwargs, expected in VALS:
                logger.debug("Verifying %s.", NAME(func, **kwargs))
                self.assertEqual(func(**kwargs), expected, ERR(func, **kwargs))


if "__main__" == __name__:
    TestCommon.run_rostest()

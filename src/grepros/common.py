# -*- coding: utf-8 -*-
## @namespace grepros.common
"""
Common utilities.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    30.10.2021
------------------------------------------------------------------------------
"""
from __future__ import print_function
try: import builtins  # Py3
except ImportError: import __builtin__ as builtins  # Py2
import datetime
import glob
import math
import os
import random
import re
import shutil
import sys
import textwrap
import time
try: import curses
except ImportError: curses = None

import genpy
import rospy


ROS_NUMERIC_TYPES = ["byte", "char", "int8", "int16", "int32", "int64", "uint8",
                     "uint16", "uint32", "uint64", "float32", "float64", "bool"]
ROS_BUILTIN_TYPES = ROS_NUMERIC_TYPES + ["string"]


class MatchMarkers(object):
    """Highlight markers for matches in message values."""

    ID    = "%08x" % random.randint(1, 1E9)  ## Unique marker for match highlight replacements
    START = "<%s>"  % ID                     ## Temporary placeholder in front of match
    END   = "</%s>" % ID                     ## Temporary placeholder at end of match
    EMPTY = START + END                      ## Temporary placeholder for empty string match
    EMPTY_REPL = "%s''%s" % (START, END)     ## Replacement for empty string match


class ConsolePrinter(object):
    """Prints to console, supports color output."""

    STYLE_RESET     = "\x1b(B\x1b[m"            ## Default color+weight
    STYLE_HIGHLIGHT = "\x1b[31m"                ## Red
    STYLE_LOWLIGHT  = "\x1b[38;2;105;105;105m"  ## Dim gray
    STYLE_SPECIAL   = "\x1b[35m"                ## Purple
    STYLE_SPECIAL2  = "\x1b[36m"                ## Cyan
    STYLE_ERROR     = "\x1b[31m\x1b[2m"         ## Dim red

    NOCOLOR_HIGHLIGHT_WRAPPERS = "**", "**"  ## Default highlight wrappers if not color output

    HIGHLIGHT_START, HIGHLIGHT_END = STYLE_HIGHLIGHT, STYLE_RESET  ## Matched value wrappers
    LOWLIGHT_START,  LOWLIGHT_END  = STYLE_LOWLIGHT,  STYLE_RESET  ## Metainfo wrappers
    PREFIX_START,    PREFIX_END    = STYLE_SPECIAL,   STYLE_RESET  ## Content line prefix wrappers
    ERROR_START,     ERROR_END     = STYLE_ERROR,     STYLE_RESET  ## Error message wrappers
    SEP_START,       SEP_END       = STYLE_SPECIAL2,  STYLE_RESET  ## Filename prefix separator wrappers

    VERBOSE = False  ## Whether to print debug information

    WIDTH = 80       ## Console width in characters, updated from shutil and curses

    PRINTS = {}      ## {sys.stdout: number of texts printed, sys.stderr: ..}

    @classmethod
    def configure(cls, args):
        """
        Initializes terminal for color output, or disables color output if unsupported.

        @param   args                 arguments object like argparse.Namespace
        @param   args.COLOR           "never", "always", or "auto" for when supported by TTY
        @param   args.MATCH_WRAPPER   string to wrap around matched values,
                                      both sides if one value, start and end if more than one,
                                      or no wrapping if zero values (default ** in colorless output)
        @param   args.VERBOSE         whether to print debug information
        """
        cls.VERBOSE = args.VERBOSE
        try: cls.WIDTH = shutil.get_terminal_size().columns  # Py3
        except Exception: pass  # Py2
        do_color = ("never" != args.COLOR)
        try:
            curses.setupterm()
            if do_color and not sys.stdout.isatty():
                raise Exception()
        except Exception:
            do_color = ("always" == args.COLOR)
        try:
            if sys.stdout.isatty():
                cls.WIDTH = curses.initscr().getmaxyx()[1]
            curses.endwin()
        except Exception: pass

        if do_color:
            cls.HIGHLIGHT_START, cls.HIGHLIGHT_END = cls.STYLE_HIGHLIGHT, cls.STYLE_RESET
            cls.LOWLIGHT_START,  cls.LOWLIGHT_END  = cls.STYLE_LOWLIGHT,  cls.STYLE_RESET
            cls.PREFIX_START,    cls.PREFIX_END    = cls.STYLE_SPECIAL,   cls.STYLE_RESET
            cls.SEP_START,       cls.SEP_END       = cls.STYLE_SPECIAL2,  cls.STYLE_RESET
            cls.ERROR_START,     cls.ERROR_END     = cls.STYLE_ERROR,     cls.STYLE_RESET
        else:
            cls.HIGHLIGHT_START, cls.HIGHLIGHT_END = "", ""
            cls.LOWLIGHT_START,  cls.LOWLIGHT_END  = "", ""
            cls.ERROR_START,     cls.ERROR_END     = "", ""
            cls.SEP_START,       cls.SEP_END       = "", ""
            cls.PREFIX_START,    cls.PREFIX_END    = "", ""

        WRAPS = args.MATCH_WRAPPER
        WRAPS = cls.NOCOLOR_HIGHLIGHT_WRAPPERS if WRAPS is None and not do_color else WRAPS
        WRAPS = ((WRAPS or [""]) * 2)[:2]
        cls.HIGHLIGHT_START = cls.HIGHLIGHT_START + WRAPS[0]
        cls.HIGHLIGHT_END   = WRAPS[1] + cls.HIGHLIGHT_END


    @classmethod
    def print(cls, text, *args, **kwargs):
        """Prints text, formatted with args and kwargs."""
        fileobj = kwargs.pop("__file", sys.stdout)
        pref, suff = kwargs.pop("__prefix", ""), kwargs.pop("__suffix", "")
        text = str(text)
        try: text = text % args if args else text
        except Exception: pass
        try: text = text % kwargs if kwargs else text
        except Exception: pass
        try: text = text.format(*args, **kwargs) if args or kwargs else text
        except Exception: pass
        print(pref + text + suff, file=fileobj)
        cls.PRINTS[fileobj] = cls.PRINTS.get(fileobj, 0) + 1


    @classmethod
    def error(cls, text, *args, **kwargs):
        """Prints error to stderr, formatted with args and kwargs, in error colors if supported."""
        KWS = dict(__file=sys.stderr, __prefix=cls.ERROR_START, __suffix=cls.ERROR_END)
        cls.print(text, *args, **dict(kwargs, **KWS))


    @classmethod
    def debug(cls, text, *args, **kwargs):
        """
        Prints debug text to stderr if verbose.

        Formatted with args and kwargs, in lowlight colors if supported.
        """
        if cls.VERBOSE:
            KWS = dict(__file=sys.stderr, __prefix=cls.LOWLIGHT_START, __suffix=cls.LOWLIGHT_END)
            cls.print(text, *args, **dict(kwargs, **KWS))



class ROSNode(object):
    """Interface to ROS master."""

    ## Node base name for connecting to ROS (will be anonymized).
    NAME = "grepros"

    ## Seconds between checking whether ROS master is available.
    SLEEP_INTERVAL = 0.5

    ## rospy.MasterProxy instance
    master = None

    @classmethod
    def validate(cls):
        """Returns whether ROS environment is set, prints error if not."""
        missing = [k for k in ("ROS_DISTRO", "ROS_MASTER_URI", "ROS_ROOT",
                   "ROS_PACKAGE_PATH", "ROS_VERSION") if not os.getenv(k)]
        if missing:
            ConsolePrinter.error("ROS environment not sourced: missing %s.",
                                 ", ".join(sorted(missing)))
        return not missing

    @classmethod
    def init(cls):
        """Initializes ROS node, blocks until connection established."""
        if not cls.master:
            cls.master = rospy.client.get_master()
            available = None
            while not available:
                try: cls.master.getSystemState()
                except Exception:
                    if available is None:
                        ConsolePrinter.error("Unable to register with master. Will keep trying.")
                    available = False
                    time.sleep(cls.SLEEP_INTERVAL)
                else: available = True
            rospy.init_node(cls.NAME, anonymous=True, disable_signals=True)


class TextWrapper(textwrap.TextWrapper):
    """
    textwrap.TextWrapper that supports custom substring widths in line width calculation.

    Intended for wrapping text containing ANSI control codes.
    """

    ## Defaults for textwrap.TextWrapper
    DEFAULTS = dict(break_long_words=False, break_on_hyphens=False, expand_tabs=False,
                    replace_whitespace=False, subsequent_indent="  ")
    REALLEN = builtins.len

    def __init__(self, custom_widths=None, **kwargs):
        """
        @param   custom_widths  {substring: len} to use in line width calculation
        @param   kwargs         arguments for textwrap.TextWrapper
        """
        textwrap.TextWrapper.__init__(self, **dict(self.DEFAULTS, **kwargs))
        self._customs = custom_widths or {}
        self._realwidth = self.width  # Inherited textwrap.TextWrapper.width


    def len(self, v):
        """
        Returns the number of items of a sequence or collection.

        Uses configured custom substring widths if string value.
        """
        result = self.REALLEN(v)
        for s, l in self._customs.items() if isinstance(v, str) else ():
            result -= v.count(s) * (self.REALLEN(s) - l)
        return result


    def reserve_width(self, reserved=""):
        """Decreases the configured width by given amount (number or string)."""
        reserved = self.len(reserved) if isinstance(reserved, str) else reserved
        self.width = self._realwidth - reserved


    def wrap(self, text):
        """Returns a list of wrapped text lines, without final newlines."""
        builtins.len = self.len
        try:
            return textwrap.TextWrapper.wrap(self, text)
        finally:
            builtins.len = self.REALLEN


def drop_zeros(v):
    """Drops trailing zeros and empty decimal separator, if any."""
    v = v.to_sec() if isinstance(v, genpy.TVal) else v
    return re.sub(r"\.?0+$", "", str(v))


def filter_dict(dct, keys=(), values=(), reverse=False):
    """
    Filters string dictionary by keys and values.

    Retains only entries that find a match (supports * wildcards);
    if reverse, retains only entries that do not find a match.
    """
    result = {}
    kpatterns = [wildcard_to_regex(x) for x in keys]
    vpatterns = [wildcard_to_regex(x) for x in values]
    for k, v in dct.items() if not reverse else ():
        if  (not keys   or k in keys   or any(p.match(k) for p in kpatterns)) \
        and (not values or v in values or any(p.match(v) for p in vpatterns)):
            result[k] = v
    for k, v in dct.items() if reverse else ():
        if  (k not in keys   and not any(p.match(k) for p in kpatterns)) \
        and (v not in values and not any(p.match(v) for p in vpatterns)):
            result[k] = v
    return result


def filter_fields(fieldmap, top=(), include=(), exclude=()):
    """
    Returns fieldmap filtered by include and exclude patterns.

    @param   fieldmap  {field name: field type name}
    @param   top       parent path as (rootattr, ..)
    @param   include   [((nested, path), re.Pattern())]
    @param   exclude   [((nested, path), re.Pattern())]
    """
    result = {} if include or exclude else fieldmap
    for k, v in fieldmap.items() if not result else ():
        trail, trailstr = top + (k, ), ".".join(top + (k, ))
        for is_exclude, patterns in enumerate((include, exclude)):
            matches = any(p[:len(trail)] == trail[:len(p)] or r.match(trailstr)
                          for p, r in patterns)  # Match by beginning or wildcard pattern
            if patterns and (not matches if is_exclude else matches):
                result[k] = v
            elif patterns and is_exclude and matches:
                result.pop(k, None)
            if include and exclude and k not in result:  # Failing to include takes precedence
                break  # for is_exclude
    return result


def find_files(names=(), paths=(), extensions=(), skip_extensions=(), recurse=False):
    """
    Yields filenames from current directory or given paths.

    Seeks only files with given extensions if names not given.
    Prints errors for names and paths not found.

    @param   names            list of specific files to return (supports * wildcards)
    @param   paths            list of paths to look under, if not using current directory
    @param   extensions       list of extensions to select if not using names, as (".ext1", ..)
    @param   skip_extensions  list of extensions to skip if not using names, as (".ext1", ..)
    @param   recurse          whether to recurse into subdirectories
    """
    namesfound, pathsfound = set(), set()
    def iter_files(directory):
        """Yields matching filenames from path."""
        if os.path.isfile(directory):
            ConsolePrinter.error("%s: Is a file", directory)
            return
        for path in glob.glob(directory):  # Expand * wildcards, if any
            pathsfound.add(directory)
            for n in names:
                p = n if not paths or os.path.isabs(n) else os.path.join(path, n)
                for f in (f for f in glob.glob(p) if "*" not in n
                          or not any(map(f.endswith, skip_extensions))):
                    if os.path.isdir(f):
                        ConsolePrinter.error("%s: Is a directory", f)
                        continue  # for n
                    namesfound.add(n)
                    yield f
            for root, _, files in os.walk(path) if not names else ():
                for f in (os.path.join(root, f) for f in files
                          if (not extensions or any(map(f.endswith, extensions)))
                          and not any(map(f.endswith, skip_extensions))):
                    yield f
                if not recurse:
                    break  # for root

    processed = set()
    for f in (f for p in paths or ["."] for f in iter_files(p)):
        if os.path.abspath(f) not in processed:
            processed.add(os.path.abspath(f))
            yield f

    for path in (p for p in paths if p not in pathsfound):
        ConsolePrinter.error("%s: No such directory", path)
    for name in (n for n in names if n not in namesfound):
        ConsolePrinter.error("%s: No such file", name)


def format_timedelta(delta):
    """Formats the datetime.timedelta as "3d 40h 23min 23.1sec"."""
    dd, rem = divmod(delta.total_seconds(), 24*3600)
    hh, rem = divmod(rem, 3600)
    mm, ss  = divmod(rem, 60)
    items = []
    for c, n in (dd, "d"), (hh, "h"), (mm, "min"), (ss, "sec"):
        f = "%d" % c if "sec" != n else drop_zeros(round(c, 9))
        if f != "0": items += [f + n]
    return " ".join(items or ["0sec"])


def format_bytes(size, precision=2, inter=" "):
    """Returns a formatted byte size (like 421.45 MB)."""
    result = "0 bytes"
    if size:
        UNITS = [("bytes", "byte")[1 == size]] + [x + "B" for x in "KMGTPEZY"]
        exponent = min(int(math.log(size, 1024)), len(UNITS) - 1)
        result = "%.*f" % (precision, size / (1024. ** exponent))
        result += "" if precision > 0 else "."  # Do not strip integer zeroes
        result = drop_zeros(result) + inter + UNITS[exponent]
    return result


def format_stamp(stamp):
    """Returns ISO datetime from rospy.Time or UNIX timestamp."""
    stamp = stamp if isinstance(stamp, (int, float)) else stamp.to_sec()
    return datetime.datetime.fromtimestamp(stamp).isoformat(sep=" ")


def get_message_fields(msg):
    """Returns {field name: field type name} if ROS message, else {}."""
    names = getattr(msg, "__slots__", [])
    if isinstance(msg, (rospy.Time, rospy.Duration)):  # Empty __slots__
        names = genpy.TVal.__slots__
    return dict(zip(names, getattr(msg, "_slot_types", [])))


def get_message_value(msg, name, typename):
    """Returns object attribute value, uint8[] converted to [int, ] if bytes."""
    v = getattr(msg, name)
    return list(v) if typename.startswith("uint8[") and isinstance(v, bytes) else v


def make_bag_time(stamp, bag):
    """
    Returns timestamp string or datetime instance as rospy.Time.

    Interpreted as delta from bag start/end time if numeric string with sign prefix.
    """
    if isinstance(stamp, datetime.datetime): stamp, shift = stamp.timestamp(), 0
    else:
        stamp, sign = float(stamp), ("+" == stamp[0] if stamp[0] in "+-" else None)
        shift = 0 if sign is None else bag.get_start_time() if sign else bag.get_end_time()
    return rospy.Time(stamp + shift)


def make_live_time(stamp):
    """
    Returns timestamp string or datetime instance as rospy.Time.

    Interpreted as delta from system time if numeric string with sign prefix.
    """
    if isinstance(stamp, datetime.datetime): stamp, shift = stamp.timestamp(), 0
    else:
        stamp, sign = float(stamp), ("+" == stamp[0] if stamp[0] in "+-" else None)
        shift = 0 if sign is None else time.time()
    return rospy.Time(stamp + shift)


def merge_spans(spans):
    """Returns a sorted list of (start, end) spans with overlapping spans merged."""
    result = sorted(spans)
    result, rest = result[:1], result[1:]
    for span in rest:
        if span[0] <= result[-1][1]:
            result[-1] = (result[-1][0], max(span[1], result[-1][1]))
        else:
            result.append(span)
    return result


def parse_datetime(text):
    """Returns datetime object from ISO datetime string (may be partial). Raises if invalid."""
    BASE = re.sub(r"\D", "", datetime.datetime.min.isoformat())  # "00010101000000"
    text = re.sub(r"\D", "", text)
    text += BASE[len(text):] if text else ""
    dt = datetime.datetime.strptime(text[:len(BASE)], "%Y%m%d%H%M%S")
    return dt + datetime.timedelta(microseconds=int(text[len(BASE):] or "0"))


def wildcard_to_regex(text):
    """Returns plain wildcard like "/foo*bar" as re.Pattern("\/foo.*bar", re.I)."""
    return re.compile(".*".join(map(re.escape, text.split("*"))), re.I)

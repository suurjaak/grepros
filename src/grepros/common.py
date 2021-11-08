# -*- coding: utf-8 -*-
## @namespace grepros.common
"""
Common utilities.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    08.11.2021
------------------------------------------------------------------------------
"""
from __future__ import print_function
import datetime
import glob
import math
import os
import random
import re
import shutil
import sys
try: import curses
except ImportError: curses = None


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
            if sys.stdout.isatty() or do_color:
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
    def print(cls, text="", *args, **kwargs):
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
        fileobj is sys.stdout and not fileobj.isatty() and fileobj.flush()
        cls.PRINTS[fileobj] = cls.PRINTS.get(fileobj, 0) + 1


    @classmethod
    def error(cls, text="", *args, **kwargs):
        """Prints error to stderr, formatted with args and kwargs, in error colors if supported."""
        KWS = dict(__file=sys.stderr, __prefix=cls.ERROR_START, __suffix=cls.ERROR_END)
        cls.print(text, *args, **dict(kwargs, **KWS))


    @classmethod
    def debug(cls, text="", *args, **kwargs):
        """
        Prints debug text to stderr if verbose.

        Formatted with args and kwargs, in lowlight colors if supported.
        """
        if cls.VERBOSE:
            KWS = dict(__file=sys.stderr, __prefix=cls.LOWLIGHT_START, __suffix=cls.LOWLIGHT_END)
            cls.print(text, *args, **dict(kwargs, **KWS))



class TextWrapper(object):
    """
    TextWrapper that supports custom substring widths in line width calculation.

    Intended for wrapping text containing ANSI control codes.
    Heavily refactored from Python standard library textwrap.TextWrapper.
    """

    ## Regex for breaking text at whitespace
    SPACE_RGX = re.compile(r"([%s]+)" % re.escape("\t\n\x0b\x0c\r "))


    def __init__(self, width=80, subsequent_indent="  ", max_lines=None, placeholder=" ...",
                 custom_widths=None):
        """
        @param   width              default maximum width to wrap at, 0 disables
        @param   max_lines          count to truncate lines from
        @param   placeholder        appended to last retained line when truncating
        @param   subsequent_indent  appended to last retained line when truncating
        @param   custom_widths      {substring: len} to use in line width calculation
        """
        self.width             = width
        self.subsequent_indent = subsequent_indent
        self.max_lines         = max_lines
        self.placeholder       = placeholder

        self.customs    = {s: l for s, l in (custom_widths or {}).items() if s}
        self.custom_rgx = re.compile("(%s)" % "|".join(re.escape(s) for s in self.customs))
        self.disabled   = not self.width
        self.minwidth   = 1 + self.strlen(self.subsequent_indent) \
                            + self.strlen(self.placeholder if self.max_lines else "")
        self.width      = max(self.width, self.minwidth)
        self.realwidth  = self.width


    def wrap(self, text):
        """Returns a list of wrapped text lines, without linebreaks."""
        if self.disabled: return [text]
        return self._wrap_chunks([c for c in self.SPACE_RGX.split(text) if c])


    def reserve_width(self, reserved=""):
        """Decreases the configured width by given amount (number or string)."""
        reserved = self.strlen(reserved) if isinstance(reserved, str) else reserved
        self.width = max(self.minwidth, self.realwidth - reserved)


    def strlen(self, v):
        """Returns length of string, using custom substring widths."""
        return len(v) - sum(v.count(s) * (len(s) - l) for s, l in self.customs.items())


    def isblank(self, v):
        """Returns whether string is empty or all whitespace, using custom substring widths."""
        return not self.custom_rgx.sub(lambda m: "X" * self.customs[m.group()], v).strip()


    def _wrap_chunks(self, chunks):
        """Returns a list of lines joined from text chunks, wrapped to width."""
        lines = []
        chunks.reverse()  # Reverse for efficient popping

        placeholder_len = self.strlen(self.placeholder)
        while chunks:
            cur_line, cur_len = [], 0  # [chunk, ], sum(map(len, cur_line))
            indent = self.subsequent_indent if lines else ""
            width = self.width - self.strlen(indent)

            if lines and self.isblank(chunks[-1]):
                del chunks[-1]  # Drop starting all-whitespace chunk if subsequent line

            while chunks:
                l = self.strlen(chunks[-1])
                if cur_len + l <= width:
                    cur_line.append(chunks.pop())
                    cur_len += l
                else:  # Line full
                    break  # while chunks (inner-while)

            if chunks and self.strlen(chunks[-1]) > width:
                # Current line is full, and next chunk is too big to fit on any line
                self._handle_long_word(chunks, cur_line, cur_len, width)
                cur_len = sum(map(self.strlen, cur_line))

            if cur_line and self.isblank(cur_line[-1]):
                cur_len -= self.strlen(cur_line[-1])
                del cur_line[-1]  # Last chunk is all whitespace, drop it

            if cur_line:
                if (self.max_lines is None or len(lines) + 1 < self.max_lines
                or (not chunks or len(chunks) == 1 and self.isblank(chunks[0]))
                and cur_len <= width):  # Current line ok
                    lines.append(indent + "".join(cur_line))
                    continue  # while chunks
            else:
                continue  # while chunks

            while cur_line:
                if not self.isblank(cur_line[-1]) and cur_len + placeholder_len <= width:
                    cur_line.append(self.placeholder)
                    lines.append(indent + "".join(cur_line))
                    break  # while cur_line
                cur_len -= self.strlen(cur_line[-1])
                del cur_line[-1]
            else:
                if lines:
                    prev_line = lines[-1].rstrip()
                    if self.strlen(prev_line) + placeholder_len <= self.width:
                        lines[-1] = prev_line + self.placeholder
                        break  # while chunks
                lines.append(indent + self.placeholder.lstrip())
            break  # while chunks

        return lines


    def _handle_long_word(self, reversed_chunks, cur_line, cur_len, width):
        """
        Breaks last chunk if not only containing a custom-width string,
        else adds last chunk to current line if line still empty.
        """
        text, breakable = reversed_chunks[-1], False
        break_pos = 1 if width < 1 else width - cur_len
        if text not in self.customs:
            unbreakable_spans = [m.span() for m in self.custom_rgx.finditer(text)]
            text_in_spans = [x for x in unbreakable_spans if x[0] <= break_pos < x[1]]
            last_span = text_in_spans and sorted(text_in_spans, key=lambda x: -x[1])[0]
            break_pos = last_span[1] if last_span else break_pos
            breakable = 0 < break_pos < len(text)

        if breakable:
            cur_line.append(text[:break_pos])
            reversed_chunks[-1] = text[break_pos:]
        elif not cur_line:
            cur_line.append(reversed_chunks.pop())



def drop_zeros(v, replace=""):
    """Drops or replaces trailing zeros and empty decimal separator, if any."""
    return re.sub(r"\.?0+$", lambda x: len(x.group()) * replace, str(v))


def filter_dict(dct, keys=(), values=(), reverse=False):
    """
    Filters string dictionary by keys and values. Dictionary values may be
    additional lists; keys with emptied lists are dropped.

    Retains only entries that find a match (supports * wildcards);
    if reverse, retains only entries that do not find a match.
    """
    result = type(dct)()
    kpatterns = [wildcard_to_regex(x) for x in keys]
    vpatterns = [wildcard_to_regex(x) for x in values]
    for k, vv in dct.items() if not reverse else ():
        is_array = isinstance(vv, (list, tuple))
        for v in (vv if is_array else [vv]):
            if  (not keys   or k in keys   or any(p.match(k) for p in kpatterns)) \
            and (not values or v in values or any(p.match(v) for p in vpatterns)):
                result.setdefault(k, []).append(v) if is_array else result.update({k: v})
    for k, vv in dct.items() if reverse else ():
        is_array = isinstance(vv, (list, tuple))
        for v in (vv if is_array else [vv]):
            if  (k not in keys   and not any(p.match(k) for p in kpatterns)) \
            and (v not in values and not any(p.match(v) for p in vpatterns)):
                result.setdefault(k, []).append(v) if is_array else result.update({k: v})
    return result


def filter_fields(fieldmap, top=(), include=(), exclude=()):
    """
    Returns fieldmap filtered by include and exclude patterns.

    @param   fieldmap  {field name: field type name}
    @param   top       parent path as (rootattr, ..)
    @param   include   [((nested, path), re.Pattern())] to require in parent path
    @param   exclude   [((nested, path), re.Pattern())] to reject in parent path
    """
    result = type(fieldmap)() if include or exclude else fieldmap
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
    """Returns ISO datetime from UNIX timestamp."""
    return datetime.datetime.fromtimestamp(stamp).isoformat(sep=" ")


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


def unique_path(pathname, empty_ok=False):
    """
    Returns a unique version of the path.

    If a file or directory with the same name already exists, returns a unique
    version (e.g. "/tmp/my.2.file" if ""/tmp/my.file" already exists).

    @param   empty_ok  whether to ignore existence if file is empty
    """
    result = pathname
    if "linux2" == sys.platform and sys.version_info < (3, 0) \
    and isinstance(result, unicode) and "utf-8" != sys.getfilesystemencoding():
        result = result.encode("utf-8") # Linux has trouble if locale not UTF-8
    if os.path.isfile(result) and empty_ok and not os.path.getsize(result):
        return result
    path, name = os.path.split(result)
    base, ext = os.path.splitext(name)
    if len(name) > 255: # Filesystem limitation
        name = base[:255 - len(ext) - 2] + ".." + ext
        result = os.path.join(path, name)
    counter = 2
    while os.path.exists(result):
        suffix = ".%s%s" % (counter, ext)
        name = base + suffix
        if len(name) > 255:
            name = base[:255 - len(suffix) - 2] + ".." + suffix
        result = os.path.join(path, name)
        counter += 1
    return result


def wildcard_to_regex(text):
    """Returns plain wildcard like "/foo*bar" as re.Pattern("\/foo.*bar", re.I)."""
    return re.compile(".*".join(map(re.escape, text.split("*"))), re.I)

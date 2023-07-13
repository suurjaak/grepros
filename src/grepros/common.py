# -*- coding: utf-8 -*-
"""
Common utilities.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    02.07.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.common
from __future__ import print_function
import argparse
import copy
import datetime
import functools
import glob
import importlib
import inspect
import io
import itertools
import logging
import math
import os
import re
import shutil
import sys
import threading
import time
try: import curses
except ImportError: curses = None

import six
try: import zstandard
except ImportError: zstandard = None


## Python types for filesystem paths
PATH_TYPES = (six.binary_type, six.text_type)
if six.PY34: PATH_TYPES += (importlib.import_module("pathlib").Path, )
## Python types for both byte strings and text strings
STRING_TYPES = (six.binary_type, six.text_type)
## Python types for text strings
TEXT_TYPES = (six.binary_type, six.text_type) if six.PY2 else (six.text_type, )


class MatchMarkers(object):
    """Highlight markers for matches in message values."""

    ## Unique marker for match highlight replacements
    ID    = "matching"
    ## Placeholder in front of match
    START = "<%s>"  % ID
    ## Placeholder at end of match
    END   = "</%s>" % ID
    ## Placeholder for empty string match
    EMPTY = START + END
    ## Replacement for empty string match
    EMPTY_REPL = "%s''%s" % (START, END)

    @classmethod
    def populate(cls, value):
        """Populates highlight markers with specified value."""
        cls.ID    = str(value)
        cls.START = "<%s>"  % cls.ID
        cls.END   = "</%s>" % cls.ID
        cls.EMPTY = cls.START + cls.END
        cls.EMPTY_REPL = "%s''%s" % (cls.START, cls.END)



class ConsolePrinter(object):
    """
    Prints to console, supports color output.

    If configured with `apimode=True`, logs debugs and warnings to logger and raises errors.
    """

    STYLE_RESET     = "\x1b(B\x1b[m"            # Default color+weight
    STYLE_HIGHLIGHT = "\x1b[31m"                # Red
    STYLE_LOWLIGHT  = "\x1b[38;2;105;105;105m"  # Dim gray
    STYLE_SPECIAL   = "\x1b[35m"                # Purple
    STYLE_SPECIAL2  = "\x1b[36m"                # Cyan
    STYLE_WARN      = "\x1b[33m"                # Yellow
    STYLE_ERROR     = "\x1b[31m\x1b[2m"         # Dim red

    DEBUG_START, DEBUG_END = STYLE_LOWLIGHT, STYLE_RESET  # Metainfo wrappers
    WARN_START,  WARN_END =  STYLE_WARN,     STYLE_RESET  # Warning message wrappers
    ERROR_START, ERROR_END = STYLE_ERROR,    STYLE_RESET  # Error message wrappers

    ## Whether using colors in output
    COLOR = None

    ## Console width in characters, updated from shutil and curses
    WIDTH = 80

    ## {sys.stdout: number of texts printed, sys.stderr: ..}
    PRINTS = {}

    ## Whether logging debugs and warnings and raising errors, instead of printing
    APIMODE = False

    _COLORFLAG = None  ## Color flag set in configure()

    _LINEOPEN = False  ## Whether last print was without linefeed

    _UNIQUES = set()   ## Unique texts printed with `__once=True`

    @classmethod
    def configure(cls, color=True, apimode=False):
        """
        Initializes printer, for terminal output or library mode.

        For terminal output, initializes terminal colors, or disables colors if unsupported.

        @param   color    True / False / None for auto-detect from TTY support;
                          will be disabled if terminal does not support colors
        @param   apimode  whether to log debugs and warnings to logger and raise errors,
                          instead of printing
        """
        cls.APIMODE    = bool(apimode)
        cls._COLORFLAG = color
        if apimode:
            cls.DEBUG_START, cls.DEBUG_END = "", ""
            cls.WARN_START,  cls.WARN_END  = "", ""
            cls.ERROR_START, cls.ERROR_END = "", ""
        else: cls.init_terminal()


    @classmethod
    def init_terminal(cls):
        """Initializes terminal for color output, or disables color output if unsupported."""
        if cls.COLOR is not None: return

        try: cls.WIDTH = shutil.get_terminal_size().columns  # Py3
        except Exception: pass  # Py2
        cls.COLOR = (cls._COLORFLAG is not False)
        try:
            curses.setupterm()
            if cls.COLOR and not sys.stdout.isatty():
                raise Exception()
        except Exception:
            cls.COLOR = bool(cls._COLORFLAG)
        try:
            if sys.stdout.isatty() or cls.COLOR:
                cls.WIDTH = curses.initscr().getmaxyx()[1]
            curses.endwin()
        except Exception: pass

        if cls.COLOR:
            cls.DEBUG_START, cls.DEBUG_END = cls.STYLE_LOWLIGHT, cls.STYLE_RESET
            cls.WARN_START,  cls.WARN_END  = cls.STYLE_WARN,     cls.STYLE_RESET
            cls.ERROR_START, cls.ERROR_END = cls.STYLE_ERROR,    cls.STYLE_RESET
        else:
            cls.DEBUG_START, cls.DEBUG_END = "", ""
            cls.WARN_START,  cls.WARN_END  = "", ""
            cls.ERROR_START, cls.ERROR_END = "", ""


    @classmethod
    def print(cls, text="", *args, **kwargs):
        """
        Prints text, formatted with args and kwargs.

        @param   __file   file object to print to if not sys.stdout
        @param   __end    line end to use if not linefeed "\n"
        @param   __once   whether text should be printed only once
                          and discarded on any further calls (applies to unformatted text)
        """
        text = str(text)
        if kwargs.pop("__once", False):
            if text in cls._UNIQUES: return
            cls._UNIQUES.add(text)
        fileobj, end = kwargs.pop("__file", sys.stdout), kwargs.pop("__end", "\n")
        pref, suff = kwargs.pop("__prefix", ""), kwargs.pop("__suffix", "")
        if cls._LINEOPEN and "\n" in end: pref = "\n" + pref  # Add linefeed to end open line
        text = cls._format(text, *args, **kwargs)

        cls.PRINTS[fileobj] = cls.PRINTS.get(fileobj, 0) + 1
        cls._LINEOPEN = "\n" not in end
        cls.init_terminal()
        print(pref + text + suff, end=end, file=fileobj)
        not fileobj.isatty() and fileobj.flush()


    @classmethod
    def error(cls, text="", *args, **kwargs):
        """
        Prints error to stderr, formatted with args and kwargs, in error colors if supported.

        Raises exception instead if APIMODE.
        """
        if cls.APIMODE:
            raise Exception(cls._format(text, *args, __once=False, **kwargs))
        KWS = dict(__file=sys.stderr, __prefix=cls.ERROR_START, __suffix=cls.ERROR_END)
        cls.print(text, *args, **dict(kwargs, **KWS))


    @classmethod
    def warn(cls, text="", *args, **kwargs):
        """
        Prints warning to stderr, or logs to logger if APIMODE.

        Text is formatted with args and kwargs, in warning colors if supported.
        """
        if cls.APIMODE:
            text = cls._format(text, *args, **kwargs)
            if text: logging.getLogger(__name__).warning(text)
            return
        KWS = dict(__file=sys.stderr, __prefix=cls.WARN_START, __suffix=cls.WARN_END)
        cls.print(text, *args, **dict(kwargs, **KWS))


    @classmethod
    def debug(cls, text="", *args, **kwargs):
        """
        Prints debug text to stderr, or logs to logger if APIMODE.

        Text is formatted with args and kwargs, in warning colors if supported.
        """
        if cls.APIMODE:
            text = cls._format(text, *args, **kwargs)
            if text: logging.getLogger(__name__).debug(text)
            return
        KWS = dict(__file=sys.stderr, __prefix=cls.DEBUG_START, __suffix=cls.DEBUG_END)
        cls.print(text, *args, **dict(kwargs, **KWS))


    @classmethod
    def log(cls, level, text="", *args, **kwargs):
        """
        Prints text to stderr, or logs to logger if APIMODE.

        Text is formatted with args and kwargs, in level colors if supported.

        @param   level  logging level like `logging.ERROR` or "ERROR"
        """
        if cls.APIMODE:
            text = cls._format(text, *args, **kwargs)
            if text: logging.getLogger(__name__).log(level, text)
            return
        level = logging.getLevelName(level)
        if not isinstance(level, TEXT_TYPES): level = logging.getLevelName(level)
        func = {"DEBUG": cls.debug, "WARNING": cls.warn, "ERROR": cls.error}.get(level, cls.print)
        func(text, *args, **dict(kwargs, __file=sys.stderr))


    @classmethod
    def flush(cls):
        """Ends current open line, if any."""
        if cls._LINEOPEN: print()
        cls._LINEOPEN = False


    @classmethod
    def _format(cls, text="", *args, **kwargs):
        """
        Returns text formatted with printf-style or format() arguments.

        @param  __once  registers text, returns "" if text not unique
        """
        text, fmted = str(text), False
        if kwargs.get("__once"):
            if text in cls._UNIQUES: return ""
            cls._UNIQUES.add(text)
        for k in ("__file", "__end", "__once", "__prefix", "__suffix"): kwargs.pop(k, None)
        try: text, fmted = (text % args if args else text), bool(args)
        except Exception: pass
        try: text, fmted = (text % kwargs if kwargs else text), fmted or bool(kwargs)
        except Exception: pass
        try: text = text.format(*args, **kwargs) if not fmted and (args or kwargs) else text
        except Exception: pass
        return text


class Decompressor(object):
    """Decompresses zstandard archives."""

    ## Supported archive extensions
    EXTENSIONS = (".zst", ".zstd")

    ## zstd file header magic start bytes
    ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


    @classmethod
    def decompress(cls, path, progress=False):
        """
        Decompresses file to same directory, showing optional progress bar.

        @return  uncompressed file path
        """
        cls.validate()
        path2, bar, size, processed = os.path.splitext(path)[0], None, os.path.getsize(path), 0
        fmt = lambda s: format_bytes(s, strip=False)
        if progress:
            tpl = " Decompressing %s (%s): {afterword}" % (os.path.basename(path), fmt(size))
            bar = ProgressBar(pulse=True, aftertemplate=tpl)

        ConsolePrinter.warn("Compressed file %s (%s), decompressing to %s.", path, fmt(size), path2)
        bar and bar.update(0).start()  # Start progress pulse
        try:
            with open(path, "rb") as f, open(path2, "wb") as g:
                reader = zstandard.ZstdDecompressor().stream_reader(f)
                while True:
                    chunk = reader.read(1048576)
                    if not chunk: break  # while

                    g.write(chunk)
                    processed += len(chunk)
                    bar and (setattr(bar, "afterword", fmt(processed)), bar.update(processed))
                reader.close()
        except Exception:
            os.remove(path2)
            raise
        finally: bar and (setattr(bar, "pulse", False), bar.update(processed).stop())
        return path2


    @classmethod
    def is_compressed(cls, path):
        """Returns whether file is a recognized archive."""
        result = os.path.isfile(path)
        if result:
            result = any(str(path).lower().endswith(x) for x in cls.EXTENSIONS)
        if result:
            with open(path, "rb") as f:
                result = (f.read(len(cls.ZSTD_MAGIC)) == cls.ZSTD_MAGIC)
        return result


    @classmethod
    def make_decompressed_name(cls, path):
        """Returns the path without archive extension, if any."""
        return os.path.splitext(path)[0] if cls.is_compressed(path) else path


    @classmethod
    def validate(cls):
        """Raises error if decompression library not available."""
        if not zstandard: raise Exception("zstandard not installed, cannot decompress")



class ProgressBar(threading.Thread):
    """
    A simple ASCII progress bar with a ticker thread

    Drawn like
    '[---------/   36%            ] Progressing text..'.
    or for pulse mode
    '[    ----                    ] Progressing text..'.
    """

    def __init__(self, max=100, value=0, min=0, width=30, forechar="-",
                 backchar=" ", foreword="", afterword="", interval=1,
                 pulse=False, aftertemplate=" {afterword}"):
        """
        Creates a new progress bar, without drawing it yet.

        @param   max            progress bar maximum value, 100%
        @param   value          progress bar initial value
        @param   min            progress bar minimum value, for 0%
        @param   width          progress bar width (in characters)
        @param   forechar       character used for filling the progress bar
        @param   backchar       character used for filling the background
        @param   foreword       text in front of progress bar
        @param   afterword      text after progress bar
        @param   interval       ticker thread interval, in seconds
        @param   pulse          ignore value-min-max, use constant pulse instead
        @param   aftertemplate  afterword format() template, populated with vars(self)
        """
        threading.Thread.__init__(self)
        for k, v in locals().items(): setattr(self, k, v) if "self" != k else 0
        afterword = aftertemplate.format(**vars(self))
        self.daemon    = True   # Daemon threads do not keep application running
        self.percent   = None   # Current progress ratio in per cent
        self.value     = None   # Current progress bar value
        self.pause     = False  # Whether drawing is currently paused
        self.pulse_pos = 0      # Current pulse position
        self.bar = "%s[%s%s]%s" % (foreword,
                                   backchar if pulse else forechar,
                                   backchar * (width - 3),
                                   afterword)
        self.printbar = self.bar   # Printable text, with padding to clear previous
        self.progresschar = itertools.cycle("-\\|/")
        self.is_running = False


    def update(self, value=None, draw=True, flush=False):
        """Updates the progress bar value, and refreshes by default; returns self."""
        if value is not None: self.value = min(self.max, max(self.min, value))
        afterword = self.aftertemplate.format(**vars(self))
        w_full = self.width - 2
        if self.pulse:
            if self.pulse_pos is None:
                bartext = "%s[%s]%s" % (self.foreword,
                                        self.forechar * (self.width - 2),
                                        afterword)
            else:
                dash = self.forechar * max(1, int((self.width - 2) / 7))
                pos = self.pulse_pos
                if pos < len(dash):
                    dash = dash[:pos]
                elif pos >= self.width - 1:
                    dash = dash[:-(pos - self.width - 2)]

                bar = "[%s]" % (self.backchar * w_full)
                # Write pulse dash into the middle of the bar
                pos1 = min(self.width - 1, pos + 1)
                bar = bar[:pos1 - len(dash)] + dash + bar[pos1:]
                bartext = "%s%s%s" % (self.foreword, bar, afterword)
                self.pulse_pos = (self.pulse_pos + 1) % (self.width + 2)
        else:
            percent = int(round(100.0 * self.value / (self.max or 1)))
            percent = 99 if percent == 100 and self.value < self.max else percent
            w_done = max(1, int(round((percent / 100.0) * w_full)))
            # Build bar outline, animate by cycling last char from progress chars
            char_last = self.forechar
            if draw and w_done < w_full: char_last = next(self.progresschar)
            bartext = "%s[%s%s%s]%s" % (
                       self.foreword, self.forechar * (w_done - 1), char_last,
                       self.backchar * (w_full - w_done), afterword)
            # Write percentage into the middle of the bar
            centertxt = " %2d%% " % percent
            pos = len(self.foreword) + int(self.width / 2 - len(centertxt) / 2)
            bartext = bartext[:pos] + centertxt + bartext[pos + len(centertxt):]
            self.percent = percent
        self.printbar = bartext + " " * max(0, len(self.bar) - len(bartext))
        self.bar, prevbar = bartext, self.bar
        if draw and (flush or prevbar != self.bar): self.draw(flush)
        return self


    def draw(self, flush=False):
        """
        Prints the progress bar, from the beginning of the current line.

        @param   flush  add linefeed to end, forcing a new line for any next print
        """
        ConsolePrinter.print("\r" + self.printbar, __end=" ")
        if len(self.printbar) != len(self.bar):  # Draw twice to position caret at true content end
            self.printbar = self.bar
            ConsolePrinter.print("\r" + self.printbar, __end=" ")
        if flush: ConsolePrinter.flush()


    def run(self):
        self.is_running = True
        while self.is_running:
            if not self.pause: self.update(self.value)
            time.sleep(self.interval)


    def stop(self):
        self.is_running = False



class LenIterable(object):
    """Wrapper for iterable value with specified fixed length."""

    def __init__(self, iterable, count):
        """
        @param   iterable  any iterable value
        @param   count     value to return for len(self), or callable to return value from
        """
        self._iterer = iter(iterable)
        self._count  = count

    def __iter__(self): return self

    def __next__(self): return next(self._iterer)

    def __len__(self):  return self._count() if callable(self._count) else self._count



class TextWrapper(object):
    """
    TextWrapper that supports custom substring widths in line width calculation.

    Intended for wrapping text containing ANSI control codes.
    Heavily refactored from Python standard library textwrap.TextWrapper.
    """

    ## Regex for breaking text at whitespace
    SPACE_RGX = re.compile(r"([%s]+)" % re.escape("\t\n\x0b\x0c\r "))

    ## Max length of strlen cache
    LENCACHEMAX = 10000


    def __init__(self, width=80, subsequent_indent="  ", break_long_words=True,
                 drop_whitespace=False, max_lines=None, placeholder=" ...", custom_widths=None):
        """
        @param   width              default maximum width to wrap at, 0 disables
        @param   subsequent_indent  string prepended to all consecutive lines
        @param   break_long_words   break words longer than width
        @param   drop_whitespace    drop leading and trailing whitespace from lines
        @param   max_lines          count to truncate lines from
        @param   placeholder        appended to last retained line when truncating
        @param   custom_widths      {substring: len} to use in line width calculation
        """
        self.width             = width
        self.subsequent_indent = subsequent_indent
        self.break_long_words  = break_long_words
        self.drop_whitespace   = drop_whitespace
        self.max_lines         = max_lines
        self.placeholder       = placeholder

        self.lencache    = {}
        self.customs     = {s: l for s, l in (custom_widths or {}).items() if s}
        self.custom_lens = [(s, len(s) - l) for s, l in self.customs.items()]
        self.custom_rgx  = re.compile("(%s)" % "|".join(re.escape(s) for s in self.customs))
        self.disabled    = not self.width
        self.minwidth    = 1 + self.strlen(self.subsequent_indent) \
                             + self.strlen(self.placeholder if self.max_lines else "")
        self.width       = max(self.width, self.minwidth)
        self.realwidth   = self.width


    def wrap(self, text):
        """Returns a list of wrapped text lines, without linebreaks."""
        if self.disabled: return [text]
        result = []
        for i, line in enumerate(text.splitlines()):
            chunks = [c for c in self.SPACE_RGX.split(line) if c]
            lines = self._wrap_chunks(chunks)
            if i and lines and self.subsequent_indent:
                lines[0] = self.subsequent_indent + lines[0]
            result.extend(lines)
            if self.max_lines and result and len(result) >= self.max_lines:
                break  # for i, line
        if self.max_lines and result and (len(result) > self.max_lines
        or len(result) == self.max_lines and not text.endswith(result[-1].strip())):
            result = result[:self.max_lines]
            if not result[-1].endswith(self.placeholder.lstrip()):
                result[-1] += self.placeholder
        if len(self.lencache)  > self.LENCACHEMAX:  self.lencache.clear()
        return result


    def reserve_width(self, reserved=""):
        """Decreases the configured width by given amount (number or string)."""
        reserved = self.strlen(reserved) if isinstance(reserved, TEXT_TYPES) else reserved
        self.width = max(self.minwidth, self.realwidth - reserved)


    def strlen(self, v):
        """Returns length of string, using custom substring widths."""
        if v not in self.lencache:
            self.lencache[v] = len(v) - sum(v.count(s) * ld for s, ld in self.custom_lens)
        return self.lencache[v]


    def strip(self, v):
        """Returns string with custom substrings and whitespace stripped."""
        return self.custom_rgx.sub("", v).strip()


    def _wrap_chunks(self, chunks):
        """Returns a list of lines joined from text chunks, wrapped to width."""
        lines = []
        chunks.reverse()  # Reverse for efficient popping

        placeholder_len = self.strlen(self.placeholder)
        while chunks:
            cur_line, cur_len = [], 0  # [chunk, ], sum(map(len, cur_line))
            indent = self.subsequent_indent if lines else ""
            width = self.width - self.strlen(indent)

            if self.drop_whitespace and lines and not self.strip(chunks[-1]):
                del chunks[-1]  # Drop initial whitespace on subsequent lines

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

            if self.drop_whitespace and cur_line and not self.strip(cur_line[-1]):
                cur_len -= len(cur_line[-1])  # Drop line last whitespace chunk
                del cur_line[-1]

            if cur_line:
                if (self.max_lines is None or len(lines) + 1 < self.max_lines
                or (not chunks or self.drop_whitespace
                    and len(chunks) == 1 and not self.strip(chunks[0])) \
                and cur_len <= width):  # Current line ok
                    lines.append(indent + "".join(cur_line))
                    continue  # while chunks
            else:
                continue  # while chunks

            while cur_line:  # Truncate for max_lines
                if self.strip(cur_line[-1]):
                    if cur_len + placeholder_len <= width:
                        lines.append(indent + "".join(cur_line))
                        break  # while cur_line
                    if len(cur_line) == 1:
                        lines.append(indent + cur_line[-1])
                cur_len -= self.strlen(cur_line[-1])
                del cur_line[-1]
            else:
                if not lines or self.strlen(lines[-1]) + placeholder_len > self.width:
                    lines.append(indent + self.placeholder.lstrip())
            break  # while chunks

        return lines


    def _handle_long_word(self, reversed_chunks, cur_line, cur_len, width):
        """
        Breaks last chunk if not only containing a custom-width string,
        else adds last chunk to current line if line still empty.
        """
        text = reversed_chunks[-1]
        break_pos = 1 if width < 1 else width - cur_len
        breakable = self.break_long_words and text not in self.customs
        if breakable:
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


def ellipsize(text, limit, ellipsis=".."):
    """Returns text ellipsized if beyond limit."""
    if limit <= 0 or len(text) < limit:
        return text
    return text[:max(0, limit - len(ellipsis))] + ellipsis


def ensure_namespace(val, defaults=None, **kwargs):
    """
    Returns a copy of value as `argparse.Namespace`, with all keys uppercase.

    Arguments with list/tuple values in defaults are ensured to have list/tuple values.

    @param  val       `argparse.Namespace` or dictionary or `None`
    @param  defaults  additional arguments to set to namespace if missing
    @param  kwargs    any and all argument overrides as keyword overrides
    """
    if val is None or isinstance(val, dict): val = argparse.Namespace(**val or {})
    else: val = structcopy(val)
    for k, v in vars(val).items():
        if not k.isupper():
            delattr(val, k)
            setattr(val, k.upper(), v)
    for k, v in ((k.upper(), v) for k, v in (defaults.items() if defaults else ())):
        if not hasattr(val, k): setattr(val, k, structcopy(v))
    for k, v in ((k.upper(), v) for k, v in kwargs.items()): setattr(val, k, v)
    for k, v in ((k.upper(), v) for k, v in (defaults.items() if defaults else ())):
        if isinstance(v, (tuple, list)) and not isinstance(getattr(val, k), (tuple, list)):
            setattr(val, k, [getattr(val, k)])
    return val


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


def find_files(names=(), paths=(), extensions=(), skip_extensions=(), recurse=False):
    """
    Yields filenames from current directory or given paths.

    Seeks only files with given extensions if names not given.
    Logs errors for names and paths not found.

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
            ConsolePrinter.log(logging.ERROR, "%s: Is a file", directory)
            return
        for path in sorted(glob.glob(directory)):  # Expand * wildcards, if any
            pathsfound.add(directory)
            for n in names:
                p = n if not paths or os.path.isabs(n) else os.path.join(path, n)
                for f in (f for f in glob.glob(p) if "*" not in n
                          or not any(map(f.endswith, skip_extensions))):
                    if os.path.isdir(f):
                        ConsolePrinter.log(logging.ERROR, "%s: Is a directory", f)
                        continue  # for n
                    namesfound.add(n)
                    yield f
            for root, _, files in os.walk(path) if not names else ():
                for f in (os.path.join(root, f) for f in sorted(files)
                          if (not extensions or any(map(f.endswith, extensions)))
                          and not any(map(f.endswith, skip_extensions))):
                    yield f
                if not recurse:
                    break  # for root

    processed = set()
    for f in (f for p in paths or ["."] for f in iter_files(p)):
        if os.path.abspath(f) not in processed:
            processed.add(os.path.abspath(f))
            if not paths and f == os.path.join(".", os.path.basename(f)):
                f = os.path.basename(f)  # Strip leading "./"
            yield f

    for path in (p for p in paths if p not in pathsfound):
        ConsolePrinter.log(logging.ERROR, "%s: No such directory", path)
    for name in (n for n in names if n not in namesfound):
        ConsolePrinter.log(logging.ERROR, "%s: No such file", name)


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


def format_bytes(size, precision=2, inter=" ", strip=True):
    """Returns a formatted byte size (like 421.40 MB), trailing zeros optionally removed."""
    result = "0 bytes"
    if size:
        UNITS = [("bytes", "byte")[1 == size]] + [x + "B" for x in "KMGTPEZY"]
        exponent = min(int(math.log(size, 1024)), len(UNITS) - 1)
        result = "%.*f" % (precision, size / (1024. ** exponent))
        result += "" if precision > 0 else "."  # Do not strip integer zeroes
        result = (drop_zeros(result) if strip else result) + inter + UNITS[exponent]
    return result


def format_stamp(stamp):
    """Returns ISO datetime from UNIX timestamp."""
    return datetime.datetime.fromtimestamp(stamp).isoformat(sep=" ")


def get_name(obj):
    """
    Returns the fully namespaced name for a Python module, class, function or object.

    E.g. "my.thing" or "my.module.MyCls" or "my.module.MyCls.my_method"
    or "my.module.MyCls<0x1234abcd>" or "my.module.MyCls<0x1234abcd>.my_method".
    """
    namer = lambda x: getattr(x, "__qualname__", getattr(x, "__name__", ""))
    if inspect.ismodule(obj): return namer(obj)
    if inspect.isclass(obj):  return ".".join((obj.__module__, namer(obj)))
    if inspect.isroutine(obj):
        parts, self = [], six.get_method_self(obj)
        if self is not None:           parts.extend((get_name(self), obj.__name__))
        elif hasattr(obj, "im_class"): parts.extend((get_name(obj.im_class), namer(obj)))  # Py2
        else:                          parts.extend((obj.__module__, namer(obj)))          # Py3
        return ".".join(parts)
    cls = type(obj)
    return "%s.%s<0x%x>" % (cls.__module__, namer(cls), id(obj))


def has_arg(func, name):
    """Returns whether function supports taking specified argument by name."""
    spec = getattr(inspect, "getfullargspec", inspect.getargspec)(func)  # Py3/Py2
    return name in spec.args or name in getattr(spec, "kwonlyargs", ()) or \
           getattr(spec, "varkw", None) or getattr(spec, "keywords", None)


def import_item(name):
    """
    Returns imported module, or identifier from imported namespace; raises on error.

    @param   name  Python module name like "my.module"
                   or module namespace identifier like "my.module.Class"
    """
    result, parts = None, name.split(".")
    for i, item in enumerate(parts):
        path, success = ".".join(parts[:i + 1]), False
        try: result, success = importlib.import_module(path), True
        except ImportError: pass
        if not success and i:
            try: result, success = getattr(result, item), True
            except AttributeError: pass
        if not success:
            raise ImportError("No module or identifier named %r" % path)
    return result


def is_iterable(value):
    """Returns whether value is iterable."""
    try: iter(value)
    except Exception: return False
    return True


def is_stream(value):
    """Returns whether value is a file-like object."""
    try: return isinstance(value, (file, io.IOBase))       # Py2
    except NameError: return isinstance(value, io.IOBase)  # Py3


def makedirs(path):
    """Creates directory structure for path if not already existing."""
    parts, accum = list(filter(bool, os.path.realpath(path).split(os.sep))), []
    while parts:
        accum.append(parts.pop(0))
        curpath = os.path.join(os.sep, accum[0] + os.sep, *accum[1:])  # Windows drive letter thing
        if not os.path.exists(curpath):
            os.mkdir(curpath)


def structcopy(value):
    """
    Returns a deep copy of a standard data structure (dict, list, set, tuple),
    other object types reused instead of copied.
    """
    COLLECTIONS = (dict, list, set, tuple)
    memo = {}
    def collect(x):  # Walk structure and collect objects to skip copying
        if isinstance(x, argparse.Namespace): x = vars(x)
        if not isinstance(x, COLLECTIONS): return memo.update([(id(x), x)])
        for y in sum(map(list, x.items()), []) if isinstance(x, dict) else x: collect(y)
    collect(value)
    return copy.deepcopy(value, memo)


def memoize(func):
    """Returns a results-caching wrapper for the function, cache used if arguments hashable."""
    cache = {}
    def inner(*args, **kwargs):
        key = args + sum(kwargs.items(), ())
        try: hash(key)
        except Exception: return func(*args, **kwargs)
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]
    return functools.update_wrapper(inner, func)


def merge_dicts(d1, d2):
    """Merges d2 into d1, recursively for nested dicts."""
    for k, v in d2.items():
        if k in d1 and isinstance(v, dict) and isinstance(d1[k], dict):
            merge_dicts(d1[k], v)
        else:
            d1[k] = v


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


def plural(word, items=None, numbers=True, single="1", sep=",", pref="", suf=""):
    """
    Returns the word as 'count words', or '1 word' if count is 1,
    or 'words' if count omitted.

    @param   items      item collection or count,
                        or None to get just the plural of the word
    @param   numbers    if False, count is omitted from final result
    @param   single     prefix to use for word if count is 1, e.g. "a"
    @param   sep        thousand-separator to use for count
    @param   pref       prefix to prepend to count, e.g. "~150"
    @param   suf        suffix to append to count, e.g. "150+"
    """
    count   = len(items) if hasattr(items, "__len__") else items or 0
    isupper = word[-1:].isupper()
    suffix = "es" if word and word[-1:].lower() in "xyz" \
             and not word[-2:].lower().endswith("ay") \
             else "s" if word else ""
    if isupper: suffix = suffix.upper()
    if count != 1 and "es" == suffix and "y" == word[-1:].lower():
        word = word[:-1] + ("I" if isupper else "i")
    result = word + ("" if 1 == count else suffix)
    if numbers and items is not None:
        if 1 == count: fmtcount = single
        elif not count: fmtcount = "0"
        elif sep: fmtcount = "".join([
            x + (sep if i and not i % 3 else "") for i, x in enumerate(str(count)[::-1])
        ][::-1])
        else: fmtcount = str(count)

        fmtcount = pref + fmtcount + suf
        result = "%s %s" % (single if 1 == count else fmtcount, result)
    return result.strip()


def unique_path(pathname, empty_ok=False):
    """
    Returns a unique version of the path.

    If a file or directory with the same name already exists, returns a unique
    version (e.g. "/tmp/my.2.file" if ""/tmp/my.file" already exists).

    @param   empty_ok  whether to ignore existence if file is empty
    """
    result = pathname
    if "linux2" == sys.platform and six.PY2 and isinstance(result, six.text_type) \
    and "utf-8" != sys.getfilesystemencoding():
        result = result.encode("utf-8") # Linux has trouble if locale not UTF-8
    if os.path.isfile(result) and empty_ok and not os.path.getsize(result):
        return result if isinstance(result, STRING_TYPES) else str(result)
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


def verify_io(f, mode):
    """
    Returns whether stream or file path can be read from and/or written to as binary.

    Prints or raises error if not.

    Tries to open file in append mode if verifying path writability,
    auto-creating missing directories if any, will delete any file or directory created.

    @param   f     file path, or stream
    @param   mode  "r" for readable, "w" for writable, "a" for readable and writable
    """
    result, op = True, ""
    if is_stream(f):
        try:
            pos = f.tell()
            if mode in ("r", "a"):
                op = " reading from"
                result = isinstance(f.read(1), bytes)
            if result and mode in ("w", "a"):
                op = " writing to"
                result, _ = True, f.write(b"")
            f.seek(pos)
            return result
        except Exception as e:
            ConsolePrinter.log(logging.ERROR, "Error%s %s: %s", op, type(f).__name__, e)
            return False

    present, paths_created = os.path.exists(f), []
    try:
        if not present and mode in ("w", "a"):
            op = " writing to"
            path = os.path.realpath(os.path.dirname(f))
            parts, accum = [x for x in path.split(os.sep) if x], []
            while parts:
                accum.append(parts.pop(0))
                curpath = os.path.join(os.sep, accum[0] + os.sep, *accum[1:])  # Windows drive letter thing
                if not os.path.exists(curpath):
                    os.mkdir(curpath)
                    paths_created.append(curpath)
        elif not present and "r" == mode:
            return False
        with open(f, {"r": "rb", "w": "ab", "a": "ab+"}[mode]) as g:
            if mode in ("r", "a"):
                op = " reading from"
                result = isinstance(g.read(1), bytes)
            if result and mode in ("w", "a"):
                op = " writing to"
                result, _ = True, g.write(b"")
            return result
    except Exception as e:
        ConsolePrinter.log(logging.ERROR, "Error%s %s: %s", f, e)
        return False
    finally:
        if not present:
            try: os.remove(f)
            except Exception: pass
            for path in paths_created[::-1]:
                try: os.rmdir(path)
                except Exception: pass


def wildcard_to_regex(text, end=False):
    """
    Returns plain wildcard like "foo*bar" as re.Pattern("foo.*bar", re.I).

    @param   end  whether pattern should match until end (adds $)
    """
    suff = "$" if end else ""
    return re.compile(".*".join(map(re.escape, text.split("*"))) + suff, re.I)


__all__ = [
    "PATH_TYPES", "ConsolePrinter", "Decompressor", "MatchMarkers", "ProgressBar", "TextWrapper",
    "drop_zeros", "ellipsize", "ensure_namespace", "filter_dict", "find_files",
    "format_bytes", "format_stamp", "format_timedelta", "get_name", "has_arg", "import_item",
    "is_iterable", "is_stream", "makedirs", "memoize", "merge_dicts", "merge_spans",
    "parse_datetime", "plural", "unique_path", "verify_io", "wildcard_to_regex",
]

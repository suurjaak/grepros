# -*- coding: utf-8 -*-
## @namespace grepros.common
"""
Common utilities.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    28.11.2021
------------------------------------------------------------------------------
"""
from __future__ import print_function
import datetime
import glob
import itertools
import math
import os
import random
import re
import shutil
import sys
import threading
import time
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

    DEBUG_START, DEBUG_END = STYLE_LOWLIGHT, STYLE_RESET  ## Metainfo wrappers
    ERROR_START, ERROR_END = STYLE_ERROR,    STYLE_RESET  ## Error message wrappers

    COLOR = None       ## Whether using colors in output

    WIDTH = 80         ## Console width in characters, updated from shutil and curses

    PRINTS = {}        ## {sys.stdout: number of texts printed, sys.stderr: ..}

    _LINEOPEN = False  ## Whether last print was without linefeed

    @classmethod
    def configure(cls, args):
        """
        Initializes terminal for color output, or disables color output if unsupported.

        @param   args                 arguments object like argparse.Namespace
        @param   args.COLOR           "never", "always", or "auto" for when supported by TTY
        """
        try: cls.WIDTH = shutil.get_terminal_size().columns  # Py3
        except Exception: pass  # Py2
        cls.COLOR = ("never" != args.COLOR)
        try:
            curses.setupterm()
            if cls.COLOR and not sys.stdout.isatty():
                raise Exception()
        except Exception:
            cls.COLOR = ("always" == args.COLOR)
        try:
            if sys.stdout.isatty() or cls.COLOR:
                cls.WIDTH = curses.initscr().getmaxyx()[1]
            curses.endwin()
        except Exception: pass

        if cls.COLOR:
            cls.DEBUG_START, cls.DEBUG_END = cls.STYLE_LOWLIGHT, cls.STYLE_RESET
            cls.ERROR_START, cls.ERROR_END = cls.STYLE_ERROR,    cls.STYLE_RESET
        else:
            cls.DEBUG_START, cls.DEBUG_END = "", ""
            cls.ERROR_START, cls.ERROR_END = "", ""


    @classmethod
    def print(cls, text="", *args, **kwargs):
        """Prints text, formatted with args and kwargs."""
        fileobj, end = kwargs.pop("__file", sys.stdout), kwargs.pop("__end", "\n")
        pref, suff = kwargs.pop("__prefix", ""), kwargs.pop("__suffix", "")
        text = str(text)
        try: text = text % args if args else text
        except Exception: pass
        try: text = text % kwargs if kwargs else text
        except Exception: pass
        try: text = text.format(*args, **kwargs) if args or kwargs else text
        except Exception: pass
        if cls._LINEOPEN and "\n" in end: pref = "\n" + pref  # Add linefeed to end open line

        cls.PRINTS[fileobj] = cls.PRINTS.get(fileobj, 0) + 1
        cls._LINEOPEN = "\n" not in end
        print(pref + text + suff, end=end, file=fileobj)
        not fileobj.isatty() and fileobj.flush()


    @classmethod
    def error(cls, text="", *args, **kwargs):
        """Prints error to stderr, formatted with args and kwargs, in error colors if supported."""
        KWS = dict(__file=sys.stderr, __prefix=cls.ERROR_START, __suffix=cls.ERROR_END)
        cls.print(text, *args, **dict(kwargs, **KWS))


    @classmethod
    def debug(cls, text="", *args, **kwargs):
        """
        Prints debug text to stderr.

        Formatted with args and kwargs, in lowlight colors if supported.
        """
        KWS = dict(__file=sys.stderr, __prefix=cls.DEBUG_START, __suffix=cls.DEBUG_END)
        cls.print(text, *args, **dict(kwargs, **KWS))



class ProgressBar(threading.Thread):
    """
    A simple ASCII progress bar with a ticker thread

    Drawn like
    '[---------\   36%            ] Progressing text..'.
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
        @param   counts         print value and nax afterword
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
        if not pulse: self.update(value, draw=False)


    def update(self, value=None, draw=True, flush=False):
        """Updates the progress bar value, and refreshes by default."""
        if value is not None: self.value = min(self.max, max(self.min, value))
        afterword = self.aftertemplate.format(**vars(self))
        w_full = self.width - 2
        if self.pulse:
            if self.pulse_pos is None:
                bartext = "%s[%s]%s" % (self.foreword,
                                        self.forechar * (self.width - 2),
                                        afterword)
            else:
                dash = self.forechar * max(1, (self.width - 2) / 7)
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
        if draw and prevbar != self.bar: self.draw(flush)


    def draw(self, flush=False):
        """Prints the progress bar, from the beginning of the current line."""
        ConsolePrinter.print("\r" + self.printbar, __end=" ")
        if len(self.printbar) != len(self.bar):  # Draw twice to position caret at true content end
            self.printbar = self.bar
            ConsolePrinter.print("\r" + self.printbar, __end=" ")
        if flush: ConsolePrinter.print()


    def run(self):
        self.is_running = True
        while self.is_running:
            if not self.pause: self.update(self.value)
            time.sleep(self.interval)


    def stop(self):
        self.is_running = False



class TextWrapper(object):
    """
    TextWrapper that supports custom substring widths in line width calculation.

    Intended for wrapping text containing ANSI control codes.
    Heavily refactored from Python standard library textwrap.TextWrapper.
    """

    ## Regex for breaking text at whitespace
    SPACE_RGX = re.compile(r"([%s]+)" % re.escape("\t\n\x0b\x0c\r "))


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
        return result


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

            if self.drop_whitespace and lines and self.isblank(chunks[-1]):
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

            if self.drop_whitespace and cur_line and self.isblank(cur_line[-1]):
                cur_len -= len(cur_line[-1])  # Drop line last whitespace chunk
                del cur_line[-1]

            if cur_line:
                if (self.max_lines is None or len(lines) + 1 < self.max_lines
                or (not chunks or self.drop_whitespace
                    and len(chunks) == 1 and self.isblank(chunks[0])) \
                and cur_len <= width):  # Current line ok
                    lines.append(indent + "".join(cur_line))
                    continue  # while chunks
            else:
                continue  # while chunks

            while cur_line:  # Truncate for max_lines
                if not self.isblank(cur_line[-1]):
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


def quote(name, force=False):
    """
    Returns name in quotes and proper-escaped for SQLite queries.

    @param   force  quote even if name does not need quoting (starts with a letter,
                    contains only alphanumerics, and is not a reserved keyword)
    """
    ## Words that need quoting if in name context, e.g. table name.
    SQLITE_RESERVED_KEYWORDS = [
        "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE",
        "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN",
        "BY", "CASE", "CAST", "CHECK", "COLLATE", "COMMIT", "CONSTRAINT", "CREATE",
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DEFAULT", "DEFERRABLE",
        "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH",
        "ELSE", "END", "ESCAPE", "EXCEPT", "EXISTS", "EXPLAIN", "FOR", "FOREIGN",
        "FROM", "GENERATED", "GROUP", "HAVING", "IF", "IMMEDIATE", "IN", "INDEX",
        "INITIALLY", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL",
        "JOIN", "KEY", "LIKE", "LIMIT", "MATCH", "NO", "NOT", "NOTHING", "NOTNULL",
        "NULL", "OF", "ON", "OR", "ORDER", "OVER", "PRAGMA", "PRECEDING", "PRIMARY",
        "RAISE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME",
        "REPLACE", "RESTRICT", "ROLLBACK", "SAVEPOINT", "SELECT", "SET", "TABLE",
        "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED",
        "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "WHEN",
        "WHERE", "WITHOUT"
    ]
    result = name
    if force or result.upper() in SQLITE_RESERVED_KEYWORDS \
    or re.search(r"(^[\W\d])|(?=\W)", result, re.U):
        result = '"%s"' % result.replace('"', '""')
    return result


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

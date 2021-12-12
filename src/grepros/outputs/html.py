# -*- coding: utf-8 -*-
"""
HTML output for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.12.2021
@modified    12.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs.html
import atexit
import copy
import os
try: import queue  # Py3
except ImportError: import Queue as queue  # Py2
import re
import sys
import threading

from .. common import ConsolePrinter, MatchMarkers, format_bytes, plural, unique_path
from .. import rosapi
from .. vendor import step
from . base import SinkBase, TextSinkMixin


class HtmlSink(SinkBase, TextSinkMixin):
    """Writes messages to an HTML file."""

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".htm", ".html")

    ## HTML template path
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "html.tpl")

    ## Character wrap width for message YAML
    WRAP_WIDTH = 120

    def __init__(self, args):
        """
        @param   args                 arguments object like argparse.Namespace
        @param   args.META            whether to print metainfo
        @param   args.DUMP_TARGET     name of HTML file to write,
                                      will add counter like .2 to filename if exists
        @param   args.DUMP_OPTIONS    {"template": path to custom HTML template, if any}
        @param   args.VERBOSE         whether to print debug information
        @param   args.MATCH_WRAPPER   string to wrap around matched values,
                                      both sides if one value, start and end if more than one,
                                      or no wrapping if zero values
        @param   args.ORDERBY         "topic" or "type" if any to group results by
        """
        args = copy.deepcopy(args)
        args.WRAP_WIDTH = self.WRAP_WIDTH
        args.COLOR = "always"

        super(HtmlSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)
        self._queue    = queue.Queue()
        self._writer   = None              # threading.Thread running _stream()
        self._filename = args.DUMP_TARGET  # Filename base, will be made unique
        self._template_path = args.DUMP_OPTIONS.get("template") or self.TEMPLATE_PATH
        self._close_printed = False

        WRAPS = ((args.MATCH_WRAPPER or [""]) * 2)[:2]
        self._tag_repls = {MatchMarkers.START:            '<span class="match">' +
                                                          step.escape_html(WRAPS[0]),
                           MatchMarkers.END:              step.escape_html(WRAPS[1]) + '</span>',
                           ConsolePrinter.STYLE_LOWLIGHT: '<span class="lowlight">',
                           ConsolePrinter.STYLE_RESET:    '</span>'}
        self._tag_rgx = re.compile("(%s)" % "|".join(map(re.escape, self._tag_repls)))

        self._format_repls.clear()
        atexit.register(self.close)

    def emit(self, topic, index, stamp, msg, match):
        """Writes message to output file."""
        self._queue.put((topic, index, stamp, msg, match))
        if not self._writer:
            self._writer = threading.Thread(target=self._stream)
            self._writer.start()
        if self._queue.qsize() > 100: self._queue.join()

    def validate(self):
        """
        Returns whether custom template exists and ROS environment is set, prints error if not.
        """
        result = True
        if self._args.DUMP_OPTIONS.get("template") and not os.path.isfile(self._template_path):
            result = False
            ConsolePrinter.error("Template does not exist: %s.", self._template_path)
        return rosapi.validate() and result

    def close(self):
        """Closes output file, if any."""
        if self._writer:
            writer, self._writer = self._writer, None
            self._queue.put(None)
            writer.is_alive() and writer.join()
        if not self._close_printed and self._counts:
            self._close_printed = True
            ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                 plural("message", sum(self._counts.values())),
                                 plural("topic", len(self._counts)), self._filename,
                                 format_bytes(os.path.getsize(self._filename)))
        super(HtmlSink, self).close()

    def flush(self):
        """Writes out any pending data to disk."""
        self._queue.join()

    def format_message(self, msg, highlight=False):
        """Returns message as formatted string, optionally highlighted for matches."""
        text = TextSinkMixin.format_message(self, msg, highlight)
        text = "".join(self._tag_repls.get(x) or step.escape_html(x)
                       for x in self._tag_rgx.split(text))
        return text

    def is_highlighting(self):
        """Returns True (requires highlighted matches)."""
        return True

    def _stream(self):
        """Writer-loop, streams HTML template to file."""
        if not self._writer:
            return

        try:
            with open(self._template_path, "r") as f: tpl = f.read()
            template = step.Template(tpl, escape=True, strip=False)
            ns = dict(source=self.source, sink=self, args=["grepros"] + sys.argv[1:],
                      timeline=not self._args.ORDERBY, messages=self._produce())
            self._filename = unique_path(self._filename, empty_ok=True)
            if self._args.VERBOSE:
                ConsolePrinter.debug("Creating %s.", self._filename)
            with open(self._filename, "wb") as f:
                template.stream(f, ns, unbuffered=True)
        except Exception as e:
            self.thread_excepthook(e)
        finally:
            self._writer = None

    def _produce(self):
        """Yields messages from emit queue, as (topic, index, stamp, msg, match)."""
        while True:
            entry = self._queue.get()
            self._queue.task_done()
            if entry is None:
                break  # while
            (topic, index, stamp, msg, match) = entry
            topickey = (topic, self.source.get_message_type_hash(msg))
            if self._args.VERBOSE and topickey not in self._counts:
                ConsolePrinter.debug("Adding topic %s.", topic)
            yield entry
            super(HtmlSink, self).emit(topic, index, stamp, msg, match)
        try:
            while self._queue.get_nowait() or True: self._queue.task_done()
        except queue.Empty: pass

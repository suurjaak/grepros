# -*- coding: utf-8 -*-
"""
HTML output plugin.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     03.12.2021
@modified    04.07.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins.auto.html
import atexit
import os
try: import queue  # Py3
except ImportError: import Queue as queue  # Py2
import re
import threading

from ... import api
from ... import common
from ... import main
from ... common import ConsolePrinter, MatchMarkers, plural
from ... outputs import Sink, TextSinkMixin
from ... vendor import step


class HtmlSink(Sink, TextSinkMixin):
    """Writes messages to an HTML file."""

    ## Auto-detection file extensions
    FILE_EXTENSIONS = (".htm", ".html")

    ## HTML template path
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "html.tpl")

    ## Character wrap width for message YAML
    WRAP_WIDTH = 120

    ## Constructor argument defaults
    DEFAULT_ARGS = dict(META=False, WRITE_OPTIONS={}, HIGHLIGHT=True, MATCH_WRAPPER=None,
                        ORDERBY=None, VERBOSE=False, COLOR=True, EMIT_FIELD=(), NOEMIT_FIELD=(), 
                        MAX_FIELD_LINES=None, START_LINE=None, END_LINE=None,
                        MAX_MESSAGE_LINES=None, LINES_AROUND_MATCH=None, MATCHED_FIELDS_ONLY=False,
                        WRAP_WIDTH=None)

    def __init__(self, args=None, **kwargs):
        """
        @param   args                       arguments as namespace or dictionary, case-insensitive;
                                            or a single path as the name of HTML file to write
        @param   args.write                 name of HTML file to write,
                                            will add counter like .2 to filename if exists
        @param   args.write_options         ```
                                            {"template": path to custom HTML template, if any,
                                             "overwrite": whether to overwrite existing file
                                                          (default false)}
                                            ```
        @param   args.highlight             highlight matched values (default true)
        @param   args.orderby               "topic" or "type" if any to group results by
        @param   args.color                 False or "never" for not using colors in replacements
        @param   args.emit_field            message fields to emit if not all
        @param   args.noemit_field          message fields to skip in output
        @param   args.max_field_lines       maximum number of lines to output per field
        @param   args.start_line            message line number to start output from
        @param   args.end_line              message line number to stop output at
        @param   args.max_message_lines     maximum number of lines to output per message
        @param   args.lines_around_match    number of message lines around matched fields to output
        @param   args.matched_fields_only   output only the fields where match was found
        @param   args.wrap_width            character width to wrap message YAML output at
        @param   args.match_wrapper         string to wrap around matched values,
                                            both sides if one value, start and end if more than one,
                                            or no wrapping if zero values
        @param   args.meta                  whether to emit metainfo
        @param   args.verbose               whether to emit debug information
        @param   kwargs                     any and all arguments as keyword overrides,
                                            case-insensitive
        """
        args = {"WRITE": str(args)} if isinstance(args, common.PATH_TYPES) else args
        args = common.ensure_namespace(args, HtmlSink.DEFAULT_ARGS, **kwargs)
        args.WRAP_WIDTH = self.WRAP_WIDTH
        args.COLOR = bool(args.HIGHLIGHT)

        super(HtmlSink, self).__init__(args)
        TextSinkMixin.__init__(self, args)
        self._queue     = queue.Queue()
        self._writer    = None        # threading.Thread running _stream()
        self._filename  = args.WRITE  # Filename base, will be made unique
        self._overwrite = (args.WRITE_OPTIONS.get("overwrite") in (True, "true"))
        self._template_path = args.WRITE_OPTIONS.get("template") or self.TEMPLATE_PATH
        self._close_printed = False

        WRAPS = ((args.MATCH_WRAPPER or [""]) * 2)[:2]
        START = ('<span class="match">' + step.escape_html(WRAPS[0])) if args.HIGHLIGHT else ""
        END = (step.escape_html(WRAPS[1]) + '</span>') if args.HIGHLIGHT else ""
        self._tag_repls = {MatchMarkers.START: START,
                           MatchMarkers.END:   END,
                           ConsolePrinter.STYLE_LOWLIGHT: '<span class="lowlight">',
                           ConsolePrinter.STYLE_RESET:    '</span>'}
        self._tag_rgx = re.compile("(%s)" % "|".join(map(re.escape, self._tag_repls)))

        self._format_repls.clear()
        atexit.register(self.close)

    def emit(self, topic, msg, stamp=None, match=None, index=None):
        """Writes message to output file."""
        if not self.validate(): raise Exception("invalid")
        stamp, index = self._ensure_stamp_index(topic, msg, stamp, index)
        self._queue.put((topic, msg, stamp, match, index))
        if not self._writer:
            self._writer = threading.Thread(target=self._stream)
            self._writer.start()
        if self._queue.qsize() > 100: self._queue.join()

    def validate(self):
        """
        Returns whether write options are valid and ROS environment is set and file is writable,
        emits error if not.
        """
        if self.valid is not None: return self.valid
        result = True
        if self.args.WRITE_OPTIONS.get("template") and not os.path.isfile(self._template_path):
            result = False
            ConsolePrinter.error("Template does not exist: %s.", self._template_path)
        if self.args.WRITE_OPTIONS.get("overwrite") not in (None, True, False, "true", "false"):
            ConsolePrinter.error("Invalid overwrite option for HTML: %r. "
                                 "Choose one of {true, false}.",
                                 self.args.WRITE_OPTIONS["overwrite"])
            result = False
        if not common.verify_io(self.args.WRITE, "w"):
            result = False
        self.valid = api.validate() and result
        return self.valid

    def close(self):
        """Closes output file, if any."""
        try:
            if self._writer:
                writer, self._writer = self._writer, None
                self._queue.put(None)
                writer.is_alive() and writer.join()
        finally:
            if not self._close_printed and self._counts:
                self._close_printed = True
                try: sz = common.format_bytes(os.path.getsize(self._filename))
                except Exception as e:
                    ConsolePrinter.warn("Error getting size of %s: %s", self._filename, e)
                    sz = "error getting size"
                ConsolePrinter.debug("Wrote %s in %s to %s (%s).",
                                     plural("message", sum(self._counts.values())),
                                     plural("topic", self._counts), self._filename, sz)
            super(HtmlSink, self).close()

    def flush(self):
        """Writes out any pending data to disk."""
        self._queue.join()

    def format_message(self, msg, highlight=False):
        """Returns message as formatted string, optionally highlighted for matches if configured."""
        text = TextSinkMixin.format_message(self, msg, self.args.HIGHLIGHT and highlight)
        text = "".join(self._tag_repls.get(x) or step.escape_html(x)
                       for x in self._tag_rgx.split(text))
        return text

    def is_highlighting(self):
        """Returns True if sink is configured to highlight matched values."""
        return bool(self.args.HIGHLIGHT)

    def _stream(self):
        """Writer-loop, streams HTML template to file."""
        if not self._writer:
            return

        try:
            with open(self._template_path, "r") as f: tpl = f.read()
            template = step.Template(tpl, escape=True, strip=False)
            ns = dict(source=self.source, sink=self, messages=self._produce(),
                      args=None, timeline=not self.args.ORDERBY)
            if main.CLI_ARGS: ns.update(args=main.CLI_ARGS)
            common.makedirs(os.path.dirname(self.args.WRITE))
            if not self._overwrite:
                self._filename = common.unique_path(self.args.WRITE, empty_ok=True)
            if self.args.VERBOSE:
                sz = os.path.isfile(self._filename) and os.path.getsize(self._filename)
                action = "Overwriting" if sz and self._overwrite else "Creating"
                ConsolePrinter.debug("%s %s.", action, self._filename)
            with open(self._filename, "wb") as f:
                template.stream(f, ns, unbuffered=True)
        except Exception as e:
            self.thread_excepthook("Error writing HTML output %r: %r" % (self._filename, e), e)
        finally:
            self._writer = None

    def _produce(self):
        """Yields messages from emit queue, as (topic, msg, stamp, match, index)."""
        while True:
            entry = self._queue.get()
            if entry is None:
                self._queue.task_done()
                break  # while
            (topic, msg, stamp, match, index) = entry
            topickey = api.TypeMeta.make(msg, topic).topickey
            if self.args.VERBOSE and topickey not in self._counts:
                ConsolePrinter.debug("Adding topic %s in HTML output.", topic)
            yield entry
            super(HtmlSink, self).emit(topic, msg, stamp, match, index)
            self._queue.task_done()
        try:
            while self._queue.get_nowait() or True: self._queue.task_done()
        except queue.Empty: pass



def init(*_, **__):
    """Adds HTML output format support."""
    from ... import plugins  # Late import to avoid circular
    plugins.add_write_format("html", HtmlSink, "HTML", [
        ("template=/my/path.tpl",  "custom template to use for HTML output"),
        ("overwrite=true|false",   "overwrite existing file in HTML output\n"
                                   "instead of appending unique counter (default false)")
    ])
    plugins.add_output_label("HTML", ["--emit-field", "--no-emit-field", "--matched-fields-only",
                                      "--lines-around-match", "--lines-per-field", "--start-line",
                                      "--end-line", "--lines-per-message", "--match-wrapper"])


__all__ = ["HtmlSink", "init"]

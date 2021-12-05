# -*- coding: utf-8 -*-
"""
Outputs for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    05.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs
import os

from .. import rosapi
from .. common import ConsolePrinter
from . base     import SinkBase, TextSinkMixin, ConsoleSink, BagSink, TopicSink
from . csv      import CsvSink
from . html     import HtmlSink
from . postgres import PostgresSink
from . sqlite   import SqliteSink


class MultiSink(SinkBase):
    """Combines any number of sinks."""

    ## Autobinding between argument flags and sink classes
    FLAG_CLASSES = {"PUBLISH": TopicSink, "CONSOLE": ConsoleSink}

    ## Autobinding between argument flags+subflags and sink classes
    SUBFLAG_CLASSES = {"DUMP_TARGET": {"DUMP_FORMAT": {
        "bag": BagSink, "csv": CsvSink, "html": HtmlSink,
        "postgres": PostgresSink, "sqlite": SqliteSink,
    }}}

    def __init__(self, args):
        """
        @param   args               arguments object like argparse.Namespace
        @param   args.CONSOLE       print matches to console
        @param   args.DUMP_TARGET   write matches to output file
        @param   args.DUMP_FORMAT   output file format, "bag" or "html"
        @param   args.PUBLISH       publish matches to live topics
        """
        super(MultiSink, self).__init__(args)

        ## List of all combined sinks
        self.sinks = [cls(args) for flag, cls in self.FLAG_CLASSES.items()
                      if getattr(args, flag, None)]
        for flag, opts in self.SUBFLAG_CLASSES.items():
            cls = None
            for subflag, binding in opts.items() if getattr(args, flag, None) else ():
                cls = getattr(args, subflag, None) and binding.get(getattr(args, subflag, None))

                for sinkcls in binding.values() if not cls else ():
                    if callable(getattr(sinkcls, "autodetect", None)):
                        cls = sinkcls.autodetect(getattr(args, flag)) and sinkcls
                        if cls:
                            break  # for sinkcls
                if cls:
                    break  # for subflag
            if cls:
                self.sinks += [cls(args)]
                break  # for flag
            elif getattr(args, flag, None):
                raise Exception("Unknown output format.")

    def emit_meta(self):
        """Outputs source metainfo in one sink, if not already emitted."""
        sink = next((s for s in self.sinks if isinstance(s, ConsoleSink)), None)
        # Print meta in one sink only, prefer console
        sink = sink or self.sinks[0] if self.sinks else None
        sink and sink.emit_meta()

    def emit(self, topic, index, stamp, msg, match):
        """Outputs ROS message to all sinks."""
        for sink in self.sinks:
            sink.emit(topic, index, stamp, msg, match)

    def bind(self, source):
        """Attaches source to all sinks."""
        SinkBase.bind(self, source)
        for sink in self.sinks:
            sink.bind(source)

    def validate(self):
        """Returns whether prerequisites are met for all sinks."""
        if not self.sinks:
            ConsolePrinter.error("No output configured.")
        return bool(self.sinks) and all([sink.validate() for sink in self.sinks])

    def close(self):
        """Closes all sinks."""
        for sink in self.sinks:
            sink.close()

    def flush(self):
        """Flushes all sinks."""
        for sink in self.sinks:
            sink.flush()

    def is_highlighting(self):
        """Returns whether any sink requires highlighted matches."""
        return any(s.is_highlighting() for s in self.sinks)

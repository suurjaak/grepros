# -*- coding: utf-8 -*-
"""
Outputs for search results.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    03.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.outputs
import os

from .. import rosapi
from . base   import SinkBase, TextSinkMixin, ConsoleSink, BagSink, TopicSink
from . csv    import CsvSink
from . html   import HtmlSink
from . sqlite import SqliteSink


class MultiSink(SinkBase):
    """Combines any number of sinks."""

    ## Autobinding between argument flags and sink classes
    FLAG_CLASSES = {"PUBLISH": TopicSink, "CONSOLE": ConsoleSink}

    ## Autobinding between argument flags+subflags and sink classes
    SUBFLAG_CLASSES = {"OUTFILE": {"OUTFILE_FORMAT": {"bag": BagSink, "html":   HtmlSink,
                                                      "csv": CsvSink, "sqlite": SqliteSink}}}

    ## Autobinding between flag value file extension and subflags
    SUBFLAG_AUTODETECTS = {"OUTFILE": {"OUTFILE_FORMAT": {
        "bag":  lambda: rosapi.BAG_EXTENSIONS, "csv": (".csv", ),  # Need late binding
        "html": (".htm", ".html"),             "sqlite": (".sqlite", ".sqlite3")}
    }}

    def __init__(self, args):
        """
        @param   args                  arguments object like argparse.Namespace
        @param   args.CONSOLE          print matches to console
        @param   args.OUTFILE          write matches to output file
        @param   args.OUTFILE_FORMAT   output file format, "bag" or "html"
        @param   args.PUBLISH          publish matches to live topics
        """
        super(MultiSink, self).__init__(args)

        ## List of all combined sinks
        self.sinks = [cls(args) for flag, cls in self.FLAG_CLASSES.items()
                      if getattr(args, flag, None)]
        for flag, opts in self.SUBFLAG_CLASSES.items():
            if not getattr(args, flag, None): continue  # for glag

            found = None
            for subflag, binding in opts.items():
                ext = os.path.splitext(getattr(args, flag))[-1].lower()
                found = False if getattr(args, subflag, None) else None
                for cls in filter(bool, [binding.get(getattr(args, subflag, None))]):
                    self.sinks += [cls(args)]
                    found = True
                    break  # for cls
                if found is None and subflag in self.SUBFLAG_AUTODETECTS.get(flag, {}):
                    subopts = self.SUBFLAG_AUTODETECTS[flag][subflag]
                    subflagval = next((k for k, v in subopts.items()
                                       if ext in (v() if callable(v) else v)), None)
                    if subflagval in opts[subflag]:
                        cls = opts[subflag][subflagval]
                        self.sinks += [cls(args)]
                        found = True
            if not found:
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
        return all([sink.validate() for sink in self.sinks])

    def close(self):
        """Closes all sinks."""
        for sink in self.sinks:
            sink.close()

    def flush(self):
        """Flushes all sinks."""
        for sink in self.sinks:
            sink.flush()

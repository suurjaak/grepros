# -*- coding: utf-8 -*-
"""
Plugins interface.

Allows specifying a custom plugin for "source", "search" or "sink".

Supported (but not required) plugin interface methods:

- `init(args)`: invoked at startup with command-line arguments
- `load(category, args)`: invoked with category "search" or "source" or "sink",
                          using returned value if not None

Plugins are free to modify package internals, like adding command-line arguments
to `main.ARGUMENTS` or sink types to `outputs.MultiSink`.

`plugins.add_format()` has been provided as a convenience method for



Auto-inits any plugins in grepros.plugins.auto.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     18.12.2021
@modified    18.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins
import glob
import importlib
import os

from .. common import ConsolePrinter
from .. outputs import MultiSink
from . import auto


## {"some.module": <module 'some.module' from ..>}
PLUGINS = {}


def init(args=None):
    """Imports and initializes all plugins from given Python module."""
    for f in sorted(glob.glob(os.path.join(os.path.dirname(__file__), "auto", "*.py"))):
        if f.startswith("__"): continue # for f

        name = os.path.splitext(os.path.split(f)[-1])[0]
        modulename = "%s.auto.%s" % (__package__, name)
        try:
            plugin = import_item(modulename)
            if callable(getattr(plugin, "init", None)): plugin.init(args)
            PLUGINS[name] = plugin
        except Exception:
            ConsolePrinter.error("Error loading plugin %s.", modulename)
    if args: configure(args)


def configure(args):
    """
    Imports plugin Python packages, invokes init(args) if any, raises on error.

    @param   args           arguments object like argparse.Namespace
    @param   args.PLUGINS   list of Python modules or classes to import,
                            as ["my.module", "other.module.SomeClass", ]
    """
    for name in (n for n in args.PLUGINS if n not in PLUGINS):
        try:
            plugin = import_item(name)
            if callable(getattr(plugin, "init", None)): plugin.init(args)
            PLUGINS[name] = plugin
        except Exception:
            ConsolePrinter.error("Error loading plugin %s.", name)
            raise


def load(category, args, every=False):
    """
    Returns a plugin category instance loaded from any configured plugin, or None.

    @param   category  item category like "source", "sink", or "search"
    @param   args      arguments object like argparse.Namespace
    @param   every     if true, returns a list of instances,
                       using all plugins that return something
    """
    result = []
    for name, plugin in PLUGINS.items():
        if callable(getattr(plugin, "load", None)):
            try:
                instance = plugin.load(category, args)
                if instance is not None:
                    result.append(instance)
                    if not every:
                        break  # for name, plugin
            except Exception:
                ConsolePrinter.error("Error invoking %s.load(%r, args).", name, category)
                raise
    return result if every else result[0] if result else None


def add_sink_format(name, sinkcls):
    """
    Adds plugin to main.ARGUMENTS and MultiSink formats.

    @param   name     format name like "csv", added to `--write-format`
    @param   sinkcls  class providing SinkBase interface
    """
    from .. import main  # Late import to avoid circular
    writearg = next((d for d in main.ARGUMENTS.get("arguments", [])
                     if ["--write-format"] == d.get("args")), None)
    if writearg and writearg.get("choices"):
        writearg["choices"] = sorted(set(writearg["choices"] + [name]))
    MultiSink.SUBFLAG_CLASSES.setdefault("DUMP_TARGET", {}).setdefault("DUMP_FORMAT", {})
    MultiSink.SUBFLAG_CLASSES["DUMP_TARGET"]["DUMP_FORMAT"][name] = sinkcls


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

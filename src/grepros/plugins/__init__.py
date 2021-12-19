# -*- coding: utf-8 -*-
"""
Plugins interface.

Allows specifying custom plugins for "source", "search" or "sink".
Auto-inits any plugins in grepros.plugins.auto.

Supported (but not required) plugin interface methods:

- `init(args)`: invoked at startup with command-line arguments
- `load(category, args)`: invoked with category "search" or "source" or "sink",
                          using returned value if not None

Plugins are free to modify package internals, like adding command-line arguments
to `main.ARGUMENTS` or sink types to `outputs.MultiSink`.

Convenience methods:

- `plugins.add_write_format(name, cls)`: adds an output plugin to defaults
- `plugins.add_write_options(label, [(name, help)])`: adds options for an output plugin
- `plugins.get_argument(name)`: returns a command-line argument dictionary, or None

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     18.12.2021
@modified    19.12.2021
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins
import glob
import importlib
import os

from .. common import ConsolePrinter
from .. outputs import MultiSink
from . import auto


## {"some.module" or "some.module.Cls": <module 'some.module' from ..> or <class 'some.module.Cls'>}
PLUGINS = {}

## Added write options, as {plugin label: [(name, help), ]}
WRITE_OPTIONS = {}


def init(args=None):
    """
    Imports and initializes all plugins from auto and from given arguments.

    @param   args           arguments object like argparse.Namespace
    @param   args.PLUGINS   list of Python modules or classes to import,
                            as ["my.module", "other.module.SomeClass", ]
    """
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
    populate_write_options()


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
        except ImportWarning:
            raise
        except Exception:
            ConsolePrinter.error("Error loading plugin %s.", name)
            raise


def load(category, args, collect=False):
    """
    Returns a plugin category instance loaded from any configured plugin, or None.

    @param   category  item category like "source", "sink", or "search"
    @param   args      arguments object like argparse.Namespace
    @param   collect   if true, returns a list of instances,
                       using all plugins that return something
    """
    result = []
    for name, plugin in PLUGINS.items():
        if callable(getattr(plugin, "load", None)):
            try:
                instance = plugin.load(category, args)
                if instance is not None:
                    result.append(instance)
                    if not collect:
                        break  # for name, plugin
            except Exception:
                ConsolePrinter.error("Error invoking %s.load(%r, args).", name, category)
                raise
    return result if collect else result[0] if result else None


def add_write_format(name, cls):
    """
    Adds plugin to `--write-format` in main.ARGUMENTS and MultiSink formats.

    @param   name  format name like "csv", added to `--write-format`
    @param   cls   class providing SinkBase interface
    """
    writearg = get_argument("--write-format")
    if writearg and writearg.get("choices"):
        writearg["choices"] = sorted(set(writearg["choices"] + [name]))
    MultiSink.SUBFLAG_CLASSES.setdefault("DUMP_TARGET", {}).setdefault("DUMP_FORMAT", {})
    MultiSink.SUBFLAG_CLASSES["DUMP_TARGET"]["DUMP_FORMAT"][name] = cls


def add_write_options(label, options):
    """
    @param   label    plugin label; if multiple plugins add the same option,
                      the label in help text is replaced with a list of all labels
    @param   options  a sequence of (name, help) to add to --write-option help, like
                      [("template=/my/path.tpl", "custom template to use for HTML output")]
    """
    WRITE_OPTIONS[label] = list(options)


def get_argument(name, group=None):
    """
    Returns a command-line argument dictionary, or None if not found.

    @param   name   argument name like "--write-format"
    @param   group  argument group like "Output control", if any
    """
    from .. import main  # Late import to avoid circular
    if group:
        return next((d for d in main.ARGUMENTS.get("groups", {}).get(group, [])
                     if name in d.get("args")), None)
    return next((d for d in main.ARGUMENTS.get("arguments", [])
                 if name in d.get("args")), None)


def populate_write_options():
    """Populates main.ARGUMENTS with added write options."""
    optionarg = get_argument("--write-option", "Output control")
    if not optionarg or not any(WRITE_OPTIONS.values()): return

    MAXNAME    = 24    # Maximum space for name on same line as help
    LEADING    = "  "  # Leading indent on all option lines 

    texts      = {}  # {name: help}
    inters     = {}  # {name: indent between name and first line of help}
    namelabels = {}  # {name: [label,]}
    namelens   = {}  # {name: len}

    # First pass: collect names
    for label, opts in WRITE_OPTIONS.items():
        for name, help in opts:
            texts.setdefault(name, help)
            namelabels.setdefault(name, []).append(label)
            namelens[name] = len(name)

    # Second pass: calculate indent and inters
    maxname = max(x if x <= MAXNAME else 0 for x in namelens.values())
    for label, opts in WRITE_OPTIONS.items():
        for name, help in opts:
            inters[name] = "\n" if len(name) > MAXNAME else " " * (maxname - len(name) + 2)
    indent = LEADING + "  " + " " * (maxname or MAXNAME)

    # Third pass: replace labels for duplicate options
    PLACEHOLDER = "<plugin label replacement>"
    for name in list(texts):
        if len(namelabels[name]) > 1:
            for label in namelabels[name]:
                texts[name] = texts[name].replace(label, PLACEHOLDER)
            texts[name] = texts[name].replace(PLACEHOLDER, "/".join(sorted(namelabels[name])))

    fmt = lambda n, h: "\n".join((indent if i or "\n" == inters[n] else "") + l
                                 for i, l in enumerate(h.splitlines()))
    text = "\n".join(sorted("".join((LEADING, n, inters[n], fmt(n, h)))
                            for n, h in texts.items()))
    optionarg["help"] = ("output options as key=value pairs:\n" + text).strip()


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

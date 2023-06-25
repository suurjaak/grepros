# -*- coding: utf-8 -*-
"""
Plugins interface.

Allows specifying custom plugins for "source", "scan" or "sink".
Auto-inits any plugins in grepros.plugins.auto.

Supported (but not required) plugin interface methods:

- `init(args)`: invoked at startup with command-line arguments
- `load(category, args)`: invoked with category "scan" or "source" or "sink",
                          using returned value if not None

Plugins are free to modify package internals, like adding command-line arguments
to `main.ARGUMENTS` or sink types to `outputs.MultiSink`.

Convenience methods:

- `plugins.add_write_format(name, cls, label=None, options=((name, help), ))`:
   adds an output plugin to defaults
- `plugins.add_output_label(label, flags)`:
   adds plugin label to outputs enumerated in given argument help texts
- `plugins.get_argument(name, group=None)`:
   returns a command-line argument configuration dictionary, or None

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     18.12.2021
@modified    26.06.2023
------------------------------------------------------------------------------
"""
## @namespace grepros.plugins
import glob
import os
import re

import six

from .. common import ConsolePrinter, ensure_namespace, get_name, import_item
from .. outputs import MultiSink
from . import auto


## {"some.module" or "some.module.Cls": <module 'some.module' from ..> or <class 'some.module.Cls'>}
PLUGINS = {}

## Added output labels to insert into argument texts, as {label: [argument flag, ]}
OUTPUT_LABELS = {}

## Added write options, as {plugin label: [(name, help), ]}
WRITE_OPTIONS = {}

## Function argument defaults
DEFAULT_ARGS = dict(PLUGIN=[], STOP_ON_ERROR=False)


def init(args=None, **kwargs):
    """
    Imports and initializes all plugins from auto and from given arguments.

    @param   args                arguments as namespace or dictionary, case-insensitive
    @param   args.plugin         list of Python modules or classes to import,
                                 as ["my.module", "other.module.SomeClass", ],
                                 or module or class instances
    @param   args.stop_on_error  stop execution on any error like failing to load plugin
    @param   kwargs              any and all arguments as keyword overrides, case-insensitive
    """
    args = ensure_namespace(args, DEFAULT_ARGS, **kwargs)
    for f in sorted(glob.glob(os.path.join(os.path.dirname(__file__), "auto", "*"))):
        if not f.lower().endswith((".py", ".pyc")): continue  # for f
        name = os.path.splitext(os.path.split(f)[-1])[0]
        if name.startswith("__") or name in PLUGINS: continue # for f

        modulename = "%s.auto.%s" % (__package__, name)
        try:
            plugin = import_item(modulename)
            if callable(getattr(plugin, "init", None)): plugin.init(args)
            PLUGINS[name] = plugin
        except Exception:
            ConsolePrinter.error("Error loading plugin %s.", modulename)
            if args.STOP_ON_ERROR: raise
    if args: configure(args)
    populate_known_plugins()
    populate_output_arguments()
    populate_write_formats()


def configure(args=None, **kwargs):
    """
    Imports plugin Python packages, invokes init(args) if any, raises on error.

    @param   args          arguments as namespace or dictionary, case-insensitive
    @param   args.plugin   list of Python modules or classes to import,
                           as ["my.module", "other.module.SomeClass", ],
                           or module or class instances
    @param   kwargs        any and all arguments as keyword overrides, case-insensitive
    """
    args = ensure_namespace(args, DEFAULT_ARGS, **kwargs)
    for obj in args.PLUGIN:
        name = obj if isinstance(obj, six.string_types) else get_name(obj)
        if name in PLUGINS: continue  # for obj
        try:
            plugin = import_item(name) if isinstance(obj, six.string_types) else obj
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

    @param   category  item category like "source", "sink", or "scan"
    @param   args      arguments as namespace or dictionary, case-insensitive
    @param   collect   if true, returns a list of instances,
                       using all plugins that return something
    """
    result = []
    args = ensure_namespace(args)
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


def add_output_label(label, flags):
    """
    Adds plugin label to outputs enumerated in given argument help texts.

    @param   label  output label to add, like "Parquet"
    @param   flags  list of argument flags like "--emit-field" to add the output label to
    """
    OUTPUT_LABELS.setdefault(label, []).extend(flags)


def add_write_format(name, cls, label=None, options=()):
    """
    Adds plugin to `--write` in main.ARGUMENTS and MultiSink formats.

    @param   name     format name like "csv", added to `--write .. format=FORMAT`
    @param   cls      class providing Sink interface
    @param   label    plugin label; if multiple plugins add the same option,
                      "label output" in help text is replaced with "label1/label2/.. output"
    @param   options  a sequence of (name, help) to add to --write help, like
                      [("template=/my/path.tpl", "custom template to use for HTML output")]
    """
    MultiSink.FORMAT_CLASSES[name] = cls
    if options: WRITE_OPTIONS.setdefault(label, []).extend(options)


def get_argument(name, group=None):
    """
    Returns a command-line argument dictionary, or None if not found.

    @param   name   argument name like "--write"
    @param   group  argument group like "Output control", if any
    """
    from .. import main  # Late import to avoid circular
    if group:
        return next((d for d in main.ARGUMENTS.get("groups", {}).get(group, [])
                     if name in d.get("args")), None)
    return next((d for d in main.ARGUMENTS.get("arguments", [])
                 if name in d.get("args")), None)


def populate_output_arguments():
    """Populates argument texts with added output labels."""
    if not OUTPUT_LABELS: return
    from .. import main  # Late import to avoid circular

    argslist = sum(main.ARGUMENTS.get("groups", {}).values(), main.ARGUMENTS["arguments"][:])
    args = {f: x for x in argslist for f in x["args"]}  # {flag or id(argdict): argdict}
    args.update((id(x), x) for x in argslist)
    arglabels = {}  # {id(argdict): [label, ]}

    # First pass: collect arguments where to update output labels
    for label, flag in ((l, f) for l, ff in OUTPUT_LABELS.items() for f in ff):
        if flag in args: arglabels.setdefault(id(args[flag]), []).append(label)
        else: ConsolePrinter.warn("Unknown command-line flag %r from output %r.", flag, label)

    # Second pass: replace argument help with full set of output labels
    for arg, labels in ((args[x], ll) for x, ll in arglabels.items()):
        match = re.search(r"(\A.*?\s*in\s)(\S+)(\s+output.*\Z)", arg["help"], re.DOTALL)
        if not match:
            ConsolePrinter.warn("Command-line flag %s has no text on output for labels %s.",
                                arg["args"], ", ".join(map(repr, sorted(set(labels)))))
            continue  # for arg, labels
        labels2 = sorted(set(labels + match[2].split("/")), key=lambda x: x.lower())
        arg["help"] = match.expand(r"\1%s\3" % "/".join(labels2))

    OUTPUT_LABELS.clear()


def populate_known_plugins():
    """Adds known non-auto plugins to `--plugin` argument help."""
    plugins = []
    for f in sorted(glob.glob(os.path.join(os.path.dirname(__file__), "*"))):
        if not f.lower().endswith((".py", ".pyc")): continue  # for f
        name = os.path.splitext(os.path.split(f)[-1])[0]
        if not name.startswith("__"):
            plugins.append("%s.%s" % (__package__, name))

    pluginarg = get_argument("--plugin")
    if pluginarg and plugins:
        MAXLINELEN = 60
        lines = ["load a Python module or class as plugin", "(built-in plugins: "]
        for i, name in enumerate(plugins):
            if not i: lines[-1] += name
            else:
                if len(lines[-1] + ", " + name) > MAXLINELEN:
                    lines[-1] += ", "
                    lines.append(" " + name)
                else: lines[-1] += ", " + name
        lines[-1] += ")"
        pluginarg["help"] = "\n".join(lines)


def populate_write_formats():
    """Populates main.ARGUMENTS with added write formats and options."""
    writearg = get_argument("--write")
    if not writearg: return

    formats = sorted(set(MultiSink.FORMAT_CLASSES))
    writearg["metavar"] = "TARGET [format=%s] [KEY=VALUE ...]" % "|".join(formats)
    if not WRITE_OPTIONS: return

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
                texts[name] = texts[name].replace("%s output" % label, PLACEHOLDER)
            labels = "/".join(sorted(filter(bool, namelabels[name]), key=lambda x: x.lower()))
            texts[name] = texts[name].replace(PLACEHOLDER, labels + " output")

    fmt = lambda n, h: "\n".join((indent if i or "\n" == inters[n] else "") + l
                                 for i, l in enumerate(h.splitlines()))
    text = "\n".join(sorted("".join((LEADING, n, inters[n], fmt(n, h)))
                            for n, h in texts.items()))
    writearg["help"] += "\n" + text

    WRITE_OPTIONS.clear()




__all__ = [
    "PLUGINS", "init", "configure", "load", "add_write_format", "get_argument",
    "populate_known_plugins", "populate_write_formats",
]

# -*- coding: utf-8 -*-
"""
Plugins interface.

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
import importlib
import types

from .. common import ConsolePrinter
from . auto import *


## {"some.module": <module 'some.module' from ..>}
PLUGINS = {}


def init(args=None):
    """Imports and initializes all plugins from given Python module."""
    for name, plugin in globals().items():
        if not isinstance(plugin, types.ModuleType) \
        or __package__ + ".auto" != getattr(plugin, "__package__", None):
            continue  # for name

        try:
            if callable(getattr(plugin, "init", None)): plugin.init(args)
            PLUGINS[name] = plugin
        except Exception:
            ConsolePrinter.error("Error loading plugin %s.", name)
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


def load(category, args):
    """
    Returns a plugin category instance loaded from any configured plugin, or None.

    @param   category  item category like "source", "sink", or "search"
    @param   args      arguments object like argparse.Namespace
    """
    for name, plugin in PLUGINS.items():
        if callable(getattr(plugin, "load", None)):
            try:
                result = plugin.load(category, args)
                if result is not None:
                    return result
            except Exception:
                ConsolePrinter.error("Error invoking %s.load(%r, args).", name, category)
                raise
                

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

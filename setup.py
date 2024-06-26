# -*- coding: utf-8 -*-
"""
Setup.py for grepros.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    21.04.2024
------------------------------------------------------------------------------
"""
from __future__ import print_function

import os
import re
import setuptools
try: from catkin_pkg.python_setup import generate_distutils_setup
except ImportError: generate_distutils_setup = None


PACKAGE = "grepros"


def readfile(path):
    """Returns contents of path, relative to current file."""
    root = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(root, path), encoding="utf-8") as f: return f.read()

def get_description():
    """Returns package description from README."""
    LINK_RGX = r"\[([^\]]+)\]\(([^\)]+)\)"  # 1: content in [], 2: content in ()
    KEEP = ("ftp://", "http://", "https://", "www.")
    # Unwrap local links like [Page link](#page-link) and [DETAIL.md](doc/DETAIL.md)
    repl = lambda m: m.group(0 if any(map(m.group(2).startswith, KEEP)) else
                             1 if m.group(2).startswith("#") else 2)
    return re.sub(LINK_RGX, repl, readfile("README.md"))

def get_version():
    """Returns package current version number from source code."""
    VERSION_RGX = r'__version__\s*\=\s*\"*([^\n\"]+)'
    content = readfile(os.path.join("src", PACKAGE, "__init__.py"))
    match = re.search(VERSION_RGX, content)
    return match.group(1).strip() if match else None


common_args = dict(
    install_requires = ["pyyaml", "six"],
    package_dir      = {"": "src"},
    packages         = setuptools.find_packages("src"),
    package_data     = {"": ["plugins/auto/*.tpl"]},
)
version_args = dict(
    data_files      = [
        (os.path.join("share", "ament_index", "resource_index", "packages"),
         [os.path.join("resource", PACKAGE)]),
        (os.path.join("share", PACKAGE), ["package.xml"]),
    ],
    tests_require   = ["pytest"],
) if "2" == os.getenv("ROS_VERSION") else {}


setup_args = generate_distutils_setup(  # fetch values from package.xml
    scripts         = [os.path.join("scripts", PACKAGE)],
) if generate_distutils_setup and "1" == os.getenv("ROS_VERSION") else dict(  # Normal pip setup
    name            = PACKAGE,
    version         = get_version(),
    entry_points    = {"console_scripts": ["{0} = {0}.main:run".format(PACKAGE)]},

    description     = "grep for ROS bag files and live topics: read, filter, export",
    url             = "https://github.com/suurjaak/" + PACKAGE,
    author          = "Erki Suurjaak",
    author_email    = "erki@lap.ee",
    license         = "BSD",
    platforms       = ["any"],
    keywords        = "ROS ROS1 ROS2 rosbag grep",
    python_requires = ">=2.7",

    include_package_data = True, # Use MANIFEST.in for data files
    classifiers  = [
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Environment :: Console :: Curses",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ],

    long_description_content_type = "text/markdown",
    long_description = get_description(),
)
setuptools.setup(**dict(common_args, **dict(setup_args, **version_args)))

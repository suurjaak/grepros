# -*- coding: utf-8 -*-
"""
Setup.py for grepros.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    03.11.2021
------------------------------------------------------------------------------
"""
import os
from setuptools import setup
try: from catkin_pkg.python_setup import generate_distutils_setup
except ImportError: generate_distutils_setup = None


PACKAGE = "grepros"

common_args = dict(
    install_requires = ["pyyaml"],
    package_dir      = {"": "src"},
    packages         = [PACKAGE],
)
version_args = dict(
    data_files=[
        (os.path.join("share", "ament_index", "resource_index", "packages"),
         [os.path.join("resource", PACKAGE)]),
        (os.path.join("share", PACKAGE), ["package.xml"]),
    ],
) if "2" == os.getenv("ROS_VERSION") else {}


setup_args = generate_distutils_setup(  # fetch values from package.xml
    scripts      = [os.path.join("scripts", PACKAGE)],
) if generate_distutils_setup and "1" == os.getenv("ROS_VERSION") else dict(  # Normal pip setup
    name         = PACKAGE,
    version      = "0.2.0",
    entry_points = {"console_scripts": ["{0} = {0}.main:run".format(PACKAGE)]},

    description  = "grep for ROS bag files and live topics",
    url          = "https://github.com/suurjaak/" + PACKAGE,
    author       = "Erki Suurjaak",
    author_email = "erki@lap.ee",
    license      = "BSD",
    platforms    = ["any"],
    keywords     = "ROS ROS1 ROS2 rosbag grep",
    python_requires = ">=2.7",

    classifiers  = [
        "Development Status :: 4 - Beta",
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
    long_description              = (
"""grep for ROS bag files and live topics.

Searches through ROS messages and matches any message field value by regular
expression patterns or plain text, regardless of field type.
Can also look for specific values in specific message fields only.

By default, matches are printed to console. Additionally, matches can be written
to a bagfile, or published to live topics.

Supports both ROS1 and ROS2. ROS environment variables need to be set.

Grepping from or publishing to ROS1 live topics requires ROS master to be running.
"""),

)
setup(**dict(common_args, **dict(setup_args, **version_args)))

# -*- coding: utf-8 -*-
"""
Setup.py for grepros. 

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS1 bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     23.10.2021
@modified    30.10.2021
------------------------------------------------------------------------------
"""
from setuptools import setup
try: from catkin_pkg.python_setup import generate_distutils_setup
except ImportError: generate_distutils_setup = None


common_args = dict(
    packages=["grepros"],
    install_requires=["pyyaml"],
)


setup_args = generate_distutils_setup(  # fetch values from package.xml
    package_dir={"": "src"},
    scripts=["scripts/grepros"],
) if generate_distutils_setup else dict(  # Normal pip setup
    name="grepros",
    version="0.1.0",
    entry_points={"console_scripts": ["grepros = grepros.main:run"]},

    description="grep for ROS1 bag files and live topics",
    url="https://github.com/suurjaak/grepros",
    author="Erki Suurjaak",
    author_email="erki@lap.ee",
    license="BSD",
    platforms=["any"],
    keywords="ROS rosbag grep",

    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Environment :: Console :: Curses",
        "Framework :: Robot Operating System",
        "Framework :: Robot Operating System :: ROS1",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ],

    long_description_content_type="text/markdown",
    long_description=(
"""Searches through ROS messages and matches any message field value by regular
expression patterns or plain text, regardless of field type.
Can also look for specific values in specific message fields only.

By default, matches are printed to console. Additionally, matches can be written
to a bagfile, or published to live topics.
"""),

)
setup(**dict(common_args, **setup_args))

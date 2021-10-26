# ! DO NOT MANUALLY INVOKE THIS setup.py, USE CATKIN INSTEAD

from setuptools import setup
from catkin_pkg.python_setup import generate_distutils_setup

# fetch values from package.xml
setup_args = generate_distutils_setup(
    packages=["grepros"],
    package_dir={"": "src"},
    scripts=["scripts/grepros"],

    author="Erki Suurjaak",
    author_email="erki@lap.ee",
    license="BSD",
    platforms=["any"],
    install_requires=["pyyaml"],
    entry_points={"console_scripts": ["grepros = grepros.main:run"]},
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
    long_description=
"""Can search through ROS messages and match any message field value by regular
expression patterns or plain text, regardless of field type.
Can also look for specific values in specific message fields only.

By default, matches are printed to console. Additionally, matches can be written
to a bagfile, or published to live topics.
""",
)
setup(**setup_args)

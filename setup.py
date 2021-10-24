# ! DO NOT MANUALLY INVOKE THIS setup.py, USE CATKIN INSTEAD

from setuptools import setup
from catkin_pkg.python_setup import generate_distutils_setup

# fetch values from package.xml
setup_args = generate_distutils_setup(
    install_requires=["pyyaml"],
    packages=["grepros"],
    package_dir={"": "src"},
    scripts=["scripts/grepros"]
)
setup(**setup_args)

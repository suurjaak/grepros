# -*- coding: utf-8 -*-
"""
Module entry point.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     24.10.2021
@modified    02.11.2021
------------------------------------------------------------------------------
"""
from . import main

if "__main__" == __name__:
    main.run()

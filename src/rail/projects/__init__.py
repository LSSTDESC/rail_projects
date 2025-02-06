"""
The :py:mod:`rail.projects` package collects a set of tools to manage RAIL-based data
analysis studies.  These tools help users define common pieced to analyses,
while also quickly testing many analysis variants with slight configuration
modifications.
"""

from . import library, name_utils
from .project import RailFlavor, RailProject

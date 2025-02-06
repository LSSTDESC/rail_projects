from __future__ import annotations

import os
from typing import Any

from ceci.config import StageParameter

from .configurable import Configurable


class RailProjectFileInstance(Configurable):
    """Simple class for holding information about a single file"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="File name"),
        path=StageParameter(
            str, None, fmt="%s", required=True, msg="Template for path to file"
        ),
    )
    yaml_tag = "FileInstance"

    def __init__(self, **kwargs: Any) -> None:
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailProjectFileInstance, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        self._file_exists: bool | None = None

    def resolve(self) -> str:
        """Return the corresponding path"""
        return self._config.path

    def check_file(self, **kwargs: dict[str, Any]) -> bool:
        """Check to see if the file exists

        Notes
        -----
        When called more than once, this uses the cached value unless called with
        update=True
        """
        update = kwargs.pop("update", False)
        if self._file_exists is not None:
            if not update:
                return self._file_exists
        self._file_exists = os.path.exists(os.path.expandvars(self.config.path))
        return self._file_exists


class RailProjectFileTemplate(Configurable):
    """Simple class for holding a template that can be resolved to a single file

    For example the path_template might be 'a_file/{flavor}_data.hdf5'
    and the interpolants would be ['flavor']

    When called with a dict such as flavor: 'baseline
    path_template would get expanded out to
    a_file/baseline_data.hdf5
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        path_template=StageParameter(
            str, None, fmt="%s", required=True, msg="Template for path to file files"
        ),
    )
    yaml_tag = "FileTemplate"

    def __init__(self, **kwargs: Any) -> None:
        """C'tor

        Parameters
        ----------
        **kwargs
            Configuration parameters for this RailProjectFileTemplate, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def make_file_instance(
        self, name: str, **kwargs: dict[str, Any]
    ) -> RailProjectFileInstance:
        """Resolve the interpolants and construct a RailProjectFileInstance

        Parameters
        ----------
        name:
            Name for the FileInstance object

        **kwargs:
            Interpolants needed to resolve the path template

        Returns
        -------
        RailProjectFileInstance:
            Newly created object
        """
        formatted_path = self.config.path_template.format(**kwargs)
        return RailProjectFileInstance(
            name=name,
            path=formatted_path,
        )

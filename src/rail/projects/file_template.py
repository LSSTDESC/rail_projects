from __future__ import annotations

from typing import Any

import os

from ceci.config import StageParameter

from rail.plotting.configurable import Configurable


class RailProjectFileInstance(Configurable):
    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="File name"),
        path=StageParameter(
            str, None, fmt="%s", required=True, msg="Template for path to file"
        ),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailProjectFileInstance, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        self._file_exists: bool | None = None

    def __call__(self) -> str:
        return self._config.path

    def check_file(self, **kwargs: dict[str, Any]) -> bool:
        update = kwargs.pop("update", False)
        if self._file_exists is not None:
            if not update:
                return self._file_exists
        self._file_exists = os.path.exists(os.path.expandvars(self.config.path))
        return self._file_exists


class RailProjectFileTemplate(Configurable):
    """Simple class for holding a template for a file associated with a project"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        path_template=StageParameter(
            str, None, fmt="%s", required=True, msg="Template for path to file files"
        ),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailProjectFileTemplate, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def make_file_instance(
        self, name: str, **kwargs: dict[str, Any]
    ) -> RailProjectFileInstance:
        formatted_path = self.config.path_template.format(**kwargs)
        return RailProjectFileInstance(
            name=name,
            path=formatted_path,
        )

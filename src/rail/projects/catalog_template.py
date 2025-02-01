from __future__ import annotations

import os
from typing import Any
import itertools

from ceci.config import StageParameter

from .configurable import Configurable


class RailProjectCatalogInstance(Configurable):
    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        path_template=StageParameter(
            str, None, fmt="%s", required=True, msg="Template for path to catalog files"
        ),
        iteration_vars=StageParameter(
            list,
            [],
            fmt="%s",
            msg="Variables to iterate over to construct catalog",
        ),
    )
    yaml_tag = "CatalogInstance"

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailProjectCatalogInstance, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        self._file_list: list[str] | None = None
        self._file_exists: list[bool] | None = None

    def __repr__(self) -> str:
        return f"{self.config.path_template}"

    def __call__(self, **kwargs: dict[str, Any]) -> list[str]:
        if self._file_list is not None:
            return self._file_list
        iterations = itertools.product(*[kwargs.get(key, []) for key in kwargs])
        self._file_list = []
        for iteration_args in iterations:
            zipped_tuples = zip(self.config.iteration_vars, iteration_args)
            iteration_kwargs = {val_[0]: val_[1] for val_ in zipped_tuples}
            self._file_list.append(self.config.path_template.format(**iteration_kwargs))
        return self._file_list

    def check_files(self, **kwargs: dict[str, Any]) -> list[bool]:
        update = kwargs.pop("update", False)
        if self._file_exists is not None:
            if not update:
                return self._file_exists
        self._file_exists = []
        the_files = self(**kwargs)
        for file_ in the_files:
            self._file_exists.append(os.path.exists(os.path.expandvars(file_)))
        return self._file_exists


class RailProjectCatalogTemplate(Configurable):
    """Simple class for holding a template for a catalog associated with a project"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        path_template=StageParameter(
            str, None, fmt="%s", required=True, msg="Template for path to catalog files"
        ),
        iteration_vars=StageParameter(
            list,
            [],
            fmt="%s",
            msg="Variables to iterate over to construct catalog",
        ),
    )
    yaml_tag = "CatalogTemplate"

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailProjectCatalogTemplate, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.path_template}"

    def make_catalog_instance(
        self, name: str, **kwargs: dict[str, Any]
    ) -> RailProjectCatalogInstance:
        iteration_var_dict = {
            key: "{" + key + "}" for key in self.config.iteration_vars
        }
        formatted_path = self.config.path_template.format(
            **kwargs, **iteration_var_dict
        )
        return RailProjectCatalogInstance(
            name=name,
            path_template=formatted_path,
            iteration_vars=self.config.iteration_vars.copy(),
        )

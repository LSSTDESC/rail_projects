from __future__ import annotations

import itertools
import os
from typing import Any

from ceci.config import StageParameter

from .configurable import Configurable


class RailProjectCatalogInstance(Configurable):
    """Simple class for holding information need to make a coherent catalog
    of files using a templated file name and iteration_vars to fill in
    the interpolation in the file name.

    For example the path_template might be 'a_file/{healpix}/data.parqut'
    and the interation_vars would be ['healpix'].

    When called with a dict such as healpix : [3433, 3344] it would the
    path_template would get expanded out to two files:

    a_file/3433/data.parqut
    a_file/3344/data.parqut

    """

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
        **kwargs: Any
            Configuration parameters for this RailProjectCatalogInstance, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        self._file_list: list[str] | None = None
        self._file_exists: list[bool] | None = None

    def __repr__(self) -> str:
        return f"{self.config.path_template}"

    def resolve(self, **kwargs: dict[str, Any]) -> list[str]:
        """Resolve the list of files in this catalog

        :meta public:

        Parameters
        ----------
        **kwargs:
            Set of interpolants and iteration_vars needed to resolve the catalog

        Returns
        -------
        list[str]:
            List of resolved catalog files

        Notes
        -----
        By default this will used cached values, to override this and force rechecking
        use update=True keyword argument
        """
        update = kwargs.pop("update", False)
        if self._file_list is not None:
            if not update:
                return self._file_list
        iterations = itertools.product(*[kwargs.get(key, []) for key in kwargs])
        self._file_list = []
        for iteration_args in iterations:
            zipped_tuples = zip(self.config.iteration_vars, iteration_args)
            iteration_kwargs = {val_[0]: val_[1] for val_ in zipped_tuples}
            self._file_list.append(self.config.path_template.format(**iteration_kwargs))
        return self._file_list

    def check_files(self, **kwargs: dict[str, Any]) -> list[bool]:
        """Check if the files in the catalog exist

        Parameters
        ----------
        **kwargs:
            Set of interpolants and iteration_vars needed to resolve the catalog

        Returns
        -------
        list[bool]:
            List of True/False values for existance of each file in catalog

        Notes
        -----
        By default this will used cached values, to override this and force rechecking
        use update=True keyword argument
        """
        update = kwargs.get("update", False)
        if self._file_exists is not None:
            if not update:
                return self._file_exists
        self._file_exists = []
        the_files = self.resolve(**kwargs)
        for file_ in the_files:
            self._file_exists.append(os.path.exists(os.path.expandvars(file_)))
        return self._file_exists


class RailProjectCatalogTemplate(Configurable):
    """Simple class for holding a template for a catalog associated with a project

    The makes a coherent catalog of files using a templated file name, interpolants,
    and iteration_vars to fill in the interpolation in the file name.

    For example the path_template might be 'a_file/{healpix}/{flavor}_data.hdf5'
    and the interpolants would be ['flavor'] and interation_vars would be ['healpix'].

    When called with a dict such as flavor: 'baseline, healpix : [3433, 3344] it would the
    path_template would get expanded out to two files:

    a_file/3433/baseline_data.hdf5
    a_file/3344/baseline_data.hdf5
    """

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

    def __init__(self, **kwargs: Any) -> None:
        """C'tor

        Parameters
        ----------
        **kwargs: Any
            Configuration parameters for this RailProjectCatalogTemplate, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.path_template}"

    def make_catalog_instance(
        self, name: str, **kwargs: dict[str, Any]
    ) -> RailProjectCatalogInstance:
        """Make and return a specific instance of this CatalogTemplate
        by resolving interpolants and iterating over the iteration_vars.

        Parameters
        ----------
        name:
            Name for the CatalogInstance object

        **kwargs:
            Interpolants needed to resolve the path template

        Returns
        -------
        RailProjectCatalogInstance:
            Newly created object
        """
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

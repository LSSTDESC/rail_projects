from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any

from ceci.config import StageParameter

from rail.projects import RailProject
from rail.projects.configurable import Configurable
from rail.projects.dynamic_class import DynamicClass

from .dataset import RailDataset
from .validation import validate_inputs

if TYPE_CHECKING:
    from .dataset_factory import RailDatasetFactory


class DatasetSplitMode(enum.Enum):
    """Choose how to split datasets within a project"""

    no_split = 0  # put all the datasets in a single list
    by_flavor = 1  # split into lists by flavor
    by_algo = 2  # split into lists by algorithm


class RailDatasetHolder(Configurable, DynamicClass):
    """Base class for extracting data from a RailProject

    The resolve method will return the wrapped dataset

    Sub-classes should implement

    a class member:
    extractor_inputs: a dict [str, type]

    that specifies the inputs that the sub-classes expect,
    this is used the check the kwargs that are passed to the _get_data()
    function

    a class member:
    output_type: type[RailDataset]

    that specifies the output dataset type

    A function:
    get_extractor_inputs(self) -> dict[str, Any]

    The resolves anything for the call to _get_data from the configuration
    parameters.  For example, loading the underlying project if needed.

    A function:
    _get_data(self,**kwargs: Any) -> dict[str, Any]:

    That actually gets the data.  It does not need to do the checking
    that the correct kwargs have been given.

    A class method:
    generate_dataset_dict()

    that will find all the datasets that the extractor can extract
    """

    extractor_inputs: dict = {}

    output_type: type[RailDataset] = RailDataset

    sub_classes: dict[str, type[DynamicClass]] = {}

    yaml_tag = "Dataset"

    @classmethod
    def _validate_extractor_inputs(cls, **kwargs: Any) -> None:
        validate_inputs(cls, cls.extractor_inputs, **kwargs)

    @classmethod
    def _validate_outputs(cls, **kwargs: Any) -> None:
        cls.output_type.validate_inputs(**kwargs)

    @classmethod
    def generate_dataset_dict(
        cls,
        **kwargs: dict[str, Any],
    ) -> tuple[
        list[RailProjectHolder], list[RailDatasetHolder], list[RailDatasetListHolder]
    ]:
        """Create a dict of the datasets that this extractor can extract

        Returns
        -------
        list[RailProjectHolder]
            Underlying RailProjects

        list[RailDatasetHolder]
            Extracted datasets

        list[RailDatasetListHolder]
            Extracted dataset lists
        """
        raise NotImplementedError()

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailDatasetHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        DynamicClass.__init__(self)
        self._data: dict[str, Any] | None = None

    def __repr__(self) -> str:
        return f"{self.config.to_dict()}"

    def set_data(self, the_data: dict[str, Any] | None) -> None:
        """Set the data in this holder"""
        self._data = the_data

    @property
    def data(self) -> dict[str, Any] | None:
        """Return the RailDatasetHolder data"""
        return self._data

    def _extract_data(self, **kwargs: Any) -> dict[str, Any] | None:
        """Extract the data

        Parameters
        ----------
        kwargs: dict[str, Any]
            Used to pass the inputs to the extractor

        Returns
        -------
        dict[str, Any] | None
            Dictionary of the newly extracted data
        """
        self._validate_extractor_inputs(**kwargs)
        the_data = self._get_data(**kwargs)
        if the_data is not None:
            self._validate_outputs(**the_data)
        return the_data

    def resolve(self) -> dict[str, Any]:
        """Extract and return the data in question"""
        if self.data is None:
            the_extractor_inputs = self.get_extractor_inputs()
            the_data = self._extract_data(**the_extractor_inputs)
            self.set_data(the_data)
            assert self.data is not None
        return self.data

    def get_extractor_inputs(self) -> dict[str, Any]:
        """Resolve the inputs needed to get the data
        from the configuration paramters.

        For example, load RailProject configurations,
        resolve the set of requested interpolants, etc...
        """
        raise NotImplementedError()

    def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
        raise NotImplementedError()

    def to_yaml_dict(self) -> dict[str, dict[str, Any]]:
        """Create a yaml-convertable dict for this object"""
        yaml_dict = Configurable.to_yaml_dict(self)
        yaml_dict[self.yaml_tag].update(class_name=f"{self.full_class_name()}")
        return yaml_dict


class RailDatasetListHolder(Configurable):
    """Class to wrap a list of consistent RailDatasetHolders

    i.e., all of the RailDatasetHolders should return the
    same type of dataets, meaning that they should all
    contain the same columns.

    The resolve method will return the list of RailDatasetHolders
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        dataset_class=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Type of data expected by plotters on this list",
        ),
        datasets=StageParameter(
            list,
            [],
            fmt="%s",
            msg="List of datasets to include",
        ),
    )

    yaml_tag = "DatasetList"

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailDatasetListHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.datasets}"

    def resolve(self, dataset_factory: RailDatasetFactory) -> list[RailDatasetHolder]:
        """Get all the associated RailDatasetHolder objects

        Paramters
        ---------
        dataset_factory:
            Factory used to make the dataset_holders.

        Returns
        -------
        list[RailDatasetHolder]
            Requested datasets

        Notes
        -----
        This will enforce that each dataset_holders expects the compatible dataset_types
        """
        the_list: list[RailDatasetHolder] = []

        dataset_class = RailDataset.load_sub_class(self.config.dataset_class)

        for name_ in self.config.datasets:
            a_dataset_holder = dataset_factory.get_dataset(name_)
            if not issubclass(
                a_dataset_holder.output_type, dataset_class
            ):  # pragma: no cover
                raise TypeError(
                    f"DatasetHolder.output_type {a_dataset_holder.output_type} is"
                    f"not a subclass of RailDatasetListHolder dataset_class {dataset_class}."
                )
            the_list.append(a_dataset_holder)
        return the_list


class RailProjectHolder(Configurable):
    """Class to wrap a RailProject

    This is just the path to the yaml file that define the project

    The resolve method will create a RailProject object by reading that file
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        yaml_file=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="path to project yaml file",
        ),
    )

    yaml_tag = "Project"

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailAlgorithmHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        self._project: RailProject | None = None

    def __repr__(self) -> str:
        return f"{self.config.yaml_file}"

    def resolve(self) -> RailProject:
        """Read the associated yaml file and create a RailProject"""
        if self._project is None:
            self._project = RailProject.load_config(self.config.yaml_file)
        return self._project

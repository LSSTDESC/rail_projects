from __future__ import annotations

from types import GenericAlias
from typing import TYPE_CHECKING, Any

from ceci.config import StageParameter

from rail.projects import RailProject
from rail.projects.configurable import Configurable
from rail.projects.dynamic_class import DynamicClass

if TYPE_CHECKING:
    from .dataset_factory import RailDatasetFactory


class RailDatasetHolder(Configurable, DynamicClass):
    """Base class for classes that wrap particular datasets

    The __call__ method will return the wrapped dataset

    Subclasses should implement the get_extractor_inputs
    method, which should return a RailProjectDataExtractor
    object and the arguments needed to call it properly
    """

    extractor_inputs: dict = {}

    sub_classes: dict[str, type[DynamicClass]] = {}

    yaml_tag = "Dataset"

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

    def __call__(self) -> dict[str, Any]:
        """Extract and return the data in question"""
        if self.data is None:
            the_extractor_inputs = self.get_extractor_inputs()
            the_extractor = the_extractor_inputs.pop("extractor")
            the_data = the_extractor(**the_extractor_inputs)
            self.set_data(the_data)
            assert self.data is not None
        return self.data

    def get_extractor_inputs(self) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def _validate_extractor_inputs(cls, **kwargs: Any) -> None:
        for key, expected_type in cls.extractor_inputs.items():
            try:
                data = kwargs[key]
            except KeyError as missing_key:
                raise KeyError(
                    f"{key} not provided to RailDatasetHolder {cls} in {list(kwargs.keys())}"
                ) from missing_key
            if isinstance(expected_type, GenericAlias):
                if not isinstance(data, expected_type.__origin__):  # pragma: no cover
                    raise TypeError(
                        f"{key} provided to RailDatasetHolder was "
                        f"{type(data)}, not {expected_type.__origin__}"
                    )
                continue  # pragma: no cover
            if not isinstance(data, expected_type):  # pragma: no cover
                raise TypeError(
                    f"{key} provided to RailDatasetHolder was {type(data)}, expected {expected_type}"
                )

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

    The __call__ method will return the list of RailDatasetHolders
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
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

    def __call__(self, dataset_factory: RailDatasetFactory) -> list[RailDatasetHolder]:
        """Get all the associated RailDatasetHolder objects"""
        the_list = [
            dataset_factory.get_dataset(name_) for name_ in self.config.datasets
        ]
        return the_list


class RailProjectHolder(Configurable):
    """Class to wrap a RailProject

    This is just the path to the yaml file that define the project

    The __call__ method will create a RailProject object by reading that file
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

    def __call__(self) -> RailProject:
        """Read the associated yaml file and create a RailProject"""
        if self._project is None:
            self._project = RailProject.load_config(self.config.yaml_file)
        return self._project

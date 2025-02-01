from __future__ import annotations

from typing import Any, TYPE_CHECKING
from types import GenericAlias

from ceci.config import StageParameter

from rail.projects import RailProject
from rail.projects.configurable import Configurable
from rail.projects.dynamic_class import DynamicClass

if TYPE_CHECKING:
    from .dataset_factory import RailDatasetFactory


class RailDatasetHolder(Configurable, DynamicClass):
    """Simple class for holding a dataset for plotting data"""

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


class RailDatasetListHolder(Configurable):
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
            Configuration parameters for this RailAlgorithmHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.datasets}"

    def __call__(self, dataset_factory: RailDatasetFactory) -> list[RailDatasetHolder]:
        the_list = [
            dataset_factory.get_dataset(name_) for name_ in self.config.datasets
        ]
        return the_list


class RailProjectHolder(Configurable):
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
        if self._project is None:
            self._project = RailProject.load_config(self.config.yaml_file)
        return self._project

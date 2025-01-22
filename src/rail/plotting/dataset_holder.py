from __future__ import annotations

from typing import Any
from types import GenericAlias

from .configurable import Configurable
from .dynamic_class import DynamicClass


class RailDatasetHolder(Configurable, DynamicClass):
    """Simple class for holding a dataset for plotting data"""

    extractor_inputs: dict = {}

    sub_classes: dict[str, type[DynamicClass]] = {}

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
            except KeyError as msg:  # pragma: no cover
                raise KeyError(
                    f"{key} not provided to RailDatasetHolder {cls} in {list(kwargs.keys())}"
                ) from msg
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

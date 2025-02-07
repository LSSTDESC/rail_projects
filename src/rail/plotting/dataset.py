from __future__ import annotations

from typing import Any

from rail.projects.dynamic_class import DynamicClass

from .validation import validate_inputs


class RailDataset(DynamicClass):
    """Base class for datasets

    Subclasses should implement the `data_types` data member
    to show what the expected data are.

    For example:

    .. highlight:: python
    .. code-block:: python

        data_types = dict(
            truth: np.ndarray,
            point_estimate: np.ndarry,
        )

    """

    data_types: dict[str, type] = {}

    sub_classes: dict[str, type[DynamicClass]] = {}

    @classmethod
    def full_class_name(cls) -> str:
        """Return the full name of the class, including the parent module"""
        return f"{cls.__module__}.{cls.__name__}"

    @classmethod
    def validate_inputs(cls, **kwargs: Any) -> None:
        if not cls.data_types:
            raise NotImplementedError(
                f"RailDataset class {cls.__name__} has not defined data_types"
            )
        validate_inputs(cls, cls.data_types, **kwargs)

    def __repr__(self) -> str:
        the_class = self.__class__
        return f"{the_class.__name__}"

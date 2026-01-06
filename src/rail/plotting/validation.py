from __future__ import annotations

from types import GenericAlias
from typing import Any, get_origin


def validate_inputs(a_class: type, expected_inputs: dict, **kwargs: Any) -> None:
    """Validate that the kwargs given to a class contructor
    match the expected inputs

    Raises
    ------
    TypeError if a kwarg is not of the expected type

    KeyError is a kwaags is not in the set of expected inptus
    """
    for key, expected_type in expected_inputs.items():
        try:
            data = kwargs[key]
        except KeyError as missing_key:
            raise KeyError(
                f"{key} not provided to {a_class.__name__} in {list(kwargs.keys())}"
            ) from missing_key
        if isinstance(expected_type, GenericAlias):
            origin = get_origin(expected_type)
            if origin is not None and not isinstance(data, origin):
                raise TypeError(
                    f"{key} provided to {a_class.__name__} was "
                    f"{type(data)}, not {expected_type.__origin__}"
                )  # pragma: no cover
            continue  # pragma: no cover
        if not isinstance(data, expected_type):  # pragma: no cover
            raise TypeError(
                f"{key} provided to {a_class.__name__} was {type(data)}, expected {expected_type}"
            )

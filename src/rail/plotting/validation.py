from __future__ import annotations

from typing import Any
from types import GenericAlias


def validate_inputs(a_class: type, expected_inputs: dict, **kwargs: Any) -> None:
    for key, expected_type in expected_inputs.items():
        try:
            data = kwargs[key]
        except KeyError as missing_key:
            raise KeyError(
                f"{key} not provided to {a_class.__name__} in {list(kwargs.keys())}"
            ) from missing_key
        if isinstance(expected_type, GenericAlias):
            if not isinstance(data, expected_type.__origin__):  # pragma: no cover
                raise TypeError(
                    f"{key} provided to {a_class.__name__} was "
                    f"{type(data)}, not {expected_type.__origin__}"
                )
            continue  # pragma: no cover
        if not isinstance(data, expected_type):  # pragma: no cover
            raise TypeError(
                f"{key} provided to {a_class.__name__} was {type(data)}, expected {expected_type}"
            )

from __future__ import annotations

from typing import Any

from ceci.config import StageConfig, StageParameter


class Configurable:
    """Base class used to attach Ceci.StageParamters to a class"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, 0.0, fmt="%s", msg="Name for the plotter"),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this object, must match
            class.config_options data members
        """
        self._config = StageConfig(**self.config_options)
        self._set_config(**kwargs)

    def _set_config(self, **kwargs: Any) -> None:
        kwcopy = kwargs.copy()
        for key in self.config.keys():
            if key in kwargs:
                self.config[key] = kwcopy.pop(key)
            else:  # pragma: no cover
                attr = self.config.get(key)
                if attr.required:
                    raise ValueError(f"Missing configuration option {key}")
                self.config[key] = attr.default
        if kwcopy:  # pragma: no cover
            raise ValueError(
                f"Unrecogonized configruation parameters {kwcopy.keys()} "
                f"for type {type(self)}.  "
                f"Known parameters are {list(self.config.to_dict().keys())}"
            )

    @property
    def config(self) -> StageConfig:
        """Return the RailDatasetHolder configuration"""
        return self._config

    def __repr__(self) -> str:
        return f"{self.config.name}"

from __future__ import annotations

from typing import Any

from ceci.config import StageParameter

from .configurable import Configurable
from .factory_mixin import RailFactoryMixin


class RailSubsample(Configurable):
    """Paramters for a simple data subsample

    This is just defined as a random number seed and a number of objects
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Subsample name"),
        seed=StageParameter(
            int, None, fmt="%i", required=True, msg="Random numbers seed"
        ),
        num_objects=StageParameter(
            int, None, fmt="%i", required=True, msg="Number of objects to pick"
        ),
    )
    yaml_tag = "Subsample"

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        **kwargs:
            Configuration parameters for this RailAlgorithmHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"N={self.config.num_objects} seed={self.config.seed}"


class RailSubsampleFactory(RailFactoryMixin):
    """Factory class to make subsamples

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Subsamples:
        - Subsample:
          name: test_100k
          seed: 1234
          num_objects: 100000
    """

    yaml_tag = "Subsamples"

    client_classes = [RailSubsample]

    _instance: RailSubsampleFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._subsamples = self.add_dict(RailSubsample)

    @classmethod
    def get_subsamples(cls) -> dict[str, RailSubsample]:
        """Return the dict of all the subsamples"""
        return cls.instance().subsamples

    @classmethod
    def get_subsample_names(cls) -> list[str]:
        """Return the names of the subsamples"""
        return list(cls.instance().subsamples.keys())

    @classmethod
    def get_subsample(cls, name: str) -> RailSubsample:
        """Get a subsample by it's assigned name

        Parameters
        ----------
        name: str
            Name of the subsample to return

        Returns
        -------
        RailSubsample:
            subsample  in question
        """
        try:
            return cls.instance().subsamples[name]
        except KeyError as msg:
            raise KeyError(
                f"RailSubsample named {name} not found in RailSubsampleFactory "
                f"{list(cls.instance().subsamples.keys())}"
            ) from msg

    @classmethod
    def add_subsample(cls, subsample: RailSubsample) -> None:
        cls.instance().add_to_dict(subsample)

    @property
    def subsamples(self) -> dict[str, RailSubsample]:
        """Return the dictionary of subsample templates"""
        return self._subsamples

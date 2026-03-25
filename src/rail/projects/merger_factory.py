from __future__ import annotations

from typing import Any

from ceci.config import StageParameter
from rail.core.configurable import Configurable
from rail.core.factory_mixin import RailFactoryMixin


class RailMerger(Configurable):
    """Paramters for a simple data subsample

    This is just defined as a random number seed and a number of objects
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Merger name"),
        seed=StageParameter(
            int, None, fmt="%i", required=True, msg="Random numbers seed"
        ),
        num_objects=StageParameter(
            int, None, fmt="%i", required=True, msg="Number of objects to pick"
        ),
        weight_map=StageParameter(
            str, None, required=False, fmt="%s", msg="File with selection weights"
        ),
        object_id_col=StageParameter(
            str, "object_id", required=False, fmt="%s", msg="Object Id column name"
        ),
        cuts=StageParameter(list, None, fmt="%s", required=False, msg="Selection cuts"),
        inputs=StageParameter(
            dict, None, fmt="%s", required=False, msg="Input catalog detatils"
        ),
    )
    yaml_tag = "Merger"

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


class RailMergerFactory(RailFactoryMixin):
    """Factory class to make mergers

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Mergers:
        - Merger:
          name: test_100k
          seed: 1234
          num_objects: 100000
    """

    yaml_tag = "Mergers"

    client_classes = [RailMerger]

    _instance: RailMergerFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._mergers = self.add_dict(RailMerger)

    @classmethod
    def get_mergers(cls) -> dict[str, RailMerger]:
        """Return the dict of all the mergers"""
        return cls.instance().mergers

    @classmethod
    def get_merger_names(cls) -> list[str]:
        """Return the names of the mergers"""
        return list(cls.instance().mergers.keys())

    @classmethod
    def get_merger(cls, name: str) -> RailMerger:
        """Get a merger by it's assigned name

        Parameters
        ----------
        name: str
            Name of the merger to return

        Returns
        -------
        RailMerger:
            merger  in question
        """
        try:
            return cls.instance().mergers[name]
        except KeyError as msg:
            raise KeyError(
                f"RailMerger named {name} not found in RailMergerFactory "
                f"{list(cls.instance().mergers.keys())}"
            ) from msg

    @classmethod
    def add_merger(cls, merger: RailMerger) -> None:
        cls.instance().add_to_dict(merger)

    @property
    def mergers(self) -> dict[str, RailMerger]:
        """Return the dictionary of merger templates"""
        return self._mergers

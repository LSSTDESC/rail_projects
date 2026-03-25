from __future__ import annotations

from typing import Any

from ceci.config import StageParameter
from rail.core.configurable import Configurable
from rail.core.factory_mixin import RailFactoryMixin


class RailMerge(Configurable):
    """Paramters for a simple data subsample

    This is just defined as a random number seed and a number of objects
    """
    config_options: dict[str, StageParameter] = dict(
        merge_col=StageParameter(str, "object_id", fmt="%s", required=True, msg="Merge column name"),
        inputs=StageParameter(dict, None, fmt="%s", msg="Input catalog detatils"),
        output_basename=StageParameter(dict, None, fmt="%s", msg="Input catalog detatils"),
    )
    
    yaml_tag = "Merge"

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


class RailMergeFactory(RailFactoryMixin):
    """Factory class to make mergers

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Merges:
        - Merge:
            name: select_lsst_all
            inputs: ['output_select_lsst_HSC.pq', 'output_select_lsst_zCOSMOS.pq']
            output_basename: output_select_lsst_all.pq
    """

    yaml_tag = "Merges"

    client_classes = [RailMerge]

    _instance: RailMergeFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._merges = self.add_dict(RailMerge)

    @classmethod
    def get_merges(cls) -> dict[str, RailMerge]:
        """Return the dict of all the mergers"""
        return cls.instance().merges

    @classmethod
    def get_merge_names(cls) -> list[str]:
        """Return the names of the mergers"""
        return list(cls.instance().merges.keys())

    @classmethod
    def get_merge(cls, name: str) -> RailMerge:
        """Get a merger by it's assigned name

        Parameters
        ----------
        name: str
            Name of the merger to return

        Returns
        -------
        RailMerge:
            merge in question
        """
        try:
            return cls.instance().merges[name]
        except KeyError as msg:
            raise KeyError(
                f"RailMerger named {name} not found in RailMergeFactory "
                f"{list(cls.instance().merges.keys())}"
            ) from msg

    @classmethod
    def add_merge(cls, merge: RailMerge) -> None:
        cls.instance().add_to_dict(merge)

    @property
    def merges(self) -> dict[str, RailMerge]:
        """Return the dictionary of merge templates"""
        return self._merges

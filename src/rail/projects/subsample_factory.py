from __future__ import annotations

from typing import Any
import os
import yaml


from ceci.config import StageParameter
from .configurable import Configurable
from .factory_mixin import RailFactoryMixin


class RailSubsample(Configurable):
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
        kwargs: Any
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
    Subsamples:
      - Subsample:
        name: test_100k
        seed: 1234
        num_objects: 100000
    """

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
        subsample: RailSubsample
            subsample  in question
        """
        try:
            return cls.instance().subsamples[name]
        except KeyError as msg:
            raise KeyError(
                f"RailSubsample named {name} not found in RailSubsampleFactory "
                f"{list(cls.instance().subsamples.keys())}"
            ) from msg

    @property
    def subsamples(self) -> dict[str, RailSubsample]:
        """Return the dictionary of subsample templates"""
        return self._subsamples

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Subsamples:")
        for subsample_name, subsample in self.subsamples.items():
            print(f"  {subsample_name}: {subsample}")

    def add_subsample(self, subsample: RailSubsample) -> None:
        self.add_to_dict(subsample)

    def load_subsamples_from_yaml_tag(
        self,
        subsamples_config: list[dict[str, Any]],
    ) -> None:
        """Read a yaml "Subsamples" tag and load the factory accordingy

        Parameters
        ----------
        subsamples_config: list[dict[str, Any]]
            Yaml tag to load

        Notes
        -----
        See class description for yaml file syntax
        """
        self.load_instance_yaml_tag(subsamples_config)

    def load_instance_yaml(self, yaml_file: str) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_file: str
            File to read

        Notes
        -----
        See class description for yaml file syntax
        """
        with open(os.path.expandvars(yaml_file), encoding="utf-8") as fin:
            yaml_data = yaml.safe_load(fin)

        try:
            subsamples_config = yaml_data["Subsamples"]
        except KeyError as missing_key:
            raise KeyError(
                f"Did not find key Subsamples in {yaml_file}"
            ) from missing_key

        self.load_subsamples_from_yaml_tag(subsamples_config)

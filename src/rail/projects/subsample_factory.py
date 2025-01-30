from __future__ import annotations

from typing import Any
import os
import yaml


from ceci.config import StageParameter
from rail.plotting.configurable import Configurable


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

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailAlgorithmHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)


class RailSubsampleFactory:
    """Factory class to make subsamples

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:
    Subsamples:
      - Subsample:
        name: test_100k
        seed: 1234
        num_objects: 100000
    """

    _instance: RailSubsampleFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._subsamples: dict[str, RailSubsample] = {}

    @classmethod
    def instance(cls) -> RailSubsampleFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:
            cls._instance = RailSubsampleFactory()
        return cls._instance

    @classmethod
    def clear(cls) -> None:
        """Clear the contents of the factory"""
        if cls._instance is None:
            return
        cls._instance.clear_instance()

    @classmethod
    def print_contents(cls) -> None:
        """Print the contents of the factory"""
        if cls._instance is None:
            cls._instance = RailSubsampleFactory()
        cls._instance.print_instance_contents()

    @classmethod
    def load_yaml(cls, yaml_file: str) -> None:
        """Load a yaml file

        Parameters
        ----------
        yaml_file: str
            File to read and load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = RailSubsampleFactory()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def load_yaml_tag(cls, subsamples_config: list[dict[str, Any]]) -> None:
        """Load from a yaml tag

        Parameters
        ----------
        subsamples_config: list[dict[str, Any]]
            Yaml tag used to load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = RailSubsampleFactory()
        cls._instance.load_subsamples_from_yaml_tag(subsamples_config)

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

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._subsamples.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("SubsampleTemplates:")
        for subsample_name, subsample in self.subsamples.items():
            print(f"  {subsample_name}: {subsample}")
        print("----------------")

    def _make_subsample(self, **kwargs: Any) -> RailSubsample:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "SubsampleInstance yaml block does not contain name for subsample_instance: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._subsamples:  # pragma: no cover
            raise KeyError(f"RailSubsample {name} is already defined")
        subsample = RailSubsample(**kwargs)
        self._subsamples[name] = subsample
        return subsample

    def load_subsample_from_yaml_tag(self, subsample_config: dict[str, Any]) -> None:
        """Load a dataset from a Subsample tag in yaml

        Paramters
        ---------
        subsample_config: dict[str, Any]
            Yaml data in question
        """
        self._make_subsample(**subsample_config)

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
        for subsamples_item in subsamples_config:
            if "Subsample" in subsamples_item:
                subsample_config = subsamples_item["Subsample"]
                self.load_subsample_from_yaml_tag(subsample_config)
            else:  # pragma: no cover
                good_keys = ["Subsample"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {subsamples_item.keys()})"
                )

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

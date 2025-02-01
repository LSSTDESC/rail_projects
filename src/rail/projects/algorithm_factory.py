from __future__ import annotations

from typing import Any
import os
import yaml

from .algorithm_holder import (
    RailAlgorithmHolder,
    RailPZAlgorithmHolder,
    RailSpecSelectionAlgorithmHolder,
    RailClassificationAlgorithmHolder,
    RailSummarizerAlgorithmHolder,
    RailErrorModelAlgorithmHolder,
    RailSubsamplerAlgorithmHolder,
    RailReducerAlgorithmHolder,
)
from .factory_mixin import RailFactoryMixin


ALGORITHM_TYPES: list[str] = [
    "SpecSelections",
    "PZAlgorithms",
    "Classifiers",
    "Summarizers",
    "ErrorModels",
    "Subsamplers",
    "Reducers",
]


class RailAlgorithmFactory(RailFactoryMixin):
    """Factory class to make holder for Algorithms

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:


    Or the used can specifiy particular catalog instances where everything except the
    interation_vars are resolved

    """

    client_classes = [
        RailPZAlgorithmHolder,
        RailSpecSelectionAlgorithmHolder,
        RailClassificationAlgorithmHolder,
        RailSummarizerAlgorithmHolder,
        RailErrorModelAlgorithmHolder,
        RailSubsamplerAlgorithmHolder,
        RailReducerAlgorithmHolder,
    ]

    _instance: RailAlgorithmFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._algorithm_holder_dict = {
            aclass_.yaml_tag: self.add_dict(aclass_) for aclass_ in self.client_classes
        }

    @classmethod
    def get_algorithm_holder_dict(cls) -> dict[str, dict[str, RailAlgorithmHolder]]:
        """Return the dict of all the algorithms"""
        return cls.instance().algorithm_holder_dict

    @classmethod
    def get_algorithm_types(cls) -> list[str]:
        """Return the names of the algorithms types"""
        return list(cls.instance().algorithm_holder_dict.keys())

    @classmethod
    def get_algorithms(cls, algorithm_type: str) -> dict[str, RailAlgorithmHolder]:
        """Return a dict of all the algorithms of a particular type"""
        return cls.instance().algorithm_holder_dict[algorithm_type]

    @classmethod
    def get_algorithm_names(cls, algorithm_type: str) -> list[str]:
        """Return the names of all the algorithms of a particular type"""
        return list(cls.instance().algorithm_holder_dict[algorithm_type].keys())

    @classmethod
    def get_algorithm(cls, algorithm_type: str, algo_name: str) -> RailAlgorithmHolder:
        """Return the names of all the algorithms of a particular type"""
        algorithms = cls.get_algorithms(algorithm_type)
        return algorithms[algo_name]

    @classmethod
    def get_algorithm_class(cls, algorithm_type: str, algo_name: str, key: str) -> type:
        """Return the names of all the algorithms of a particular type"""
        algorithm_holder = cls.get_algorithm(algorithm_type, algo_name)
        return algorithm_holder(key)

    @property
    def algorithm_holder_dict(self) -> dict[str, dict[str, RailAlgorithmHolder]]:
        """Return the dictionary of catalog templates"""
        return self._algorithm_holder_dict

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Algorithms")
        for algorithm_type, algo_dict in self.algorithm_holder_dict.items():
            print("----------------")
            print(f"{algorithm_type}:")
            for algorithm_name, algorithm in algo_dict.items():
                print(f"  {algorithm_name}: {algorithm}")

    def add_algorithm(self, algorithm: RailAlgorithmHolder) -> None:
        self.add_to_dict(algorithm)

    def load_instance_algorithm_type_from_yaml_tag(
        self,
        yaml_item_value: list[dict[str, Any]],
    ) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_item_value: list[dict[str, Any]],
            Instance of the algorithm

        Notes
        -----
        See class description for yaml file syntax
        """
        self.load_instance_yaml_tag(yaml_item_value)

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

        for yaml_item_key, yaml_item_value in yaml_data.items():
            if yaml_item_key in ALGORITHM_TYPES:
                self.load_instance_algorithm_type_from_yaml_tag(yaml_item_value)
            else:  # pragma: no cover
                raise KeyError(
                    f"Expecting one of {ALGORITHM_TYPES} not: {yaml_item_key})"
                )

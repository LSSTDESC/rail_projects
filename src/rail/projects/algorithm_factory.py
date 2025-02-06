from __future__ import annotations

import os

import yaml

from .algorithm_holder import (
    RailAlgorithmHolder,
    RailClassificationAlgorithmHolder,
    RailErrorModelAlgorithmHolder,
    RailPZAlgorithmHolder,
    RailReducerAlgorithmHolder,
    RailSpecSelectionAlgorithmHolder,
    RailSubsamplerAlgorithmHolder,
    RailSummarizerAlgorithmHolder,
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

    .. highlight:: yaml
    .. code-block:: yaml

      SpecSelections:
        - SpecSelection:
            name: zCOSMOS
            Select: SpecSelection_zCOSMOS
            Module: rail.creation.degraders.spectroscopic_selections

      PZAlgorithms:
        - PZAlgorithm:
            name: trainz
            Estimate: TrainZEstimator
            Inform: TrainZInformer
            Module: rail.estimation.algos.train_z
        - PZAlgorithm:
            name: simplenn
            Estimate: SklNeurNetEstimator
            Inform: SklNeurNetInformer
            Module: rail.estimation.algos.sklearn_neurnet

    and so on.
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
        """Return a particular algorithm of a particular type"""
        algorithms = cls.get_algorithms(algorithm_type)
        return algorithms[algo_name]

    @classmethod
    def get_algorithm_class(cls, algorithm_type: str, algo_name: str, key: str) -> type:
        """Return the class of a particular algorithm of a particular type"""
        algorithm_holder = cls.get_algorithm(algorithm_type, algo_name)
        return algorithm_holder.resolve(key)

    @classmethod
    def add_algorithm(cls, algorithm: RailAlgorithmHolder) -> None:
        """Add a particular aglorithm to the factory"""
        cls.instance().add_to_dict(algorithm)

    @property
    def algorithm_holder_dict(self) -> dict[str, dict[str, RailAlgorithmHolder]]:
        """Return the dictionary of all the algorithms, keyed by type"""
        return self._algorithm_holder_dict

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Algorithms")
        RailFactoryMixin.print_instance_contents(self)

    def load_instance_yaml(self, yaml_file: str) -> None:
        with open(os.path.expandvars(yaml_file), encoding="utf-8") as fin:
            yaml_data = yaml.safe_load(fin)

        for yaml_item_key, yaml_item_value in yaml_data.items():
            if yaml_item_key in ALGORITHM_TYPES:
                self.load_instance_yaml_tag(
                    yaml_item_value, f"{yaml_file}#{yaml_item_key})"
                )
            else:  # pragma: no cover
                raise KeyError(
                    f"Expecting one of {ALGORITHM_TYPES} not: {yaml_item_key})"
                )

    def to_instance_yaml_dict(self) -> dict:
        ret_dict: dict[str, list] = {}
        for a_dict_name, a_dict in self._the_dicts.items():
            sub_list: list[dict] = []
            for value_ in a_dict.values():
                sub_list.append(value_.to_yaml_dict())
            ret_dict[f"{a_dict_name}s"] = sub_list
        return ret_dict

from __future__ import annotations

from typing import Any
import os
import yaml

from .algorithm_holder import RailAlgorithmHolder


ALGORITHM_TYPES_AND_TAGS: dict[str, tuple[str, str]] = dict(
    SpecSelections=(
        "SpecSelection",
        "rail.project.algorithm_holder.RailSpecSelectionAlgorithmHolder",
    ),
    PZAlgorithms=("PZAlgorithm", "rail.project.algorithm_holder.RailPZAlgorithmHolder"),
    Classifiers=(
        "Classifier",
        "rail.project.algorithm_holder.RailClassificationAlgorithmHolder",
    ),
    Summarizers=(
        "Summarizer",
        "rail.project.algorithm_holder.RailSummarizerAlgorithmHolder",
    ),
    ErrorModels=(
        "ErrorModel",
        "rail.project.algorithm_holder.RailErrorModelAlgorithmHolder",
    ),
    Subsamplers=(
        "Subsampler",
        "rail.project.algorithm_holder.RailSubsamplerAlgorithmHolder",
    ),
    Reducers=(
        "Reducer",
        "rail.project.algorithm_holder.RailReducerAlgorithmHolder",
    ),
)


class RailAlgorithmFactory:
    """Factory class to make holder for Algorithms

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:


    Or the used can specifiy particular catalog instances where everything except the
    interation_vars are resolved

    """

    _instance: RailAlgorithmFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._algorithm_holder_dict: dict[str, dict[str, RailAlgorithmHolder]] = {}

    @classmethod
    def instance(cls) -> RailAlgorithmFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:
            cls._instance = RailAlgorithmFactory()
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
            cls._instance = RailAlgorithmFactory()
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
            cls._instance = RailAlgorithmFactory()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def load_yaml_tag(
        cls,
        yaml_item_key: str,
        yaml_item_value: list[dict[str, Any]],
    ) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_item_key: str
            Type of algorithm being loaded

        yaml_item_value: list[dict[str, Any]],
            Instance of the algorithm

        Notes
        -----
        See class description for yaml file syntax
        """
        if cls._instance is None:
            cls._instance = RailAlgorithmFactory()
        cls._instance.load_algorithm_type_from_yaml_tag(yaml_item_key, yaml_item_value)

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

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._algorithm_holder_dict.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Algorithms")
        for algorithm_type, algo_dict in self.algorithm_holder_dict.items():
            print("----------------")
            print(f"{algorithm_type}:")
            for algorithm_name, algorithm in algo_dict.items():
                print(f"  {algorithm_name}: {algorithm}")

    def _make_algorithm(self, **kwargs: Any) -> RailAlgorithmHolder:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "RailAlgorithmHolder yaml block does not contain name for algorithm: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        algorithm_type = kwargs.pop("algorithm_type")
        if algorithm_type not in self._algorithm_holder_dict:
            self._algorithm_holder_dict[algorithm_type] = {}
        if name in self._algorithm_holder_dict[algorithm_type]:  # pragma: no cover
            raise KeyError(f"Algorithm {name} is already defined")
        algorithm = RailAlgorithmHolder.create_from_dict(kwargs)
        self._algorithm_holder_dict[algorithm_type][name] = algorithm
        return algorithm

    def load_algorithm_from_yaml_tag(
        self, algorithm_type: str, algorithm_config: dict[str, Any]
    ) -> None:
        """Load an algorithm holder from a tag in yaml

        Paramters
        ---------
        algorithm_config: dict[str, Any]
            Yaml data in question
        """
        self._make_algorithm(algorithm_type=algorithm_type, **algorithm_config)

    def load_algorithm_type_from_yaml_tag(
        self,
        yaml_item_key: str,
        yaml_item_value: list[dict[str, Any]],
    ) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_item_key: str
            Type of algorithm being loaded

        yaml_item_value: list[dict[str, Any]],
            Instance of the algorithm

        Notes
        -----
        See class description for yaml file syntax
        """
        expected_tag, class_name = ALGORITHM_TYPES_AND_TAGS[yaml_item_key]
        for yaml_item_ in yaml_item_value:
            if expected_tag in yaml_item_:
                algorithm_config = yaml_item_[expected_tag]
                algorithm_config["class_name"] = class_name
                self.load_algorithm_from_yaml_tag(yaml_item_key, algorithm_config)
            else:  # pragma: no cover
                raise KeyError(
                    f"Expecting one of {expected_tag} not: {yaml_item_.keys()})"
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

        for yaml_item_key, yaml_item_value in yaml_data.items():
            if yaml_item_key in ALGORITHM_TYPES_AND_TAGS:
                self.load_algorithm_type_from_yaml_tag(yaml_item_key, yaml_item_value)
            else:  # pragma: no cover
                raise KeyError(
                    f"Expecting one of {ALGORITHM_TYPES_AND_TAGS.keys()} not: {yaml_item_key})"
                )

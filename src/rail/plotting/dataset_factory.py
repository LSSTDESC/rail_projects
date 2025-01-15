from __future__ import annotations

from typing import Any

import yaml

from rail.projects import RailProject

from .data_extraction import RailProjectDataExtractor


class RailDatasetFactory:
    """Factory class to make datasets

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:
    - Project:
         name: some_project
         yaml_file: /path/to/rail_project_file
    - Dataset:
          name: gold_baseline_test
          extractor: rail.plotting.pz_data_extraction.PZPointEstimateDataExtractor
          project: some_project
          selection: gold
          flavor: baseline
          tag: test
          algos: ['all']
    - Dataset:
          name: blend_baseline_test
          exctractor: rail.plottings.pz_data_extraction.PZPointEstimateDataExtractorxs
          project: some_project
          selection: blend
          flavor: baseline
          tag: test
          algos: ['all']

    And group them into lists of dataset that can be run over particular types
    of data, using the following example syntax:

    - DatasetList:
          name: baseline_test
          datasets:
              - gold_baseline_test
              - blend_baseline_test
    """
    _instance: RailDatasetFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._projects: dict[str, RailProject] = {}
        self._datasets: dict[str, dict] = {}
        self._dataset_dicts: dict[str, dict[str, dict]] = {}

    @classmethod
    def instance(cls) -> RailDatasetFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:  # pragma: no cover
            cls._instance = RailDatasetFactory()
        return cls._instance

    @classmethod
    def clear(cls) -> None:
        """Clear the contents of the factory"""
        if cls._instance is None:  # pragma: no cover
            return
        cls._instance.clear_instance()

    @classmethod
    def print_contents(cls) -> None:
        """Print the contents of the factory """
        if cls._instance is None:  # pragma: no cover
            cls._instance = RailDatasetFactory()
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
        The format of the yaml file should be

        - Project
              name: some_project
              yaml_file: /path/to/rail_project_file
        - Dataset:
              name: gold_baseline_test
              extractor: rail.plotters.pz_data_extraction.PZPointEstimateDataExtractor
              project: some_project
              selection: gold
              flavor: baseline
              tag: test
              algos: ['all']
        - Dataset:
              name: blend_baseline_test
              extractor: rail.plotters.pz_data_extraction.PZPointEstimateDataExtractor
              project: some_project
              selection: blend
              flavor: baseline
              tag: test
              algos: ['all']
        - DatasetDict:
              name: baseline_test
              datasets:
                  - gold_baseline_test
                  - blend_baseline_test
        """
        if cls._instance is None:
            cls._instance = RailDatasetFactory()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def get_projects(cls) -> dict[str, RailProject]:
        """Return the dict of all the projects"""
        return cls.instance().projects

    @classmethod
    def get_project_names(cls) -> list[str]:
        """Return the dict of all the projects"""
        return list(cls.instance().projects.keys())

    @classmethod
    def get_datasets(cls) -> dict[str, dict]:
        """Return the dict of all the datasets"""
        return cls.instance().datasets

    @classmethod
    def get_dataset_names(cls) -> list[str]:
        """Return the names of the datasets"""
        return list(cls.instance().datasets.keys())

    @classmethod
    def get_dataset_dicts(cls) -> dict[str, dict[str, dict]]:
        """Return the dict of all the datasets"""
        return cls.instance().dataset_dicts

    @classmethod
    def get_dataset_dict_names(cls) -> list[str]:
        """Return the names of the dataset lists"""
        return list(cls.instance().dataset_dicts.keys())

    @classmethod
    def get_dataset(cls, name: str) -> dict:
        """Get dataset it's assigned name

        Parameters
        ----------
        name: str
            Name of the dataset to return

        Returns
        -------
        dataset: dict
            Dataset in question
        """
        try:
            return cls.instance().datasets[name]
        except KeyError as msg:
            raise KeyError(
                f"Dataset named {name} not found in RailDatasetFactory "
                f"{list(cls.instance().datasets.keys())}"
            ) from msg

    @classmethod
    def get_dataset_dict(cls, name: str) -> dict[str, dict]:
        """Get a list of datasets their assigned name

        Parameters
        ----------
        name: str
            Name of the dataset list to return

        Returns
        -------
        datasets: list[dict]
            Datasets in question
        """
        try:
            return cls.instance().dataset_dicts[name]
        except KeyError as msg:
            raise KeyError(
                f"DatasetList named {name} not found in RailDatasetFactory "
                f"{list(cls.instance().dataset_dicts.keys())}"
            ) from msg

    @property
    def projects(self) -> dict[str, RailProject]:
        """Return the dictionary of RailProjects"""
        return self._projects

    @property
    def datasets(self) -> dict[str, dict]:
        """Return the dictionary of individual datasets"""
        return self._datasets

    @property
    def dataset_dicts(self) -> dict[str, dict[str, dict]]:
        """Return the dictionary of lists of datasets"""
        return self._dataset_dicts

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._projects.clear()
        self._datasets.clear()
        self._dataset_dicts.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("Projects:")
        for project_name, project in self.projects.items():
            print(f"  {project_name}: {project}")
        print("----------------")
        print("Datasets:")
        for dataset_name, dataset in self.datasets.items():
            print(f"  {dataset_name}: {dataset}")
        print("----------------")
        print("DatasetLists")
        for dataset_dict_name, dataset_dict in self.dataset_dicts.items():
            print(f"  {dataset_dict_name}: {dataset_dict}")

    def _get_extractor(self, name: str, class_name: str) -> RailProjectDataExtractor:
        return RailProjectDataExtractor.create(name, class_name)

    def _make_dataset(self, name: str, class_name: str, **kwargs: Any) -> dict:
        if name in self._datasets:  # pragma: no cover
            raise KeyError(f"Dataset {name} is already defined")
        extractor = self._get_extractor(name, class_name)
        project_name = kwargs.pop('project')
        try:
            project = self._projects[project_name]
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                f"Could not find project {project_name} in {list(self._projects.keys())}"
            ) from msg
        dataset = extractor(project=project, **kwargs)
        self._datasets[name] = dataset
        return dataset

    def _make_dataset_dict(
        self, name: str, dataset_name_list: list[str]
    ) -> dict[str, dict]:
        if name in self._datasets:  # pragma: no cover
            raise KeyError(f"DatasetDict {name} is already defined")
        datasets: dict[str, dict] = {}
        for dataset_name in dataset_name_list:
            try:
                dataset = self._datasets[dataset_name]
            except KeyError as msg:  # pragma: no cover
                raise KeyError(
                    f"Dataset {dataset_name} used in DatasetList "
                    f"is not found {list(self._datasets.keys())}"
                ) from msg
            datasets[dataset_name] = dataset
        self._dataset_dicts[name] = datasets
        return datasets

    def load_dataset_from_yaml_tag(self, dataset_config: dict[str, Any]) -> None:
        """Load a dataset from a Dataset tag in yaml

        Paramters
        ---------
        dataset_config: dict[str, Any]
            Yaml data in question
        """
        try:
            name = dataset_config.pop("name")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "Dataset yaml block does not contain name for dataset: "
                f"{list(dataset_config.keys())}"
            ) from msg
        try:
            extractor = dataset_config.pop("extractor")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "Dataset yaml block does not contain extractor for dataset: "
                f"{list(dataset_config.keys())}"
            ) from msg
        self._make_dataset(name, extractor, **dataset_config)

    def load_dataset_list_from_yaml_tag(self, dataset_list_config: dict[str, Any]) -> None:
        """Load a list of datasets from a DatasetList tag in yaml

        Paramters
        ---------
        dataset_list_config: dict[str, Any]
            Yaml data in question
        """
        try:
            name = dataset_list_config.pop("name")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "DatasetList yaml block does not contain name for dataset: "
                f"{list(dataset_list_config.keys())}"
            ) from msg
        try:
            dataset_names = dataset_list_config.pop("datasets")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "DatasetList yaml block does not contain dataset: "
                f"{list(dataset_list_config.keys())}"
            ) from msg
        self._make_dataset_dict(name, dataset_names)

    def load_project_from_yaml_tag(self, project_config: dict[str, Any]) -> None:
        """Load a RailProject from a Project tag in yaml

        Paramters
        ---------
        project_config: dict[str, Any]
            Yaml data in question
        """
        try:
            name = project_config.pop("name")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "Project yaml block does not contain name for project: "
                f"{list(project_config.keys())}"
            ) from msg
        try:
            project_yaml = project_config.pop("yaml_file")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "Project yaml block does not contain yaml_file: "
                f"{list(project_config.keys())}"
            ) from msg
        self._projects[name] = RailProject.load_config(project_yaml)

    def load_instance_yaml(self, yaml_file: str) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_file: str
            File to read

        Notes
        -----
        See `RailDatasetFactory.load_yaml` for yaml file syntax
        """
        with open(yaml_file, encoding="utf-8") as fin:
            dataset_data = yaml.safe_load(fin)

        for dataset_item in dataset_data:
            if "Dataset" in dataset_item:
                dataset_config = dataset_item["Dataset"]
                self.load_dataset_from_yaml_tag(dataset_config)
            elif "DatasetList" in dataset_item:
                dataset_list_config = dataset_item["DatasetList"]
                self.load_dataset_list_from_yaml_tag(dataset_list_config)
            elif "Project" in dataset_item:
                project_config = dataset_item["Project"]
                self.load_project_from_yaml_tag(project_config)
            else:  # pragma: no cover
                good_keys = ["Dataset", "DatasetList", "Project"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {dataset_item.keys()})"
                )

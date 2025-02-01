from __future__ import annotations

from typing import Any, TypeVar, TYPE_CHECKING
import os
import yaml

from rail.projects import RailProject
from rail.projects.factory_mixin import RailFactoryMixin

from .dataset_holder import RailProjectHolder, RailDatasetHolder, RailDatasetListHolder

if TYPE_CHECKING:
    from rail.projects.configurable import Configurable

    C = TypeVar("C", bound="Configurable")


class RailDatasetFactory(RailFactoryMixin):
    """Factory class to make datasets

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:
    Data:
      - Project:
         name: some_project
         yaml_file: /path/to/rail_project_file
      - Dataset:
          name: gold_baseline_test
          class: rail.plotting.project_dataset_holder.RailProjectDatasetHolder
          extractor: rail.plotting.pz_data_extractor.PZPointEstimateDataExtractor
          project: some_project
          selection: gold
          flavor: baseline
          tag: test
          algos: ['all']
      - Dataset:
          name: blend_baseline_test
          class: rail.plotting.project_dataset_holder.RailProjectDatasetHolder
          exctractor: rail.plottings.pz_data_extractor.PZPointEstimateDataExtractor
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

    client_classes = [RailProjectHolder, RailDatasetHolder, RailDatasetListHolder]

    _instance: RailDatasetFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._projects = self.add_dict(RailProjectHolder)
        self._datasets = self.add_dict(RailDatasetHolder)
        self._dataset_lists = self.add_dict(RailDatasetListHolder)

    @classmethod
    def get_projects(cls) -> dict[str, RailProject]:
        """Return the dict of all the projects"""
        return cls.instance().projects

    @classmethod
    def get_project_names(cls) -> list[str]:
        """Return the names of all the projects"""
        return list(cls.instance().projects.keys())

    @classmethod
    def get_project(cls, key: str) -> RailProject:
        """Return a project by name"""
        return cls.instance().projects[key]

    @classmethod
    def get_datasets(cls) -> dict[str, RailDatasetHolder]:
        """Return the dict of all the datasets"""
        return cls.instance().datasets

    @classmethod
    def get_dataset_names(cls) -> list[str]:
        """Return the names of the datasets"""
        return list(cls.instance().datasets.keys())

    @classmethod
    def get_dataset_lists(cls) -> dict[str, RailDatasetListHolder]:
        """Return the dict of all the datasets"""
        return cls.instance().dataset_lists

    @classmethod
    def get_dataset_list_names(cls) -> list[str]:
        """Return the names of the dataset lists"""
        return list(cls.instance().dataset_lists.keys())

    @classmethod
    def get_dataset(cls, name: str) -> RailDatasetHolder:
        """Get dataset by it's assigned name

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
    def get_dataset_list(cls, name: str) -> RailDatasetListHolder:
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
            return cls.instance().dataset_lists[name]
        except KeyError as msg:
            raise KeyError(
                f"DatasetList named {name} not found in RailDatasetFactory "
                f"{list(cls.instance().dataset_lists.keys())}"
            ) from msg

    @property
    def projects(self) -> dict[str, RailProjectHolder]:
        """Return the dictionary of RailProjects"""
        return self._projects

    @property
    def datasets(self) -> dict[str, RailDatasetHolder]:
        """Return the dictionary of individual datasets"""
        return self._datasets

    @property
    def dataset_lists(self) -> dict[str, RailDatasetListHolder]:
        """Return the dictionary of lists of datasets"""
        return self._dataset_lists

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Projects:")
        for project_name, project in self.projects.items():
            print(f"  {project_name}: {project}")
        print("----------------")
        print("Datasets:")
        for dataset_name, dataset in self.datasets.items():
            print(f"  {dataset_name}: {dataset}")
        print("----------------")
        print("DatasetLists")
        for dataset_list_name, dataset_lists in self._dataset_lists.items():
            print(f"  {dataset_list_name}: {dataset_lists}")

    def load_object_from_yaml_tag(
        self, configurable_class: type[C], yaml_tag: dict[str, Any]
    ) -> None:
        if configurable_class == RailDatasetHolder:
            the_object = RailDatasetHolder.create_from_dict(yaml_tag)
            self.add_to_dict(the_object)
            the_object()
            return
        RailFactoryMixin.load_object_from_yaml_tag(self, configurable_class, yaml_tag)

    def add_project(self, project_holder: RailProjectHolder) -> None:
        self.add_to_dict(project_holder)

    def add_dataset(self, dataset_holder: RailDatasetHolder) -> None:
        self.add_to_dict(dataset_holder)

    def add_dataset_list(self, dataset_list: RailDatasetListHolder) -> None:
        self.add_to_dict(dataset_list)

    def load_data_from_yaml_tag(
        self,
        data_config: list[dict[str, Any]],
    ) -> None:
        """Read a yaml "Data" tag and load the factory accordingy

        Parameters
        ----------
        data_config: list[dict[str, Any]]
            Yaml tag to load

        Notes
        -----
        See class description for yaml file syntax
        """
        self.load_instance_yaml_tag(data_config)

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
        with open(os.path.expandvars(yaml_file), encoding="utf-8") as fin:
            yaml_data = yaml.safe_load(fin)

        try:
            data_config = yaml_data["Data"]
        except KeyError as missing_key:
            raise KeyError(f"Did not find key Data in {yaml_file}") from missing_key

        self.load_data_from_yaml_tag(data_config)

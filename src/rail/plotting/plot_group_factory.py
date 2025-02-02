from __future__ import annotations

import re
from typing import Any

import yaml

from rail.projects.factory_mixin import RailFactoryMixin

from .dataset_factory import RailDatasetFactory
from .plot_group import RailPlotGroup
from .plotter_factory import RailPlotterFactory


class RailPlotGroupFactory(RailFactoryMixin):
    """Factory class to make plot_groups

    The yaml file should look something like this:. .
    Includes:
      - <path_to_yaml_file_defining_plotter_lists>
      - <path_to_yaml_file defining_dataset_lists>
    PlotGroups:
      - PlotGroup:
          name: some_name
          plotter_list_name: nice_plots
          dataset_dict_name: nice_data
      - PlotGroup:
          name: some_other_name
          plotter_list_name: janky_plots
          dataset_dict_name: janky_data
    """

    yaml_tag: str = "PlotGroups"

    client_classes = [RailPlotGroup]

    _instance: RailPlotGroupFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._plot_groups = self.add_dict(RailPlotGroup)

    @classmethod
    def make_yaml(
        cls,
        output_yaml: str,
        plotter_yaml_path: str,
        dataset_yaml_path: str,
        plotter_list_name: str,
        output_prefix: str = "",
        dataset_list_name: list[str] | None = None,
    ) -> None:
        """Construct a yaml file defining plot groups

        Parameters
        ----------
        output_yaml: str
            Path to the output file

        plotter_yaml_path: str
            Path to the yaml file defining the plotter_lists

        dataset_yaml_path: str
            Path to the yaml file defining the datasets

        plotter_list_name: str
            Name of plotter list to use

        output_prefix: str=""
            Prefix for PlotGroup names we construct

        dataset_list_names: list[str] | None=None
            Names of dataset lists to use
        """
        if cls._instance is None:
            cls._instance = RailPlotGroupFactory()
        cls._instance.make_instance_yaml(
            output_yaml=output_yaml,
            plotter_yaml_path=plotter_yaml_path,
            dataset_yaml_path=dataset_yaml_path,
            plotter_list_name=plotter_list_name,
            output_prefix=output_prefix,
            dataset_list_name=dataset_list_name,
        )

    @classmethod
    def get_plot_groups(cls) -> dict[str, RailPlotGroup]:
        """Return the dict of all the RailPlotGroup"""
        return cls.instance().plot_groups

    @classmethod
    def get_plot_group_names(cls) -> list[str]:
        """Return the names of all the projectsRailPlotGroup"""
        return list(cls.instance().plot_groups.keys())

    @classmethod
    def add_plot_group(cls, plot_group: RailPlotGroup) -> None:
        """Add a particular RailPlotGroup to the factory"""
        cls.instance().add_to_dict(plot_group)

    @classmethod
    def get_plot_group(cls, key: str) -> RailPlotGroup:
        """Return a project by name"""
        return cls.instance().plot_groups[key]

    @property
    def plot_groups(self) -> dict[str, RailPlotGroup]:
        """Return the dictionary of RailProjects"""
        return self._plot_groups

    def make_instance_yaml(
        self,
        output_yaml: str,
        plotter_yaml_path: str,
        dataset_yaml_path: str,
        plotter_list_name: str,
        output_prefix: str = "",
        dataset_list_name: list[str] | None = None,
    ) -> None:
        """Construct a yaml file defining plot groups

        Parameters
        ----------
        output_yaml: str
            Path to the output file

        plotter_yaml_path: str
            Path to the yaml file defining the plotter_lists

        dataset_yaml_path: str
            Path to the yaml file defining the datasets

        plotter_list_name: str
            Name of plotter list to use

        output_prefix: str=""
            Prefix for PlotGroup names we construct

        dataset_list_name: list[str]
            Names of dataset lists to use
        """
        RailPlotterFactory.clear()
        RailPlotterFactory.load_yaml(plotter_yaml_path)
        RailDatasetFactory.clear()
        RailDatasetFactory.load_yaml(dataset_yaml_path)

        plotter_list = RailPlotterFactory.get_plotter_list(plotter_list_name)
        assert plotter_list
        if not dataset_list_name:  # pragma: no cover
            dataset_list_name = RailDatasetFactory.get_dataset_list_names()

        plotter_path = re.sub(
            ".*rail_project_config", "${RAIL_PROJECT_CONFIG_DIR}", plotter_yaml_path
        )
        dataset_path = re.sub(
            ".*rail_project_config", "${RAIL_PROJECT_CONFIG_DIR}", dataset_yaml_path
        )
        output: list[dict[str, Any]] = []
        output.append(dict(Includes=[plotter_path, dataset_path]))
        for ds_name in dataset_list_name:
            group_name = f"{output_prefix}{ds_name}_{plotter_list_name}"
            output.append(
                dict(
                    PlotGroup=dict(
                        name=group_name,
                        plotter_list_name=plotter_list_name,
                        dataset_dict_name=ds_name,
                    )
                )
            )
        with open(output_yaml, "w", encoding="utf-8") as fout:
            yaml.dump(output, fout)

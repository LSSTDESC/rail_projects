from __future__ import annotations

from typing import Any

import yaml

from .dataset_factory import RailDatasetFactory
from .plotter_factory import RailPlotterFactory
from .plot_group import RailPlotGroup


class RailPlotGroupFactory:
    """Factory class to make plot_groups

    The yaml file should look something like this:. .

    - PlotterYaml:
          path: <path_to_yaml_file_defining_plotter_lists>
    - DatasetYaml:
          path: <path_to_yaml_file defining_dataset_lists>
    - PlotGroup:
          name: some_name
          plotter_list_name: nice_plots
          dataset_dict_name: nice_data
    - PlotGroup:
          name: some_other_name
          plotter_list_name: janky_plots
          dataset_dict_name: janky_data
    """

    _instance: RailPlotGroupFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._plot_groups: dict[str, RailPlotGroup] = {}

    @classmethod
    def instance(cls) -> RailPlotGroupFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:  # pragma: no cover
            cls._instance = RailPlotGroupFactory()
        return cls._instance

    @classmethod
    def clear(cls) -> None:
        """Clear the contents of the factory"""
        if cls._instance is None:  # pragma: no cover
            return
        cls._instance.clear_instance()

    @classmethod
    def print_contents(cls) -> None:
        """Print the contents of the factory"""
        if cls._instance is None:  # pragma: no cover
            cls._instance = RailPlotGroupFactory()
        cls._instance.print_instance_contents()

    @classmethod
    def make_yaml(
        cls,
        output_yaml: str,
        plotter_yaml_path: str,
        dataset_yaml_path: str,
        plotter_list_name: str,
        output_prefix: str = "",
        dataset_list_names: list[str] | None = None,
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
            dataset_list_names=dataset_list_names,
        )

    @classmethod
    def load_yaml(cls, yaml_file: str) -> dict[str, RailPlotGroup]:
        """Load a yaml file

        Parameters
        ----------
        yaml_file: str
            File to read and load
        """
        if cls._instance is None:
            cls._instance = RailPlotGroupFactory()
        return cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def get_plot_groups(cls) -> dict[str, RailPlotGroup]:
        """Return the dict of all the RailPlotGroup"""
        return cls.instance().plot_groups

    @classmethod
    def get_plot_group_names(cls) -> list[str]:
        """Return the names of all the projectsRailPlotGroup"""
        return list(cls.instance().plot_groups.keys())

    @classmethod
    def get_plot_group(cls, key: str) -> RailPlotGroup:
        """Return a project by name"""
        return cls.instance().plot_groups[key]

    @property
    def plot_groups(self) -> dict[str, RailPlotGroup]:
        """Return the dictionary of RailProjects"""
        return self._plot_groups

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._plot_groups.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("RailPlotGroups:")
        for plot_group_name, plot_group in self.plot_groups.items():
            print(f"  {plot_group_name}: {plot_group}")
        print("----------------")

    def make_instance_yaml(
        self,
        output_yaml: str,
        plotter_yaml_path: str,
        dataset_yaml_path: str,
        plotter_list_name: str,
        output_prefix: str = "",
        dataset_list_names: list[str] | None = None,
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
        RailPlotterFactory.clear()
        RailPlotterFactory.load_yaml(plotter_yaml_path)
        RailDatasetFactory.clear()
        RailDatasetFactory.load_yaml(dataset_yaml_path)

        plotter_list = RailPlotterFactory.get_plotter_list(plotter_list_name)
        assert plotter_list
        if dataset_list_names is None:
            dataset_list_names = RailDatasetFactory.get_dataset_dict_names()

        output: list[dict[str, Any]] = []
        output.append(
            dict(PlotterYaml=dict(path=plotter_yaml_path)),
        )
        output.append(
            dict(DatasetYaml=dict(path=dataset_yaml_path)),
        )
        for ds_name in dataset_list_names:
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

    def load_instance_yaml(
        self,
        yaml_file: str,
    ) -> dict[str, RailPlotGroup]:
        """Read a yaml file and load build the RailPlotGroup objects

        Parameters
        ----------
        yaml_file: str
            File to read

        Returns
        -------
        out_dict: dict[str, RailPlotGroup]
            Newly created RailPlotGroups

        Notes
        -----
        The yaml file should look something like this:

        - PlotterYaml:
              path: <path_to_yaml_file_defining_plotter_lists>
        - DatasetYaml:
              path: <path_to_yaml_file defining_dataset_lists>
        - PlotGroup:
              name: some_name
              plotter_list_name: nice_plots
              dataset_dict_name: nice_data
        - PlotGroup:
              name: some_other_name
              plotter_list_name: janky_plots
              dataset_dict_name: janky_data
        """
        with open(yaml_file, encoding="utf-8") as fin:
            all_data = yaml.safe_load(fin)

        for group_item in all_data:
            if "PlotterYaml" in group_item:
                plotter_yaml_config = group_item["PlotterYaml"]
                try:
                    plotter_yaml_path = plotter_yaml_config.pop("path")
                except KeyError as msg:  # pragma: no cover
                    raise KeyError(
                        "PlotterYaml yaml block does not contain path: "
                        f"{list(plotter_yaml_config.keys())}"
                    ) from msg
                RailPlotterFactory.clear()
                RailPlotterFactory.load_yaml(plotter_yaml_path)
            elif "DatasetYaml" in group_item:
                dataset_yaml_config = group_item["DatasetYaml"]
                try:
                    dataset_yaml_path = dataset_yaml_config.pop("path")
                except KeyError as msg:  # pragma: no cover
                    raise KeyError(
                        "PlotterYamlDatasetYaml yaml block does not contain path: "
                        f"{list(dataset_yaml_config.keys())}"
                    ) from msg
                RailDatasetFactory.clear()
                RailDatasetFactory.load_yaml(dataset_yaml_path)
            elif "PlotGroup" in group_item:
                plot_group_config = group_item["PlotGroup"]
                try:
                    name = plot_group_config.pop("name")
                except KeyError as msg:  # pragma: no cover
                    raise KeyError(
                        "PlotGroup yaml block does not contain name for plot group: "
                        f"{list(plot_group_config.keys())}"
                    ) from msg
                self._plot_groups[name] = RailPlotGroup.create(name, plot_group_config)
            else:  # pragma: no cover
                good_keys = ["PlotterYaml", "DatasetYaml", "PlotGroup"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {group_item.keys()})"
                )

        return self._plot_groups

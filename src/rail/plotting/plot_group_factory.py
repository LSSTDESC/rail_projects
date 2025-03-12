from __future__ import annotations

import re
from typing import Any

import yaml

from rail.projects.factory_mixin import RailFactoryMixin
from rail.projects.configurable import Configurable

from .dataset_factory import RailDatasetFactory
from .dataset_holder import RailDatasetHolder, RailDatasetListHolder, RailProjectHolder
from .plot_group import RailPlotGroup
from .plotter import RailPlotterList
from .plotter_factory import RailPlotterFactory


class RailPlotGroupFactory(RailFactoryMixin):
    """Factory class to make plot_groups

    The yaml file should look something like this:

    .. highlight:: yaml
    .. code-block:: yaml

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
    def make_plot_groups(
        cls,
        plotter_list: RailPlotterList,
        **kwargs: Any,
    ) -> dict:
        """Extract the datasets from a project and construct the approporiate PlotGroups

        Paramters
        ---------
        **kwargs:
            Passed to the generate_dataset_dict() functions of the holder classes

        Returns
        -------
        dict:
            Projects: the extracted RailProjects
            Datasets: the extracted DatasetHolders
            DatasetLists: the extracted dataset lists
            PlotGroups: the constructed PlotGropus
        """
        if cls._instance is None:
            cls._instance = RailPlotGroupFactory()
        return cls._instance.make_plot_groups_instance(
            plotter_list,
            **kwargs,
        )

    @classmethod
    def make_yaml_for_project(
        cls,
        output_yaml: str,
        plotter_yaml_path: str,
        project_yaml_path: str,
        **kwargs: Any,
    ) -> None:
        """Construct a yaml file defining plot groups

        Parameters
        ----------
        output_yaml:
            Path to the output file

        plotter_yaml_path:
            Path to the yaml file defining the plotter_lists

        project_yaml_path:
            Path to the underlying project file

        **kwargs:
            See notes
        """
        if cls._instance is None:
            cls._instance = RailPlotGroupFactory()
        cls._instance.make_yaml_for_project_instance(
            output_yaml,
            plotter_yaml_path,
            project_yaml_path,
            **kwargs,
        )

    @classmethod
    def make_yaml_for_dataset_list(
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
        output_yaml:
            Path to the output file

        plotter_yaml_path:
            Path to the yaml file defining the plotter_lists

        dataset_yaml_path:
            Path to the yaml file defining the datasets

        plotter_list_name:
            Name of plotter list to use

        output_prefix:
            Prefix for PlotGroup names we construct

        dataset_list_names:
            Names of dataset lists to use
        """
        if cls._instance is None:
            cls._instance = RailPlotGroupFactory()
        cls._instance.make_yaml_for_dataset_list_instance(
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

    def make_yaml_for_dataset_list_instance(
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
        plot_groups: list[RailPlotGroup] = []
        for ds_name in dataset_list_name:
            group_name = f"{output_prefix}{ds_name}_{plotter_list_name}"
            plot_groups.append(
                RailPlotGroup(
                    name=group_name,
                    plotter_list_name=plotter_list_name,
                    dataset_list_name=ds_name,
                )
            )

        output: dict[str, Any] = dict(
            Includes=[plotter_path, dataset_path],
            PlotGroups=[plot_group_.to_yaml_dict() for plot_group_ in plot_groups],
        )
        with open(output_yaml, "w", encoding="utf-8") as fout:
            yaml.dump(output, fout)

    def make_yaml_for_project_instance(
        self,
        output_yaml: str,
        plotter_yaml_path: str,
        project_yaml_path: str,
        **kwargs: Any,
    ) -> None:
        """Construct a yaml file defining plot groups

        Parameters
        ----------
        output_yaml:
            Path to the output file

        plotter_yaml_path:
            Path to the yaml file defining the plotter_lists

        project_yaml_path:
            Path to the underlying project file

        **kwargs:
            See notes
        """
        outdir = kwargs.get("outdir", ".")
        figtype = kwargs.get("outdir", "png")

        RailPlotterFactory.load_yaml(plotter_yaml_path)
        plotter_lists = RailPlotterFactory.get_plotter_lists()

        projects: list[list[RailProjectHolder]] = []
        datasets: list[list[RailDatasetHolder]] = []
        dataset_lists: list[list[RailDatasetListHolder]] = []
        plot_groups: list[list[RailPlotGroup]] = []

        for _key, val in plotter_lists.items():
            plot_group_stuff = self.make_plot_groups_instance(
                val,
                project_file=project_yaml_path,
                **kwargs,
            )
            projects.append(plot_group_stuff["Projects"])
            datasets.append(plot_group_stuff["Datasets"])
            dataset_lists_ = plot_group_stuff["DatasetLists"]
            dataset_lists.append(dataset_lists_)
            plot_groups_: list[RailPlotGroup] = []
            for dataset_list_ in dataset_lists_:
                plot_groups_.append(
                    RailPlotGroup(
                        name=f"{val.config.name}_{dataset_list_.config.name}",
                        plotter_list_name=val.config.name,
                        dataset_list_name=dataset_list_.config.name,
                        outdir=outdir,
                        figtype=figtype,
                    )
                )
            plot_groups.append(plot_groups_)

        merged_projects = Configurable.merge_named_lists(projects)
        merged_datasets = Configurable.merge_named_lists(datasets)
        merged_dataset_lists = Configurable.merge_named_lists(dataset_lists)
        merged_plot_groups = Configurable.merge_named_lists(plot_groups)

        data_yaml_list: list[dict[str, Any]] = []
        for project_ in merged_projects:
            data_yaml_list.append(project_.to_yaml_dict())
        for dataset_ in merged_datasets:
            data_yaml_list.append(dataset_.to_yaml_dict())
        for dataset_list_ in merged_dataset_lists:
            data_yaml_list.append(dataset_list_.to_yaml_dict())

        plot_group_yaml_list: list[dict[str, Any]] = []
        for plot_group_ in merged_plot_groups:
            plot_group_yaml_list.append(plot_group_.to_yaml_dict())

        plotter_path = re.sub(
            ".*rail_project_config", "${RAIL_PROJECT_CONFIG_DIR}", plotter_yaml_path
        )

        output_yaml_dict: dict[str, list] = dict(
            Includes=[plotter_path],
            Data=data_yaml_list,
            PlotGroups=plot_group_yaml_list,
        )

        with open(output_yaml, "w", encoding="utf-8") as fout:
            yaml.dump(output_yaml_dict, fout)

    def make_plot_groups_instance(
        self,
        plotter_list: RailPlotterList,
        **kwargs: Any,
    ) -> dict:
        """Extract the datasets from a project and construct the approporiate PlotGroups

        Paramters
        ---------
        **kwargs:
            Passed to the generate_dataset_dict() functions of the holder classes

        Returns
        -------
        dict:
            Projects: the extracted RailProjects
            Datasets: the extracted DatasetHolders
            DatasetLists: the extracted dataset lists
            PlotGroups: the constructed PlotGropus
        """
        dataset_holder_class = RailDatasetHolder.load_sub_class(
            plotter_list.config.dataset_holder_class
        )

        try:
            (
                projects,
                datasets,
                dataset_lists,
            ) = dataset_holder_class.generate_dataset_dict(**kwargs)
        except NotImplementedError:
            return dict(
                Projects=[],
                Datasets=[],
                DatasetLists=[],
                PlotGroups=[],
            )
        output_data = dict(
            Projects=projects,
            Datasets=datasets,
            DatasetLists=dataset_lists,
        )
        plot_groups_list: list[RailPlotGroup] = []

        outdir = kwargs.get("outdir", ".")
        figtype = kwargs.get("figtype", "png")

        for dslist_ in dataset_lists:
            dataset_list_name = dslist_.config.name
            plot_group = RailPlotGroup(
                name=f"{plotter_list.config.name}_{dataset_list_name}",
                plotter_list_name=plotter_list.config.name,
                dataset_list_name=dataset_list_name,
                outdir=outdir,
                figtype=figtype,
            )
            self.add_plot_group(plot_group)
            plot_groups_list.append(plot_group)
        output_data.update(
            PlotGroups=plot_groups_list,
        )
        return output_data

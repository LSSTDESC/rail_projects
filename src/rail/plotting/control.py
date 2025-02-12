"""Functions to control plot making in the context of a RailProject"""

from __future__ import annotations

import os
from typing import Any

import yaml

from rail.projects.factory_mixin import RailFactoryMixin

from .dataset_factory import RailDatasetFactory
from .dataset_holder import RailDatasetHolder
from .plot_group import RailPlotGroup
from .plot_group_factory import RailPlotGroupFactory
from .plot_holder import RailPlotDict
from .plotter import RailPlotter
from .plotter_factory import RailPlotterFactory

THE_FACTORIES: list[type[RailFactoryMixin]] = [
    RailPlotterFactory,
    RailDatasetFactory,
    RailPlotGroupFactory,
]

YAML_HANDLERS: dict[str, type[RailFactoryMixin]] = {
    factory.yaml_tag: factory for factory in THE_FACTORIES
}


# Lift the RailDatasetFactory class methods

load_dataset_yaml = RailDatasetFactory.load_yaml

load_dataset_yaml_tag = RailDatasetFactory.load_yaml_tag

print_dataset_contents = RailDatasetFactory.print_contents

get_datasets = RailDatasetFactory.get_datasets

get_dataset_names = RailDatasetFactory.get_dataset_names

get_dataset_lists = RailDatasetFactory.get_dataset_lists

get_dataset_list_names = RailDatasetFactory.get_dataset_list_names

get_dataset = RailDatasetFactory.get_dataset

get_dataset_list = RailDatasetFactory.get_dataset_list

add_dataset = RailDatasetFactory.add_dataset

add_dataset_list = RailDatasetFactory.add_dataset_list

add_project = RailDatasetFactory.add_project


# Lift the RailPlotterFactory class methods

load_plotter_yaml = RailPlotterFactory.load_yaml

load_plotter_yaml_tag = RailPlotterFactory.load_yaml_tag

print_plotter_contents = RailPlotterFactory.print_contents

get_plotters = RailPlotterFactory.get_plotters

get_plotter_names = RailPlotterFactory.get_plotter_names

get_plotter_lists = RailPlotterFactory.get_plotter_lists

get_plotter_list_names = RailPlotterFactory.get_plotter_list_names

get_plotter = RailPlotterFactory.get_plotter

get_plotter_list = RailPlotterFactory.get_plotter_list

add_plotter = RailPlotterFactory.add_plotter

add_plotter_list = RailPlotterFactory.add_plotter_list


# Lift methods from the RailPlotGroupFactory

load_plot_group_yaml = RailPlotGroupFactory.load_yaml

load_plot_group_yaml_tag = RailPlotGroupFactory.load_yaml_tag

make_plot_group_yaml_for_dataset_list = RailPlotGroupFactory.make_yaml_for_dataset_list

make_plot_group_yaml_for_project = RailPlotGroupFactory.make_yaml_for_project

print_plot_group_contents = RailPlotGroupFactory.print_contents

get_plot_group_dict = RailPlotGroupFactory.get_plot_groups

get_plot_group_names = RailPlotGroupFactory.get_plot_group_names

get_plot_group = RailPlotGroupFactory.get_plot_group

add_plot_group = RailPlotGroupFactory.add_plot_group


# Lift methods from RailDatasetHolder

print_dataset_holder_classes = RailDatasetHolder.print_classes

get_dataset_holder_class = RailDatasetHolder.get_sub_class

load_dataset_holder_class = RailDatasetHolder.load_sub_class

create_dataset_holder_from_dict = RailDatasetHolder.create_from_dict


# Lift methods from RailPlotter

print_plotter_classes = RailPlotter.print_classes

get_plotter_class = RailPlotter.get_sub_class

load_plotter_class = RailPlotter.load_sub_class

write_plots = RailPlotter.write_plots


# Lift methods from RailPlotGroup

make_plots = RailPlotGroup.make_plots


# Define a few additional functions
def clear() -> None:
    """Clean all the factories"""
    for factory_ in THE_FACTORIES:
        factory_.clear()


def print_contents() -> None:
    """Print the contents of the factories"""
    for factory_ in THE_FACTORIES:
        factory_.print_contents()
        print("----------------")
        print("")


def print_classes() -> None:
    """Print the loaded classes"""
    RailPlotter.print_classes()
    print("----------------")
    print("")
    RailDatasetHolder.print_classes()
    print("----------------")


def run(
    yaml_file: str,
    include_groups: list[str] | None = None,
    exclude_groups: list[str] | None = None,
    **kwargs: Any,
) -> dict[str, RailPlotDict]:
    """Read a yaml file an make the corresponding plots

    Parameters
    ----------
    yaml_file: str
        Top level yaml file with definitinos

    include_groups: list[str]
        PlotGroups to explicitly include
        Use `None` for all plots

    exclude_groups: list[str]
        PlotGroups to explicity exclude
        Use `None` to not exclude anything

    Keywords
    --------
    find_only: bool=False
        If true, only look for existing plots

    save_plots: bool=True
        Save plots to disk

    purge_plots: bool=True
        Remove plots from memory after saving

    outdir: str | None
        If set, prepend this to the groups output dir

    make_html: bool
        If set, make an html page to browse plots

    Returns
    -------
    dict[str, RailPlotDict]:
        Newly created plots.   If purge=True this will be empty
    """
    clear()
    out_dict: dict[str, RailPlotDict] = {}
    load_yaml(yaml_file)
    group_dict = get_plot_group_dict()

    include_groups = kwargs.pop("include_groups", None)
    exclude_groups = kwargs.pop("exclude_groups", None)
    make_html = kwargs.get("make_html", False)
    outdir = kwargs.get("outdir", "plots")

    if include_groups is None or not include_groups:
        include_groups = list(group_dict.keys())
    if exclude_groups is None or not exclude_groups:
        exclude_groups = []
    for exclude_group_ in exclude_groups:  # pragma: no cover
        include_groups.remove(exclude_group_)

    output_pages: list[str] = []
    for group_ in include_groups:
        plot_group = group_dict[group_]
        out_dict.update(plot_group.run(**kwargs))
        if make_html:
            output_pages.append(f"plots_{plot_group.config.name}.html")
    if make_html:
        RailPlotGroup.make_html_index(
            os.path.join(outdir, "plot_index.html"), output_pages
        )
    return out_dict


def extract_datasets(
    config_file: str,
    dataset_holder_class: str,
    output_yaml: str,
    **kwargs: dict[str, Any],
) -> None:
    """Extract datasets into a yaml file

    Parameters
    ----------
    config_file: str
        Yaml project configuration file

    dataset_holder_class: str
        Class used to extract Datasets

    output_yaml: str
        Path to output file

    **kwargs:
        See notes for details

    Notes
    -----
    dataset_list_name: str
        Name for the resulting DatasetList

    selections: list[str]
        Selections to use

    flavors: list[str]
        Flavors to use

    split_by_flavor: bool
        Split dataset lists by flavor
    """
    extractor_cls = load_dataset_holder_class(dataset_holder_class)
    projects, datasets, dataset_lists = extractor_cls.generate_dataset_dict(
        project_file=config_file,
        **kwargs,
    )
    output_list: list[dict] = []
    output_list += [project_.to_yaml_dict() for project_ in projects]
    output_list += [dataset_.to_yaml_dict() for dataset_ in datasets]
    output_list += [dataset_list_.to_yaml_dict() for dataset_list_ in dataset_lists]

    output_data = dict(Data=output_list)
    with open(output_yaml, "w", encoding="utf-8") as fout:
        yaml.dump(output_data, fout)


def load_yaml(yaml_file: str) -> None:
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

    includes = yaml_data.pop("Includes", [])
    for include_ in includes:
        load_yaml(os.path.expandvars(include_))

    for yaml_key, yaml_item in yaml_data.items():
        if yaml_key == RailDatasetFactory.yaml_tag:
            load_dataset_yaml_tag(yaml_item, yaml_file)
        elif yaml_key == RailPlotterFactory.yaml_tag:
            load_plotter_yaml_tag(yaml_item, yaml_file)
        elif yaml_key == RailPlotGroupFactory.yaml_tag:
            load_plot_group_yaml_tag(yaml_item, yaml_file)
        else:  # pragma: no cover
            good_tags = [
                RailDatasetFactory.yaml_tag,
                RailPlotterFactory.yaml_tag,
                RailPlotGroupFactory.yaml_tag,
            ]
            raise KeyError(f"Yaml Tag {yaml_key} not in expected keys {good_tags}")


def write_yaml(yaml_file: str) -> None:
    """Write the current contents for the factories to a yaml file

    Parameters
    ----------
    yaml_file: str
        File to write

    Notes
    -----
    See class description for yaml file syntax
    """
    yaml_dict: dict[str, dict] = {}
    for a_factory in THE_FACTORIES:
        yaml_dict.update(**a_factory.to_yaml_dict())
    with open(os.path.expandvars(yaml_file), mode="w", encoding="utf-8") as fout:
        yaml.dump(yaml_dict, fout)

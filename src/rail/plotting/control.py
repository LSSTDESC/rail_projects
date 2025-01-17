"""Functions to control plot making in the context of a RailProject"""

from __future__ import annotations

from typing import Any
import yaml

from rail.projects import RailProject

from .dataset_factory import RailDatasetFactory
from .plotter_factory import RailPlotterFactory
from .data_extraction import RailProjectDataExtractor
from .plotter import RailPlotter
from .plot_group import RailPlotGroup
from .plot_holder import RailPlotHolder


# Lift the RailDatasetFactory class methods

load_dataset_yaml = RailDatasetFactory.load_yaml

print_dataset_contents = RailDatasetFactory.print_contents

get_datasets = RailDatasetFactory.get_datasets

get_dataset_names = RailDatasetFactory.get_dataset_names

get_dataset_dicts = RailDatasetFactory.get_dataset_dicts

get_dataset_dict_names = RailDatasetFactory.get_dataset_dict_names

get_dataset = RailDatasetFactory.get_dataset

get_dataset_dict = RailDatasetFactory.get_dataset_dict


# Lift the RailPlotterFactory class methods

load_plotter_yaml = RailPlotterFactory.load_yaml

print_plotter_contents = RailPlotterFactory.print_contents

get_plotter_dict = RailPlotterFactory.get_plotter_dict

get_plotter_names = RailPlotterFactory.get_plotter_names

get_plotter_list_dict = RailPlotterFactory.get_plotter_list_dict

get_plotter_list_names = RailPlotterFactory.get_plotter_list_names

get_plotter = RailPlotterFactory.get_plotter

get_plotter_list = RailPlotterFactory.get_plotter_list


# Lift methods from RailPlotter

write_plots = RailPlotter.write_plots


# Lift methods from RailPlotGroup

load_plot_group_yaml = RailPlotGroup.load_yaml

make_plots = RailPlotGroup.make_plots


# Define a few additional functions
def clear() -> None:
    RailPlotterFactory.clear()
    RailDatasetFactory.clear()


def print_contents() -> None:
    """Print the contents of the factories """
    print("----------------")
    RailPlotterFactory.print_contents()
    print("----------------")
    RailDatasetFactory.print_contents()


def run(
    yaml_file: str,
    include_groups: list[str] | None=None,
    exclude_groups: list[str] | None=None,
    **kwargs : Any,
) -> dict[str, RailPlotHolder]:
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

    output_html: str | None=None,
        Path to html page

    Returns
    -------
    out_dict: dict[str, RailPlotHolder]
        Newly created plots.   If purge=True this will be empty
    """
    clear()
    out_dict: dict[str, RailPlotHolder] = {}
    group_dict = RailPlotGroup.load_yaml(yaml_file)

    include_groups = kwargs.pop('include_groups', None)
    exclude_groups = kwargs.pop('exclude_groups', None)

    if include_groups is None or not include_groups:
        include_groups = list(group_dict.keys())
    if exclude_groups is None or not exclude_groups:
        exclude_groups = []
    for exclude_group_ in exclude_groups:  # pragma: no cover
        include_groups.remove(exclude_group_)

    for group_ in include_groups:
        plot_group = group_dict[group_]
        out_dict.update(plot_group(**kwargs))
    return out_dict


def extract_datasets(
    config_file: str,
    dataset_list_name: str,
    extractor_class: str,
    flavors: list[str],
    selections: list[str],
    output_yaml: str,
) -> None:

    """Extract datasets into a yaml file

    Parameters
    ----------
    config_file: str
        Yaml project configuration file

    dataset_list_name: str
        Name for the resulting DatasetList

    extractor_class: str
        Class used to extract Datasets

    selections: list[str]
        Selections to use

    flavors: list[str]
        Flavors to use

    output_yaml: str
        Path to output file
    """
    extractor_cls = RailProjectDataExtractor.load_extractor_class(extractor_class)
    project = RailProject.load_config(config_file)
    output_data = extractor_cls.generate_dataset_dict(
        dataset_list_name,
        project,
        selections,
        flavors,
    )
    with open(output_yaml, 'w', encoding="utf-8") as fout:
        yaml.dump(output_data, fout)

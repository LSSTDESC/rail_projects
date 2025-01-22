from typing import Any

import click

from rail.core import __version__

from rail.cli.rail import options
from rail.cli.rail_project import project_options
from rail.plotting import control
from . import plot_options


@click.group()
@click.version_option(__version__)
def plot_cli() -> None:
    """RAIL plotting functions"""


@plot_cli.command(name="run")
@project_options.config_file()
@plot_options.include_groups()
@plot_options.exclude_groups()
@plot_options.save_plots()
@plot_options.purge_plots()
@options.outdir()
def run_command(config_file: str, **kwargs: Any) -> int:
    """Make a bunch of plots"""
    control.clear()
    control.run(config_file, **kwargs)
    return 0


@plot_cli.command(name="inspect")
@project_options.config_file()
def inspect_command(config_file: str) -> int:
    """Inspect a configuration yaml file"""
    control.clear()
    plot_groups = control.load_plot_group_yaml(config_file)
    control.print_contents()
    print("----------------")
    print("Plot groups:")
    for key, val in plot_groups.items():
        print(f"  {key}:")
        print(f"    PlotterList: {val.plotter_list_name}")
        print(f"    DatasetList: {val.dataset_dict_name}")
    return 0


@plot_cli.command(name="extract-datasets")
@project_options.config_file()
@plot_options.dataset_list_name()
@plot_options.extractor_class()
@project_options.flavor()
@project_options.selection()
@options.output_yaml()
@plot_options.split_by_flavor()
def extract_datasets_command(
    config_file: str,
    dataset_list_name: str,
    extractor_class: str,
    flavor: list[str],
    selection: list[str],
    output_yaml: str,
    split_by_flavor: bool,
) -> int:
    """Create a yaml file with the datasets in a project"""
    control.clear()
    control.extract_datasets(
        config_file,
        dataset_list_name,
        extractor_class,
        flavors=flavor,
        selections=selection,
        output_yaml=output_yaml,
        split_by_flavor=split_by_flavor,
    )
    return 0


@plot_cli.command(name="make-plot-groups")
@options.output_yaml()
@plot_options.plotter_yaml_path()
@plot_options.dataset_yaml_path()
@plot_options.plotter_list_name()
@plot_options.output_prefix()
@plot_options.dataset_list_names(multiple=True)
def make_plot_groups(output_yaml: str, **kwargs: dict[str, Any]) -> int:
    """Create a yaml file with the datasets in a project"""
    control.clear()
    control.make_plot_group_yaml(output_yaml, **kwargs)
    return 0

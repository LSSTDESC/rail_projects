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
    """Make a bunch of plots"""
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

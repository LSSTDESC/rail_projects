from typing import Any

import click
from rail.cli.rail import options
from rail.core import __version__

from rail.cli.rail_project import project_options
from rail.plotting import control

from . import plot_options

__all__=[
    'plot_cli',
    'run_command',
    'inspect_command',
    'extract_datasets_command',
    'make_plot_groups',
]    


@click.group()
@click.version_option(__version__)
def plot_cli() -> None:
    """RAIL plotting functions

    These commands to work with RailProject infrastructure to generate
    sets of standard plots and html pages to help browse them.

    The configuration file can include these yaml_tags
    
    1. `Plots` with type of plots available
    2. `Data` with specific datasets we can make those plots with
    3. `PlotGroup` with combinations of the two
    """


@plot_cli.command(name="run")
@project_options.config_file()
@plot_options.include_groups()
@plot_options.exclude_groups()
@plot_options.save_plots()
@plot_options.purge_plots()
@plot_options.find_only()
@plot_options.make_html()
@options.outdir()
def run_command(config_file: str, **kwargs: Any) -> int:
    """Make a bunch of plots

    The configuration file should define both the plots to make
    and the datasets to use.
    """
    control.clear()
    control.run(config_file, **kwargs)
    return 0


@plot_cli.command(name="inspect")
@project_options.config_file()
def inspect_command(config_file: str) -> int:
    """Inspect a configuration yaml file

    These will load the configuration file, and any files that that it includes
    and then print out the contents of the component library.
    """
    control.clear()
    control.load_yaml(config_file)
    control.print_contents()
    return 0


@plot_cli.command(name="extract-datasets")
@project_options.config_file()
@plot_options.extractor_class()
@options.output_yaml()
@plot_options.dataset_list_name()
@plot_options.dataset_holder_class()
@project_options.flavor()
@project_options.selection()
@plot_options.split_by_flavor()
def extract_datasets_command(
    config_file: str,
    extractor_class: str,
    output_yaml: str,
    **kwargs: dict[str, Any],
) -> int:
    """Create a yaml file with the datasets in a project

    This will read a yaml project configuration file and
    search the project for all the datasets that the
    extractor_class is able to extract and write the
    results to the output_yaml file.
    """
    
    control.clear()
    control.extract_datasets(
        config_file,
        extractor_class,
        output_yaml=output_yaml,
        **kwargs,
    )
    return 0


@plot_cli.command(name="make-plot-groups")
@options.output_yaml()
@plot_options.plotter_yaml_path()
@plot_options.dataset_yaml_path()
@plot_options.plotter_list_name()
@plot_options.output_prefix()
@plot_options.dataset_list_name(multiple=True)
def make_plot_groups(output_yaml: str, **kwargs: dict[str, Any]) -> int:
    """Combine plotters with availble datsets

    This will read the plotter_yaml and dataset_yaml
    files, and combine all the datasets in the list 
    given by dataset_list_name with all the plots given
    by plotter_list_name and write the results to the output_yaml 
    file.
    """
    control.clear()
    control.make_plot_group_yaml(output_yaml, **kwargs)
    return 0

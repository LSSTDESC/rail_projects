import enum

import click

from rail.cli.rail.options import (
    EnumChoice,
    PartialOption,
    PartialArgument,
)


__all__: list[str] = [
    "RunMode",
    "config_path",
    "force",
    "flavor",
    "input_dir",
    "input_file",
    "input_selection",
    "input_tag",
    "label",
    "maglim",
    "model_dir",
    "model_name",
    "model_path",
    "output_dir",
    "pdf_dir",
    "pdf_path",
    "run_mode",
    "selection",
    "output_dir",
    "output_file",
    "truth_path",
    "seed",
]


class RunMode(enum.Enum):
    """Choose the run mode"""

    dry_run = 0
    bash = 1
    slurm = 2


config_file = PartialArgument(
    "config_file",
    type=click.Path(),
)


config_path = PartialOption(
    "--config_path",
    help="Path to configuration file",
    type=click.Path(),
)

force = PartialOption(
    "--force",
    help="Overwrite existing ceci configuration files",
    is_flag=True,
)

flavor = PartialOption(
    "--flavor",
    help="Pipeline configuraiton flavor",
    multiple=True,
    default=["baseline"],
)


label = PartialOption(
    "--label",
    help="File label (e.g., 'test' or 'train')",
    type=str,
)


selection = PartialOption(
    "--selection",
    help="Data selection",
    multiple=True,
    default=["gold"],
)


input_dir = PartialOption(
    "--input_dir",
    help="Input Directory",
    type=click.Path(),
)


input_file = PartialOption(
    "--input_file",
    type=click.Path(),
    help="Input file",
)


input_selection = PartialOption(
    "--input_selection",
    help="Data selection",
    multiple=True,
    default=[None],
)


input_tag = PartialOption(
    "--input_tag",
    type=str,
    default=None,
    help="Input Catalog tag",
)


maglim = PartialOption(
    "--maglim",
    help="Magnitude limit",
    type=float,
    default=25.5,
)


model_dir = PartialOption(
    "--model_dir",
    help="Path to directory with model files",
    type=click.Path(),
)


model_path = PartialOption(
    "--model_path",
    help="Path to model file",
    type=click.Path(),
)


model_name = PartialOption(
    "--model_name",
    help="Model Name",
    type=str,
)

output_dir = PartialOption(
    "--output_dir",
    help="Path to for output files",
    type=click.Path(),
)


pdf_dir = PartialOption(
    "--pdf_dir",
    help="Path to directory with p(z) files",
    type=click.Path(),
)


pdf_path = PartialOption(
    "--pdf_path",
    help="Path to p(z) estimate file",
    type=click.Path(),
)


run_mode = PartialOption(
    "--run_mode",
    type=EnumChoice(RunMode),
    default="bash",
    help="Mode to run script",
)

size = PartialOption(
    "--size",
    type=int,
    default=100_000,
    help="Number of objects in file",
)


output_dir = PartialOption(
    "--output_dir",
    type=click.Path(),
    help="Path to directory for output",
)


output_file = PartialOption(
    "--output_file",
    type=click.Path(),
    help="Output file",
)


truth_path = PartialOption(
    "--truth_path",
    help="Path to truth redshift file",
    type=click.Path(),
)

seed = PartialOption(
    "--seed",
    help="Random seed",
    type=int,
)

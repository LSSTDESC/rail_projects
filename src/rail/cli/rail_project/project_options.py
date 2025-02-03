import click
from rail.cli.rail.options import EnumChoice, PartialArgument, PartialOption

from rail.projects.execution import RunMode

__all__: list[str] = [
    "RunMode",
    "args",
    "basename",
    "config_path",
    "catalog_template",
    "file_template",
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
    "reducer_class_name",
    "run_mode",
    "selection",
    "site",
    "subsampler_class_name",
    "subsample_name",
    "output_dir",
    "output_file",
    "output_catalog_template",
    "truth_path",
    "seed",
]


args = PartialArgument(
    "args",
    type=str,
    nargs=-1,
)


basename = PartialOption(
    "--basename",
    type=str,
    help="Basename for catalog template resolution",
)


config_file = PartialArgument(
    "config_file",
    type=click.Path(),
)


config_path = PartialOption(
    "--config_path",
    help="Path to configuration file",
    type=click.Path(),
)


catalog_template = PartialOption(
    "--catalog_template",
    type=str,
    help="Name of the catalog template to use",
)


file_template = PartialOption(
    "--file_template",
    type=str,
    help="Name of the file template to use",
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


reducer_class_name = PartialOption(
    "--reducer_class_name",
    help="Class for the reducer",
    type=str,
    default="roman_rubin",
)


selection = PartialOption(
    "--selection",
    help="Data selection",
    multiple=True,
    default=["gold"],
)


site = PartialOption(
    "--site",
    help="site for slurm submission",
    default="s3df",
)


subsampler_class_name = PartialOption(
    "--subsampler_class_name",
    help="Class for the subsampler",
    type=str,
    default="random_subsample",
)


subsample_name = PartialOption(
    "--subsample_name",
    help="Name for the subsample",
    type=str,
    default=None,
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


output_catalog_template = PartialOption(
    "--output_catalog_template",
    type=str,
    help="Name of the catalog template to use for output",
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

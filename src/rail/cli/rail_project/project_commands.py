from typing import Any

import click
import yaml
from rail.core import __version__

from rail.projects import RailProject, execution, library

from . import project_options

__all__ = [
    "project_cli",
    "inspect_command",
    "build_command",
    "subsample_command",
    "reduce_command",
    "run_group",
]


@click.group()
@click.version_option(__version__)
def project_cli() -> None:
    """RAIL project management scripts

    These all expect a yaml_configuration file
    defining the RailProject.

    That file can, in turn, include other yaml
    configuration files that define a 'library' of
    possible analysis components
    """


@project_cli.command(name="inspect")
@project_options.config_file()
def inspect_command(config_file: str) -> int:
    """Inspect a rail pipeline project config"""
    print("RAIL Project Library")
    print(">>>>>>>>")
    project = RailProject.load_config(config_file)
    library.print_contents()
    print("<<<<<<<<")
    print(f"RAIL Project: {project}")
    print(">>>>>>>>")
    for key, val in project.config.items():
        if key == "Flavors":
            print(f"{key}:")
            for flavor_ in val:
                flavor_name = flavor_["Flavor"]["name"]
                print(f"- {flavor_name}")
            continue
        print(yaml.dump({key: val}, indent=2))
    print("<<<<<<<<")
    return 0


@project_cli.command(name="build")
@project_options.config_file()
@project_options.flavor()
@project_options.force()
def build_command(config_file: str, **kwargs: Any) -> int:
    """Build the ceci pipeline configuration files

    This will build all of the pipelines associated to
    a particular flavor or flavors, and write them to the
    the project pipelines area.
    """
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.build_pipelines(**kw, **kwargs)
    return ok


@project_cli.command(name="subsample")
@project_options.config_file()
@project_options.run_mode()
@project_options.catalog_template()
@project_options.file_template()
@project_options.subsampler_class_name()
@project_options.subsample_name()
@project_options.selection()
@project_options.flavor()
@project_options.basename()
def subsample_command(
    config_file: str, run_mode: project_options.RunMode, **kwargs: Any
) -> int:
    """Make a training or test data set by randomly selecting objects from
    a catalog of input files

    This will:
    resolve a catalog of input files from the catalog_template,
    flavor, selection and basename parameters,
    resolve a single output file from the file_template, flavor and selection
    parameters,
    subsample from the catalog files and write to the output file.
    """

    if run_mode == project_options.RunMode.slurm:
        raise NotImplementedError("subsample_command not set up to run under slurm")

    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)

    dry_run = run_mode == project_options.RunMode.dry_run

    ok = 0
    for kw in iter_kwargs:
        output_path = project.subsample_data(
            dry_run=dry_run,
            **kw,
            **kwargs,
        )
        hdf5_output = output_path.replace(".parquet", ".hdf5")
        ok |= execution.handle_command(
            run_mode,
            [
                "tables-io",
                "convert",
                "--input",
                f"{output_path}",
                "--output",
                f"{hdf5_output}",
            ],
        )
    return ok


@project_cli.command(name="sbatch")
@project_options.run_mode()
@project_options.site()
@project_options.args()
def sbatch_command(
    run_mode: project_options.RunMode, site: str, args: list[str]
) -> int:  # pragma: no cover
    """Wrap a rail_pipe command with site-based arguements for slurm"""
    return execution.sbatch_wrap(run_mode, site, args)


@project_cli.command(name="reduce")
@project_options.config_file()
@project_options.run_mode()
@project_options.catalog_template()
@project_options.output_catalog_template()
@project_options.reducer_class_name()
@project_options.input_selection()
@project_options.selection()
def reduce_command(
    config_file: str, run_mode: project_options.RunMode, **kwargs: Any
) -> int:
    """Reduce the roman rubin simulations for analysis

    This will:
    resolve a catalog of input files from the catalog_template,
    and input_selection parameters,
    resolve a catalog of output files from the output_catalog_template
    and selection parameters,
    reduce the input catalog to the output catalog
    """
    project = RailProject.load_config(config_file)
    selections = project.get_selection_args(kwargs.pop("selection"))
    input_selections = kwargs.pop("input_selection")
    iter_kwargs = project.generate_kwargs_iterable(
        selection=selections,
        input_selection=input_selections,
    )
    dry_run = run_mode == project_options.RunMode.dry_run

    ok = 0
    for kw in iter_kwargs:
        files = project.reduce_data(dry_run=dry_run, **kw, **kwargs)
        ok |= 0 if files else 1
    return ok


@project_cli.group(name="run")
def run_group() -> None:
    """Run a pipeline"""


@run_group.command(name="phot-errors")
@project_options.config_file()
@project_options.selection()
@project_options.flavor()
@project_options.run_mode()
def photmetric_errors_pipeline(config_file: str, **kwargs: Any) -> int:
    """Run the photometric errors analysis pipeline"""
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    pipeline_name = "photometric_errors"

    for kw in iter_kwargs:
        ok |= project.run_pipeline_catalog(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="prepare")
@project_options.config_file()
@project_options.selection()
@project_options.flavor()
@project_options.run_mode()
def prepare_pipeline(config_file: str, **kwargs: Any) -> int:
    """Run the truth-to-observed data pipeline"""
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    pipeline_name = "prepare"

    for kw in iter_kwargs:
        ok |= project.run_pipeline_catalog(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="truth-to-observed")
@project_options.config_file()
@project_options.selection()
@project_options.flavor()
@project_options.run_mode()
def truth_to_observed_pipeline(config_file: str, **kwargs: Any) -> int:
    """Run the truth-to-observed data pipeline"""
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    pipeline_name = "truth_to_observed"

    for kw in iter_kwargs:
        ok |= project.run_pipeline_catalog(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="blending")
@project_options.config_file()
@project_options.selection()
@project_options.flavor()
@project_options.run_mode()
def blending_pipeline(config_file: str, **kwargs: Any) -> int:
    """Run the blending analysis pipeline"""
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    pipeline_name = "blending"

    for kw in iter_kwargs:
        ok |= project.run_pipeline_catalog(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="spec-selection")
@project_options.config_file()
@project_options.selection()
@project_options.flavor()
@project_options.run_mode()
def spectroscopic_selection_pipeline(config_file: str, **kwargs: Any) -> int:
    """Run the spectroscopic selection data pipeline"""
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    pipeline_name = "spec_selection"

    for kw in iter_kwargs:
        ok |= project.run_pipeline_catalog(
            pipeline_name,
            spec_selections=list(project.get_spec_selections().keys()),
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="inform")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def inform_single(config_file: str, **kwargs: Any) -> int:
    """Run the inform pipeline"""
    pipeline_name = "inform"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.run_pipeline_single(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="estimate")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
@project_options.input_tag()
def estimate_single(config_file: str, **kwargs: Any) -> int:
    """Run the estimation pipeline"""
    pipeline_name = "estimate"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.run_pipeline_single(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="evaluate")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def evaluate_single(config_file: str, **kwargs: Any) -> int:
    """Run the evaluation pipeline"""
    pipeline_name = "evaluate"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.run_pipeline_single(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="pz")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def pz_single(config_file: str, **kwargs: Any) -> int:
    """Run the pz pipeline"""
    pipeline_name = "pz"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.run_pipeline_single(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="tomography")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def tomography_single(config_file: str, **kwargs: Any) -> int:
    """Run the tomography pipeline"""
    pipeline_name = "tomography"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.run_pipeline_single(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="inform-sompz")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def inform_sompz_single(config_file: str, **kwargs: Any) -> int:  # pragma: no cover
    """Run the sompz inform pipeline"""
    pipeline_name = "inform_sompz"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.run_pipeline_single(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="estimate-sompz")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def estimate_sompz_single(config_file: str, **kwargs: Any) -> int:  # pragma: no cover
    """Run the sompz estimate pipeline"""
    pipeline_name = "estimate_sompz"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project.run_pipeline_single(
            pipeline_name,
            **kw,
            **kwargs,
        )
    return ok

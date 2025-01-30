from typing import Any

import click

from rail.core import __version__

from rail.projects import RailProject
from . import project_options, project_scripts

@click.group()
@click.version_option(__version__)
def project_cli() -> None:
    """RAIL project management scripts"""


@project_cli.command(name="inspect")
@project_options.config_file()
def inspect_command(config_file: str) -> int:
    """Inspect a rail pipeline project config"""
    return project_scripts.inspect(config_file)


@project_cli.command(name="build")
@project_options.config_file()
@project_options.flavor()
@project_options.force()
def build_command(config_file: str, **kwargs: Any) -> int:
    """Build the ceci pipeline configuration files"""
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
    """Make a training or test data set by randomly selecting objects"""
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
        ok |= project_scripts.handle_command(
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
    return project_scripts.sbatch_wrap(run_mode, site, args)


@project_cli.command(name="reduce")
@project_options.config_file()
@project_options.run_mode()
@project_options.catalog_template()
@project_options.output_catalog_template()
@project_options.reducer_class_name()
@project_options.input_selection()
@project_options.selection()
def reduce_roman_rubin(
    config_file: str, run_mode: project_options.RunMode, **kwargs: Any
) -> int:
    """Reduce the roman rubin simulations for PZ analysis"""
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
    pipeline_info = project.get_pipeline(pipeline_name)
    input_catalog_name = pipeline_info["input_catalog_template"]
    pipeline_catalog_config = (
        project_scripts.PhotmetricErrorsPipelineCatalogConfiguration(
            project,
            source_catalog_tag=input_catalog_name,
            sink_catalog_tag="degraded",
        )
    )

    for kw in iter_kwargs:
        ok |= project_scripts.run_pipeline_on_catalog(
            project,
            pipeline_name,
            pipeline_catalog_config,
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
    pipeline_info = project.get_pipeline(pipeline_name)
    input_catalog_name = pipeline_info["input_catalog_template"]
    pipeline_catalog_config = project_scripts.SpectroscopicPipelineCatalogConfiguration(
        project,
        source_catalog_tag=input_catalog_name,
        sink_catalog_tag="degraded",
        source_catalog_basename="output_dereddener_errors.pq",
    )

    for kw in iter_kwargs:
        ok |= project_scripts.run_pipeline_on_catalog(
            project,
            pipeline_name,
            pipeline_catalog_config,
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
    pipeline_info = project.get_pipeline(pipeline_name)
    input_catalog_name = pipeline_info["input_catalog_template"]
    pipeline_catalog_config = project_scripts.BlendingPipelineCatalogConfiguration(
        project,
        source_catalog_tag=input_catalog_name,
        sink_catalog_tag="degraded",
    )

    for kw in iter_kwargs:
        ok |= project_scripts.run_pipeline_on_catalog(
            project,
            pipeline_name,
            pipeline_catalog_config,
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
    pipeline_info = project.get_pipeline(pipeline_name)
    input_catalog_name = pipeline_info["input_catalog_template"]
    pipeline_catalog_config = project_scripts.SpectroscopicPipelineCatalogConfiguration(
        project,
        source_catalog_tag=input_catalog_name,
        sink_catalog_tag="degraded",
        source_catalog_basename="output_dereddener_errors.pq",
    )

    for kw in iter_kwargs:
        ok |= project_scripts.run_pipeline_on_catalog(
            project,
            pipeline_name,
            pipeline_catalog_config,
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
        ok |= project_scripts.run_pipeline_on_single_input(
            project,
            pipeline_name,
            project_scripts.inform_input_callback,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="estimate")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def estimate_single(config_file: str, **kwargs: Any) -> int:
    """Run the estimation pipeline"""
    pipeline_name = "estimate"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project_scripts.run_pipeline_on_single_input(
            project,
            pipeline_name,
            project_scripts.estimate_input_callback,
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
        ok |= project_scripts.run_pipeline_on_single_input(
            project,
            pipeline_name,
            project_scripts.evaluate_input_callback,
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
        ok |= project_scripts.run_pipeline_on_single_input(
            project,
            pipeline_name,
            project_scripts.pz_input_callback,
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
        ok |= project_scripts.run_pipeline_on_single_input(
            project,
            pipeline_name,
            project_scripts.tomography_input_callback,
            **kw,
            **kwargs,
        )
    return ok


@run_group.command(name="sompz")
@project_options.config_file()
@project_options.flavor()
@project_options.selection()
@project_options.run_mode()
def sompz_single(config_file: str, **kwargs: Any) -> int:  # pragma: no cover
    """Run the sompz pipeline"""
    pipeline_name = "sompz"
    project = RailProject.load_config(config_file)
    flavors = project.get_flavor_args(kwargs.pop("flavor"))
    selections = project.get_selection_args(kwargs.pop("selection"))
    iter_kwargs = project.generate_kwargs_iterable(flavor=flavors, selection=selections)
    ok = 0
    for kw in iter_kwargs:
        ok |= project_scripts.run_pipeline_on_single_input(
            project,
            pipeline_name,
            project_scripts.sompz_input_callback,
            **kw,
            **kwargs,
        )
    return ok

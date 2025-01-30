import os
import subprocess
import time
import itertools
from typing import Any, Callable

import yaml

from rail.projects import RailProject, library

from .project_options import RunMode

S3DF_SLURM_OPTIONS: list[str] = [
    "-p",
    "milano",
    "--account",
    "rubin:commissioning@milano",
    "--mem",
    "16G",
    "--parsable",
]
PERLMUTTER_SLURM_OPTIONS: list[str] = [
    "--account",
    "m1727",
    "--constraint",
    "cpu",
    "--qos",
    "regular",
    "--parsable",
]

SLURM_OPTIONS = {
    "s3df": S3DF_SLURM_OPTIONS,
    "perlmutter": PERLMUTTER_SLURM_OPTIONS,
}


def handle_command(
    run_mode: RunMode,
    command_line: list[str],
) -> int:
    """Run a single command in the mode requested

    Parameters
    ----------
    run_mode: RunMode
        How to run the command, e.g., dry_run, bash or slurm

    command_line: list[str]
        Tokens in the command line

    Returns
    -------
    returncode: int
        Status returned by the command.  0 for success, exit code otherwise
    """
    print("subprocess:", *command_line)
    _start_time = time.time()
    print(">>>>>>>>")
    if run_mode == RunMode.dry_run:
        # print(command_line)
        command_line.insert(0, "echo")
        finished = subprocess.run(command_line, check=False)
    elif run_mode == RunMode.bash:  # pragma: no cover
        # return os.system(command_line)
        finished = subprocess.run(command_line, check=False)
    elif run_mode == RunMode.slurm:  # pragma: no cover
        raise RuntimeError(
            "handle_command should not be called with run_mode == RunMode.slurm"
        )

    returncode = finished.returncode
    _end_time = time.time()
    _elapsed_time = _end_time - _start_time
    print("<<<<<<<<")
    print(f"subprocess completed with status {returncode} in {_elapsed_time} seconds\n")
    return returncode


def handle_commands(
    run_mode: RunMode,
    command_lines: list[list[str]],
    script_path: str | None = None,
) -> int:  # pragma: no cover
    """Run a multiple commands in the mode requested

    Parameters
    ----------
    run_mode: RunMode
        How to run the command, e.g., dry_run, bash or slurm

    command_lines: list[list[str]]
        List of commands to run, each one is the list of tokens in the command line

    script_path: str | None
        Path to write the slurm submit script to

    Returns
    -------
    returncode: int
        Status returned by the commands.  0 for success, exit code otherwise
    """

    if run_mode in [RunMode.dry_run, RunMode.bash]:
        for command_ in command_lines:
            retcode = handle_command(run_mode, command_)
            if retcode:
                return retcode
        return 0

    # At this point we are using slurm and need a script to send to batch
    if script_path is None:
        raise ValueError(
            "handle_commands with run_mode == RunMode.slurm requires a path to a script to write",
        )

    try:
        os.makedirs(os.path.dirname(script_path))
    except FileExistsError:
        pass
    with open(script_path, "w", encoding="utf-8") as fout:
        fout.write("#!/usr/bin/bash\n\n")
        for command_ in command_lines:
            com_line = " ".join(command_)
            fout.write(f"{com_line}\n")

    script_out = script_path.replace(".sh", ".out")

    command_line = ["srun", "--output", script_out, "--error", script_path]
    try:
        with subprocess.Popen(
            command_line,
            stdout=subprocess.PIPE,
        ) as srun:
            assert srun.stdout
            line = srun.stdout.read().decode().strip()
            ret_val = int(line.split("|")[0])
    except TypeError as msg:
        raise TypeError(f"Bad slurm submit: {msg}") from msg

    return ret_val


def sbatch_wrap(
    run_mode: RunMode, site: str, args: list[str]
) -> int:  # pragma: no cover
    """Wrap a rail_pipe command with site-based arguements
    Parameters
    ----------
    run_mode: RunMode
        How to run the command, e.g., dry_run, bash or slurm
    site: str
        Execution site, used to set sbatch options

    args: list[str]
        Additional arguments
    Returns
    -------
    returncode: int
        Status.  0 for success, exit code otherwise
    """
    try:
        slurm_options = SLURM_OPTIONS[site]
    except KeyError as msg:
        raise KeyError(
            f"{site} is not a recognized site, options are {SLURM_OPTIONS.keys()}"
        ) from msg
    command_line = (
        ["sbatch"] + slurm_options + ["rail_pipe", "--run_mode", "slurm"] + list(args)
    )
    return handle_command(run_mode, command_line)


def inspect(config_file: str) -> int:
    """Inspect a rail project file and print out the configuration

    Parameters
    ----------
    config_file: str
        Project file in question

    Returns
    -------
    returncode: int
        Status.  0 for success, exit code otherwise
    """
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


class PipelineCatalogConfiguration:
    """Small plugin class to handle configuring a pipeline to run on a catalog

    Sub-classes will have to implment "get_convert_commands" function
    """

    def __init__(
        self,
        project: RailProject,
        source_catalog_tag: str,
        sink_catalog_tag: str,
        source_catalog_basename: str | None = None,
        sink_catalog_basename: str | None = None,
    ):
        self._project = project
        self._source_catalog_tag = source_catalog_tag
        self._sink_catalog_tag = sink_catalog_tag
        self._source_catalog_basename = source_catalog_basename
        self._sink_catalog_basename = sink_catalog_basename

    def get_source_catalog(self, **kwargs: Any) -> str:
        """Get the name of the source (i.e. input) catalog file"""
        return self._project.get_catalog(
            self._source_catalog_tag,
            basename=self._source_catalog_basename,
            **kwargs,
        )

    def get_sink_catalog(self, **kwargs: Any) -> str:
        """Get the name of the sink (i.e., output) catalog file"""
        return self._project.get_catalog(
            self._sink_catalog_tag,
            basename=self._sink_catalog_basename,
            **kwargs,
        )

    def get_script_path(self, pipeline_name: str, sink_dir: str, **kwargs: Any) -> str:
        """Get path to use for the slurm batch submit script"""
        selection = kwargs["selection"]
        flavor = kwargs["flavor"]
        return os.path.join(sink_dir, f"submit_{pipeline_name}_{selection}_{flavor}.sh")

    def get_convert_commands(self, sink_dir: str) -> list[list[str]]:
        """Get the set of commands to run after the pipeline to
        convert output files
        """
        raise NotImplementedError()


class TruthToObservedPipelineCatalogConfiguration(
    PipelineCatalogConfiguration
):  # pragma: no cover
    def get_convert_commands(self, sink_dir: str) -> list[list[str]]:
        convert_command = [
            "tables-io",
            "convert",
            "--input",
            f"{sink_dir}/output_dereddener_errors.pq",
            "--output",
            f"{sink_dir}/output.hdf5",
        ]
        convert_commands = [convert_command]
        return convert_commands


class PhotmetricErrorsPipelineCatalogConfiguration(PipelineCatalogConfiguration):
    def get_convert_commands(self, sink_dir: str) -> list[list[str]]:
        convert_command = [
            "tables-io",
            "convert",
            "--input",
            f"{sink_dir}/output_dereddener_errors.pq",
            "--output",
            f"{sink_dir}/output.hdf5",
        ]
        convert_commands = [convert_command]
        return convert_commands


class SpectroscopicPipelineCatalogConfiguration(PipelineCatalogConfiguration):
    def get_convert_commands(self, sink_dir: str) -> list[list[str]]:
        convert_commands = []
        spec_selections = self._project.get_spec_selections()
        for spec_selection_ in spec_selections.keys():
            convert_command = [
                "tables-io",
                "convert",
                "--input",
                f"{sink_dir}/output_select_{spec_selection_}.pq",
                "--output",
                f"{sink_dir}/output_select_{spec_selection_}.hdf5",
            ]
            convert_commands.append(convert_command)
        return convert_commands


class BlendingPipelineCatalogConfiguration(PipelineCatalogConfiguration):
    def get_convert_commands(self, sink_dir: str) -> list[list[str]]:
        convert_command = [
            "tables-io",
            "convert",
            "--input",
            f"{sink_dir}/output_blended.pq",
            "--output",
            f"{sink_dir}/output_blended.hdf5",
        ]
        convert_commands = [convert_command]
        return convert_commands


def run_pipeline_on_catalog(
    project: RailProject,
    pipeline_name: str,
    pipeline_catalog_configuration: PipelineCatalogConfiguration,
    run_mode: RunMode = RunMode.bash,
    **kwargs: Any,
) -> int:
    """Run a pipeline on an entire catalog

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    pipeline_catalog_configuration: PipelineCatalogConfiguration
        Class to manage input and output catalogs and files

    run_mode: RunMode
        How to run the command, e.g., dry_run, bash or slurm

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...


    Returns
    -------
    returncode: int
        Status returned by the command.  0 for success, exit code otherwise
    """

    pipeline_info = project.get_pipeline(pipeline_name)
    pipeline_path = project.get_path("pipeline_path", pipeline=pipeline_name, **kwargs)

    input_catalog_name = pipeline_info["input_catalog_template"]
    input_catalog = project.get_catalogs().get(input_catalog_name, {})

    # Loop through all possible combinations of the iteration variables that are
    # relevant to this pipeline
    if (iteration_vars := input_catalog.get("IterationVars", {})) is not None:
        iterations = itertools.product(
            *[
                project.config.get("IterationVars", {}).get(iteration_var, "")
                for iteration_var in iteration_vars
            ]
        )
        for iteration_args in iterations:
            iteration_kwargs = {
                iteration_vars[i]: iteration_args[i] for i in range(len(iteration_vars))
            }

            source_catalog = pipeline_catalog_configuration.get_source_catalog(
                **kwargs, **iteration_kwargs
            )
            sink_catalog = pipeline_catalog_configuration.get_sink_catalog(
                **kwargs, **iteration_kwargs
            )
            sink_dir = os.path.dirname(sink_catalog)
            script_path = pipeline_catalog_configuration.get_script_path(
                pipeline_name,
                sink_dir,
                **kwargs,
                **iteration_kwargs,
            )
            convert_commands = pipeline_catalog_configuration.get_convert_commands(
                sink_dir
            )

            ceci_command = project.generate_ceci_command(
                pipeline_path=pipeline_path,
                config=pipeline_path.replace(".yaml", "_config.yml"),
                inputs=dict(input=source_catalog),
                output_dir=sink_dir,
                log_dir=sink_dir,
            )

            if (
                not os.path.isfile(source_catalog) and run_mode != RunMode.dry_run
            ):  # pragma: no cover
                raise ValueError(f"Input file {source_catalog} not found")
            try:
                handle_commands(
                    run_mode,
                    [
                        ["mkdir", "-p", f"{sink_dir}"],
                        ceci_command,
                        *convert_commands,
                    ],
                    script_path,
                )
            except Exception as msg:  # pragma: no cover
                print(msg)
                return 1
        return 0

    # FIXME need to get catalogs even if iteration not specified; this return fallback isn't ideal
    return 1  # pragma: no cover


def run_pipeline_on_single_input(
    project: RailProject,
    pipeline_name: str,
    input_callback: Callable,
    run_mode: RunMode = RunMode.bash,
    **kwargs: Any,
) -> int:
    """Run a single pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    input_callback: Callable
        Function that creates dict of input files

    run_mode: RunMode
        How to run the command, e.g., dry_run, bash or slurm

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...


    Returns
    -------
    returncode: int
        Status returned by the command.  0 for success, exit code otherwise
    """
    pipeline_path = project.get_path("pipeline_path", pipeline=pipeline_name, **kwargs)
    pipeline_config = pipeline_path.replace(".yaml", "_config.yml")
    sink_dir = project.get_path("ceci_output_dir", **kwargs)
    script_path = os.path.join(sink_dir, f"submit_{pipeline_name}.sh")

    input_files = input_callback(project, pipeline_name, sink_dir, **kwargs)

    command_line = project.generate_ceci_command(
        pipeline_path=pipeline_path,
        config=pipeline_config,
        inputs=input_files,
        output_dir=sink_dir,
        log_dir=f"{sink_dir}/logs",
    )

    try:
        statuscode = handle_commands(run_mode, [command_line], script_path)
    except Exception as msg:  # pragma: no cover
        print(msg)
        statuscode = 1
    return statuscode


def inform_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,  # pylint: disable=unused-argument
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the inform pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    input_files: dict[str, str]
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor", "baseline")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )
    return input_files


def estimate_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the estimate pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    input_files: dict[str, str]
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor", "baseline")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )

    pz_algorithms = project.get_pzalgorithms()
    for pz_algo_ in pz_algorithms.keys():
        input_files[f"model_{pz_algo_}"] = os.path.join(
            sink_dir, f"inform_model_{pz_algo_}.pkl"
        )
    return input_files


def evaluate_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the evalute pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    input_files: dict[str, str]
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor", "baseline")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )

    pdfs_dir = sink_dir
    pz_algorithms = project.get_pzalgorithms()
    for pz_algo_ in pz_algorithms.keys():
        input_files[f"input_evaluate_{pz_algo_}"] = os.path.join(
            pdfs_dir, f"estimate_output_{pz_algo_}.hdf5"
        )
    return input_files


def pz_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,  # pylint: disable=unused-argument
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the pz pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    input_files: dict[str, str]
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )
    return input_files


def tomography_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the tomography pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    input_files: dict[str, str]
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor")
    selection = kwargs.get("selection")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], selection=selection
        )

    pdfs_dir = sink_dir
    pz_algorithms = project.get_pzalgorithms()
    for pz_algo_ in pz_algorithms.keys():
        input_files[f"input_{pz_algo_}"] = os.path.join(
            pdfs_dir, f"output_estimate_{pz_algo_}.hdf5"
        )

    return input_files


def sompz_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,  # pylint: disable=unused-argument
    **kwargs: Any,
) -> dict[str, str]:  # pragma: no cover
    """Make dict of input tags and paths for the sompz pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    input_files: dict[str, str]
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_file_dict = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor")
    selection = kwargs.get("selection")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_file_dict[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], selection=selection
        )

    input_files = dict(
        train_deep_data=input_file_dict["input_train"],
        train_wide_data=input_file_dict["input_train"],
        test_spec_data=input_file_dict["input_test"],
        test_balrog_data=input_file_dict["input_test"],
        test_wide_data=input_file_dict["input_test"],
        truth=input_file_dict["input_test"],
    )
    return input_files

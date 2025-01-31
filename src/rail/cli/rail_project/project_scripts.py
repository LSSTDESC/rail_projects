import os
import subprocess
import time
from typing import Any

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


def run_pipeline_on_catalog(
    project: RailProject,
    pipeline_name: str,
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
    kwcopy = kwargs.copy()
    flavor = kwcopy.pop("flavor")
    all_commands = project.make_pipeline_catalog_commands(
        pipeline_name, flavor, **kwcopy
    )

    ok = 0
    for commands, script_path in all_commands:
        try:
            handle_commands(
                run_mode,
                commands,
                script_path,
            )
        except Exception as msg:  # pragma: no cover
            print(msg)
            ok |= 1

    return ok


def run_pipeline_on_single_input(
    project: RailProject,
    pipeline_name: str,
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

    flavor: str
        Flavor of pipeline to run

    run_mode: RunMode
        How to run the command, e.g., dry_run, bash or slurm

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...


    Returns
    -------
    returncode: int
        Status returned by the command.  0 for success, exit code otherwise
    """
    kwcopy = kwargs.copy()
    flavor = kwcopy.pop("flavor")
    sink_dir = project.get_path("ceci_output_dir", flavor=flavor, **kwcopy)
    script_path = os.path.join(sink_dir, f"submit_{pipeline_name}.sh")
    command_line = project.make_pipeline_single_input_command(
        pipeline_name, flavor, **kwcopy
    )

    try:
        statuscode = handle_commands(run_mode, [command_line], script_path)
    except Exception as msg:  # pragma: no cover
        print(msg)
        statuscode = 1
    return statuscode

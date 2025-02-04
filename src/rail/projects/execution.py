""" Functions to execute pipeline and other shell commands """

import enum
import os
import subprocess
import time


class RunMode(enum.Enum):
    """Choose the run mode"""

    dry_run = 0
    bash = 1
    slurm = 2


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
    int:
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
    int:
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
    int
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

"""Functions to execute pipeline and other shell commands"""

import enum
import os
import subprocess
from pathlib import Path
import time
from typing import Any


class RunMode(enum.Enum):
    """Choose the run mode"""

    dry_run = 0
    bash = 1
    slurm = 2


S3DF_SITE_CONFIG: dict[str, Any] = dict(
    slurm_batch_size=8,
    slurm_options=[
        "-p=milano",
        "--account=rubin:commissioning@milano",
        "--mem=16G",
        "--parsable",
    ],
    srun_command="srun",
    sbatch_commands=["sbatch"],
)
PERLMUTTER_SITE_CONFIG: dict[str, Any] = dict(
    slurm_batch_size=32,
    slurm_options=[
        "--account=m1727",
        "--constraint=cpu",
        "--qos=regular",
        "--parsable",
    ],
    srun_command="srun",
    sbatch_commands=["sbatch"],
)
TEST_SITE_CONFIG: dict[str, Any] = dict(
    slurm_batch_size=4,
    slurm_options=[
        "--dummy=test",
    ],
    srun_command="echo 0 srun",
    sbatch_commands=["echo", "0", "sbatch"],
)

DEFAULT_SITE_CONFIGS: dict[str, dict[str, Any]] = dict(
    test=TEST_SITE_CONFIG,
    perlmutter=PERLMUTTER_SITE_CONFIG,
    s3df=S3DF_SITE_CONFIG,
)

BASH_LINE = "#!/usr/bin/bash"


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
    else:  # pragma: no cover
        raise AssertionError(f"Unknown run mode {run_mode}")

    returncode = finished.returncode
    _end_time = time.time()
    _elapsed_time = _end_time - _start_time
    print("<<<<<<<<")
    print(f"subprocess completed with status {returncode} in {_elapsed_time} seconds\n")
    return returncode


def handle_commands(
    run_mode: RunMode,
    command_lines: list[list[str]],
) -> int:  # pragma: no cover
    """Run a multiple commands in the mode requested

    Parameters
    ----------
    run_mode: RunMode
        How to run the command, e.g., dry_run, bash or slurm

    command_lines: list[list[str]]
        List of commands to run, each one is the list of tokens in the command line

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

    raise RuntimeError(
        "handle_commands should only be called with run_mode in [RunMode.dry_run, RunMode.bash]",
    )


def write_run_script(
    command_lines: list[list[str]],
    script_path: Path,
) -> None:  # pragma: no cover
    """Write a script to run multiple commands

    Parameters
    ----------
    command_lines:
        List of commands to run, each one is the list of tokens in the command line

    script_path:
        Path to write the script to
    """
    try:
        os.makedirs(os.path.dirname(script_path))
    except FileExistsError:
        pass

    contents = f"{BASH_LINE}\n\n"
    for command_ in command_lines:
        com_line = " ".join(command_)
        contents += f"{com_line}\n"
    contents += "echo Done!\n"
    script_path.write_text(contents)
    script_path.chmod(0o755)


def write_submit_script(
    scripts_in_batch: list[str],
    batch_submit_script: Path,
    slurm_options: list[str],
    exec_command: str = "srun",
) -> None:  # pragma: no cover
    """Write a script to run multiple commands

    Parameters
    ----------
    scripts_in_batch:
        List of scripts we are going to run

    batch_submit_script:
        Path to write the script to

    slurm_options:
        List of options for slurm

    exec_commands:
        Command used to execute commands
    """
    try:
        os.makedirs(os.path.dirname(batch_submit_script))
    except FileExistsError:
        pass

    contents = f"{BASH_LINE}\n\n"
    for opt_ in slurm_options:
        contents += f"#SBATCH {opt_}\n"

    for script_ in scripts_in_batch:
        contents += f"{exec_command} {script_}\n"

    contents += "echo Done!\n"
    batch_submit_script.write_text(contents)
    batch_submit_script.chmod(0o755)


def submit_slurm_job(
    script_path: Path | str,
    sbatch_commands: list[str],
) -> str:
    """Submit a SLURM job and return the job ID."""
    result = subprocess.run(
        sbatch_commands + [str(script_path)],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode == 0:
        # Parse job ID from output like "Submitted batch job 12345"
        job_id = result.stdout.strip().split()[-1]
        return job_id
    raise RuntimeError(f"SLURM submission failed: {result.stderr}")


def handle_all_commands(
    run_mode: RunMode,
    all_commands: list[tuple[list[list[str]], str]],
    script_path: str | None = None,
    site_config: dict[str, Any] | None = None,
) -> int:  # pragma: no cover
    """Run all the commands in the mode requested

    Parameters
    ----------
    run_mode:
        How to run the command, e.g., dry_run, bash or slurm

    all_commands:
        List of commands and associated place to write scripts

    script_path:
        Path to write the slurm submit script to

    site_config:
        Config for site we are running at

    Returns
    -------
    int:
        Status returned by the commands.  0 for success, exit code otherwise
    """
    if run_mode in [RunMode.dry_run, RunMode.bash]:
        ok = 0
        for commands_, _script_path in all_commands:
            try:
                handle_commands(
                    run_mode,
                    commands_,
                )
            except Exception as msg:  # pragma: no cover
                print(msg)
                ok |= 1

        return ok

    # At this point we are using slurm and need a script to send to batch
    if script_path is None:
        raise ValueError(
            "handle_all_commands with run_mode == RunMode.slurm requires a path to a script to write",
        )

    assert site_config is not None
    return run_batches(all_commands, Path(script_path), site_config)


def run_batches(
    all_commands: list[tuple[list[list[str]], str]],
    script_path: Path,
    site_config: dict[str, Any],
) -> int:  # pragma: no cover
    """Run all the commands in the mode requested

    Parameters
    ----------
    all_commands:
        List of commands lists and associated locations for scripts

    script_path:
        Path to write the slurm submit script to

    site_config:
        Which site we are running at

    Returns
    -------
    int:
        Status returned by the commands.  0 for success, exit code otherwise
    """
    slurm_options = site_config.get("slurm_options", []).copy()
    batch_size = site_config.get("slurm_batch_size", 4)
    srun_command = site_config.get("srun_command", "srun")
    sbatch_commands = site_config.get("sbatch_commands", ["sbatch"])

    job_idx = 0
    start = 0
    stop = len(all_commands)
    status = 0

    while start < stop:
        command_batch = all_commands[start : start + batch_size]

        batch_submit_script = Path(str(script_path).replace(".sh", f"_{job_idx}.sh"))
        batch_log = Path(str(script_path).replace(".sh", f"_{job_idx}.log"))
        batch_err_log = Path(str(script_path).replace(".sh", f"_{job_idx}.err"))
        scripts_in_batch: list[str] = []

        for commands_, script_path_ in command_batch:
            try:
                write_run_script(
                    commands_,
                    Path(script_path_),
                )
                scripts_in_batch.append(script_path_)
            except Exception as msg:  # pragma: no cover
                print(msg)
                status |= 1

        try:
            slurm_options += [
                f"--output={batch_log}",
                f"--error={batch_err_log}",
                f"--ntasks={batch_size}",
            )

            write_submit_script(
                scripts_in_batch,
                batch_submit_script,
                slurm_options,
                srun_command,
            )
            submit_slurm_job(batch_submit_script, sbatch_commands)
        except Exception as msg:  # pragma: no cover
            print(msg)
            status |= 1

        start += batch_size
        job_idx += 1

    return status

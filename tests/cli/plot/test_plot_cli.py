import os
import pytest

from click.testing import CliRunner, Result

from rail.cli.rail_plot.plot_commands import plot_cli


def check_result(
    result: Result,
) -> None:
    if not result.exit_code == 0:
        raise ValueError(f"{result} failed with {result.exit_code} {result.output}")


def test_cli_help() -> None:

    runner = CliRunner()

    result = runner.invoke(plot_cli, "--help")
    check_result(result)


def test_cli_run() -> None:

    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "run --outdir test/temp_dir/plots/ tests/ci_plot_groups.yaml"
    )
    check_result(result)

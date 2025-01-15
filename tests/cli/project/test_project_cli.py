import os
import pytest

from click.testing import CliRunner, Result

from rail.cli.rail_project.project_commands import project_cli


def check_result(
    result: Result,
) -> None:
    if not result.exit_code == 0:
        raise ValueError(f"{result} failed with {result.exit_code} {result.output}")


def test_cli_help() -> None:

    runner = CliRunner()

    result = runner.invoke(project_cli, "--help")
    check_result(result)


def test_cli_inspect() -> None:

    runner = CliRunner()

    result = runner.invoke(project_cli, "inspect tests/ci_project.yaml")
    check_result(result)


def test_cli_build() -> None:

    runner = CliRunner()

    os.system('\\rm -rf tests/temp_data/projects/ci_test/pipelines')
    os.system('\\rm -rf tests/temp_data/projects/ci_test/logs')

    result = runner.invoke(project_cli, "build --flavor all tests/ci_project.yaml")
    check_result(result)


def test_cli_reduce() -> None:

    runner = CliRunner()

    result = runner.invoke(
        project_cli,
        "reduce roman_rubin --selection gold --input_tag truth --run_mode dry_run tests/ci_project.yaml"
    )
    check_result(result)


def test_cli_subsample() -> None:

    runner = CliRunner()

    result = runner.invoke(
        project_cli,
        "subsample --selection gold --flavor baseline --label test --run_mode dry_run tests/ci_project.yaml"
    )
    check_result(result)


@pytest.mark.parametrize(
    "pipeline",
    [
        "blending",
        "estimate",
        "evaluate",
        "inform",
        "phot-errors",
        "pz",
        # "sompz",
        "spec-selection",
        "tomography",
        "truth-to-observed",
    ],
)
def test_cli_run(pipeline: str) -> None:

    runner = CliRunner()

    result = runner.invoke(
        project_cli,
        f"run {pipeline} --selection gold --flavor baseline --run_mode dry_run tests/ci_project.yaml"
    )
    check_result(result)
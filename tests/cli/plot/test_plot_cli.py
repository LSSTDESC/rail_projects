import os
import pytest

from click.testing import CliRunner, Result

from rail.cli.rail_plot.plot_commands import plot_cli

missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


def check_result(
    result: Result,
) -> None:
    if not result.exit_code == 0:
        raise ValueError(f"{result} failed with {result.exit_code} {result.output}")


def test_cli_help() -> None:

    runner = CliRunner()

    result = runner.invoke(plot_cli, "--help")
    check_result(result)


@pytest.mark.skipif(missing_ci_data, reason="NO CI data")
def test_cli_run(setup_project_area: int) -> None:

    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "run --outdir test/temp_dir/plots/ tests/ci_plot_groups.yaml"
    )
    check_result(result)


@pytest.mark.skipif(missing_ci_data, reason="NO CI data")
def test_cli_inspect(setup_project_area: int) -> None:

    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "inspect tests/ci_plot_groups.yaml"
    )
    check_result(result)


@pytest.mark.skipif(missing_ci_data, reason="NO CI data")
def test_cli_extract_datasets(setup_project_area: int) -> None:

    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "extract-datasets "
        "--extractor_class rail.plotting.pz_data_extraction.PZPointEstimateDataExtractor "
        "--flavor all "
        "--selection all "
        "--output_yaml tests/temp_data/dataset_out.yaml "
        "tests/ci_project.yaml"
    )
    check_result(result)

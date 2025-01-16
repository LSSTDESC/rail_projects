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


def test_cli_inspect() -> None:

    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "inspect tests/ci_plot_groups.yaml"
    )
    check_result(result)


def test_cli_extract_datasets() -> None:

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

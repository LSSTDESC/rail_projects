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


def test_cli_run(setup_project_area: int) -> None:
    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(
        plot_cli, "run --outdir test/temp_dir/plots/ tests/ci_plot_groups.yaml"
    )
    check_result(result)


def test_cli_inspect(setup_project_area: int) -> None:
    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(plot_cli, "inspect tests/ci_plot_groups.yaml")
    check_result(result)


def test_cli_extract_nz_datasets(setup_project_area: int) -> None:
    assert setup_project_area == 0
    runner = CliRunner()

    # run with split by algo
    result = runner.invoke(
        plot_cli,
        "extract-datasets "
        "--dataset_holder_class rail.plotting.nz_data_holders.RailNZTomoBinsDataHolder "
        "--flavor all "
        "--split_mode by_algo "
        "--output_yaml tests/temp_data/dataset_nz_out.yaml "
        "tests/ci_project.yaml",
    )
    check_result(result)

    # run with split by flavor
    result = runner.invoke(
        plot_cli,
        "extract-datasets "
        "--dataset_holder_class rail.plotting.nz_data_holders.RailNZTomoBinsDataHolder "
        "--flavor all "
        "--split_mode by_flavor "
        "--output_yaml tests/temp_data/dataset_nz_out.yaml "
        "tests/ci_project.yaml",
    )
    check_result(result)

    # run without split
    result = runner.invoke(
        plot_cli,
        "extract-datasets "
        "--dataset_holder_class rail.plotting.nz_data_holders.RailNZTomoBinsDataHolder "
        "--flavor all "
        "--split_mode no_split "
        "--output_yaml tests/temp_data/dataset_nz_out.yaml "
        "tests/ci_project.yaml",
    )
    check_result(result)


def test_cli_extract_pz_datasets(setup_project_area: int) -> None:
    assert setup_project_area == 0
    runner = CliRunner()

    # run with split by algo
    result = runner.invoke(
        plot_cli,
        "extract-datasets "
        "--dataset_holder_class rail.plotting.pz_data_holders.RailPZPointEstimateDataHolder "
        "--flavor all "
        "--split_mode by_algo "
        "--output_yaml tests/temp_data/dataset_out.yaml "
        "tests/ci_project.yaml",
    )
    check_result(result)

    # run with split by flavor
    result = runner.invoke(
        plot_cli,
        "extract-datasets "
        "--dataset_holder_class rail.plotting.pz_data_holders.RailPZPointEstimateDataHolder "
        "--flavor all "
        "--split_mode by_flavor "
        "--output_yaml tests/temp_data/dataset_out.yaml "
        "tests/ci_project.yaml",
    )
    check_result(result)

    # run without split
    result = runner.invoke(
        plot_cli,
        "extract-datasets "
        "--dataset_holder_class rail.plotting.pz_data_holders.RailPZPointEstimateDataHolder "
        "--flavor all "
        "--split_mode no_split "
        "--output_yaml tests/temp_data/dataset_out.yaml "
        "tests/ci_project.yaml",
    )
    check_result(result)


def test_cli_make_plot_groups(setup_project_area: int) -> None:
    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "make-plot-groups-for-dataset-list "
        "--output_yaml tests/temp_data/check_plot_group.yaml "
        "--plotter_yaml_path tests/ci_plots.yaml "
        "--dataset_yaml_path tests/ci_datasets.yaml "
        "--plotter_list_name zestimate_v_ztrue "
        "--dataset_list_name blend_baseline_all",
    )
    check_result(result)


def test_cli_make_nz_plot_groups(setup_project_area: int) -> None:
    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "make-plot-groups-for-dataset-list "
        "--output_yaml tests/temp_data/check_nz_plot_group.yaml "
        "--plotter_yaml_path tests/ci_plots.yaml "
        "--dataset_yaml_path tests/ci_datasets.yaml "
        "--plotter_list_name tomo_bins "
        "--dataset_list_name blend_baseline_tomo_knn",
    )
    check_result(result)


def test_cli_make_plot_groups_for_project(setup_project_area: int) -> None:
    assert setup_project_area == 0
    runner = CliRunner()

    result = runner.invoke(
        plot_cli,
        "make-plot-groups-for-project "
        "--output_yaml tests/temp_data/check_project_plot_groups_by_algo.yaml "
        "--plotter_yaml_path tests/ci_plots.yaml "
        "--flavor all "
        "--split_mode by_algo "
        "tests/ci_project.yaml",
    )
    check_result(result)

    result = runner.invoke(
        plot_cli,
        "make-plot-groups-for-project "
        "--output_yaml tests/temp_data/check_project_plot_groups_by_flavor.yaml "
        "--plotter_yaml_path tests/ci_plots.yaml "
        "--flavor all "
        "--split_mode by_flavor "
        "tests/ci_project.yaml",
    )
    check_result(result)

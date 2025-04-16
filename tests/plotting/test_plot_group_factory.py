import os

from rail.plotting.dataset_holder import DatasetSplitMode
from rail.plotting.plot_group import RailPlotGroup
from rail.plotting.plot_group_factory import RailPlotGroupFactory


def test_load_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailPlotGroupFactory.clear()
    RailPlotGroupFactory.load_yaml("tests/ci_plot_groups.yaml")
    RailPlotGroupFactory.print_contents()

    # Make sure the names of the plot_groups got loaded
    the_plot_group_names = RailPlotGroupFactory.get_plot_group_names()
    assert "accuracy_v_ztrue" in the_plot_group_names

    # Make sure the plot_groups got loaded
    the_dict = RailPlotGroupFactory.get_plot_groups()
    assert isinstance(the_dict["accuracy_v_ztrue"], RailPlotGroup)

    the_plot_group = RailPlotGroupFactory.get_plot_group("accuracy_v_ztrue")
    assert isinstance(the_plot_group, RailPlotGroup)

    # Test the interactive stuff
    RailPlotGroupFactory.clear()

    RailPlotGroupFactory.add_plot_group(the_plot_group)

    check_plot_group = RailPlotGroupFactory.get_plot_group("accuracy_v_ztrue")
    assert isinstance(check_plot_group, RailPlotGroup)

    # check writing the yaml dict
    RailPlotGroupFactory.write_yaml("tests/temp.yaml")
    RailPlotGroupFactory.clear()
    RailPlotGroupFactory.load_yaml("tests/temp.yaml")
    os.unlink("tests/temp.yaml")

    check_plot_group = RailPlotGroupFactory.get_plot_group("accuracy_v_ztrue")
    assert isinstance(check_plot_group, RailPlotGroup)


def test_make_yaml_for_dataset_list(setup_project_area: int) -> None:
    assert setup_project_area == 0

    RailPlotGroupFactory.make_yaml_for_dataset_list(
        output_yaml="tests/temp_data/check_pz_plot_groups.yaml",
        plotter_yaml_path="tests/ci_plots.yaml",
        dataset_yaml_path="tests/ci_datasets.yaml",
        plotter_list_name="zestimate_v_ztrue",
        output_prefix="",
        dataset_list_name="baseline_test",
    )
    RailPlotGroupFactory.make_yaml_for_dataset_list(
        output_yaml="tests/temp_data/check_nz_plot_groups.yaml",
        plotter_yaml_path="tests/ci_plots.yaml",
        dataset_yaml_path="tests/ci_datasets.yaml",
        plotter_list_name="tomo_bins",
        output_prefix="",
        dataset_list_name="blend_baseline_tomo_knn",
    )


def test_make_yaml_for_project(setup_project_area: int) -> None:
    assert setup_project_area == 0

    RailPlotGroupFactory.make_yaml_for_project(
        output_yaml="tests/temp_data/check_project_plot_groups_by_algo.yaml",
        plotter_yaml_path="tests/ci_plots.yaml",
        project_yaml_path="tests/ci_project.yaml",
        flavors=["all"],
        split_mode=DatasetSplitMode.by_algo,
    )
    RailPlotGroupFactory.make_yaml_for_project(
        output_yaml="tests/temp_data/check_project_plot_groups_by_flavor.yaml",
        plotter_yaml_path="tests/ci_plots.yaml",
        project_yaml_path="tests/ci_project.yaml",
        flavors=["all"],
        split_mode=DatasetSplitMode.by_flavor,
    )

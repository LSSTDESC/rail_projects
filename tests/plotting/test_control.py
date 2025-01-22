import os

import pytest

from rail.plotting import control
from rail.plotting.plotter import RailPlotter
from rail.plotting.plot_group import RailPlotGroup
from rail.plotting.dataset_holder import RailDatasetHolder


missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
def test_load_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0
    control.clear()

    # Load the testing yaml file
    plot_groups = control.load_plot_group_yaml("tests/ci_plot_groups.yaml")

    assert isinstance(
        plot_groups["zestimate_v_ztrue_test_plots"], control.RailPlotGroup
    )
    control.print_contents()
    control.print_classes()

    an_extractor = control.get_extractor_class("PZPointEstimateDataExtractor")
    assert an_extractor


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
def test_run(setup_project_area: int) -> None:
    assert setup_project_area == 0
    control.clear()

    _out_dict = control.run("tests/ci_plot_groups.yaml", outdir="tests/temp_data/plots")
    check_path = os.path.join(
        "tests",
        "temp_data",
        "plots",
        "blend_baseline_fzboost",
        "zestimate_v_ztrue_profile.png",
    )

    plot_group = control.get_plot_group("accuracy_v_ztrue")
    assert isinstance(plot_group, RailPlotGroup)

    assert plot_group.find_plot_path("blend_baseline_all", "accuracy_v_ztrue")
    with pytest.raises(KeyError):
        plot_group.find_plot_path("bad", "bad")
    with pytest.raises(KeyError):
        plot_group.find_plot_path("blend_baseline_all", "bad")

    assert os.path.exists(check_path)

    control.clear()
    out_dict2 = control.run(
        "tests/ci_plot_groups.yaml",
        outdir="tests/temp_data/plots",
        find_only=True,
        make_html=True,
    )

    assert out_dict2
    assert os.path.exists(
        "tests/temp_data/plots/plots_accuracy_v_ztrue.html",
    )

    plot_dict = out_dict2["blend_baseline_all"]
    assert plot_dict.name == "blend_baseline_all"

    plot_holder = plot_dict.plots["accuracy_v_ztrue"]
    assert plot_holder

    assert isinstance(plot_holder.plotter, RailPlotter)
    assert isinstance(plot_holder.dataset_holder, RailDatasetHolder)

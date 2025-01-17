import os

import pytest

from rail.plotting import control


missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
def test_load_yaml(setup_project_area: int) -> None:

    assert setup_project_area == 0
    control.clear()

    # Load the testing yaml file
    plot_groups = control.load_plot_group_yaml('tests/ci_plot_groups.yaml')

    assert isinstance(plot_groups['zestimate_v_ztrue_test_plots'], control.RailPlotGroup)
    control.print_contents()


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
def test_run(setup_project_area: int) -> None:

    assert setup_project_area == 0
    control.clear()

    _out_dict = control.run('tests/ci_plot_groups.yaml', outdir='tests/temp_data/plots')
    assert os.path.exists(
        'tests/temp_data/plots/zestimate_v_ztrue_hist2d_blend_baseline_test_fzboost_hist.png'
    )

    control.clear()
    out_dict2 = control.run(
        'tests/ci_plot_groups.yaml',
        outdir='tests/temp_data/plots',
        find_only=True,
        make_html=True,
        output_html='tests/temp_data/plots/plot.html',
    )

    assert out_dict2

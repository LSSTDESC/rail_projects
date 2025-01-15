from rail.plotting import control


def test_load_yaml() -> None:

    # Load the testing yaml file
    plot_groups = control.load_plot_group_yaml('tests/ci_plot_groups.yaml')

    assert isinstance(plot_groups['zestimate_v_ztrue_test_plots'], control.RailPlotGroup)
    control.print_contents()


def test_run() -> None:

    _out_dict = control.run('tests/ci_plot_groups.yaml', outdir='tests/temp_data/plots')

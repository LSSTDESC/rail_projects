import os

from rail.plotting import control


def setup_project_area() -> None:

    if not os.path.exists('tests/temp_data/projects/ci_test/data'):
        os.makedirs('tests/temp_data/projects/ci_test/data')
        # FIXME, replace with a curl command
        os.system('cp ~/xfer/ci_test.tgz tests/temp_data/projects')
        os.system('tar zxvf tests/temp_data/projects/ci_test.tgz -C tests/temp_data/projects')

    if not os.path.exists('tests/temp_data/data/test'):
        os.makedirs('tests/temp_data/data/test')
        # FIXME, replace with a curl command
        os.system(
            'cp ~/xfer/roman_rubin_2023_maglim_25.5_baseline_100k.hdf5 '
            'tests/temp_data/data/test/ci_test_blend_baseline_100k.hdf5'
        )


def test_load_yaml() -> None:

    control.clear()
    setup_project_area()

    # Load the testing yaml file
    plot_groups = control.load_plot_group_yaml('tests/ci_plot_groups.yaml')

    assert isinstance(plot_groups['zestimate_v_ztrue_test_plots'], control.RailPlotGroup)
    control.print_contents()


def test_run() -> None:

    control.clear()
    setup_project_area()

    _out_dict = control.run('tests/ci_plot_groups.yaml', outdir='tests/temp_data/plots')
    assert os.path.exists(
        'tests/temp_data/plots/zestimate_v_ztrue_hist2d_blend_baseline_test_fzboost_hist.png'
    )

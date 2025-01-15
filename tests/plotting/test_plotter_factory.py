import pytest

from rail.plotting.plotter import RailPlotter
from rail.plotting.plotter_factory import RailPlotterFactory
from rail.plotting import pz_plotters


def test_load_yaml() -> None:

    # Load the testing yaml file
    RailPlotterFactory.clear()
    RailPlotterFactory.load_yaml('tests/ci_plots.yaml')
    RailPlotterFactory.print_contents()
    RailPlotter.print_classes()

    # Make sure the names of the plotters got loaded
    the_names = RailPlotterFactory.get_plotter_names()
    assert 'zestimate_v_ztrue_hist2d' in the_names

    # Make sure the plotters got loaded
    the_dict = RailPlotterFactory.get_plotter_dict()
    assert isinstance(the_dict['zestimate_v_ztrue_hist2d'], pz_plotters.PZPlotterPointEstimateVsTrueHist2D)

    # Make sure the names of the plotter lists got loaded
    plotter_list_names = RailPlotterFactory.get_plotter_list_names()
    assert 'zestimate_v_ztrue' in plotter_list_names

    # Make sure the plotter lists got loaded
    list_dict = RailPlotterFactory.get_plotter_list_dict()
    assert len(list_dict['zestimate_v_ztrue']) == 2

    # Get a plotter by name
    a_plotter = RailPlotterFactory.get_plotter('zestimate_v_ztrue_hist2d')
    assert isinstance(a_plotter, pz_plotters.PZPlotterPointEstimateVsTrueHist2D)

    with pytest.raises(KeyError):
        RailPlotterFactory.get_plotter('bad')

    # Get a plotter list by name
    a_plotter_list = RailPlotterFactory.get_plotter_list('zestimate_v_ztrue')
    assert isinstance(a_plotter_list[0], pz_plotters.PZPlotterPointEstimateVsTrueHist2D)

    with pytest.raises(KeyError):
        RailPlotterFactory.get_plotter_list('bad')

    with pytest.raises(KeyError):
        RailPlotter.get_plotter_class('bad')

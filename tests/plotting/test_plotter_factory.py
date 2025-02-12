import os

import pytest

from rail.plotting import pz_plotters
from rail.plotting.plotter import RailPlotter, RailPlotterList
from rail.plotting.plotter_factory import RailPlotterFactory


def test_load_yaml() -> None:
    # Load the testing yaml file
    RailPlotterFactory.clear()
    RailPlotterFactory.load_yaml("tests/ci_plots.yaml")

    # test the mechanism to avoid repeat loading
    RailPlotterFactory.load_yaml("tests/ci_plots.yaml")
    RailPlotterFactory.print_contents()
    RailPlotter.print_classes()

    # Make sure the names of the plotters got loaded
    the_names = RailPlotterFactory.get_plotter_names()
    assert "zestimate_v_ztrue_hist2d" in the_names

    # Make sure the plotters got loaded
    the_dict = RailPlotterFactory.get_plotters()
    assert isinstance(
        the_dict["zestimate_v_ztrue_hist2d"],
        pz_plotters.PZPlotterPointEstimateVsTrueHist2D,
    )

    # Make sure the names of the plotter lists got loaded
    plotter_list_names = RailPlotterFactory.get_plotter_list_names()
    assert "zestimate_v_ztrue" in plotter_list_names

    # Make sure the plotter lists got loaded
    list_dict = RailPlotterFactory.get_plotter_lists()
    assert len(list_dict["zestimate_v_ztrue"].config.plotters) == 2

    # Get a plotter by name
    a_plotter = RailPlotterFactory.get_plotter("zestimate_v_ztrue_hist2d")
    assert isinstance(a_plotter, pz_plotters.PZPlotterPointEstimateVsTrueHist2D)

    with pytest.raises(KeyError):
        RailPlotterFactory.get_plotter("bad")

    # Get a plotter list by name
    a_plotter_list = RailPlotterFactory.get_plotter_list("zestimate_v_ztrue")
    plotters = a_plotter_list.resolve(RailPlotterFactory.instance())
    assert isinstance(plotters[0], pz_plotters.PZPlotterPointEstimateVsTrueHist2D)

    with pytest.raises(KeyError):
        RailPlotterFactory.get_plotter_list("bad")

    with pytest.raises(KeyError):
        RailPlotter.get_sub_class("bad")

    # Test the interactive stuff
    RailPlotterFactory.clear()

    RailPlotterFactory.add_plotter(a_plotter)
    RailPlotterFactory.add_plotter_list(
        RailPlotterList(
            name="test_list",
            dataset_holder_class="rail.plotting.pz_data_holders.RailPZPointEstimateDataHolder",
            plotters=[a_plotter.config.name],
        )
    )

    check_plotter = RailPlotterFactory.get_plotter("zestimate_v_ztrue_hist2d")
    assert isinstance(check_plotter, type(a_plotter))

    check_list = RailPlotterFactory.get_plotter_list("test_list")
    assert a_plotter.config.name in check_list.config.plotters

    # check writing the yaml dict
    RailPlotterFactory.write_yaml("tests/temp.yaml")
    RailPlotterFactory.clear()
    RailPlotterFactory.load_yaml("tests/temp.yaml")
    os.unlink("tests/temp.yaml")

    check_plotter = RailPlotterFactory.get_plotter("zestimate_v_ztrue_hist2d")
    assert isinstance(check_plotter, type(a_plotter))

    check_list = RailPlotterFactory.get_plotter_list("test_list")
    assert a_plotter.config.name in check_list.config.plotters

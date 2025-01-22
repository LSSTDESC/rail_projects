import os
import pytest

from rail.plotting.plot_group import RailPlotGroup
from rail.plotting.plot_group_factory import RailPlotGroupFactory

missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
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

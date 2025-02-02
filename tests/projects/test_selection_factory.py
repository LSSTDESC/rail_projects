import os

import pytest

from rail.projects.selection_factory import RailSelection, RailSelectionFactory


def test_load_selection_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailSelectionFactory.clear()

    RailSelectionFactory.load_yaml("tests/ci_selections.yaml")
    RailSelectionFactory.print_contents()

    # Make sure the names of the selections got loaded
    the_selection_names = RailSelectionFactory.get_selection_names()
    assert "gold" in the_selection_names

    # Make sure the selections got loaded
    the_dict = RailSelectionFactory.get_selections()
    assert isinstance(the_dict["gold"], RailSelection)

    # get a specfic selections
    the_selection = RailSelectionFactory.get_selection("gold")
    assert isinstance(the_selection, RailSelection)

    with pytest.raises(KeyError):
        RailSelectionFactory.get_selection("bad")

    # Test the interactive stuff
    RailSelectionFactory.clear()

    RailSelectionFactory.add_selection(the_selection)

    check_selection = RailSelectionFactory.get_selection("gold")
    assert isinstance(check_selection, type(the_selection))

    # check writing the yaml dict
    RailSelectionFactory.write_yaml("tests/temp.yaml")
    RailSelectionFactory.clear()
    RailSelectionFactory.load_yaml("tests/temp.yaml")
    os.unlink("tests/temp.yaml")

    check_selection = RailSelectionFactory.get_selection("gold")
    assert isinstance(check_selection, type(the_selection))

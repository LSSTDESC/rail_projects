import pytest

from rail.projects.selection_factory import RailSelectionFactory, RailSelection


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

    RailSelectionFactory.clear()

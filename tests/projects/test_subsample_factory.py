import pytest

from rail.projects.subsample_factory import RailSubsampleFactory, RailSubsample


def test_load_subsample_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailSubsampleFactory.clear()

    RailSubsampleFactory.load_yaml("tests/ci_subsamples.yaml")
    RailSubsampleFactory.print_contents()

    # Make sure the names of the subsamples got loaded
    the_subsample_names = RailSubsampleFactory.get_subsample_names()
    assert "test_100k" in the_subsample_names

    # Make sure the subsamples got loaded
    the_dict = RailSubsampleFactory.get_subsamples()
    assert isinstance(the_dict["test_100k"], RailSubsample)

    # get a specfic subsamples
    the_subsample = RailSubsampleFactory.get_subsample("test_100k")
    assert isinstance(the_subsample, RailSubsample)

    with pytest.raises(KeyError):
        RailSubsampleFactory.get_subsample("bad")

    RailSubsampleFactory.clear()

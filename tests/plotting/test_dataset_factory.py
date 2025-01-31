import os
import pytest

from rail.plotting.dataset_factory import RailDatasetFactory
from rail.plotting.data_extractor import RailProjectDataExtractor
from rail.plotting.dataset_holder import RailDatasetHolder
from rail.projects import RailProject


def test_load_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailDatasetFactory.clear()
    RailDatasetFactory.load_yaml("tests/ci_datasets.yaml")
    RailDatasetFactory.print_contents()
    RailProjectDataExtractor.print_classes()
    RailDatasetHolder.print_classes()

    with pytest.raises(KeyError):
        RailDatasetHolder.get_sub_class("bad")

    # Make sure the names of the projects got loaded
    the_project_names = RailDatasetFactory.get_project_names()
    assert "ci_test" in the_project_names

    # Make sure the projects got loaded
    the_dict = RailDatasetFactory.get_projects()
    assert isinstance(the_dict["ci_test"], RailProject)

    # Make sure the names of the datasets got loaded
    the_dataset_names = RailDatasetFactory.get_dataset_names()
    assert "blend_baseline_knn" in the_dataset_names

    # Make sure the datasets got loaded
    the_datasets = RailDatasetFactory.get_datasets()
    assert isinstance(the_datasets["blend_baseline_knn"], RailDatasetHolder)

    # get a specfic dataset
    the_dataset = RailDatasetFactory.get_dataset("blend_baseline_knn")
    assert isinstance(the_dataset, RailDatasetHolder)

    with pytest.raises(KeyError):
        RailDatasetFactory.get_dataset("bad")

    # get a specfic dataset dict
    the_dataset_dict = RailDatasetFactory.get_dataset_dict("baseline_test")
    assert isinstance(the_dataset_dict, dict)

    with pytest.raises(KeyError):
        RailDatasetFactory.get_dataset_dict("bad")

    # Make sure the names of the datasets lists got loaded
    the_dataset_list_names = RailDatasetFactory.get_dataset_dict_names()
    assert "baseline_test" in the_dataset_list_names

    # Make sure the  datasets lists got loaded
    the_dataset_lists = RailDatasetFactory.get_dataset_dicts()
    assert isinstance(the_dataset_lists["baseline_test"], dict)

import os

import pytest

from rail.plotting.dataset_factory import RailDatasetFactory
from rail.plotting.dataset_holder import (
    RailDatasetHolder,
    RailDatasetListHolder,
    RailProjectHolder,
)


def test_load_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailDatasetFactory.clear()
    RailDatasetFactory.load_yaml("tests/ci_datasets.yaml")
    RailDatasetFactory.print_contents()
    RailDatasetHolder.print_classes()

    with pytest.raises(KeyError):
        RailDatasetHolder.get_sub_class("bad")

    # Make sure the names of the projects got loaded
    the_project_names = RailDatasetFactory.get_project_names()
    assert "ci_test" in the_project_names

    # Make sure the projects got loaded
    the_dict = RailDatasetFactory.get_projects()
    assert isinstance(the_dict["ci_test"], RailProjectHolder)

    a_project_holder = RailDatasetFactory.get_project("ci_test")
    assert isinstance(a_project_holder, RailProjectHolder)

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
    the_dataset_list = RailDatasetFactory.get_dataset_list("baseline_test")
    assert isinstance(the_dataset_list, RailDatasetListHolder)

    with pytest.raises(KeyError):
        RailDatasetFactory.get_dataset_list("bad")

    # Make sure the names of the datasets lists got loaded
    the_dataset_list_names = RailDatasetFactory.get_dataset_list_names()
    assert "baseline_test" in the_dataset_list_names

    # Make sure the  datasets lists got loaded
    the_dataset_lists = RailDatasetFactory.get_dataset_lists()
    assert isinstance(the_dataset_lists["baseline_test"], RailDatasetListHolder)

    # Test the interactive stuff
    RailDatasetFactory.clear()

    RailDatasetFactory.add_project(a_project_holder)
    check_project_holder = RailDatasetFactory.get_project("ci_test")
    assert isinstance(check_project_holder, RailProjectHolder)

    RailDatasetFactory.add_dataset(the_dataset)
    RailDatasetFactory.add_dataset_list(
        RailDatasetListHolder(
            name="test_list",
            dataset_class=the_dataset.output_type.full_class_name(),
            datasets=[the_dataset.config.name],
        )
    )

    check_dataset = RailDatasetFactory.get_dataset("blend_baseline_knn")
    assert isinstance(check_dataset, type(the_dataset))

    check_list = RailDatasetFactory.get_dataset_list("test_list")
    assert the_dataset.config.name in check_list.config.datasets

    # check writing the yaml dict
    RailDatasetFactory.write_yaml("tests/temp.yaml")
    RailDatasetFactory.clear()
    RailDatasetFactory.load_yaml("tests/temp.yaml")
    os.unlink("tests/temp.yaml")

    check_dataset = RailDatasetFactory.get_dataset("blend_baseline_knn")
    assert isinstance(check_dataset, type(the_dataset))

    check_list = RailDatasetFactory.get_dataset_list("test_list")
    assert the_dataset.config.name in check_list.config.datasets

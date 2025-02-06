import os

import pytest

from rail.projects.file_template import RailProjectFileInstance, RailProjectFileTemplate
from rail.projects.project_file_factory import RailProjectFileFactory


def test_load_file_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailProjectFileFactory.clear()

    RailProjectFileFactory.load_yaml("tests/ci_project_files.yaml")
    RailProjectFileFactory.print_contents()

    # Make sure the names of the file_templates got loaded
    the_file_templates_name = RailProjectFileFactory.get_file_template_names()
    assert "test_file_100k" in the_file_templates_name

    # Make sure the file_templates got loaded
    the_dict = RailProjectFileFactory.get_file_templates()
    assert isinstance(the_dict["test_file_100k"], RailProjectFileTemplate)

    # Make sure the names of the file_instances got loaded
    the_file_instance_name = RailProjectFileFactory.get_file_instance_names()
    assert "test_file_baseline_100k_ci_test_blend" in the_file_instance_name

    # Make sure the file_instances got loaded
    the_file_instances = RailProjectFileFactory.get_file_instances()
    assert isinstance(
        the_file_instances["test_file_baseline_100k_ci_test_blend"],
        RailProjectFileInstance,
    )

    # get a specfic file_templates
    the_file_template = RailProjectFileFactory.get_file_template("test_file_100k")
    assert isinstance(the_file_template, RailProjectFileTemplate)

    # make a file_instance
    check_file_instance = the_file_template.make_file_instance(
        name="dummy",
        catalogs_dir="tests/temp_data/data",
        project="ci_test",
        selection="blend",
    )
    assert isinstance(check_file_instance, RailProjectFileInstance)
    the_path = check_file_instance.resolve()
    assert os.path.exists(the_path)
    assert check_file_instance.check_file()
    assert check_file_instance.check_file(update=True)
    assert check_file_instance.check_file()

    with pytest.raises(KeyError):
        RailProjectFileFactory.get_file_template("bad")

    # get a specfic file_instance
    the_file_instance = RailProjectFileFactory.get_file_instance(
        "test_file_baseline_100k_ci_test_blend"
    )
    assert isinstance(the_file_instance, RailProjectFileInstance)

    with pytest.raises(KeyError):
        RailProjectFileFactory.get_file_instance("bad")

    # Test the interactive stuff
    RailProjectFileFactory.clear()
    RailProjectFileFactory.add_file_template(the_file_template)
    RailProjectFileFactory.add_file_instance(the_file_instance)

    check_file_instance = RailProjectFileFactory.get_file_instance(
        "test_file_baseline_100k_ci_test_blend"
    )
    assert isinstance(check_file_instance, type(the_file_instance))

    # check writing the yaml dict
    RailProjectFileFactory.write_yaml("tests/temp.yaml")
    RailProjectFileFactory.clear()
    RailProjectFileFactory.load_yaml("tests/temp.yaml")
    os.unlink("tests/temp.yaml")

    check_file_instance = RailProjectFileFactory.get_file_instance(
        "test_file_baseline_100k_ci_test_blend"
    )
    assert isinstance(check_file_instance, type(check_file_instance))

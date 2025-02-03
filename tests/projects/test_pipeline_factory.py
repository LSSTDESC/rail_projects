import os

import pytest

from rail.projects.pipeline_factory import RailPipelineFactory
from rail.projects.pipeline_holder import RailPipelineInstance, RailPipelineTemplate


def test_load_pipeline_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailPipelineFactory.clear()

    RailPipelineFactory.load_yaml("tests/ci_pipelines.yaml")
    RailPipelineFactory.print_contents()

    # Make sure the names of the pipeline_templates got loaded
    the_pipeline_templates_name = RailPipelineFactory.get_pipeline_template_names()
    assert "truth_to_observed" in the_pipeline_templates_name

    # Make sure the pipeline_templates got loaded
    the_dict = RailPipelineFactory.get_pipeline_templates()
    assert isinstance(the_dict["truth_to_observed"], RailPipelineTemplate)

    # Make sure the names of the pipeline_instances got loaded
    the_pipeline_instance_name = RailPipelineFactory.get_pipeline_instance_names()
    assert "tomography_baseline" in the_pipeline_instance_name

    # Make sure the pipeline_instances got loaded
    the_pipeline_instances = RailPipelineFactory.get_pipeline_instances()
    assert isinstance(
        the_pipeline_instances["tomography_baseline"], RailPipelineInstance
    )

    # get a specfic pipeline_templates
    the_pipeline_template = RailPipelineFactory.get_pipeline_template(
        "truth_to_observed"
    )
    assert isinstance(the_pipeline_template, RailPipelineTemplate)

    with pytest.raises(KeyError):
        RailPipelineFactory.get_pipeline_template("bad")

    # get a specfic pipeline_instance
    the_pipeline_instance = RailPipelineFactory.get_pipeline_instance(
        "tomography_baseline"
    )
    assert isinstance(the_pipeline_instance, RailPipelineInstance)

    with pytest.raises(KeyError):
        RailPipelineFactory.get_pipeline_instance("bad")

    # Test the interactive stuff
    RailPipelineFactory.clear()
    RailPipelineFactory.add_pipeline_template(the_pipeline_template)
    RailPipelineFactory.add_pipeline_instance(the_pipeline_instance)

    check_pipeline_instance = RailPipelineFactory.get_pipeline_instance(
        "tomography_baseline"
    )
    assert isinstance(check_pipeline_instance, type(the_pipeline_instance))

    check_pipeline_template = RailPipelineFactory.get_pipeline_template(
        "truth_to_observed"
    )
    assert isinstance(check_pipeline_template, type(the_pipeline_template))

    # check writing the yaml dict
    RailPipelineFactory.write_yaml("tests/temp.yaml")
    RailPipelineFactory.clear()
    RailPipelineFactory.load_yaml("tests/temp.yaml")
    os.unlink("tests/temp.yaml")

    check_pipeline_template = RailPipelineFactory.get_pipeline_template(
        "truth_to_observed"
    )
    assert isinstance(check_pipeline_template, type(the_pipeline_template))

import os
from typing import Any, Callable

import pytest

from rail.projects.project import RailProject


def check_get_func(func: Callable, check_dict: dict[str, Any]) -> None:
    for key, val in check_dict.items():
        check_val = func(key)
        if isinstance(check_val, dict):
            for kk, vv in check_val.items():
                assert vv == val[kk]
    with pytest.raises(KeyError):
        func("does_not_exist")


def test_project_doc() -> None:
    RailProject.functionality_help()
    RailProject.configuration_help()


def test_project_class(setup_project_area: int) -> None:
    assert setup_project_area == 0

    project = RailProject.load_config("tests/ci_project.yaml")

    print(project)

    templates = project.get_path_templates()
    check_get_func(project.get_path, templates)

    common_paths = project.get_common_paths()
    check_get_func(project.get_common_path, common_paths)

    files = project.get_files()
    check_get_func(project.get_file, files)

    flavors = project.get_flavors()
    check_get_func(project.get_flavor, flavors)
    all_flavors = project.get_flavor_args(["all"])
    assert set(all_flavors) == set(flavors.keys())
    assert project.get_flavor_args(["dummy"])[0] == "dummy"

    project.get_file_for_flavor("baseline", "test")
    with pytest.raises(KeyError):
        project.get_file_for_flavor("baseline", "does not exist")

    project.get_file_metadata_for_flavor("baseline", "test")
    with pytest.raises(KeyError):
        project.get_file_metadata_for_flavor("baseline", "does not exist")

    selections = project.get_selections()
    check_get_func(project.get_selection, selections)
    project.clear_cache()
    check_get_func(project.get_selection, selections)
    all_selections = project.get_selection_args(["all"])
    assert set(all_selections) == set(selections.keys())
    assert project.get_selection_args(["dummy"])[0] == "dummy"

    subsamples = project.get_subsamples()
    check_get_func(project.get_subsample, subsamples)

    itr = project.generate_kwargs_iterable(
        selections=all_selections,
        flavors=all_flavors,
    )
    for x_ in itr:
        assert isinstance(x_, dict)

    error_models = project.get_error_models()
    check_get_func(project.get_error_model, error_models)

    pz_algos = project.get_pzalgorithms()
    check_get_func(project.get_pzalgorithm, pz_algos)

    spec_selections = project.get_spec_selections()
    check_get_func(project.get_spec_selection, spec_selections)

    classifiers = project.get_classifiers()
    check_get_func(project.get_classifier, classifiers)

    summarizers = project.get_summarizers()
    check_get_func(project.get_summarizer, summarizers)

    catalogs = project.get_catalogs()
    check_get_func(project.get_catalog, catalogs)

    pipelines = project.get_pipelines()
    check_get_func(project.get_pipeline, pipelines)

    _ceci_command = project.generate_ceci_command(
        pipeline_path="dummy.yaml",
        config=None,
        inputs={"bob": "bob.pkl"},
        output_dir=".",
        log_dir=".",
        alice="bob",
    )

    project.build_pipelines(flavor="baseline")

    catalog_files_truth = project.get_catalog_files("truth")
    check_path = "tests/temp_data/data/ci_test_v1.1.3/9925/part-0.parquet"
    assert check_path in catalog_files_truth

    catalog_files_reduced = project.get_catalog_files("reduced", selection="gold")
    check_path = "tests/temp_data/data/ci_test_v1.1.3_gold/9925/part-0.pq"
    assert check_path in catalog_files_reduced

    catalog_files_degraded = project.get_catalog_files(
        "degraded", selection="gold", flavor="baseline", basename="output.hdf5"
    )
    check_path = "tests/temp_data/data/ci_test_v1.1.3_gold_baseline/9925/output.hdf5"
    assert check_path in catalog_files_degraded

    project.reduce_data(
        catalog_template="truth",
        output_catalog_template="reduced",
        reducer_class_name="roman_rubin",
        input_selection="",
        selection="gold",
    )

    project.subsample_data(
        catalog_template="reduced",
        file_template="train_file_10",
        subsampler_class_name="random_subsampler",
        subsample_name="train_10",
        flavor="baseline",
        selection="gold",
    )

    single_ceci_command = project.make_pipeline_single_input_command(
        pipeline_name="pz",
        flavor="basline",
        selection="gold",
    )
    assert single_ceci_command

    ceci_catalog_commands = project.make_pipeline_catalog_commands(
        pipeline_name="spec_selection",
        flavor="basline",
        selection="gold",
        spec_selections=list(project.get_spec_selections().keys()),
    )
    assert ceci_catalog_commands

    project.write_yaml("tests/temp.yaml")

    RailProject.projects.clear()

    project = RailProject.load_config("tests/temp.yaml")
    os.unlink("tests/temp.yaml")
    project.get_file_for_flavor("baseline", "test")

    project.add_flavor(
        name="test_flavor",
        catalog_tag="roman_rubin",
        pipelines=["pz"],
    )
    with pytest.raises(KeyError):
        project.add_flavor(
            name="test_flavor",
            catalog_tag="roman_rubin",
            pipelines=["pz"],
        )

    flavor_info = project.get_flavor("test_flavor")
    assert flavor_info

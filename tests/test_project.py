from typing import Any, Callable

import pytest
from rail.projects import RailProject


def check_get_func(func: Callable, check_dict: dict[str, Any]) -> None:
    for key, val in check_dict.items():
        check_val = func(key)
        if isinstance(check_val, dict):
            for kk, vv in check_val.items():
                assert vv == val[kk]
    with pytest.raises(KeyError):
        func("does_not_exist")


def test_project() -> None:
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
    all_selections = project.get_selection_args(["all"])
    assert set(all_selections) == set(selections.keys())
    assert project.get_selection_args(["dummy"])[0] == "dummy"

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

    nz_algos = project.get_nzalgorithms()
    check_get_func(project.get_nzalgorithm, nz_algos)

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

import os
from typing import Any, Callable

import pytest

from rail.projects.project import RailProject


def test_subsampler_multi_cat(setup_project_area: int) -> None:
    assert setup_project_area == 0

    project = RailProject.load_config("tests/ci_subsample.yaml")

    project.subsample_data(
        catalog_template="degraded",
        file_template="multi_cat_1k",
        subsampler_class_name="multi_catalog_subsampler",
        subsample_name="multi_cat",
        flavor="baseline",
        selection="gold",
    )

    
def test_subsampler_spec_area(setup_project_area: int) -> None:
    assert setup_project_area == 0

    project = RailProject.load_config("tests/ci_subsample.yaml")
    
    project.subsample_data(
        catalog_template="degraded",
        file_template="spec_area_1k",
        subsampler_class_name="spec_area_subsampler",
        subsample_name="spec_area",
        flavor="baseline",
        selection="gold",
    )


    

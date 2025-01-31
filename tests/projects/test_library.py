import os
import pytest

from rail.projects import library


def test_libray_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    library.clear()

    library.load_yaml("tests/ci_project_common.yaml")
    library.print_contents()
    library.clear()

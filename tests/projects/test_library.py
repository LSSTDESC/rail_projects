import os
import pytest

from rail.projects import library

missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
def test_libray_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    library.clear()

    library.load_yaml("tests/ci_project_common.yaml")
    library.print_contents()
    library.clear()

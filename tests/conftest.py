import pytest

from rail.projects import library


@pytest.fixture(name="setup_project_area", scope="package")
def setup_project_area(request: pytest.FixtureRequest) -> int:
    ret_val = library.setup_project_area()

    request.addfinalizer(library.teardown_project_area)

    return ret_val

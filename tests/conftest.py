import os

import pytest


@pytest.fixture(name="setup_project_area", scope="session")
def setup_project_area(request: pytest.FixtureRequest) -> int:
    if not os.path.exists("tests/temp_data"):
        os.system("\\rm -f ci_test.tgz")
        os.system("wget http://s3df.slac.stanford.edu/people/echarles/xfer/ci_test.tgz -o /dev/null")
        os.system("tar zxvf ci_test.tgz -C tests")

    if not os.path.exists("tests/temp_data/data/test/ci_test_blend_baseline_100k.hdf5"):
        os.makedirs("tests/temp_data/data/test")
        os.system(
            "wget http://s3df.slac.stanford.edu/people/echarles/xfer/roman_rubin_2023_maglim_25.5_baseline_100k.hdf5 -o /dev/null"
        )
        os.makedirs('tests/temp_data/data/test', exist_ok=True)
        os.system(
            "mv roman_rubin_2023_maglim_25.5_baseline_100k.hdf5 tests/temp_data/data/test/ci_test_blend_baseline_100k.hdf5"
        )
    
    def teardown_project_area() -> None:
        if not os.environ.get("NO_TEARDOWN"):
            os.system("\\rm -rf tests/temp_data")
            os.system("\\rm -f ci_test.tgz")

    request.addfinalizer(teardown_project_area)

    return 0

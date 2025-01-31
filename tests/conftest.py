import os

import urllib.request
import subprocess

import pytest


@pytest.fixture(name="setup_project_area", scope="package")
def setup_project_area(request: pytest.FixtureRequest) -> int:
    if not os.path.exists("tests/temp_data"):
        try:
            os.unlink("tests/ci_test.tgz")
        except FileNotFoundError:
            pass
        urllib.request.urlretrieve(
            "http://s3df.slac.stanford.edu/people/echarles/xfer/ci_test.tgz",
            "tests/ci_test.tgz",
        )
        if not os.path.exists("tests/ci_test.tgz"):
            return 1

        status = subprocess.run(["tar", "zxvf", "tests/ci_test.tgz", "-C", "tests"], check=False)
        if status.returncode != 0:
            return status.returncode

    if not os.path.exists("tests/temp_data/data/ci_test_v1.1.3/9924/part-0.parquet"):
        return 2

    if not os.path.exists("tests/temp_data/data/test/ci_test_blend_baseline_100k.hdf5"):
        os.makedirs("tests/temp_data/data/test", exist_ok=True)
        urllib.request.urlretrieve(
            "http://s3df.slac.stanford.edu/people/echarles/xfer/"
            "roman_rubin_2023_maglim_25.5_baseline_100k.hdf5",
            "tests/temp_data/data/test/ci_test_blend_baseline_100k.hdf5",
        )
        if not os.path.exists(
            "tests/temp_data/data/test/ci_test_blend_baseline_100k.hdf5"
        ):
            return 3

    def teardown_project_area() -> None:
        if not os.environ.get("NO_TEARDOWN"):
            os.system("\\rm -rf tests/temp_data")
            try:
                os.unlink("tests/ci_test.tgz")
            except FileNotFoundError:
                pass

    request.addfinalizer(teardown_project_area)

    return 0

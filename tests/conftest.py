import os

import pytest

missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


@pytest.fixture(name="setup_project_area", scope="session")
def setup_project_area(request: pytest.FixtureRequest) -> int:
    
    if not os.path.exists('tests/temp_data/projects/ci_test/data'):
        os.makedirs('tests/temp_data/projects/ci_test/data')
        # FIXME, replace with a curl command
        os.system('cp ~/xfer/ci_test.tgz tests/temp_data/projects')
        os.system('tar zxvf tests/temp_data/projects/ci_test.tgz -C tests/temp_data/projects')

    if not os.path.exists('tests/temp_data/data/test'):
        os.makedirs('tests/temp_data/data/test')
        # FIXME, replace with a curl command
        os.system(
            'cp ~/xfer/roman_rubin_2023_maglim_25.5_baseline_100k.hdf5 '
            'tests/temp_data/data/test/ci_test_blend_baseline_100k.hdf5'
        )

    def teardown_project_area() -> None:
        os.system("\\rm -rf tests/temp_data")

    request.addfinalizer(teardown_project_area)

    return 0

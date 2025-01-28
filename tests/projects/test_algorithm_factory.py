import os
import pytest

import yaml

from rail.projects.algorithm_factory import RailAlgorithmFactory
from rail.projects.algorithm_holder import RailPZAlgorithmHolder


missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
def test_load_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailAlgorithmFactory.clear()

    RailAlgorithmFactory.load_yaml('tests/ci_algorithms.yaml')
    RailAlgorithmFactory.print_contents()

    the_algo_holder_dict = RailAlgorithmFactory.get_algorithm_holder_dict()
    assert isinstance(the_algo_holder_dict['PZAlgorithms'], dict)

    the_algo_types = RailAlgorithmFactory.get_algorithm_types()
    assert 'PZAlgorithms' in the_algo_types

    the_pz_algos = RailAlgorithmFactory.get_algorithms('PZAlgorithms')
    assert isinstance(the_pz_algos['knn'], RailPZAlgorithmHolder)

    the_pz_algo_names = RailAlgorithmFactory.get_algorithm_names('PZAlgorithms')
    assert 'knn' in the_pz_algo_names
    
    RailAlgorithmFactory.clear()

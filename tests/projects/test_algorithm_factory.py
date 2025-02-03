import os

import pytest

from rail.projects.algorithm_factory import RailAlgorithmFactory
from rail.projects.algorithm_holder import RailPZAlgorithmHolder


def test_load_algorithm_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailAlgorithmFactory.clear()

    RailAlgorithmFactory.load_yaml("tests/ci_algorithms.yaml")
    RailAlgorithmFactory.print_contents()

    the_algo_holder_dict = RailAlgorithmFactory.get_algorithm_holder_dict()
    assert isinstance(the_algo_holder_dict["PZAlgorithm"], dict)

    the_algo_types = RailAlgorithmFactory.get_algorithm_types()
    assert "PZAlgorithm" in the_algo_types

    the_pz_algos = RailAlgorithmFactory.get_algorithms("PZAlgorithm")
    knn_algo = the_pz_algos["knn"]
    assert isinstance(knn_algo, RailPZAlgorithmHolder)

    the_pz_algo_names = RailAlgorithmFactory.get_algorithm_names("PZAlgorithm")
    assert "knn" in the_pz_algo_names

    with pytest.raises(KeyError):
        RailAlgorithmFactory.get_algorithms("bad")

    knn_estimator = RailAlgorithmFactory.get_algorithm_class(
        "PZAlgorithm", "knn", "Estimate"
    )
    assert knn_estimator

    # Test the interactive stuff
    RailAlgorithmFactory.clear()
    RailAlgorithmFactory.add_algorithm(knn_algo)

    check_algo = RailAlgorithmFactory.get_algorithm("PZAlgorithm", "knn")
    assert isinstance(check_algo, type(knn_algo))

    # check writing the yaml dict
    RailAlgorithmFactory.write_yaml("tests/temp.yaml")
    RailAlgorithmFactory.clear()
    RailAlgorithmFactory.load_yaml("tests/temp.yaml")
    os.unlink("tests/temp.yaml")

    check_algo = RailAlgorithmFactory.get_algorithm("PZAlgorithm", "knn")
    assert isinstance(check_algo, type(knn_algo))

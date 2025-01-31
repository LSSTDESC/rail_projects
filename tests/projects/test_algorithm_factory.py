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
    assert isinstance(the_algo_holder_dict["PZAlgorithms"], dict)

    the_algo_types = RailAlgorithmFactory.get_algorithm_types()
    assert "PZAlgorithms" in the_algo_types

    the_pz_algos = RailAlgorithmFactory.get_algorithms("PZAlgorithms")
    assert isinstance(the_pz_algos["knn"], RailPZAlgorithmHolder)

    the_pz_algo_names = RailAlgorithmFactory.get_algorithm_names("PZAlgorithms")
    assert "knn" in the_pz_algo_names

    with pytest.raises(KeyError):
        RailAlgorithmFactory.get_algorithms("bad")

    knn_estimator = RailAlgorithmFactory.get_algorithm_class(
        "PZAlgorithms", "knn", "Estimate"
    )
    assert knn_estimator

    RailAlgorithmFactory.clear()

"""Functions to manage various objects associated to RailProjects"""

from __future__ import annotations

import os
import subprocess
import urllib.request

import yaml

from .algorithm_factory import ALGORITHM_TYPES, RailAlgorithmFactory
from .catalog_factory import RailCatalogFactory
from .factory_mixin import RailFactoryMixin
from .pipeline_factory import RailPipelineFactory
from .project_file_factory import RailProjectFileFactory
from .selection_factory import RailSelectionFactory
from .subsample_factory import RailSubsampleFactory

THE_FACTORIES: list[type[RailFactoryMixin]] = [
    RailAlgorithmFactory,
    RailCatalogFactory,
    RailPipelineFactory,
    RailProjectFileFactory,
    RailSelectionFactory,
    RailSubsampleFactory,
]

YAML_HANDLERS: dict[str, type[RailFactoryMixin]] = {
    factory.yaml_tag: factory for factory in THE_FACTORIES
}


# Lift the RailAlgorithmFactory class methods

load_algorithm_yaml = RailAlgorithmFactory.load_yaml

load_algorithm_yaml_tag = RailAlgorithmFactory.load_yaml_tag

print_algorithm_contents = RailAlgorithmFactory.print_contents

clear_algorithms = RailAlgorithmFactory.clear

get_algorithm_types = RailAlgorithmFactory.get_algorithm_types

get_algorithm_holder_dict = RailAlgorithmFactory.get_algorithm_holder_dict

get_algorithms = RailAlgorithmFactory.get_algorithms

get_algorithm_names = RailAlgorithmFactory.get_algorithm_names

get_algorithm = RailAlgorithmFactory.get_algorithm

get_algorithm_class = RailAlgorithmFactory.get_algorithm_class


# Lift the RailCatalogFactory class methods

load_catalog_yaml = RailCatalogFactory.load_yaml

load_catalog_yaml_tag = RailCatalogFactory.load_yaml_tag

print_catalog_contents = RailCatalogFactory.print_contents

clear_catalogs = RailCatalogFactory.clear

get_catalog_templates = RailCatalogFactory.get_catalog_templates

get_catalog_template_names = RailCatalogFactory.get_catalog_template_names

get_catalog_instances = RailCatalogFactory.get_catalog_instances

get_catalog_instance_names = RailCatalogFactory.get_catalog_instance_names

get_catalog_template = RailCatalogFactory.get_catalog_template

get_catalog_instance = RailCatalogFactory.get_catalog_instance


# Lift the RailPipelineFactory class methods

load_pipeline_yaml = RailPipelineFactory.load_yaml

load_pipeline_yaml_tag = RailPipelineFactory.load_yaml_tag

print_pipeline_contents = RailPipelineFactory.print_contents

clear_pipelines = RailPipelineFactory.clear

get_pipeline_templates = RailPipelineFactory.get_pipeline_templates

get_pipeline_template_names = RailPipelineFactory.get_pipeline_template_names

get_pipeline_instances = RailPipelineFactory.get_pipeline_instances

get_pipeline_instance_names = RailPipelineFactory.get_pipeline_instance_names

get_pipeline_template = RailPipelineFactory.get_pipeline_template

get_pipeline_instance = RailPipelineFactory.get_pipeline_instance


# Lift the RailProjectFileFactory class methods

load_project_file_yaml = RailProjectFileFactory.load_yaml

load_project_file_yaml_tag = RailProjectFileFactory.load_yaml_tag

get_file_templates = RailProjectFileFactory.get_file_templates

get_file_template_names = RailProjectFileFactory.get_file_template_names

get_file_instances = RailProjectFileFactory.get_file_instances

get_file_instance_names = RailProjectFileFactory.get_file_instance_names

get_file_template = RailProjectFileFactory.get_file_template

get_file_instance = RailProjectFileFactory.get_file_instance


# Lift the RailSelectionFactory class methods

load_selection_yaml = RailSelectionFactory.load_yaml

load_selection_yaml_tag = RailSelectionFactory.load_yaml_tag

get_selections = RailSelectionFactory.get_selections

get_selection_names = RailSelectionFactory.get_selection_names

get_selection = RailSelectionFactory.get_selection


# Lift the RailSubsampleFactory class methods

load_subsample_yaml = RailSubsampleFactory.load_yaml

load_subsample_yaml_tag = RailSubsampleFactory.load_yaml_tag

get_subsamples = RailSubsampleFactory.get_subsamples

get_subsample_names = RailSubsampleFactory.get_subsample_names

get_subsample = RailSubsampleFactory.get_subsample


# Define a few additional functions
def clear() -> None:
    """Clean all the factories"""
    for factory_ in THE_FACTORIES:
        factory_.clear()


def print_contents() -> None:
    """Print the contents of the factories"""
    for factory_ in THE_FACTORIES:
        factory_.print_contents()
        print("----------------")
        print("")


def load_yaml(yaml_file: str) -> None:
    """Read a yaml file and load the factory accordingly

    Parameters
    ----------
    yaml_file: str
        File to read

    Notes
    -----
    See class description for yaml file syntax
    """
    clear()
    with open(os.path.expandvars(yaml_file), encoding="utf-8") as fin:
        yaml_data = yaml.safe_load(fin)

    for yaml_key, yaml_item in yaml_data.items():
        if yaml_key == RailSelectionFactory.yaml_tag:
            load_selection_yaml_tag(yaml_item, yaml_file)
        elif yaml_key == RailSubsampleFactory.yaml_tag:
            load_subsample_yaml_tag(yaml_item, yaml_file)
        elif yaml_key == RailProjectFileFactory.yaml_tag:
            load_project_file_yaml_tag(yaml_item, yaml_file)
        elif yaml_key == RailCatalogFactory.yaml_tag:
            load_catalog_yaml_tag(yaml_item, yaml_file)
        elif yaml_key == RailPipelineFactory.yaml_tag:
            load_pipeline_yaml_tag(yaml_item, yaml_file)
        elif yaml_key in ALGORITHM_TYPES:
            load_algorithm_yaml_tag(yaml_item, f"{yaml_file}#{yaml_key}")
        else:  # pragma: no cover
            good_tags = ALGORITHM_TYPES + [
                RailSelectionFactory.yaml_tag,
                RailSubsampleFactory.yaml_tag,
                RailProjectFileFactory.yaml_tag,
                RailCatalogFactory.yaml_tag,
                RailPipelineFactory.yaml_tag,
            ]
            raise KeyError(f"Yaml Tag {yaml_key} not in expected keys {good_tags}")


def write_yaml(yaml_file: str) -> None:
    """Write the current contents for the factories to a yaml file

    Parameters
    ----------
    yaml_file: str
        File to write

    Notes
    -----
    See class description for yaml file syntax
    """
    yaml_dict: dict[str, dict] = {}
    for a_factory in THE_FACTORIES:
        yaml_dict.update(**a_factory.to_yaml_dict())
    with open(os.path.expandvars(yaml_file), mode="w", encoding="utf-8") as fout:
        yaml.dump(yaml_dict, fout)


def setup_project_area() -> int:  # pragma: no cover
    """Download test files to setup a project testsing area

    Returns
    -------
    int:
       0 for success, error code otherwise

    Notes
    -----
    This will download files into 'tests/temp_data', and could take a few
    minutes.

    This will not download the files if they are already present
    """

    if not os.path.exists("tests/ci_test.tgz"):
        urllib.request.urlretrieve(
            "http://s3df.slac.stanford.edu/people/echarles/xfer/ci_test.tgz",
            "tests/ci_test.tgz",
        )
        if not os.path.exists("tests/ci_test.tgz"):
            return 1

    if not os.path.exists("test/temp_data/projects"):
        status = subprocess.run(
            ["tar", "zxvf", "tests/ci_test.tgz", "-C", "tests"], check=False
        )
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
    return 0


def setup_mininal_example_files() -> int:  # pragma: no cover
    if not os.path.exists("tests/temp_data/data/test/minimal_gold_test.hdf5"):
        os.makedirs("tests/temp_data/data/test", exist_ok=True)
        urllib.request.urlretrieve(
            "http://s3df.slac.stanford.edu/people/echarles/xfer/"
            "minimal_gold_test.hdf5",
            "tests/temp_data/data/test/minimal_gold_test.hdf5",
        )
        if not os.path.exists("tests/temp_data/data/test/minimal_gold_test.hdf5"):
            return 1
    if not os.path.exists("tests/temp_data/data/train/minimal_gold_train.hdf5"):
        os.makedirs("tests/temp_data/data/train", exist_ok=True)
        urllib.request.urlretrieve(
            "http://s3df.slac.stanford.edu/people/echarles/xfer/"
            "minimal_gold_test.hdf5",
            "tests/temp_data/data/train/minimal_gold_train.hdf5",
        )
        if not os.path.exists("tests/temp_data/data/train/minimal_gold_train.hdf5"):
            return 2
    return 0


def teardown_project_area() -> None:  # pragma: no cover
    if not os.environ.get("NO_TEARDOWN"):
        os.system("\\rm -rf tests/temp_data")
        try:
            os.unlink("tests/ci_test.tgz")
        except FileNotFoundError:
            pass

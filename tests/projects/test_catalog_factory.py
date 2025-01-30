import os
import pytest

from rail.projects.catalog_factory import RailCatalogFactory
from rail.projects.catalog_template import (
    RailProjectCatalogInstance,
    RailProjectCatalogTemplate,
)


missing_ci_data = not os.path.exists(os.path.expandvars(("$HOME/xfer/ci_test.tgz")))


@pytest.mark.skipif(missing_ci_data, reason="no ci data")
def test_load_catalog_yaml(setup_project_area: int) -> None:
    assert setup_project_area == 0

    # Load the testing yaml file
    RailCatalogFactory.clear()

    RailCatalogFactory.load_yaml("tests/ci_catalogs.yaml")
    RailCatalogFactory.print_contents()

    # Make sure the names of the catalog_templates got loaded
    the_catalog_templates_name = RailCatalogFactory.get_catalog_template_names()
    assert "truth" in the_catalog_templates_name

    # Make sure the catalog_templates got loaded
    the_dict = RailCatalogFactory.get_catalog_templates()
    assert isinstance(the_dict["truth"], RailProjectCatalogTemplate)

    # Make sure the names of the catalog_instances got loaded
    the_catalog_instance_name = RailCatalogFactory.get_catalog_instance_names()
    assert "degraded_ci_test_1.1.3_gold_blend" in the_catalog_instance_name

    # Make sure the catalog_instances got loaded
    the_catalog_instances = RailCatalogFactory.get_catalog_instances()
    assert isinstance(
        the_catalog_instances["degraded_ci_test_1.1.3_gold_blend"],
        RailProjectCatalogInstance,
    )

    # get a specfic catalog_templates
    the_catalog_template = RailCatalogFactory.get_catalog_template("truth")
    assert isinstance(the_catalog_template, RailProjectCatalogTemplate)

    # make a catalog_instance
    check_catalog_instance = the_catalog_template.make_catalog_instance(
        name="dummy",
        catalogs_dir="tests/temp_data/data",
        project="ci_test",
        sim_version="1.1.3",
        flavor="baseline",
        selection="blend",
        basename="output_degraded.hdf5",
    )
    assert isinstance(check_catalog_instance, RailProjectCatalogInstance)
    catalog_files = check_catalog_instance(healpix=[5150, 5151])
    assert len(catalog_files) == 2

    check_files_exist = check_catalog_instance.check_files()
    assert len(check_files_exist) == 2
    check_files_exist = check_catalog_instance.check_files(update=False)
    assert len(check_files_exist) == 2
    check_files_exist = check_catalog_instance.check_files()
    assert len(check_files_exist) == 2

    assert not check_files_exist[0]

    with pytest.raises(KeyError):
        RailCatalogFactory.get_catalog_template("bad")

    # get a specfic catalog_instance
    the_catalog_instance = RailCatalogFactory.get_catalog_instance(
        "degraded_ci_test_1.1.3_gold_blend"
    )
    assert isinstance(the_catalog_instance, RailProjectCatalogInstance)

    with pytest.raises(KeyError):
        RailCatalogFactory.get_catalog_instance("bad")

    RailCatalogFactory.clear()

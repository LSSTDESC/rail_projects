from __future__ import annotations

from .catalog_template import RailProjectCatalogInstance, RailProjectCatalogTemplate
from .factory_mixin import RailFactoryMixin


class RailCatalogFactory(RailFactoryMixin):
    """Factory class to make catalogs

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Catalogs:
        - CatalogTemplate
            name: truth
            path_template: "{catalogs_dir}/{project}_{sim_version}/{healpix}/part-0.parquet"
            iteration_vars: ['healpix']
        - CatalogTemplate
            name: reduced
            path_template: "{catalogs_dir}/{project}_{sim_version}_{selection}/{healpix}/part-0.pq"
            iteration_vars: ['healpix']

    Or the used can specifiy particular catalog instances where everything except the
    interation_vars are resolved

    .. highlight:: yaml
    .. code-block:: yaml

      Catalogs:
        - CatalogTemplate
            name: truth_roman_rubin_v1.1.3_gold
            path_template: "full_path_to_catalog/{healpix}/part-0.parquet"
            iteration_vars: ['healpix']
    """

    yaml_tag: str = "Catalogs"

    client_classes = [RailProjectCatalogTemplate, RailProjectCatalogInstance]

    _instance: RailCatalogFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._catalog_templates = self.add_dict(RailProjectCatalogTemplate)
        self._catalog_instances = self.add_dict(RailProjectCatalogInstance)

    @classmethod
    def get_catalog_templates(cls) -> dict[str, RailProjectCatalogTemplate]:
        """Return the dict of all the catalog templates"""
        return cls.instance().catalog_templates

    @classmethod
    def get_catalog_template_names(cls) -> list[str]:
        """Return the names of the catalog templates"""
        return list(cls.instance().catalog_templates.keys())

    @classmethod
    def get_catalog_instances(cls) -> dict[str, RailProjectCatalogInstance]:
        """Return the dict of all the catalog instances"""
        return cls.instance().catalog_instances

    @classmethod
    def get_catalog_instance_names(cls) -> list[str]:
        """Return the names of the catalog instances lists"""
        return list(cls.instance().catalog_instances.keys())

    @classmethod
    def get_catalog_template(cls, name: str) -> RailProjectCatalogTemplate:
        """Get catalog templates by it's assigned name"""
        try:
            return cls.instance().catalog_templates[name]
        except KeyError as msg:
            raise KeyError(
                f"Dataset named {name} not found in RailCatalogFactory "
                f"{list(cls.instance().catalog_templates.keys())}"
            ) from msg

    @classmethod
    def get_catalog_instance(cls, name: str) -> RailProjectCatalogInstance:
        """Get a catalog instance by its assigned name"""
        try:
            return cls.instance().catalog_instances[name]
        except KeyError as msg:
            raise KeyError(
                f"RailProjectCatalogInstance named {name} not found in RailProjectCatalogInstance "
                f"{list(cls.instance().catalog_instances.keys())}"
            ) from msg

    @classmethod
    def add_catalog_instance(cls, catalog_instance: RailProjectCatalogInstance) -> None:
        """Add a particular catalog instance to the factory"""
        cls.instance().add_to_dict(catalog_instance)

    @classmethod
    def add_catalog_template(cls, catalog_template: RailProjectCatalogTemplate) -> None:
        """Add a particular catalog template to the factory"""
        cls.instance().add_to_dict(catalog_template)

    @property
    def catalog_templates(self) -> dict[str, RailProjectCatalogTemplate]:
        """Return the dictionary of catalog templates"""
        return self._catalog_templates

    @property
    def catalog_instances(self) -> dict[str, RailProjectCatalogInstance]:
        """Return the dictionary of catalog instances"""
        return self._catalog_instances

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Catalogs")
        RailFactoryMixin.print_instance_contents(self)

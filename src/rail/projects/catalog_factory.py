from __future__ import annotations

from typing import Any
import os
import yaml

from .catalog_template import RailProjectCatalogInstance, RailProjectCatalogTemplate


class RailCatalogFactory:
    """Factory class to make catalogs

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:
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

    Catalogs: 
        - CatalogTemplate
              name: truth_roman_rubin_v1.1.3_gold
              path_template: "full_path_to_catalog/{healpix}/part-0.parquet"
              iteration_vars: ['healpix']
    """

    _instance: RailCatalogFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._catalog_templates: dict[str, RailProjectCatalogTemplate] = {}
        self._catalog_instances: dict[str, RailProjectCatalogInstance] = {}

    @classmethod
    def instance(cls) -> RailCatalogFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:
            cls._instance = RailCatalogFactory()
        return cls._instance

    @classmethod
    def clear(cls) -> None:
        """Clear the contents of the factory"""
        if cls._instance is None:
            return
        cls._instance.clear_instance()

    @classmethod
    def print_contents(cls) -> None:
        """Print the contents of the factory"""
        if cls._instance is None:
            cls._instance = RailCatalogFactory()
        cls._instance.print_instance_contents()

    @classmethod
    def load_yaml(cls, yaml_file: str) -> None:
        """Load a yaml file

        Parameters
        ----------
        yaml_file: str
            File to read and load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = RailCatalogFactory()
        cls._instance.load_instance_yaml(yaml_file)

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
        return cls.instance()._catalog_instances

    @classmethod
    def get_catalog_instance_names(cls) -> list[str]:
        """Return the names of the catalog instances lists"""
        return list(cls.instance().catalog_instances.keys())

    @classmethod
    def get_catalog_template(cls, name: str) -> RailProjectCatalogTemplate:
        """Get catalog templates by it's assigned name

        Parameters
        ----------
        name: str
            Name of the catalog templates to return

        Returns
        -------
        catalog_templates: RailProjectCatalogTemplate
            catalog templates in question
        """
        try:
            return cls.instance().catalog_templates[name]
        except KeyError as msg:
            raise KeyError(
                f"Dataset named {name} not found in RailCatalogFactory "
                f"{list(cls.instance().catalog_templates.keys())}"
            ) from msg

    @classmethod
    def get_catalog_instance(cls, name: str) -> RailProjectCatalogInstance:
        """Get a catalog instance by its assigned name

        Parameters
        ----------
        name: str
            Name of the catalog instance list to return

        Returns
        -------
        catalog_instance: RailProjectCatalogInstance
            catalog instance in question
        """
        try:
            return cls.instance().catalog_instances[name]
        except KeyError as msg:
            raise KeyError(
                f"RailProjectCatalogInstance named {name} not found in RailProjectCatalogInstance "
                f"{list(cls.instance().catalog_instances.keys())}"
            ) from msg

    @property
    def catalog_templates(self) ->dict[str, RailProjectCatalogTemplate]:
        """Return the dictionary of catalog templates"""
        return self._catalog_templates

    @property
    def catalog_instances(self) -> dict[str, RailProjectCatalogInstance]:
        """Return the dictionary of catalog instances"""
        return self._catalog_instances

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._catalog_templates.clear()
        self._catalog_instances.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("CatalogTemplates:")
        for template_name, catalog_template in self.catalog_templates.items():
            print(f"  {template_name}: {catalog_template}")
        print("----------------")
        print("CatalogInstances:")
        for template_name, catalog_instance in self.catalog_instances.items():
            print(f"  {template_name}: {catalog_instance}")

    def _make_catalog_instance(self, **kwargs: Any) -> RailProjectCatalogInstance:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "CatalogInstance yaml block does not contain name for catalog_instance: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._catalog_instances:  # pragma: no cover
            raise KeyError(f"Dataset {name} is already defined")
        catalog_instance = RailProjectCatalogInstance(**kwargs)
        self._catalog_instances[name] = catalog_instance
        return catalog_instance

    def _make_catalog_template(self, **kwargs: Any) -> RailProjectCatalogTemplate:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "CatalogTemplate yaml block does not contain name for catalog_template: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._catalog_templates:  # pragma: no cover
            raise KeyError(f"Dataset {name} is already defined")
        catalog_template = RailProjectCatalogTemplate(**kwargs)
        self._catalog_templates[name] = catalog_template
        return catalog_template

    def load_catalog_template_from_yaml_tag(self, catalog_template_config: dict[str, Any]) -> None:
        """Load a dataset from a Dataset tag in yaml

        Paramters
        ---------
        catalog_template_config: dict[str, Any]
            Yaml data in question
        """
        self._make_catalog_template(**catalog_template_config)

    def load_catalog_instance_from_yaml_tag(self, catalog_instance_config: dict[str, Any]) -> None:
        """Load a dataset from a Dataset tag in yaml

        Paramters
        ---------
        catalog_instance_config: dict[str, Any]
            Yaml data in question
        """
        self._make_catalog_instance(**catalog_instance_config)

    def load_instance_yaml(self, yaml_file: str) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_file: str
            File to read

        Notes
        -----
        See class description for yaml file syntax
        """
        with open(os.path.expandvars(yaml_file), encoding="utf-8") as fin:
            yaml_data = yaml.safe_load(fin)

        try:
            catalogs_config = yaml_data['Catalogs']
        except KeyError as missing_key:
            raise KeyError(f"Did not find key Catalogs in {yaml_file}") from missing_key
            
        for catalogs_item in catalogs_config:
            if "CatalogTemplate" in catalogs_item:
                catalog_template_config = catalogs_item["CatalogTemplate"]
                self.load_catalog_template_from_yaml_tag(catalog_template_config)
            elif "CatalogInstance" in catalogs_item:
                catalog_instance_config = catalogs_item["CatalogInstance"]
                self.load_catalog_instance_from_yaml_tag(catalog_instance_config)
            else:  # pragma: no cover
                good_keys = ["CatalogTemplate", "CatalogInstance"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {dataset_item.keys()})"
                )

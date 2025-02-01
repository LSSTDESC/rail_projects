from __future__ import annotations

from typing import Any
import os
import yaml

from .file_template import RailProjectFileInstance, RailProjectFileTemplate
from .factory_mixin import RailFactoryMixin


class RailProjectFileFactory(RailFactoryMixin):
    """Factory class to make files

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:
    Files:
      - FileTemplate:
            name: test_file_100k
            path_template: "{catalogs_dir}/test/{project}_{selection}_baseline_100k.hdf5"

    Or the used can specifiy particular file instances where everything except the
    interation_vars are resolved

    Files:
      - FileInstance
            name: test_file_100k_roman_rubin_v1.1.3_gold
            path: <full_path_to_file>
    """

    client_classes = [RailProjectFileInstance, RailProjectFileTemplate]

    _instance: RailProjectFileFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._file_templates = self.add_dict(RailProjectFileTemplate)
        self._file_instances = self.add_dict(RailProjectFileInstance)

    @classmethod
    def get_file_templates(cls) -> dict[str, RailProjectFileTemplate]:
        """Return the dict of all the file templates"""
        return cls.instance().file_templates

    @classmethod
    def get_file_template_names(cls) -> list[str]:
        """Return the names of the file templates"""
        return list(cls.instance().file_templates.keys())

    @classmethod
    def get_file_instances(cls) -> dict[str, RailProjectFileInstance]:
        """Return the dict of all the file instances"""
        return cls.instance().file_instances

    @classmethod
    def get_file_instance_names(cls) -> list[str]:
        """Return the names of the file instances lists"""
        return list(cls.instance().file_instances.keys())

    @classmethod
    def get_file_template(cls, name: str) -> RailProjectFileTemplate:
        """Get file templates by it's assigned name

        Parameters
        ----------
        name: str
            Name of the file templates to return

        Returns
        -------
        file_templates: RailProjectFileTemplate
            file templates in question
        """
        try:
            return cls.instance().file_templates[name]
        except KeyError as msg:
            raise KeyError(
                f"Dataset named {name} not found in RailProjectFileFactory "
                f"{list(cls.instance().file_templates.keys())}"
            ) from msg

    @classmethod
    def get_file_instance(cls, name: str) -> RailProjectFileInstance:
        """Get a file instance by its assigned name

        Parameters
        ----------
        name: str
            Name of the file instance list to return

        Returns
        -------
        file_instance: RailProjectFileInstance
            file instance in question
        """
        try:
            return cls.instance().file_instances[name]
        except KeyError as msg:
            raise KeyError(
                f"RailProjectFileInstance named {name} not found in RailProjectFileInstance "
                f"{list(cls.instance().file_instances.keys())}"
            ) from msg

    @property
    def file_templates(self) -> dict[str, RailProjectFileTemplate]:
        """Return the dictionary of file templates"""
        return self._file_templates

    @property
    def file_instances(self) -> dict[str, RailProjectFileInstance]:
        """Return the dictionary of file instances"""
        return self._file_instances

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Files:")
        print("----------------")
        print("FileTemplates:")
        for template_name, file_template in self.file_templates.items():
            print(f"  {template_name}: {file_template}")
        print("----------------")
        print("FileInstances:")
        for template_name, file_instance in self.file_instances.items():
            print(f"  {template_name}: {file_instance}")

    def add_file_instance(self, file_instance: RailProjectFileInstance) -> None:
        self.add_to_dict(file_instance)

    def add_catalog_template(self, file_template: RailProjectFileTemplate) -> None:
        self.add_to_dict(file_template)

    def load_files_from_yaml_tag(
        self,
        files_config: list[dict[str, Any]],
    ) -> None:
        """Read a yaml "Files" tag and load the factory accordingy

        Parameters
        ----------
        files_config: list[dict[str, Any]]
            Yaml tag to load

        Notes
        -----
        See class description for yaml file syntax
        """
        self.load_instance_yaml_tag(files_config)

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
            files_config = yaml_data["Files"]
        except KeyError as missing_key:
            raise KeyError(f"Did not find key Files in {yaml_file}") from missing_key

        self.load_files_from_yaml_tag(files_config)

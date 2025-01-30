from __future__ import annotations

from typing import Any
import os
import yaml

from .file_template import RailProjectFileInstance, RailProjectFileTemplate


class RailProjectFileFactory:
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

    _instance: RailProjectFileFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._file_templates: dict[str, RailProjectFileTemplate] = {}
        self._file_instances: dict[str, RailProjectFileInstance] = {}

    @classmethod
    def instance(cls) -> RailProjectFileFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:
            cls._instance = RailProjectFileFactory()
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
            cls._instance = RailProjectFileFactory()
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
            cls._instance = RailProjectFileFactory()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def load_yaml_tag(cls, files_config: list[dict[str, Any]]) -> None:
        """Load from a yaml tag

        Parameters
        ----------
        files_config: list[dict[str, Any]]
            Yaml tag used to load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = RailProjectFileFactory()
        cls._instance.load_files_from_yaml_tag(files_config)

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

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._file_templates.clear()
        self._file_instances.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("FileTemplates:")
        for template_name, file_template in self.file_templates.items():
            print(f"  {template_name}: {file_template}")
        print("----------------")
        print("FileInstances:")
        for template_name, file_instance in self.file_instances.items():
            print(f"  {template_name}: {file_instance}")

    def _make_file_instance(self, **kwargs: Any) -> RailProjectFileInstance:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "FileInstance yaml block does not contain name for file_instance: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._file_instances:  # pragma: no cover
            raise KeyError(f"Dataset {name} is already defined")
        file_instance = RailProjectFileInstance(**kwargs)
        self._file_instances[name] = file_instance
        return file_instance

    def _make_file_template(self, **kwargs: Any) -> RailProjectFileTemplate:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "FileTemplate yaml block does not contain name for file_template: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._file_templates:  # pragma: no cover
            raise KeyError(f"Dataset {name} is already defined")
        file_template = RailProjectFileTemplate(**kwargs)
        self._file_templates[name] = file_template
        return file_template

    def load_file_template_from_yaml_tag(
        self, file_template_config: dict[str, Any]
    ) -> None:
        """Load a dataset from a Dataset tag in yaml

        Paramters
        ---------
        file_template_config: dict[str, Any]
            Yaml data in question
        """
        self._make_file_template(**file_template_config)

    def load_file_instance_from_yaml_tag(
        self, file_instance_config: dict[str, Any]
    ) -> None:
        """Load a dataset from a Dataset tag in yaml

        Paramters
        ---------
        file_instance_config: dict[str, Any]
            Yaml data in question
        """
        self._make_file_instance(**file_instance_config)

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
        for files_item in files_config:
            if "FileTemplate" in files_item:
                file_template_config = files_item["FileTemplate"]
                self.load_file_template_from_yaml_tag(file_template_config)
            elif "FileInstance" in files_item:
                file_instance_config = files_item["FileInstance"]
                self.load_file_instance_from_yaml_tag(file_instance_config)
            else:  # pragma: no cover
                good_keys = ["FileTemplate", "FileInstance"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {files_item.keys()})"
                )

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

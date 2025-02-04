from __future__ import annotations

from .factory_mixin import RailFactoryMixin
from .file_template import RailProjectFileInstance, RailProjectFileTemplate


class RailProjectFileFactory(RailFactoryMixin):
    """Factory class to make files

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Files:
        - FileTemplate:
            name: test_file_100k
            path_template: "{catalogs_dir}/test/{project}_{selection}_baseline_100k.hdf5"

    Or the used can specifiy particular file instances where everything except the
    interation_vars are resolved

    .. highlight:: yaml
    .. code-block:: yaml

      Files:
        - FileInstance
            name: test_file_100k_roman_rubin_v1.1.3_gold
            path: <full_path_to_file>
    """

    yaml_tag: str = "Files"

    client_classes = [RailProjectFileInstance, RailProjectFileTemplate]

    _instance: RailProjectFileFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailProjectFileFactory"""
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
        """Get file template by it's assigned name

        Parameters
        ----------
        name: str
            Name of the file templates to return

        Returns
        -------
        RailProjectFileTemplate:
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
        RailProjectFileInstance:
            file instance in question
        """
        try:
            return cls.instance().file_instances[name]
        except KeyError as msg:
            raise KeyError(
                f"RailProjectFileInstance named {name} not found in RailProjectFileInstance "
                f"{list(cls.instance().file_instances.keys())}"
            ) from msg

    @classmethod
    def add_file_instance(cls, file_instance: RailProjectFileInstance) -> None:
        """Add a particular RailProjectFileInstance to the factory"""
        cls.instance().add_to_dict(file_instance)

    @classmethod
    def add_file_template(cls, file_template: RailProjectFileTemplate) -> None:
        """Add a particular RailProjectFileTemplate to the factory"""
        cls.instance().add_to_dict(file_template)

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
        RailFactoryMixin.print_instance_contents(self)

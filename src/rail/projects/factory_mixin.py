from typing import Any, TypeVar

from .configurable import Configurable

T = TypeVar("T", bound="RailFactoryMixin")
C = TypeVar("C", bound="Configurable")


class RailFactoryMixin:
    client_classes: list[type[Configurable]]

    _instance: Any | None = None

    def __init__(self) -> None:
        self._the_dicts: dict[str, dict] = {}

    def add_dict(self, configurable_class: type[C]) -> dict[str, C]:
        a_dict: dict[str, C] = {}
        self._the_dicts[configurable_class.yaml_tag] = a_dict
        return a_dict

    def add_to_dict(self, the_object: C) -> None:
        the_class = type(the_object)
        try:
            the_dict = self._the_dicts[the_class.yaml_tag]
        except KeyError as missing_key:
            raise KeyError(
                f"Tried to add object with {the_class.yaml_tag}, "
                "but factory has {list(self._the_dicts.keys())}"
            ) from missing_key
        if the_object.config.name in the_dict:  # pragma: no cover
            raise KeyError(f"{the_class} {the_object.config.name} is already defined")
        the_dict[the_object.config.name] = the_object

    def load_object_from_yaml_tag(
        self, configurable_class: type[C], yaml_tag: dict[str, Any]
    ) -> None:
        the_object = configurable_class(**yaml_tag)
        self.add_to_dict(the_object)

    @classmethod
    def instance(cls: type[T]) -> T:
        """Return the singleton instance of the factory"""
        if cls._instance is None:
            cls._instance = cls()
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
            cls._instance = cls()
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
            cls._instance = cls()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def load_yaml_tag(cls, yaml_config: list[dict[str, Any]]) -> None:
        """Load from a yaml tag

        Parameters
        ----------
        yaml_config: list[dict[str, Any]]
            Yaml tag used to load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = cls()
        cls._instance.load_instance_yaml_tag(yaml_config)

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        for val in self._the_dicts.values():
            val.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        raise NotImplementedError()

    def load_instance_yaml_tag(
        self,
        yaml_config: list[dict[str, Any]],
    ) -> None:
        """Read a yaml tag and load the factory accordingy

        Parameters
        ----------
        yaml_config: list[dict[str, Any]]
            Yaml tag to load

        Notes
        -----
        See class description for yaml file syntax
        """
        for yaml_item in yaml_config:
            found_key = False
            for val in self.client_classes:
                if val.yaml_tag in yaml_item:
                    found_key = True
                    yaml_vals = yaml_item[val.yaml_tag]
                    self.load_object_from_yaml_tag(val, yaml_vals)
            if not found_key:  # pragma: no cover
                good_keys = [val.yaml_tag for val in self.client_classes]
                raise KeyError(f"Expecting one of {good_keys} not: {yaml_item.keys()})")

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
        raise NotImplementedError()

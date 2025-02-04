from __future__ import annotations

from typing import Any, TypeVar

T = TypeVar("T", bound="DynamicClass")


class DynamicClass:
    """Base class for classes that will create objects of sub-classes dynamically

    This implements:

    1. keeping track of all the loaded sub-classes,
    2. being able to load a new sub-class from the python module and class name
    3. being able to create an object of sub-class from a dict

    Subclasses that serve as parent classes to several addtional sub-classes should

    1. override the sub_classes to keep track of their sub-classes
    """

    sub_classes: dict[str, type[DynamicClass]] = {}

    def __init_subclass__(cls) -> None:
        cls.sub_classes[cls.__name__] = cls

    @classmethod
    def print_classes(cls) -> None:
        """Print the sub-classes of DynamicClass that have been loaded"""
        print(f"{cls.__name__}")
        for key, val in cls.sub_classes.items():
            print(f"  {key} {val}")

    @classmethod
    def get_sub_class(cls: type[T], key: str, class_name: str | None = None) -> type[T]:
        """Get a particular sub-class of DynamicClass by name

        Parameters
        ----------
        key: str
            Key for the subclass.

        class_name: str | None = None
           Full class name, so it can be loaded

        Returns
        -------
        type:
            Subclass in question
        """
        if key in cls.sub_classes:
            sub_class = cls.sub_classes[key]
            assert issubclass(sub_class, cls)
            return sub_class
        if class_name is None:
            raise KeyError(
                f"class {key} not found in {list(cls.sub_classes.keys())} and class_name is not provided."
            )
        return cls.load_sub_class(class_name)

    @classmethod
    def load_sub_class(cls: type[T], class_name: str) -> type[T]:
        """Import a particular sub-class of DynamicClass by name

        Parameters
        ----------
        class_name: str
            Full path and name of the subclass, e.g., rail.plotting.some_file.SomeClass

        Returns
        -------
        type:
            Subclass in question
        """
        tokens = class_name.split(".")
        module = ".".join(tokens[:-1])
        key = tokens[-1]
        __import__(module)
        sub_class = cls.get_sub_class(key)
        assert issubclass(sub_class, cls)
        return sub_class

    @classmethod
    def create_from_dict(
        cls: type[T],
        config_dict: dict[str, Any],
    ) -> T:
        """Create a DynamicClass object of type T

        Parameters
        ----------
        config_dict: dict[str, Any],
            Configuration parameters

        Returns
        -------
        T:
            Newly created object

        Notes
        -----
        config_dict must include 'class_name' which gives the path and name of the
        class, e.g., rail.plotters.some_file.SomeClass
        """
        copy_config = config_dict.copy()
        class_name = copy_config.pop("class_name")
        tokens = class_name.split(".")
        key = tokens[-1]
        sub_class = cls.get_sub_class(key, class_name)
        assert issubclass(sub_class, cls)
        return sub_class(**copy_config)

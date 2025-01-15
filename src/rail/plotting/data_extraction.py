from __future__ import annotations

from typing import Any


class RailProjectDataExtractor:
    """ Base class for extracting data from a RailProject

    The main function in this class is:
    __call__(kwargs**: Any) -> dict[str, Any]

    This function will extract data and return them in a dict.
    
    Parameters to specify the data are passed vie the kwargs.
    

    Sub-classes should implement 
    
    _inputs: a dict [str, type] that specifics the inputs
    that the sub-classes expect, this is used the check the kwargs
    that are passed to the __call__ function.

    A function:
    _get_data(self,**kwargs: Any) -> dict[str, Any]:
    
    That actually gets the data.  It does not need to do the checking
    that the correct kwargs have been given.      
    """

    inputs: dict = {}

    extractor_classes: dict[str, type] = {}

    def __init_subclass__(cls) -> None:
        cls.extractor_classes[cls.__name__] = cls

    @classmethod
    def print_classes(cls) -> None:
        """Print the sub-classes of RailProjectDataExtractor that have been loaded"""
        for key, val in cls.extractor_classes.items():
            print(f"{key} {val}")

    @classmethod
    def get_extractor_class(cls, name: str) -> type:
        """Get a particular sub-class of RailProjectDataExtractor by name

        Parameters
        ----------
        name: str
            Name of the subclass

        Returns
        -------
        subclass: type
            Subclass in question
        """
        try:
            return cls.extractor_classes[name]
        except KeyError as msg:
            raise KeyError(
                f"Could not find extractor class {name} in {list(cls.extractor_classes.keys())}"
        ) from msg

    @staticmethod
    def load_extractor_class(class_name: str) -> type:
        """Import a particular sub-class of RailProjectDataExtractor by name

        Parameters
        ----------
        class_name: str
            Full path and name of the subclass, e.g., rail.plotting.some_file.SomeClass

        Returns
        -------
        subclass: type
            Subclass in question
        """
        tokens = class_name.split('.')
        module = '.'.join(tokens[:-1])
        class_name = tokens[-1]
        __import__(module)
        extractor_class = RailProjectDataExtractor.get_extractor_class(class_name)
        return extractor_class

    @staticmethod
    def create(
        name: str,
        class_name: str,
    ) -> RailProjectDataExtractor:
        """Create a RailProjectDataExtractor object

        Parameters
        ----------
        name: str
            Name to give to the newly created object

        class_name: str
            Fully formed name of class to build, e.g.,
            rail.plotters.<some_file>.<ClassName>

        Returns
        -------
        extractor: RailProjectDataExtractor
            Newly created extractor
        """
        if class_name in RailProjectDataExtractor.extractor_classes:
            extractor_class = RailProjectDataExtractor.get_extractor_class(class_name)
        else:
            extractor_class = RailProjectDataExtractor.load_extractor_class(class_name)
        return extractor_class(name)

    def __init__(self, name: str):
        """ C'tor

        Parameters
        ----------
        name: str
            Name for this plotter, used to construct names of plots

        kwargs: Any
            Configuration parameters for this plotter, must match
            class.config_options data members
        """
        self._name = name

    def __repr__(self) -> str:
        return f"{self._name}"

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        """ Make all the plots given the data

        Parameters
        ----------
        kwargs: dict[str, Any]
            Used to pass the data to make the plots

        Returns
        -------
        out_dict: dict[str, Any]
            Dictionary of the newly extracted data
        """
        self._validate_inputs(**kwargs)
        return self._get_data(**kwargs)

    @classmethod
    def _validate_inputs(cls, **kwargs: Any) -> None:
        for key, expected_type in cls.inputs.items():
            try:
                data = kwargs[key]
            except KeyError as msg:
                raise KeyError(
                    f"{key} not provided to RailPlotter {cls} in {list(kwargs.keys())}"
                ) from msg
            try:
                if not isinstance(data, expected_type):
                    raise TypeError(
                        f"{key} provided to RailPlotter was {type(data)}, expected {expected_type}"
                    )
            # FIXME
            # if expected_type is a "Parameterized generic" (e.g., list[str]) this will be raised
            except TypeError:
                pass

    def _get_data(self, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError()

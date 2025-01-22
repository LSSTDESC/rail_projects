from __future__ import annotations

from typing import Any

from .configurable import Configurable
from .dynamic_class import DynamicClass
from .validation import validate_inputs


class RailProjectDataExtractor(Configurable, DynamicClass):
    """Base class for extracting data from a RailProject

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

    sub_classes: dict[str, type[DynamicClass]] = {}

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailProjectDataExtractor, must match
            class.config_options data members
        """
        DynamicClass.__init__(self)
        Configurable.__init__(self, **kwargs)

    def __call__(self, **kwargs: Any) -> dict[str, Any] | None:
        """Extract the data

        Parameters
        ----------
        kwargs: dict[str, Any]
            Used to pass the e

        Returns
        -------
        out_dict: dict[str, Any] | None
            Dictionary of the newly extracted data
        """
        self._validate_inputs(**kwargs)
        return self._get_data(**kwargs)

    @classmethod
    def _validate_inputs(cls, **kwargs: Any) -> None:
        validate_inputs(cls, cls.inputs, **kwargs)

    def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
        raise NotImplementedError()

    @classmethod
    def generate_dataset_dict(
        cls,
        **kwargs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Create a dict of the datasets that this extractor can extract

        Parameters
        ----------
        dataset_list_name: str
            Name for the resulting DatasetList

        dataset_holder_class: str
            Class for the dataset holder

        project: RailProject
            Project to inspect

        selections: list[str]
            Selections to use

        flavors: list[str]
            Flavors to use

        Returns
        -------
        output: list[dict[str, Any]]
            Dictionary of the extracted datasets
        """
        raise NotImplementedError()

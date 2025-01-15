from __future__ import annotations

import os
from types import GenericAlias
from typing import Any
from matplotlib.figure import Figure

from ceci.config import StageConfig, StageParameter

class RailPlotter:
    """ Base class for making matplotlib plot 

    The main function in this class is:
    __call__(prefix: str, kwargs**: Any) -> dict[str, Figure]

    This function will make a set of plots and return them in a dict.
    prefix is string that gets prepended to plot names.
    
    The data to be plotted is passed in via the kwargs.
    

    Sub-classes should implement 
    
    config_options: a dict[str, `ceci.StageParameter`] that
    will be used to configure things like the axes binning, selection functions,
    and other plot-specfic options

    _inputs: a dict [str, type] that specifics the inputs
    that the sub-classes expect, this is used the check the kwargs
    that are passed to the __call__ function.

    A function:
    _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, Figure]:
    
    That actually makes the plots.  It does not need to do the checking
    that the correct kwargs have been given.      
    """

    config_options: dict[str, StageParameter] = {}

    inputs: dict = {}

    plotter_classes: dict[str, type] = {}

    def __init_subclass__(cls) -> None:
        cls.plotter_classes[cls.__name__] = cls

    @classmethod
    def print_classes(cls) -> None:
        """Print the sub-classes of RailPlotter that have been loaded"""
        for key, val in cls.plotter_classes.items():
            print(f"{key} {val}")

    @classmethod
    def get_plotter_class(cls, name: str) -> type:
        """Get a particular sub-class of RailPlotter by name

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
            return cls.plotter_classes[name]
        except KeyError as msg:
            raise KeyError(
                f"Could not find plotter class {name} in {list(cls.plotter_classes.keys())}"
        ) from msg

    @staticmethod
    def load_plotter_class(class_name: str) -> type:
        """Import a particular sub-class of RailPlotter by name

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
        plotter_class = RailPlotter.get_plotter_class(class_name)
        return plotter_class

    @staticmethod
    def create_from_dict(
        name: str,
        config_dict: dict[str, Any],
    ) -> RailPlotter:
        """Create a RailPlotter object

        Parameters
        ----------
        name: str
            Name to give to the newly created object

        config_dict: dict[str, Any],
            Configuration parameters

        Returns
        -------
        plotter: RailPlotter
            Newly created plotter

        Notes
        -----
        config_dict must include 'class_name' which gives the path and name of the
        class, e.g., rail.plotters.some_file.SomeClass
        """
        copy_config = config_dict.copy()
        class_name = copy_config.pop('class_name')
        plotter_class = RailPlotter.load_plotter_class(class_name)
        return plotter_class(name, **copy_config)

    @staticmethod
    def iterate_plotters(
        plotters: list[RailPlotter],
        prefix: str,
        **kwargs: Any,
    ) -> dict[str, Figure]:
        """ Utility function to several plotters on the same data

        Parameters
        ----------
        plotters: list[RailPlotter]
            Plotters to run

        prefix: str
            Prefix to append to plot names, e.g., the p(z) algorithm or
            analysis 'flavor'

        kwargs: dict[str, Any]
            Used to pass the data to make the plots

        Returns
        -------
        out_dict: dict[str, Figure]
            Dictionary of the newly created figures
        """
        out_dict: dict[str, Figure] = {}
        for plotter_ in plotters:
            out_dict.update(plotter_(prefix, **kwargs))
        return out_dict

    @staticmethod
    def iterate(
        plotters: list[RailPlotter],
        data_dict: dict[str, dict],
    ) -> dict[str, Figure]:
        """ Utility function to several plotters of several data sets

        Parameters
        ----------
        plotters: list[RailPlotter]
            Plotters to run

        data_dict: dict[str, dict]
            Prefixes and datasets to iterate over

        Returns
        -------
        out_dict: dict[str, Figure]
            Dictionary of the newly created figures
        """
        out_dict: dict[str, Figure] = {}
        for key, val in data_dict.items():
            out_dict.update(RailPlotter.iterate_plotters(plotters, key, **val))
        return out_dict

    @staticmethod
    def write_plots(
        fig_dict: dict[str, Figure],
        outdir: str=".",
        figtype: str="png",
    ) -> None:
        """ Utility function to write several plots do disk

        Parameters
        ----------
        fig_dict: dict[str, Figure]
            Dictionary of figures to write

        outdir: str
            Directory to write figures in

        figtype: str
            Type of figures to write, e.g., png, pdf...
        """
        for key, val in fig_dict.items():
            try:
                os.makedirs(outdir)
            except Exception:
                pass
            out_path = os.path.join(outdir, f"{key}.{figtype}")
            val.savefig(out_path)

    def __init__(self, name: str, **kwargs: Any):
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
        self._config = StageConfig(**self.config_options)
        self._set_config(**kwargs)

    @property
    def config(self) -> StageConfig:
        """Return the plotter configuration """
        return self._config

    def __repr__(self) -> str:
        return f"{self._name}"

    def __call__(self, prefix: str, **kwargs: Any) -> dict[str, Figure]:
        """ Make all the plots given the data

        Parameters
        ----------
        prefix: str
            Prefix to append to plot names, e.g., the p(z) algorithm or
            analysis 'flavor'

        kwargs: dict[str, Any]
            Used to pass the data to make the plots

        Returns
        -------
        out_dict: dict[str, Figure]
            Dictionary of the newly created figures
        """
        self._validate_inputs(**kwargs)
        return self._make_plots(prefix, **kwargs)

    def _make_full_plot_name(self, prefix: str, plot_name: str) -> str:
        """ Create the make for a specific plot

        Parameters
        ----------
        prefix: str
            Prefix to append to plot names, e.g., the p(z) algorithm or
            analysis 'flavor'

        plot_name: str
            Specific name for a particular plot

        Returns
        -------
        plot_name: str
            Plot name, following the pattern f"{self._name}_{prefix}_{plot_name}"
        """
        return f"{self._name}_{prefix}_{plot_name}"

    def _set_config(self, **kwargs: Any) -> None:
        kwcopy = kwargs.copy()
        for key in self.config.keys():
            if key in kwargs:
                self.config[key] = kwcopy.pop(key)
            else:  # pragma: no cover
                attr = self.config.get(key)
                if attr.required:
                    raise ValueError(f"Missing configuration option {key}")
                self.config[key] = attr.default
        if kwcopy:  # pragma: no cover
            raise ValueError(f"Unrecogonized configruation parameters {kwcopy.keys()}")

    @classmethod
    def _validate_inputs(cls, **kwargs: Any) -> None:
        for key, expected_type in cls.inputs.items():
            try:
                data = kwargs[key]
            except KeyError as msg:  # pragma: no cover
                raise KeyError(
                    f"{key} not provided to RailPlotter {cls} in {list(kwargs.keys())}"
                ) from msg
            if isinstance(expected_type, GenericAlias):
                if not isinstance(data, expected_type.__origin__):  # pragma: no cover
                    raise TypeError(
                        f"{key} provided to RailPlotter was "
                        f"{type(data)}, not {expected_type.__origin__}"
                    )
                continue
            if not isinstance(data, expected_type):  # pragma: no cover
                raise TypeError(
                    f"{key} provided to RailPlotter was {type(data)}, expected {expected_type}"
                )

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, Figure]:
        raise NotImplementedError()

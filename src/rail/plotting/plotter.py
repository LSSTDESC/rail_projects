from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from ceci.config import StageParameter

from rail.projects.configurable import Configurable
from rail.projects.dynamic_class import DynamicClass

from .dataset import RailDataset
from .dataset_holder import RailDatasetHolder
from .plot_holder import RailPlotDict

if TYPE_CHECKING:
    from .plot_holder import RailPlotHolder
    from .plotter_factory import RailPlotterFactory


class RailPlotter(Configurable, DynamicClass):
    """Base class for making matplotlib plot

    The main function in this class is:

    .. highlight:: python
    .. code-block:: python

      run(prefix: str, kwargs**: Any) -> dict[str, RailPlotHolder]

    This function will make a set of plots and return them in a dict.
    prefix is string that gets prepended to plot names.

    The data to be plotted is passed in via the kwargs.


    Sub-classes should implement

    .. highlight:: python
    .. code-block:: python

      config_options: dict[str, ceci.StageParameter]

    that will be used to configure things like the axes binning, selection functions,
    and other plot-specfic options

    .. highlight:: python
    .. code-block:: python

      input_type: RailPZPointEstimateDataset

    that specifics the inputs
    that the sub-classes expect, this is used the check the kwargs
    that are passed to the `run` function.

    A function:

    .. highlight:: python
    .. code-block:: python

      _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:

    That actually makes the plots.  It does not need to do the checking
    that the correct kwargs have been given.
    """

    input_type: type[RailDataset] = RailDataset

    sub_classes: dict[str, type[DynamicClass]] = {}

    yaml_tag = "Plotter"

    @staticmethod
    def iterate_plotters(
        name: str,
        plotters: list[RailPlotter],
        prefix: str,
        dataset: RailDatasetHolder,
        **kwargs: Any,
    ) -> RailPlotDict:
        """Utility function to run several plotters on the same data

        Parameters
        ----------
        name: str
            Name to give to the RailPlotDict

        plotters: list[RailPlotter]
            Plotters to run

        prefix: str
            Prefix to append to plot names, e.g., the p(z) algorithm or
            analysis 'flavor'

        kwargs: dict[str, Any]
            Used to pass the data to make the plots

        Returns
        -------
        out_dict: RailPlotDict
            Dictionary of the newly created figures
        """
        out_dict: dict[str, RailPlotHolder] = {}
        extra_args: dict[str, Any] = dict(dataset_holder=dataset)
        for plotter_ in plotters:
            out_dict.update(
                plotter_.run(prefix, **dataset.resolve(), **kwargs, **extra_args)
            )
        return RailPlotDict(name=name, plots=out_dict)

    @staticmethod
    def iterate(
        plotters: list[RailPlotter],
        datasets: list[RailDatasetHolder],
        **kwargs: Any,
    ) -> dict[str, RailPlotDict]:
        """Utility function to several plotters of several data sets

        Parameters
        ----------
        plotters: list[RailPlotter]
            Plotters to run

        datasets: list[RailDatasetHolder]
            Prefixes and datasets to iterate over

        Returns
        -------
        out_dict: dict[str, RailPlotDict]
            Dictionary of the newly created figures
        """
        out_dict: dict[str, RailPlotDict] = {}
        for val in datasets:
            out_dict[val.config.name] = RailPlotter.iterate_plotters(
                val.config.name, plotters, "", val, **kwargs
            )
        return out_dict

    @staticmethod
    def write_plots(
        fig_dict: dict[str, RailPlotDict],
        outdir: str = ".",
        figtype: str = "png",
        purge: bool = False,
    ) -> None:
        """Utility function to write several plots do disk

        Parameters
        ----------
        fig_dict: dict[str, RailPlotDict]
            Dictionary of figures to write

        outdir: str
            Directory to write figures in

        figtype: str
            Type of figures to write, e.g., png, pdf...

        purge: bool
            Delete figure after saving
        """
        for key, val in fig_dict.items():
            try:
                os.makedirs(outdir)
            except Exception:
                pass
            out_path = os.path.join(outdir, key)
            val.savefigs(out_path, figtype=figtype, purge=purge)

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this plotter, must match
            class.config_options data members
        """
        DynamicClass.__init__(self)
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self)}"

    def run(
        self,
        prefix: str,
        **kwargs: dict[str, Any],
    ) -> dict[str, RailPlotHolder]:
        """Make all the plots given the data

        Parameters
        ----------
        prefix: str
            Prefix to append to plot names, e.g., the p(z) algorithm or
            analysis 'flavor'

        kwargs: dict[str, Any]
            Used to pass the data to make the plots

        Returns
        -------
        out_dict: dict[str, RailPlotHolder]
            Dictionary of the newly created figures
        """
        self._validate_inputs(**kwargs)
        return self._make_plots(prefix, **kwargs)

    def _make_full_plot_name(self, prefix: str, plot_name: str) -> str:
        """Create the make for a specific plot

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
            Plot name, following the pattern f"{prefix}{self._name}{plot_name}"
        """
        return f"{prefix}{self.config.name}{plot_name}"

    def to_yaml_dict(self) -> dict[str, dict[str, Any]]:
        """Create a yaml-convertable dict for this object"""
        yaml_dict = Configurable.to_yaml_dict(self)
        yaml_dict[self.yaml_tag].update(class_name=f"{self.full_class_name()}")
        return yaml_dict

    @classmethod
    def _validate_inputs(cls, **kwargs: Any) -> None:
        cls.input_type.validate_inputs(**kwargs)

    def _make_plots(
        self,
        prefix: str,
        **kwargs: Any,
    ) -> dict[str, RailPlotHolder]:
        raise NotImplementedError()


class RailPlotterList(Configurable):
    """The class collects a set of plotter that can all run on the same data.

    E.g., plotters that can all run on a dict that looks like
    `{truth:np.ndarray, pointEstimates: np.ndarray}` could be put into a PlotterList.
    This make it easier to collect similar types of plots.
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="PlotterList name"),
        dataset_holder_class=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Dataset holder that provides datset types expected by plotter on the list",
        ),
        plotters=StageParameter(
            list,
            [],
            fmt="%s",
            msg="List of plotter to include",
        ),
    )

    yaml_tag = "PlotterList"

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailPlotterListHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.plotters}"

    def resolve(self, plotter_factory: RailPlotterFactory) -> list[RailPlotter]:
        """Extract the plotters

        Paramters
        ---------
        plotter_factory:
            Factory used to make the plotters.

        Returns
        -------
        list[RailPlotter]
            Requested plotters.

        Notes
        -----
        This will enforce that each plotter expects the compatible dataset_types
        """

        the_list: list[RailPlotter] = []

        dataset_holder_class = RailDatasetHolder.load_sub_class(
            self.config.dataset_holder_class
        )
        dataset_class = dataset_holder_class.output_type

        for name_ in self.config.plotters:
            a_plotter = plotter_factory.get_plotter(name_)
            if not issubclass(a_plotter.input_type, dataset_class):  # pragma: no cover
                raise TypeError(
                    f"Plotter.input_type {a_plotter.input_type} is"
                    f"not a subclass of PlotterList dataset_class {dataset_class}."
                )
            the_list.append(a_plotter)
        return the_list

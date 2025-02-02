from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from matplotlib.figure import Figure

if TYPE_CHECKING:
    from .dataset_holder import RailDatasetHolder
    from .plotter import RailPlotter


class RailPlotHolder:
    """Simple class for wrapping matplotlib Figure

    This includes the path to where the figure is saved
    """

    def __init__(
        self,
        name: str,
        path: str | None = None,
        figure: Figure | None = None,
        plotter: RailPlotter | None = None,
        dataset_holder: RailDatasetHolder | None = None,
    ):
        """C'tor"""
        self._name = name
        self._path = path
        self._figure = figure
        self._plotter = plotter
        self._dataset_holder = dataset_holder

    @property
    def name(self) -> str:
        """Return the name of the plot"""
        return self._name

    @property
    def path(self) -> str | None:
        """Return the path to the saved plot"""
        return self._path

    @property
    def figure(self) -> Figure | None:
        """Return the matplotlib Figure"""
        return self._figure

    @property
    def plotter(self) -> RailPlotter | None:
        """Return the object used to make the plot"""
        return self._plotter

    @property
    def dataset_holder(self) -> RailDatasetHolder | None:
        """Return the dataset used to make the plot"""
        return self._dataset_holder

    def set_path(
        self,
        path: str | None = None,
    ) -> None:
        """Set the path to the saved plot"""
        self._path = path

    def set_figure(
        self,
        figure: Figure | None = None,
    ) -> None:
        """Set the Matplotlib figure"""
        self._figure = figure

    def savefig(
        self,
        relpath: str,
        outdir: str = ".",
        **kwargs: Any,
    ) -> None:
        if self.figure is None:  # pragma: no cover
            raise ValueError(f"Tried to savefig missing a Figure {self.name}")

        self.set_path(relpath)
        fullpath = os.path.join(outdir, relpath)
        self.figure.savefig(fullpath, **kwargs)


class RailPlotDict:
    """Simple class for dicts of matplotlib Figures

    This collects a set of plots made on the same dataset.
    The key is typically the name of the RailPlotter used to make each plot.
    """

    def __init__(self, name: str, plots: dict[str, RailPlotHolder]):
        self._name = name
        self._plots = plots

    @property
    def name(self) -> str:
        """Return the name of the plot dict"""
        return self._name

    @property
    def plots(self) -> dict[str, RailPlotHolder] | None:
        """Return the dict of plots"""
        return self._plots

    def savefigs(
        self,
        outpath: str,
        figtype: str = "png",
        **kwargs: Any,
    ) -> None:
        purge = kwargs.pop("purge", False)
        if not os.path.exists(outpath):  # pragma: no cover
            os.makedirs(outpath)
        for _key, val in self._plots.items():
            if val.path:  # pragma: no cover
                val.savefig(val.path, outpath, **kwargs)
            else:
                val.savefig(
                    os.path.join(os.path.basename(outpath), f"{val.name}.{figtype}"),
                    os.path.dirname(outpath),
                    **kwargs,
                )
            if purge:
                val.set_figure(None)

from __future__ import annotations

from typing import Any

from matplotlib.figure import Figure


class RailPlotHolder:
    """ Simple class for wrapping matplotlib Figure """
    def __init__(
        self,
        name: str,
        path: str | None = None,
        figure: Figure | None = None,
    ):
        """ C'tor """
        self._name = name
        self._path = path
        self._figure = figure

    @property
    def name(self) -> str:
        """ Return the name of the plot """
        return self._name

    @property
    def path(self) -> str | None:
        """ Return the path to the saved plot """
        return self._path

    @property
    def figure(self) -> Figure | None:
        """ Return the matplotlib Figure """
        return self._figure

    def set_path(
        self,
        path: str | None = None,
    ) -> None:
        """ Set the path to the saved plot """
        self._path = path

    def set_figure(
        self,
        figure: Figure | None = None,
    ) -> None:
        """ Set the Matplotlib figure """
        self._figure = figure

    def savefig(
        self,
        outpath: str,
        **kwargs: Any,
    ) -> None:
        if self.figure is None:  # pragma: no cover
            raise ValueError(f"Tried to savefig missing a Figure {self.name}")
        self.set_path(outpath)
        self.figure.savefig(self.path, **kwargs)

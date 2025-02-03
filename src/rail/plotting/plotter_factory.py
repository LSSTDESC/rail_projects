from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from rail.projects.factory_mixin import RailFactoryMixin

from .plotter import RailPlotter, RailPlotterList

if TYPE_CHECKING:
    from rail.projects.configurable import Configurable

    C = TypeVar("C", bound="Configurable")


class RailPlotterFactory(RailFactoryMixin):
    """Factory class to make plotters

    Expected usage is that user will define a yaml file with the various
    plotters that they wish to use with the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Plots:
        - Plotter:
            name: zestimate_v_ztrue_hist2d
            class_name: rail.plotters.pz_plotters.PZPlotterPointEstimateVsTrueHist2D
            z_min: 0.0
            z_max: 3.0
            n_zbins: 150
      - Plotter:
            name: zestimate_v_ztrue_profile
            class_name: rail.plotters.pz_plotters.PZPlotterPointEstimateVsTrueProfile
            z_min: 0.0
            z_max: 3.0
            n_zbins: 60

    And group them into lists of plotter that can be run over particular types
    of data, using the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Plots:
        - PlotterList:
            name: z_estimate_v_z_true
            plotters:
              - zestimate_v_ztrue_hist2d
              - zestimate_v_ztrue_profile
    """

    yaml_tag: str = "Plots"

    client_classes = [RailPlotter, RailPlotterList]

    _instance: RailPlotterFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailPlotterFactory"""
        RailFactoryMixin.__init__(self)
        self._plotters = self.add_dict(RailPlotter)
        self._plotter_lists = self.add_dict(RailPlotterList)

    @classmethod
    def get_plotters(cls) -> dict[str, RailPlotter]:
        """Return the dict of all the plotters"""
        return cls.instance().plotters

    @classmethod
    def get_plotter_names(cls) -> list[str]:
        """Return the names of the plotters"""
        return list(cls.instance().plotters.keys())

    @classmethod
    def get_plotter_lists(cls) -> dict[str, RailPlotterList]:
        """Return the dict of all the plotters"""
        return cls.instance().plotter_lists

    @classmethod
    def get_plotter_list_names(cls) -> list[str]:
        """Return the names of the plotter lists"""
        return list(cls.instance().plotter_lists.keys())

    @classmethod
    def get_plotter(cls, name: str) -> RailPlotter:
        """Get plotter it's assigned name

        Parameters
        ----------
        name: str
            Name of the plotter to return

        Returns
        -------
        plotter: RailPlotter
            Plotter in question
        """
        try:
            return cls.instance().plotters[name]
        except KeyError as msg:
            raise KeyError(
                f"Plotter named {name} not found in RailPlotterFactory "
                f"{list(cls.instance().plotters.keys())}"
            ) from msg

    @classmethod
    def get_plotter_list(cls, name: str) -> RailPlotterList:
        """Get a list of plotters their assigned name

        Parameters
        ----------
        name: str
            Name of the plotter list to return

        Returns
        -------
        plotters: list[RailPlotter]
            Plotters in question
        """
        try:
            return cls.instance().plotter_lists[name]
        except KeyError as msg:
            raise KeyError(
                f"PlotterList named {name} not found in RailPlotterFactory "
                f"{list(cls.instance().plotter_lists.keys())}"
            ) from msg

    @classmethod
    def add_plotter(cls, plotter: RailPlotter) -> None:
        """Add a particular RailPlotter to the factory"""
        cls.instance().add_to_dict(plotter)

    @classmethod
    def add_plotter_list(cls, plotter_list: RailPlotterList) -> None:
        """Add a particular RailPlotGroupRailPlotterList to the factory"""
        cls.instance().add_to_dict(plotter_list)

    @property
    def plotters(self) -> dict[str, RailPlotter]:
        """Return the dictionary of individual RailPlotter objects"""
        return self._plotters

    @property
    def plotter_lists(self) -> dict[str, RailPlotterList]:
        """Return the dictionary of lists of RailPlotter objects"""
        return self._plotter_lists

    def load_object_from_yaml_tag(
        self, configurable_class: type[C], yaml_tag: dict[str, Any]
    ) -> None:
        if configurable_class == RailPlotter:
            the_object = RailPlotter.create_from_dict(yaml_tag)
            self.add_to_dict(the_object)
            return
        RailFactoryMixin.load_object_from_yaml_tag(self, configurable_class, yaml_tag)

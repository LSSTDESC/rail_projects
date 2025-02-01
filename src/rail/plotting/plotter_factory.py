from __future__ import annotations

from typing import Any, TypeVar, TYPE_CHECKING
import os
import yaml

from rail.projects.factory_mixin import RailFactoryMixin

from .plotter import RailPlotter, RailPlotterList

if TYPE_CHECKING:
    from rail.projects.configurable import Configurable

    C = TypeVar("C", bound="Configurable")


class RailPlotterFactory(RailFactoryMixin):
    """Factory class to make plotters

    Expected usage is that user will define a yaml file with the various
    plotters that they wish to use with the following example syntax:

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

    - PlotterList:
          name: z_estimate_v_z_true
          plotters:
              - zestimate_v_ztrue_hist2d
              - zestimate_v_ztrue_profile
    """

    client_classes = [RailPlotter, RailPlotterList]

    _instance: RailPlotterFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailPlotterFactory"""
        RailFactoryMixin.__init__(self)
        self._plotter_dict = self.add_dict(RailPlotter)
        self._plotter_list_dict = self.add_dict(RailPlotterList)

    @classmethod
    def get_plotter_dict(cls) -> dict[str, RailPlotter]:
        """Return the dict of all the plotters"""
        return cls.instance().plotter_dict

    @classmethod
    def get_plotter_names(cls) -> list[str]:
        """Return the names of the plotters"""
        return list(cls.instance().plotter_dict.keys())

    @classmethod
    def get_plotter_list_dict(cls) -> dict[str, RailPlotterList]:
        """Return the dict of all the plotters"""
        return cls.instance().plotter_list_dict

    @classmethod
    def get_plotter_list_names(cls) -> list[str]:
        """Return the names of the plotter lists"""
        return list(cls.instance().plotter_list_dict.keys())

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
            return cls.instance().plotter_dict[name]
        except KeyError as msg:
            raise KeyError(
                f"Plotter named {name} not found in RailPlotterFactory "
                f"{list(cls.instance().plotter_dict.keys())}"
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
            return cls.instance().plotter_list_dict[name]
        except KeyError as msg:
            raise KeyError(
                f"PlotterList named {name} not found in RailPlotterFactory "
                f"{list(cls.instance().plotter_list_dict.keys())}"
            ) from msg

    @property
    def plotter_dict(self) -> dict[str, RailPlotter]:
        """Return the dictionary of individual RailPlotter objects"""
        return self._plotter_dict

    @property
    def plotter_list_dict(self) -> dict[str, RailPlotterList]:
        """Return the dictionary of lists of RailPlotter objects"""
        return self._plotter_list_dict

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Plotters:")
        for plotter_name, plotter in self.plotter_dict.items():
            print(f"  {plotter_name}: {plotter}")
        print("----------------")
        print("PlotterLists")
        for plotter_list_name, plotter_list in self.plotter_list_dict.items():
            print(f"  {plotter_list_name}: {plotter_list}")

    def load_object_from_yaml_tag(
        self, configurable_class: type[C], yaml_tag: dict[str, Any]
    ) -> None:
        if configurable_class == RailPlotter:
            the_object = RailPlotter.create_from_dict(yaml_tag)
            self.add_to_dict(the_object)
            return
        RailFactoryMixin.load_object_from_yaml_tag(self, configurable_class, yaml_tag)

    def add_plotter(self, plotter: RailPlotter) -> None:
        self.add_to_dict(plotter)

    def add_plotter_list(self, plotter_list: RailPlotterList) -> None:
        self.add_to_dict(plotter_list)

    def load_plots_from_yaml_tag(
        self,
        plots_config: list[dict[str, Any]],
    ) -> None:
        """Read a yaml "Plots" tag and load the factory accordingy

        Parameters
        ----------
        plots_config: list[dict[str, Any]]
            Yaml tag to load

        Notes
        -----
        See class description for yaml file syntax
        """
        self.load_instance_yaml_tag(plots_config)

    def load_instance_yaml(self, yaml_file: str) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_file: str
            File to read

        Notes
        -----
        See `RailPlotterFactory.load_yaml` for yaml file syntax
        """
        with open(os.path.expandvars(yaml_file), encoding="utf-8") as fin:
            yaml_data = yaml.safe_load(fin)

        try:
            plotter_config = yaml_data["Plots"]
        except KeyError as missing_key:
            raise KeyError(f"Did not find key Plots in {yaml_file}") from missing_key

        self.load_plots_from_yaml_tag(plotter_config)

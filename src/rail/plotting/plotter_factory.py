from __future__ import annotations

from typing import Any
import yaml

from .plotter import RailPlotter


class RailPlotterFactory:
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
    _instance: RailPlotterFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailPlotterFactory"""
        if self._instance is not None:  # pragma: no cover
            raise ValueError("RailPlotterFactory instance already exists")
        self._plotter_dict: dict[str, RailPlotter] = {}
        self._plotter_list_dict: dict[str, list[RailPlotter]] = {}

    @classmethod
    def instance(cls) -> RailPlotterFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:  # pragma: no cover
            cls._instance = RailPlotterFactory()
        return cls._instance

    @classmethod
    def clear(cls) -> None:
        """Clear the contents of the factory"""
        if cls._instance is None:  # pragma: no cover
            return
        cls._instance.clear_instance()

    @classmethod
    def print_contents(cls) -> None:
        """Print the contents of the factory """
        if cls._instance is None:  # pragma: no cover
            cls._instance = RailPlotterFactory()
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
        The format of the yaml file should be

        - Plotter
              name: some_name
              class_name: rail.plotters.<filename>.<ClassName>
              <other_parameters>
        - Plotter
              name: some_other_name
              class_name: rail.plotters.<filename>.<ClassName>
              <other_parameters>
        - PlotterList
              name: plot_list_name
              plotters:
                  - some_name
                  - some_other_name
        """
        if cls._instance is None:
            cls._instance = RailPlotterFactory()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def get_plotter_dict(cls) -> dict[str, RailPlotter]:
        """Return the dict of all the plotters"""
        return cls.instance().plotter_dict

    @classmethod
    def get_plotter_names(cls) -> list[str]:
        """Return the names of the plotters"""
        return list(cls.instance().plotter_dict.keys())

    @classmethod
    def get_plotter_list_dict(cls) -> dict[str, list[RailPlotter]]:
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
    def get_plotter_list(cls, name: str) -> list[RailPlotter]:
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
    def plotter_list_dict(self) -> dict[str, list[RailPlotter]]:
        """Return the dictionary of lists of RailPlotter objects"""
        return self._plotter_list_dict

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._plotter_dict.clear()
        self._plotter_list_dict.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("Plotters:")
        for plotter_name, plotter in self.plotter_dict.items():
            print(f"  {plotter_name}: {plotter}")
        print("----------------")
        print("PlotterLists")
        for plotter_list_name, plotter_list in self.plotter_list_dict.items():
            print(f"  {plotter_list_name}: {plotter_list}")

    def _make_plotter(self, name: str, config_dict: dict[str, Any]) -> RailPlotter:
        if name in self._plotter_dict:  # pragma: no cover
            raise KeyError(f"Plotter {name} is already defined")
        plotter = RailPlotter.create_from_dict(name, config_dict)
        self._plotter_dict[name] = plotter
        return plotter

    def _make_plotter_list(
        self, name: str, plotter_list: list[str]
    ) -> list[RailPlotter]:
        if name in self._plotter_list_dict:  # pragma: no cover
            raise KeyError(f"PlotterList {name} is already defined")
        plotters: list[RailPlotter] = []
        for plotter_name in plotter_list:
            try:
                plotter = self._plotter_dict[plotter_name]
            except KeyError as msg:  # pragma: no cover
                raise KeyError(
                    f"RailPlotter {plotter_name} used in PlotterList "
                    f"is not found {list(self._plotter_dict.keys())}"
                ) from msg
            plotters.append(plotter)
        self._plotter_list_dict[name] = plotters
        return plotters

    def load_plotter_from_yaml_tag(self, plotter_config: dict[str, Any]) -> None:
        """Load a RailPlotter from a Plotter tag in yaml

        Paramters
        ---------
        plotter_config: dict[str, Any]
            Yaml data in question
        """
        try:
            name = plotter_config.pop("name")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "Plotter yaml block does not contain name for plotter: "
                f"{list(plotter_config.keys())}"
            ) from msg
        self._make_plotter(name, plotter_config)

    def load_plotter_list_from_yaml_tag(self, plotter_list_config: dict[str, Any]) -> None:
        """Load a list of RailPlotters from a PlotterList tag in yaml

        Paramters
        ---------
        plotter_list_config: dict[str, Any]
            Yaml data in question
        """
        try:
            name = plotter_list_config.pop("name")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "PlotterList yaml block does not contain name for plotter: "
                f"{list(plotter_list_config.keys())}"
            ) from msg
        try:
            plotters = plotter_list_config.pop("plotters")
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                f"PlotterList yaml block does not contain plotter: {list(plotter_list_config.keys())}"
            ) from msg
        self._make_plotter_list(name, plotters)

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
        with open(yaml_file, encoding="utf-8") as fin:
            plotter_data = yaml.safe_load(fin)

        for plotter_item in plotter_data:
            if "Plotter" in plotter_item:
                plotter_config = plotter_item["Plotter"]
                self.load_plotter_from_yaml_tag(plotter_config)
            elif "PlotterList" in plotter_item:  # pragma: no cover
                plotter_list_config = plotter_item["PlotterList"]
                self.load_plotter_list_from_yaml_tag(plotter_list_config)
            else:  # pragma: no cover
                good_keys = ["Plotter", "PlotterList"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {plotter_data.keys()})"
                )

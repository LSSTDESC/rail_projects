"""Class to get track of groups of plots to make"""
from __future__ import annotations

import os
from typing import Any

import yaml
from matplotlib.figure import Figure

from .dataset_factory import RailDatasetFactory
from .plotter_factory import RailPlotterFactory
from .plotter import RailPlotter


class RailPlotGroup:
    """ Defining of a group on plots to make
    with a particular dataset
    """

    def __init__(
        self,
        plotter_list_name: str,
        dataset_dict_name: str,
        outdir: str=".",
        figtype: str="png",
    ):
        self.plotter_list_name = plotter_list_name
        self.dataset_dict_name = dataset_dict_name
        self.outdir = outdir
        self.figtype = figtype
        self._plots: dict[str, Figure] = {}

    @staticmethod
    def make_plots(
        plotter_list_name: str,
        datatset_dict_name: str,
    ) -> dict[str, Figure]:
        """ Make a set of plots

        Parameters
        ----------
        plotter_list_name: str
            Name of the plotter list to use to make the plots.
           This needs to have been previous loaded.

        datatset_dict_name: str
            Name of the dataset list to use to make the plots.
            This needs to have been previous loaded.

        Returns
        -------
        out_dict: dict[str, Figure]
            Dictionary of the newly created figures
        """
        plotter_list = RailPlotterFactory.get_plotter_list(plotter_list_name)
        dataset_dict = RailDatasetFactory.get_dataset_dict(datatset_dict_name)
        out_dict = RailPlotter.iterate(plotter_list, dataset_dict)
        return out_dict

    def __call__(
        self,
        save: bool=True,
        purge: bool=True,
        outdir: str | None=None
    ) -> dict[str, Figure]:
        """ Make all the plots given the data

        Parameters
        ----------
        save: bool
            If true, save the plots to disk

        purge: bool
            If true, delete the plots after saving

        outdir: str | None
            If set, prepend this to the groups output dir

        Returns
        -------
        out_dict: dict[str, Figure]
            Dictionary of the newly created figures
        """
        self._plots.update(
            self.make_plots(
                self.plotter_list_name,
                self.dataset_dict_name,
            ),
        )
        if save:
            if outdir is not None:
                output_dir = os.path.join(outdir, self.outdir)
            else:  # pragma: no cover
                output_dir = self.outdir
            RailPlotter.write_plots(self._plots, output_dir, self.figtype)
            if purge:
                self._plots.clear()
        return self._plots

    @classmethod
    def create(
        cls,
        config_dict: dict[str, Any],
    ) -> RailPlotGroup:
        """ Create a RailPlotGroup object

        Parameters
        ----------
        config_dict: dict[str, Any]
            Config parameters for this group, passed to c'tor

        Returns
        -------
        plot_group: RailPlotGroup
            Newly created object
        """
        return cls(**config_dict)

    @classmethod
    def load_yaml(
        cls,
        yaml_file: str,
    ) -> dict[str, RailPlotGroup]:
        """Read a yaml file and load build the RailPlotGroup objects

        Parameters
        ----------
        yaml_file: str
            File to read

        Notes
        -----
        The yaml file should look something like this:

        PlotterYaml: <path_to_yaml_file_defining_plotter_lists>
        DatasetYaml: <path_to_yaml_file defining_dataset_lists>
        PlotGroups:
            - PlotGroup:
                  name: some_name
                  plotter_list_name: nice_plots
                  dataset_dict_name: nice_data
            - PlotGroup:
                  name: some_other_name
                  plotter_list_name: janky_plots
                  dataset_dict_name: janky_data
        """
        out_dict: dict[str, RailPlotGroup] = {}
        with open(yaml_file, encoding="utf-8") as fin:
            all_data = yaml.safe_load(fin)

        try:
            plotter_yaml = all_data['PlotterYaml']
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "yaml file does not contain PlotterYaml key "
                f"{list(all_data.keys())}"
            ) from msg
        RailPlotterFactory.load_yaml(plotter_yaml)

        try:
            dataset_yaml = all_data['DatasetYaml']
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "yaml file does not contain DatasetYaml key "
                f"{list(all_data.keys())}"
            ) from msg

        try:
            group_data = all_data['PlotGroups']
        except KeyError as msg:  # pragma: no cover
            raise KeyError(
                "yaml file does not contain PlotGroups key "
                f"{list(all_data.keys())}"
            ) from msg
        RailDatasetFactory.load_yaml(dataset_yaml)

        for group_item in group_data:
            try:
                plot_group_config = group_item["PlotGroup"]
            except KeyError as msg:  # pragma: no cover
                raise KeyError(
                    "expected PlotGroup yaml block. "
                    f"{list(group_item.keys())}"
                ) from msg
            try:
                name = plot_group_config.pop("name")
            except KeyError as msg:  # pragma: no cover
                raise KeyError(
                    "PlotGroup yaml block does not contain name for plot group: "
                    f"{list(plot_group_config.keys())}"
                ) from msg
            out_dict[name] = cls.create(plot_group_config)

        return out_dict
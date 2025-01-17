"""Class to get track of groups of plots to make"""
from __future__ import annotations

import os
from typing import Any

import yaml
from matplotlib.figure import Figure

from jinja2 import Environment, FileSystemLoader, Template

from .dataset_factory import RailDatasetFactory
from .plotter_factory import RailPlotterFactory
from .plotter import RailPlotter


class RailPlotGroup:
    """ Defining of a group on plots to make
    with a particular dataset
    """

    jinja_env: Environment | None = None
    jinja_template: Template | None = None

    @classmethod
    def _load_jinja(cls) -> None:
        if cls.jinja_template is not None:  # pragma: no cover
            return
        cls.jinja_env = Environment(
            loader=FileSystemLoader('src/rail/projects/html_templates')
        )
        cls.jinja_template = cls.jinja_env.get_template('plot_group_table.html')

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

    @staticmethod
    def find_plots(
        plotter_list_name: str,
        datatset_dict_name: str,
        outdir: str='.',
        figtype: str='png',
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
        out_dict = RailPlotter.iterate(
            plotter_list, dataset_dict, find_only=True, outdir=outdir, figtype=figtype,
        )
        return out_dict

    def make_html(
        self,
        outfile: str,
    ) -> None:
        self._load_jinja()
        assert self.jinja_template is not None

        # Render template  data and save to HTML file
        output = self.jinja_template.render(plot_dict=self._plots, os=os)
        with open(outfile, 'w', encoding="utf-8") as file:
            file.write(output)

    def __call__(
        self,
        save_plots: bool=True,
        purge_plots: bool=True,
        find_only: bool=False,
        outdir: str | None=None,
        make_html: bool=False,
        output_html: str | None=None,
    ) -> dict[str, Figure]:
        """ Make all the plots given the data

        Parameters
        ----------
        save_plots: bool
            If true, save the plots to disk

        purge_plots: bool
            If true, delete the plots after saving

        find_only: bool
            If true, only look for existing plots

        make_html: bool
            If true, make an html table to browse plots

        outdir: str | None
            If set, prepend this to the groups output dir

        output_html: str | None
            Path for output html file

        Returns
        -------
        out_dict: dict[str, Figure]
            Dictionary of the newly created figures
        """
        if outdir is not None:
            output_dir = os.path.join(outdir, self.outdir)
        else:  # pragma: no cover
            output_dir = self.outdir

        if find_only:
            self._plots.update(
                self.find_plots(
                    self.plotter_list_name,
                    self.dataset_dict_name,
                    outdir=output_dir,
                    figtype=self.figtype,
                ),
            )
        else:
            self._plots.update(
                self.make_plots(
                    self.plotter_list_name,
                    self.dataset_dict_name,
                ),
            )
            if save_plots:
                RailPlotter.write_plots(self._plots, output_dir, self.figtype, purge=purge_plots)
        if make_html:
            if output_html is None:  # pragma: no cover
                raise ValueError("RailPlotGroup, make_html is set, but not output_html path given")
            self.make_html(output_html)
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

        - PlotterYaml:
              path: <path_to_yaml_file_defining_plotter_lists>
        - DatasetYaml:
              path: <path_to_yaml_file defining_dataset_lists>
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

        for group_item in all_data:
            if "PlotterYaml" in group_item:
                plotter_yaml_config = group_item["PlotterYaml"]
                try:
                    plotter_yaml_path = plotter_yaml_config.pop("path")
                except KeyError as msg:  # pragma: no cover
                    raise KeyError(
                        "PlotterYaml yaml block does not contain path: "
                        f"{list(plotter_yaml_config.keys())}"
                    ) from msg
                RailPlotterFactory.load_yaml(plotter_yaml_path)
            elif "DatasetYaml" in group_item:
                dataset_yaml_config = group_item["DatasetYaml"]
                try:
                    dataset_yaml_path = dataset_yaml_config.pop("path")
                except KeyError as msg:  # pragma: no cover
                    raise KeyError(
                        "PlotterYamlDatasetYaml yaml block does not contain path: "
                        f"{list(dataset_yaml_config.keys())}"
                    ) from msg
                RailDatasetFactory.load_yaml(dataset_yaml_path)
            elif "PlotGroup" in group_item:
                plot_group_config = group_item["PlotGroup"]
                try:
                    name = plot_group_config.pop("name")
                except KeyError as msg:  # pragma: no cover
                    raise KeyError(
                        "PlotGroup yaml block does not contain name for plot group: "
                        f"{list(plot_group_config.keys())}"
                    ) from msg
                out_dict[name] = cls.create(plot_group_config)
            else:  # pragma: no cover
                good_keys = ["PlotterYaml", "DatasetYaml", "PlotGroup"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {group_item.keys()})"
                )

        return out_dict

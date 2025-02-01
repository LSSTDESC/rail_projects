"""Class to get track of groups of plots to make"""

from __future__ import annotations

import os
from typing import Any

from jinja2 import Environment, FileSystemLoader, Template

from ceci.config import StageParameter
from rail.projects.configurable import Configurable

from .dataset_factory import RailDatasetFactory
from .plotter_factory import RailPlotterFactory
from .plotter import RailPlotter
from .plot_holder import RailPlotDict, RailPlotHolder
from .dataset_holder import RailDatasetHolder

HTML_TEMPLATE_DIR = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "html_templates"
)


class RailPlotGroup(Configurable):
    """Defining of a group on plots to make
    with a particular dataset
    """

    jinja_env: Environment | None = None
    jinja_template: Template | None = None
    jinja_index_template: Template | None = None

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(
            str, None, fmt="%s", required=True, msg="PlotGroupName name"
        ),
        plotter_list_name=StageParameter(
            str, None, fmt="%s", required=True, msg="PlotterList name"
        ),
        dataset_list_name=StageParameter(
            str, None, fmt="%s", required=True, msg="DatasetList name"
        ),
        outdir=StageParameter(str, ".", fmt="%s", msg="Output directory"),
        figtype=StageParameter(str, "png", fmt="%s", msg="Plot type"),
    )

    yaml_tag = "PlotGroup"

    @classmethod
    def _load_jinja(cls) -> None:
        if cls.jinja_template is not None:  # pragma: no cover
            return
        cls.jinja_env = Environment(loader=FileSystemLoader(HTML_TEMPLATE_DIR))
        cls.jinja_template = cls.jinja_env.get_template("plot_group_table.html")
        cls.jinja_index_template = cls.jinja_env.get_template("plot_group_index.html")

    def __init__(self, **kwargs: Any) -> None:
        Configurable.__init__(self, **kwargs)
        self._plots: dict[str, RailPlotDict] = {}
        self._plotter_list: list[RailPlotter] = []
        self._dataset_list: list[RailDatasetHolder] = []

    def __repr__(self) -> str:
        return f"PlotGroup: {self.config.plotter_list_name}, DatasetList: {self.config.dataset_list_name}"

    @property
    def plotter_list(self) -> list[RailPlotter]:
        return self._plotter_list

    @property
    def dataset_list(self) -> list[RailDatasetHolder]:
        return self._dataset_list

    def find_plot(self, dataset_name: str, plotter_name: str) -> RailPlotHolder:
        try:
            sub_dict = self._plots[dataset_name]
        except KeyError as msg:
            raise KeyError(
                f"Failed to find {dataset_name} in {list(self._plots.keys())}"
            ) from msg
        assert sub_dict.plots
        try:
            return sub_dict.plots[plotter_name]
        except KeyError as msg:
            raise KeyError(
                f"Failed to find {plotter_name} in {list(sub_dict.plots.keys())}"
            ) from msg

    def find_plot_path(self, dataset_name: str, plotter_name: str) -> str | None:
        return self.find_plot(dataset_name, plotter_name).path

    def make_plots(
        self,
    ) -> dict[str, RailPlotDict]:
        """Make a set of plots

        Returns
        -------
        out_dict: dict[str, RailPlotDict]
            Dictionary of the newly created figures
        """
        plotter_factory = RailPlotterFactory.instance()
        dataset_factory = RailDatasetFactory.instance()
        self._plotter_list = plotter_factory.get_plotter_list(
            self.config.plotter_list_name
        )(plotter_factory)
        self._dataset_list = dataset_factory.get_dataset_list(
            self.config.dataset_list_name
        )(dataset_factory)
        self._plots.update(
            **RailPlotter.iterate(self._plotter_list, self._dataset_list)
        )
        return self._plots

    def find_plots(
        self,
        outdir: str,
    ) -> dict[str, RailPlotDict]:
        """Make a set of plots

        Parameters
        ----------
        outdir: str
            Prepend this to the groups output dir

        Returns
        -------
        out_dict: dict[str, Figure]
            Dictionary of the newly created figures
        """
        plotter_factory = RailPlotterFactory.instance()
        dataset_factory = RailDatasetFactory.instance()
        self._plotter_list = plotter_factory.get_plotter_list(
            self.config.plotter_list_name
        )(plotter_factory)
        self._dataset_list = dataset_factory.get_dataset_list(
            self.config.dataset_list_name
        )(dataset_factory)
        self._plots.update(
            **RailPlotter.iterate(
                self._plotter_list,
                self._dataset_list,
                find_only=True,
                outdir=outdir,
                figtype=self.config.figtype,
            )
        )
        return self._plots

    @classmethod
    def make_html_index(
        cls,
        outfile: str,
        output_pages: list[str],
    ) -> None:
        cls._load_jinja()
        assert cls.jinja_index_template is not None

        # Render template  data and save to HTML file
        output = cls.jinja_index_template.render(output_pages=output_pages, os=os)
        with open(outfile, "w", encoding="utf-8") as file:
            file.write(output)

    def make_html(
        self,
        outfile: str,
    ) -> None:
        self._load_jinja()
        assert self.jinja_template is not None

        # Render template  data and save to HTML file
        output = self.jinja_template.render(plot_group=self, os=os)
        with open(outfile, "w", encoding="utf-8") as file:
            file.write(output)

    def __call__(
        self,
        save_plots: bool = True,
        purge_plots: bool = True,
        find_only: bool = False,
        outdir: str | None = None,
        make_html: bool = False,
        output_html: str | None = None,
    ) -> dict[str, RailPlotDict]:
        """Make all the plots given the data

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
            output_dir = os.path.join(outdir, self.config.outdir)
        else:  # pragma: no cover
            output_dir = self.config.outdir

        if find_only:
            self.find_plots(
                outdir=output_dir,
            )
        else:
            self.make_plots()
            if save_plots:
                RailPlotter.write_plots(
                    self._plots, output_dir, self.config.figtype, purge=purge_plots
                )
        if make_html:
            if output_html is None:
                assert outdir
                output_html = os.path.join(outdir, f"plots_{self.config.name}.html")
            self.make_html(output_html)
        return self._plots

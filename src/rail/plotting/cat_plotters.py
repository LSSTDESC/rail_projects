from __future__ import annotations

import os
from typing import Any

import numpy as np
from astropy.stats import biweight_location, biweight_scale
from ceci.config import StageParameter
from matplotlib import colors
from matplotlib import pyplot as plt
from scipy.stats import sigmaclip

from rail.raruma import plotting_functions as raruma_plot
from rail.raruma import utility_functions as raruma_util

from .dataset import RailDataset
from .dataset_holder import RailDatasetHolder
from .plot_holder import RailPlotHolder
from .plotter import RailPlotter


class RailCatTruthDataset(RailDataset):
    """Dataset to hold any array of true redshifts
    """
    data_types = dict(truth=np.ndarray)


class RailCatMagnitudesDataset(RailDataset):
    """Dataset to hold any array of magntidues and list of band names
    """

    data_types = dict(magnitudes=np.ndarray, bands=list)


class RailCatTruthAndMagnitudesDataset(RailCatTruthDataset, RailCatMagnitudesDataset):
    """Dataset to hold any array of magntidues and list of band names
    and true (or spec) redshifts
    """

    data_types = RailCatTruthAndMagnitudesDataset.copy()
    data_types.update(**RailCatTruthDataset.data_typles)


class CatPlotterTruth(RailPlotter):
    """Class to make a histogram magnitudes in each band
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0., fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3., fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 151, fmt="%i", msg="Number of Redshift bins"),
    )

    input_type = RailCatTruthDataset

    def _make_hist_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:

        figure = raruma_plot.plot_true_nz(truth)
        plot_name = self._make_full_plot_name(prefix, "")

        return RailPlotHolder(
            name=plot_name, figure=figure, plotter=self, dataset_holder=dataset_holder
        )

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get("find_only", False)
        figtype = kwargs.get("figtype", "png")
        dataset_holder = kwargs.get("dataset_holder")
        out_dict: dict[str, RailPlotHolder] = {}
        truth: np.ndarray = kwargs["truth"]
        if find_only:
            plot_name = self._make_full_plot_name(prefix, "")
            assert dataset_holder
            plot = RailPlotHolder(
                name=plot_name,
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_hist_plot(
                prefix=prefix,
                truth=truth,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict
    

    
class CatPlotterMagntidues(RailPlotter):
    """Class to make a histogram magnitudes in each band
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        mag_min=StageParameter(float, 18.0, fmt="%0.2f", msg="Minimum Magnitude"),
        mag_max=StageParameter(float, 25.0, fmt="%0.2f", msg="Maximum Magnitude"),
        n_magbins=StageParameter(int, 141, fmt="%i", msg="Number of magnitude bins"),
    )

    input_type = RailCatMagnitudesDataset

    def _make_hist_plots(
        self,
        prefix: str,
        magnitudes: np.ndarray,
        bands: list[str],
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:

        figure = raruma_plot.plot_feature_histograms(magnitudes, bands)
        plot_name = self._make_full_plot_name(prefix, "")

        return RailPlotHolder(
            name=plot_name, figure=figure, plotter=self, dataset_holder=dataset_holder
        )

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get("find_only", False)
        figtype = kwargs.get("figtype", "png")
        dataset_holder = kwargs.get("dataset_holder")
        out_dict: dict[str, RailPlotHolder] = {}
        magnitudes: np.ndarray = kwargs["magnitudes"]
        bands: list[str] = kwargs["bands"]
        if find_only:
            plot_name = self._make_full_plot_name(prefix, "")
            assert dataset_holder
            plot = RailPlotHolder(
                name=plot_name,
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_hist_plots(
                prefix=prefix,
                magnitudes=magnitudes,
                bands=bands,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict
    

class CatPlotterMagntiduesVsTruth(RailPlotter):
    """Class to make 2D histograms of magntidue
    versus true redshift in each band
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        mag_min=StageParameter(float, 18.0, fmt="%0.2f", msg="Minimum Magnitude"),
        mag_max=StageParameter(float, 25.0, fmt="%0.2f", msg="Maximum Magnitude"),
        n_magbins=StageParameter(int, 141, fmt="%i", msg="Number of magnitude bins"),
    )

    input_type = RailCatTruthAndMagnitudesDataset

    def _make_2d_hist_plots(
        self,
        prefix: str,
        truth: np.ndarray,
        magnitudes: np.ndarray,
        bands: list[str],
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:

        figure = raruma_plot.plot_feature_target_hist2d(magnitudes, truth, bands)
        plot_name = self._make_full_plot_name(prefix, "")

        return RailPlotHolder(
            name=plot_name, figure=figure, plotter=self, dataset_holder=dataset_holder
        )

    
    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get("find_only", False)
        figtype = kwargs.get("figtype", "png")
        dataset_holder = kwargs.get("dataset_holder")
        out_dict: dict[str, RailPlotHolder] = {}
        truth: np.ndarray = kwargs["truth"]
        magnitudes: np.ndarray = kwargs["magnitudes"]
        bands: list[str] = kwargs["bands"]
        if find_only:
            plot_name = self._make_full_plot_name(prefix, "")
            assert dataset_holder
            plot = RailPlotHolder(
                name=plot_name,
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_2d_hist_plots(
                prefix=prefix,
                truth=truth,
                magnitudes=magnitudes,
                bands=bands,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict



class CatPlotterColorsVsTruth(RailPlotter):
    """Class to make 2D histograms of colors
    versus true redshift in each band
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 151, fmt="%i", msg="Number of Redshift bins"),
    )

    input_type = RailCatTruthAndMagnitudesDataset

    def _make_2d_hist_plots(
        self,
        prefix: str,
        truth: np.ndarray,
        magnitudes: np.ndarray,
        bands: list[str],
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:

        colors = raruma_util.adjacent_band_colors(magnitudes)
        color_names = raruma_util.adjacent_band_color_namees(bands)
        figure = raruma_plot.plot_feature_target_hist2d(colors, truth, color_names)
        plot_name = self._make_full_plot_name(prefix, "")

        return RailPlotHolder(
            name=plot_name, figure=figure, plotter=self, dataset_holder=dataset_holder
        )

    
    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get("find_only", False)
        figtype = kwargs.get("figtype", "png")
        dataset_holder = kwargs.get("dataset_holder")
        out_dict: dict[str, RailPlotHolder] = {}
        truth: np.ndarray = kwargs["truth"]
        magnitudes: np.ndarray = kwargs["magnitudes"]
        bands: list[str] = kwargs["bands"]
        if find_only:
            plot_name = self._make_full_plot_name(prefix, "")
            assert dataset_holder
            plot = RailPlotHolder(
                name=plot_name,
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_2d_hist_plots(
                prefix=prefix,
                truth=truth,
                magnitudes=magnitudes,
                bands=bands,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict

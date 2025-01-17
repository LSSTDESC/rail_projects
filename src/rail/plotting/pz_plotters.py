from __future__ import annotations

import os
from typing import Any
import numpy as np
from matplotlib import pyplot as plt
from ceci.config import StageParameter

from .plotter import RailPlotter
from .plot_holder import RailPlotHolder


class PZPlotterPointEstimateVsTrueHist2D(RailPlotter):
    """ Class to make a 2D histogram of p(z) point estimates
    versus true redshift
    """

    config_options: dict[str, StageParameter] = dict(
        z_min=StageParameter(float, 0., fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3., fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
    )

    inputs: dict = {
        'truth':np.ndarray,
        'pointEstimates':dict[str, np.ndarray],
    }

    def _make_2d_hist_plot(
        self,
        prefix: str,
        key: str,
        truth: np.ndarray,
        pointEstimate: np.ndarray,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots()
        bin_edges = np.linspace(self.config.z_min, self.config.z_max, self.config.n_zbins+1)
        axes.hist2d(
            truth,
            pointEstimate,
            bins=(bin_edges, bin_edges),
        )
        plt.xlabel("True Redshift")
        plt.ylabel("Estimated Redshift")
        plot_name = self._make_full_plot_name(prefix, f'{key}_hist')
        return RailPlotHolder(name=plot_name, figure=figure)

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get('find_only', False)
        outdir = kwargs.get('outdir', '.')
        figtype = kwargs.get('figtype', 'png')
        out_dict: dict[str, RailPlotHolder]  = {}
        truth: np.ndarray = kwargs['truth']
        pointEstimates: dict[str, np.ndarray] = kwargs['pointEstimates']
        for key, val in pointEstimates.items():
            if find_only:
                plot_name = self._make_full_plot_name(prefix, f'{key}_hist')
                plot = RailPlotHolder(name=plot_name, path=os.path.join(outdir, f"{plot_name}.{figtype}"))
            else:
                plot = self._make_2d_hist_plot(
                    prefix=prefix,
                    key=key,
                    truth=truth,
                    pointEstimate=val,
                )
            out_dict[plot.name] = plot
        return out_dict


class PZPlotterPointEstimateVsTrueProfile(RailPlotter):
    """ Class to make a profile plot of p(z) point estimates
    versus true redshift
    """

    config_options: dict[str, StageParameter] = dict(
        z_min=StageParameter(float, 0., fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3., fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
    )

    inputs: dict = {
        'truth':np.ndarray,
        'pointEstimates':dict[str, np.ndarray],
    }

    def _make_2d_profile_plot(
        self,
        prefix: str,
        key: str,
        truth: np.ndarray,
        pointEstimate: np.ndarray,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots()
        bin_edges = np.linspace(self.config.z_min, self.config.z_max, self.config.n_zbins+1)
        bin_centers = 0.5*(bin_edges[0:-1] + bin_edges[1:])
        z_true_bin = np.searchsorted(bin_edges, truth)
        means = np.zeros((self.config.n_zbins))
        stds = np.zeros((self.config.n_zbins))
        for i in range(self.config.n_zbins):
            mask = z_true_bin == i
            data = pointEstimate[mask]
            if len(data) == 0:
                continue
            means[i] = np.mean(data) - bin_centers[i]
            stds[i] = np.std(data)

        axes.errorbar(
            bin_centers,
            means,
            stds,
        )
        plt.xlabel("True Redshift")
        plt.ylabel("Estimated Redshift")
        plot_name = self._make_full_plot_name(prefix, f'{key}_profile')
        return RailPlotHolder(name=plot_name, figure=figure)

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get('find_only', False)
        outdir = kwargs.get('outdir', '.')
        figtype = kwargs.get('figtype', 'png')
        out_dict: dict[str, RailPlotHolder]  = {}
        truth: np.ndarray = kwargs['truth']
        pointEstimates: dict[str, np.ndarray] = kwargs['pointEstimates']
        for key, val in pointEstimates.items():
            if find_only:
                plot_name = self._make_full_plot_name(prefix, f'{key}_profile')
                plot = RailPlotHolder(name=plot_name, path=os.path.join(outdir, f"{plot_name}.{figtype}"))
            else:
                plot = self._make_2d_profile_plot(
                    prefix=prefix,
                    key=key,
                    truth=truth,
                    pointEstimate=val,
                )
            out_dict[plot.name] = plot
        return out_dict


class PZPlotterAccuraciesVsTrue(RailPlotter):  # pragma: no cover
    """ Class to make a plot of the accuracy of several algorithms
    versus true redshift
    """

    config_options: dict[str, StageParameter] = dict(
        z_min=StageParameter(float, 0., fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3., fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
        delta_cutoff=StageParameter(float, 0.1, fmt="%0.2f", msg="Delta-Z Cutoff for accurary"),
    )

    inputs: dict = {
        'truth':np.ndarray,
        'pointEstimates':dict[str, np.ndarray],
    }

    def _make_accuracy_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pointEstimates: dict[str, np.ndarray],
    ) -> RailPlotHolder:
        figure, axes = plt.subplots()
        bin_edges = np.linspace(self.config.z_min, self.config.z_max, self.config.n_zbins+1)
        bin_centers = 0.5*(bin_edges[0:-1] + bin_edges[1:])
        z_true_bin = np.searchsorted(bin_edges, truth)
        for key, val in pointEstimates.items():
            deltas = val - truth
            accuracy = np.ones((self.config.n_zbins))*np.nan
            for i in range(self.config.n_zbins):
                mask = z_true_bin == i
                data = deltas[mask]
                if len(data) == 0:
                    continue
                accuracy[i] = (np.abs(data) <= self.config.delta_cutoff).sum() / float(len(data))
            axes.plot(
                bin_centers,
                accuracy,
                label=key,
            )
        plt.xlabel("True Redshift")
        plt.ylabel("Estimated Redshift")
        plot_name = self._make_full_plot_name(prefix, 'accuracy')
        return RailPlotHolder(name=plot_name, figure=figure)

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get('find_only', False)
        outdir = kwargs.get('outdir', '.')
        figtype = kwargs.get('figtype', 'png')
        out_dict: dict[str, RailPlotHolder]  = {}
        if find_only:
            plot_name = self._make_full_plot_name(prefix, 'accuracy')
            plot = RailPlotHolder(name=plot_name, path=os.path.join(outdir, f"{plot_name}.{figtype}"))
        else:
            plot = self._make_accuracy_plot(prefix=prefix, **kwargs)
        out_dict[plot.name] = plot
        return out_dict

from __future__ import annotations

import os
from typing import Any
import numpy as np
from matplotlib import pyplot as plt
from ceci.config import StageParameter

from .plotter import RailPlotter
from .plot_holder import RailPlotHolder
from .dataset_holder import RailDatasetHolder


class PZPlotterPointEstimateVsTrueHist2D(RailPlotter):
    """Class to make a 2D histogram of p(z) point estimates
    versus true redshift
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
    )

    inputs: dict = {
        "truth": np.ndarray,
        "pointEstimate": np.ndarray,
    }

    def _make_2d_hist_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pointEstimate: np.ndarray,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots()
        bin_edges = np.linspace(
            self.config.z_min, self.config.z_max, self.config.n_zbins + 1
        )
        axes.hist2d(
            truth,
            pointEstimate,
            bins=(bin_edges, bin_edges),
        )
        plt.xlabel("True Redshift")
        plt.ylabel("Estimated Redshift")
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
        pointEstimate: np.ndarray = kwargs["pointEstimate"]
        if find_only:
            plot_name = self._make_full_plot_name(prefix, "")
            assert dataset_holder
            plot = RailPlotHolder(
                name=plot_name,
                path=os.path.join(
                    dataset_holder.config.name, f"{plot_name}.{figtype}"
                ),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_2d_hist_plot(
                prefix=prefix,
                truth=truth,
                pointEstimate=pointEstimate,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict


class PZPlotterPointEstimateVsTrueProfile(RailPlotter):
    """Class to make a profile plot of p(z) point estimates
    versus true redshift
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
    )

    inputs: dict = {
        "truth": np.ndarray,
        "pointEstimate": np.ndarray,
    }

    def _make_2d_profile_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pointEstimate: np.ndarray,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots()
        bin_edges = np.linspace(
            self.config.z_min, self.config.z_max, self.config.n_zbins + 1
        )
        bin_centers = 0.5 * (bin_edges[0:-1] + bin_edges[1:])
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
        pointEstimate: np.ndarray = kwargs["pointEstimate"]
        if find_only:
            assert dataset_holder
            plot_name = self._make_full_plot_name(prefix, "")
            plot = RailPlotHolder(
                name=plot_name,
                path=os.path.join(
                    dataset_holder.config.name, f"{plot_name}.{figtype}"
                ),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_2d_profile_plot(
                prefix=prefix,
                truth=truth,
                pointEstimate=pointEstimate,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict


class PZPlotterAccuraciesVsTrue(RailPlotter):
    """Class to make a plot of the accuracy of several algorithms
    versus true redshift
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
        delta_cutoff=StageParameter(
            float, 0.1, fmt="%0.2f", msg="Delta-Z Cutoff for accurary"
        ),
    )

    inputs: dict = {
        "truth": np.ndarray,
        "pointEstimates": dict[str, np.ndarray],
    }

    def _make_accuracy_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pointEstimates: dict[str, np.ndarray],
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots()
        bin_edges = np.linspace(
            self.config.z_min, self.config.z_max, self.config.n_zbins + 1
        )
        bin_centers = 0.5 * (bin_edges[0:-1] + bin_edges[1:])
        z_true_bin = np.searchsorted(bin_edges, truth)
        for key, val in pointEstimates.items():
            deltas = val - truth
            accuracy = np.ones((self.config.n_zbins)) * np.nan
            for i in range(self.config.n_zbins):
                mask = z_true_bin == i
                data = deltas[mask]
                if len(data) == 0:
                    continue
                accuracy[i] = (np.abs(data) <= self.config.delta_cutoff).sum() / float(
                    len(data)
                )
            axes.plot(
                bin_centers,
                accuracy,
                label=key,
            )
        plt.xlabel("True Redshift")
        plt.ylabel("Estimated Redshift")
        plot_name = self._make_full_plot_name(prefix, "")
        return RailPlotHolder(
            name=plot_name, figure=figure, plotter=self, dataset_holder=dataset_holder
        )

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get("find_only", False)
        figtype = kwargs.get("figtype", "png")
        dataset_holder = kwargs.get("dataset_holder")
        out_dict: dict[str, RailPlotHolder] = {}
        if find_only:
            plot_name = self._make_full_plot_name(prefix, "")
            assert dataset_holder
            plot = RailPlotHolder(
                name=plot_name,
                path=os.path.join(
                    dataset_holder.config.name, f"{plot_name}.{figtype}"
                ),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_accuracy_plot(prefix=prefix, **kwargs)
        out_dict[plot.name] = plot
        return out_dict

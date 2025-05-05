from __future__ import annotations

import os
from typing import Any

import numpy as np
from astropy.stats import biweight_location, biweight_scale
from ceci.config import StageParameter
from matplotlib import colors
from matplotlib import pyplot as plt
from scipy.stats import sigmaclip

from .dataset import RailDataset
from .dataset_holder import RailDatasetHolder
from .plot_holder import RailPlotHolder
from .plotter import RailPlotter


class RailPZPointEstimateDataset(RailDataset):
    """Dataet to hold a vector p(z) point estimates and corresponding
    true redshifts
    """

    data_types = dict(truth=np.ndarray, pointEstimate=np.ndarray, magnitude=np.ndarray)


class RailPZMultiPointEstimateDataset(RailDataset):
    """Dataet to hold a set of vectors of p(z) point estimates and corresponding
    true redshifts
    """

    data_types = dict(
        truth=np.ndarray,
        pointEstimates=dict[str, np.ndarray],
    )


class PZPlotterPointEstimateVsTrueHist2D(RailPlotter):
    """Class to make a 2D histogram of p(z) point estimates
    versus true redshift
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
        n_clip=StageParameter(
            int, 3, fmt="%i", msg="Number of sigma cliping for outliers"
        ),
    )

    input_type = RailPZPointEstimateDataset

    def _make_2d_hist_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pointEstimate: np.ndarray,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots(figsize=(7, 6))
        bin_edges = np.linspace(
            self.config.z_min, self.config.z_max, self.config.n_zbins + 1
        )
        dz = (pointEstimate - truth) / (1 + truth)
        mean, mean_err, std, outlier_rate = self.get_biweight_mean_sigma_outlier(
            dz, nclip=self.config.n_clip
        )
        mean, std, outlier_rate = round(mean, 4), round(std, 4), round(outlier_rate, 4)
        print(mean, mean_err, std, outlier_rate)
        h = axes.hist2d(
            truth,
            pointEstimate,
            bins=(bin_edges, bin_edges),
            norm=colors.LogNorm(),
            cmap="gray",
        )
        axes.plot(
            [self.config.z_min - 10, self.config.z_max + 10],
            [self.config.z_min - 10, self.config.z_max + 10],
            "--",
            color="red",
        )
        axes.plot(
            [self.config.z_min - 10, self.config.z_max + 10],
            [self.config.z_min - 10 - 3 * std, self.config.z_max + 10 - 3 * std],
            "--",
            color="red",
        )
        axes.plot(
            [self.config.z_min - 10, self.config.z_max + 10],
            [self.config.z_min - 10 + 3 * std, self.config.z_max + 10 + 3 * std],
            "--",
            color="red",
        )
        axes.plot(
            [],
            [],
            ".",
            alpha=0.0,
            label=rf"$\Delta z = {mean} $"
            + "\n"
            + rf"$\sigma z = {std} $"
            + "\n"
            + f"outlier rate = {outlier_rate}",
        )

        plt.xlabel("True Redshift")
        plt.ylabel("Estimated Redshift")
        cb = figure.colorbar(h[3], ax=axes)
        cb.set_label("Density")

        plt.legend()

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
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
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

    def get_biweight_mean_sigma_outlier(self, subset: np.ndarray, nclip: int=3) -> tuple[float, float, float, float]:
        subset_clip, _, _ = sigmaclip(subset, low=3, high=3)
        for _j in range(nclip):
            subset_clip, _, _ = sigmaclip(subset_clip, low=3, high=3)

        mean = biweight_location(subset_clip)
        std = biweight_scale(subset_clip)
        outlier_rate = np.sum(np.abs(subset) > 3 * biweight_scale(subset_clip)) / len(
            subset
        )

        return mean, std / np.sqrt(len(subset_clip)), std, outlier_rate


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

    input_type = RailPZPointEstimateDataset

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
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
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

    input_type = RailPZMultiPointEstimateDataset

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
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_accuracy_plot(prefix=prefix, **kwargs)
        out_dict[plot.name] = plot
        return out_dict


class PZPlotterBiweightStatsVsRedshift(RailPlotter):
    """Class to make a 2D histogram of p(z) point estimates
    versus true redshift
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 15, fmt="%i", msg="Number of z bins"),
        n_clip=StageParameter(
            int, 3, fmt="%i", msg="Number of sigma cliping for outliers"
        ),
    )

    input_type = RailPZPointEstimateDataset

    def _make_biweight_stats_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pointEstimate: np.ndarray,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        dz = (pointEstimate - truth) / (1 + truth)

        results = self.process_data(
            pointEstimate,
            truth,
            nbin=self.config.n_zbins,
            low=self.config.z_min,
            high=self.config.z_max,
            nclip=self.config.n_clip,
        )
        figure, axes = plt.subplots(2, 1, figsize=(8, 6))

        plt.subplots_adjust(wspace=0.1, hspace=0.0)

        axes[0].errorbar(
            results["z_mean"],
            results["biweight_mean"],
            results["biweight_std"],
            label="Bias",
        )

        axes[0].plot(results["z_mean"], results["biweight_sigma"], label=r"$\sigma_z$")

        axes[0].plot(
            results["z_mean"], results["biweight_outlier"], label=r"Outlier rate"
        )
        axes[0].set_title(
            f"Bias, Sigma, and Outlier rates w/ {self.config.n_clip} sigma clipping"
        )
        axes[0].set_ylabel("Statistics")
        axes[0].legend()
        axes[0].tick_params(
            axis="x", which="both", bottom=False, top=False, labelbottom=False
        )
        axes[0].set_xlim(self.config.z_min, self.config.z_max)

        bin_edges_z = np.linspace(self.config.z_min, self.config.z_max, 100 + 1)
        bin_edges_dz = np.linspace(np.min(dz), np.max(dz), 100 + 1)
        axes[1].hist2d(
            pointEstimate,
            dz,
            bins=(bin_edges_z, bin_edges_dz),
            norm=colors.LogNorm(),
            cmap="gray",
        )

        axes[1].set_xlim(self.config.z_min, self.config.z_max)
        for qt in ["qt_95_low", "qt_68_low", "median", "qt_68_high", "qt_95_high"]:
            axes[1].plot(
                results["z_mean"], results[qt], "--", color="blue", linewidth=2.0
            )

        axes[1].set_xlabel("Redshift")
        axes[1].set_ylabel(r"$(z_{phot} - z_{spec})/(1+z_{spec})$")
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
                path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
                plotter=self,
                dataset_holder=dataset_holder,
            )
        else:
            plot = self._make_biweight_stats_plot(
                prefix=prefix,
                truth=truth,
                pointEstimate=pointEstimate,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict

    def process_data(
        self,
        zphot: np.ndarray,
        specz: np.ndarray,
        low: float=0.01,
        high: float=2.,
        nclip: int=3,
        nbin: int=101,
    ) -> dict[str, list[float]]:
        dz = (zphot - specz) / (1 + specz)

        z_bins = np.linspace(low, high, nbin)
        # Bin the data
        bin_indices = np.digitize(zphot, bins=z_bins) - 1  # Assign each point to a bin

        biweight_mean: list[float] = []
        biweight_std: list[float] = []
        biweight_sigma: list[float] = []
        biweight_outlier: list[float] = []
        z_mean: list[float] = []
        qt_95_low: list[float] = []
        qt_68_low: list[float] = []
        median: list[float] = []
        qt_68_high: list[float] = []
        qt_95_high: list[float] = []
        for i in range(len(z_bins) - 1):
            subset = dz[bin_indices == i]
            if len(subset) < 1:
                continue
            subset_clip, _, _ = sigmaclip(subset, low=3, high=3)
            for _j in range(nclip):
                subset_clip, _, _ = sigmaclip(subset_clip, low=3, high=3)

            biweight_mean.append(biweight_location(subset_clip))
            biweight_std.append(biweight_scale(subset_clip) / np.sqrt(len(subset_clip)))
            biweight_sigma.append(biweight_scale(subset_clip))

            outlier_rate = np.sum(
                np.abs(subset) > 3 * biweight_scale(subset_clip)
            ) / len(subset)
            biweight_outlier.append(outlier_rate)

            qt_95_low.append(np.percentile(subset, 2.5))
            qt_68_low.append(np.percentile(subset, 16))
            median.append(np.percentile(subset, 50))
            qt_68_high.append(np.percentile(subset, 84))
            qt_95_high.append(np.percentile(subset, 97.5))

            z_mean.append(np.mean(zphot[bin_indices == i]))

        return {
            "z_mean": z_mean,
            "biweight_mean": biweight_mean,
            "biweight_std": biweight_std,
            "biweight_sigma": biweight_sigma,
            "biweight_outlier": biweight_outlier,
            "qt_95_low": qt_95_low,
            "qt_68_low": qt_68_low,
            "median": median,
            "qt_68_high": qt_68_high,
            "qt_95_high": qt_95_high,
        }


class PZPlotterBiweightStatsVsMag(RailPlotter):
    """Class to make a 2D histogram of p(z) point estimates
    versus true redshift
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        mag_min=StageParameter(float, 18, fmt="%0.2f", msg="Minimum Magnitude"),
        mag_max=StageParameter(float, 25, fmt="%0.2f", msg="Maximum Magnitude"),
        n_magbins=StageParameter(int, 10, fmt="%i", msg="Number of magnitude bins"),
        n_clip=StageParameter(
            int, 3, fmt="%i", msg="Number of sigma cliping for outliers"
        ),
    )

    input_type = RailPZPointEstimateDataset

    def _make_biweight_stats_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pointEstimate: np.ndarray,
        magnitude: np.ndarray,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        dz = (pointEstimate - truth) / (1 + truth)

        results = self.process_data(
            pointEstimate,
            truth,
            magnitude,
            nbin=self.config.n_magbins,
            low=self.config.mag_min,
            high=self.config.mag_max,
            nclip=self.config.n_clip,
        )
        figure, axes = plt.subplots(2, 1, figsize=(8, 6))

        plt.subplots_adjust(wspace=0.1, hspace=0.0)

        axes[0].errorbar(
            results["mag_mean"],
            results["biweight_mean"],
            results["biweight_std"],
            label="Bias",
        )

        axes[0].plot(
            results["mag_mean"], results["biweight_sigma"], label=r"$\sigma_z$"
        )

        axes[0].plot(
            results["mag_mean"], results["biweight_outlier"], label=r"Outlier rate"
        )
        axes[0].set_title(
            f"Bias, Sigma, and Outlier rates w/ {self.config.n_clip} sigma clipping"
        )
        axes[0].set_ylabel("Statistics")
        axes[0].legend()
        axes[0].tick_params(
            axis="x", which="both", bottom=False, top=False, labelbottom=False
        )
        axes[0].set_xlim(self.config.mag_min, self.config.mag_max)

        bin_edges_mag = np.linspace(self.config.mag_min, self.config.mag_max, 100 + 1)
        bin_edges_dz = np.linspace(np.min(dz), np.max(dz), 100 + 1)
        axes[1].hist2d(
            magnitude,
            dz,
            bins=(bin_edges_mag, bin_edges_dz),
            norm=colors.LogNorm(),
            cmap="gray",
        )

        axes[1].set_xlim(self.config.mag_min, self.config.mag_max)
        for qt in ["qt_95_low", "qt_68_low", "median", "qt_68_high", "qt_95_high"]:
            axes[1].plot(
                results["mag_mean"], results[qt], "--", color="blue", linewidth=2.0
            )

        axes[1].set_xlabel("Magnitude")
        axes[1].set_ylabel(r"$(z_{phot} - z_{spec})/(1+z_{spec})$")

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
        magnitude: np.ndarray = kwargs["magnitude"]
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
            plot = self._make_biweight_stats_plot(
                prefix=prefix,
                truth=truth,
                pointEstimate=pointEstimate,
                magnitude=magnitude,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict

    def process_data(
        self,
        zphot: np.ndarray,
        specz: np.ndarray,
        mag: np.ndarray,
        low: float=0.01,
        high: float=2.,
        nclip: int=3,
        nbin: int=101,
    ) -> dict[str, list[float] | np.ndarray]:
        dz = (zphot - specz) / (1 + specz)

        mag_bins = np.linspace(low, high, nbin)
        # Bin the data
        bin_indices = np.digitize(mag, bins=mag_bins) - 1  # Assign each point to a bin

        biweight_mean: list[float] = []
        biweight_std: list[float] = []
        biweight_sigma: list[float] = []
        biweight_outlier: list[float] = []

        qt_95_low: list[float] = []
        qt_68_low: list[float] = []
        median: list[float] = []
        qt_68_high: list[float] = []
        qt_95_high: list[float] = []
        for i in range(len(mag_bins) - 1):
            subset = dz[bin_indices == i]
            subset_clip, _, _ = sigmaclip(subset, low=3, high=3)
            for _j in range(nclip):
                subset_clip, _, _ = sigmaclip(subset_clip, low=3, high=3)

            biweight_mean.append(biweight_location(subset_clip))
            biweight_std.append(biweight_scale(subset_clip) / np.sqrt(len(subset_clip)))
            biweight_sigma.append(biweight_scale(subset_clip))

            outlier_rate = np.sum(
                np.abs(subset) > 3 * biweight_scale(subset_clip)
            ) / len(subset)
            biweight_outlier.append(outlier_rate)

            qt_95_low.append(np.percentile(subset, 2.5))
            qt_68_low.append(np.percentile(subset, 16))
            median.append(np.percentile(subset, 50))
            qt_68_high.append(np.percentile(subset, 84))
            qt_95_high.append(np.percentile(subset, 97.5))

        mag_mean = (mag_bins[:-1] + mag_bins[1:]) / 2

        return {
            "mag_mean": mag_mean,
            "biweight_mean": biweight_mean,
            "biweight_std": biweight_std,
            "biweight_sigma": biweight_sigma,
            "biweight_outlier": biweight_outlier,
            "qt_95_low": qt_95_low,
            "qt_68_low": qt_68_low,
            "median": median,
            "qt_68_high": qt_68_high,
            "qt_95_high": qt_95_high,
        }

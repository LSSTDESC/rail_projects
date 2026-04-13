from __future__ import annotations

import os
from typing import Any

import numpy as np
from astropy.stats import biweight_location, biweight_scale
from ceci.config import StageParameter
from matplotlib import colors
from matplotlib import pyplot as plt
from scipy.stats import sigmaclip
import qp
from .dataset import RailDataset
from .dataset_holder import RailDatasetHolder
from .plot_holder import RailPlotHolder
from .plotter import RailPlotter


class RailPZDistributionDataset(RailDataset):
    """Dataet to hold a vector p(z) point estimates and corresponding
    true redshifts
    """
    data_types = dict(truth=np.ndarray, pz=qp.Ensemble)


class PZPlotterPITProb(RailPlotter):
    """Class to plot the p(z_true)
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        n_prob_bins=StageParameter(int, 100, fmt="%i", msg="Number of prob bins"),
    )

    input_type = RailPZDistributionDataset

    def _make_prob_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pz: qp.Ensemble,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots(figsize=(7, 6))

        pit = qp.metrics.PIT(pz, truth)
        bin_edges = np.linspace(
            0., 1., self.config.n_prob_bins + 1
        )

        pdf_vals = pit.pit.pdf(np.linspace(0, 1))
        mean = 1./pdf_vals.mean()
        _ = axes.plot(np.linspace(0, 1), pit.pit.pdf(np.linspace(0, 1)))
        _ = axes.plot([0, 1], [mean, mean], "--", color='black')
        _ = axes.set_xlabel("Q")
        _ = axes.set_ylabel(r"$P(z_{\rm ref})$")
        _ = plt.xlim(0, 1)

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
        pz: np.ndarray = kwargs["pz"]
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
            plot = self._make_prob_plot(
                prefix=prefix,
                truth=truth,
                pz=pz,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict



class PZPlotterPITQQ(RailPlotter):
    """Class to plot the p(z_true > z(Q))
    """

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        n_prob_bins=StageParameter(int, 100, fmt="%i", msg="Number of prob bins"),
    )

    input_type = RailPZDistributionDataset

    def _make_pit_qq_plot(
        self,
        prefix: str,
        truth: np.ndarray,
        pz: qp.Ensemble,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        figure, axes = plt.subplots(figsize=(7, 6))

        pit = qp.metrics.PIT(pz, truth)
        bin_edges = np.linspace(
            0., 1., self.config.n_prob_bins + 1
        )

        ks = pit.evaluate_PIT_KS().statistic
        outlier = pit.evaluate_PIT_outlier_rate()
        CvM = pit.evaluate_PIT_CvM().statistic
        ksamp = pit.evaluate_PIT_anderson_ksamp().statistic

        _ = axes.plot(np.linspace(0, 1, 101), pit.pit.cdf(np.linspace(0, 1, 101)))
        _ = axes.plot([0, 1], [0, 1], "--", color='black')
        _ = axes.plot(
            [],
            [],
            ".",
            alpha=0.0,
            label=f"KS = {ks:.2e}"
            + "\n"
            + f"CvM = {CvM:.2e}"
            + "\n"
            + f"KS Amp = {ksamp:.2e}"
            + "\n"
            + f"outlier rate  = {outlier:.2e}",
        )
        _ = plt.xlabel("Q")
        _ = plt.ylabel(r"$P(z_{\rm ref} < z(Q)$)")
        _ = plt.legend()
        _ = plt.xlim(0, 1)
        _ = plt.ylim(0, 1)

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
        pz: np.ndarray = kwargs["pz"]
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
            plot = self._make_pit_qq_plot(
                prefix=prefix,
                truth=truth,
                pz=pz,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict

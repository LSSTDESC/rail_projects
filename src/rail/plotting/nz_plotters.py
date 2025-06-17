from __future__ import annotations

import os
from typing import Any

import matplotlib as mpl
import numpy as np
import qp
from ceci.config import StageParameter
from matplotlib import pyplot as plt

from .dataset import RailDataset
from .dataset_holder import RailDatasetHolder
from .plot_holder import RailPlotHolder
from .plotter import RailPlotter


class RailNZTomoBinsDataset(RailDataset):
    """Dataet to hold a n(z) distributions for a set of tomographic bins and the
    correspoding true n(z) distributions.
    """

    data_types = dict(
        truth=qp.Ensemble,
        nz_estimates=qp.Ensemble,
    )


class NZPlotterTomoBins(RailPlotter):
    """Class to make a histogram of all the nz distributions"""

    config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
    config_options.update(
        z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
        z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
        n_zbins=StageParameter(int, 50, fmt="%i", msg="Number of z bins"),
    )

    input_type = RailNZTomoBinsDataset

    def _make_plot(
        self,
        prefix: str,
        truth: qp.Ensemble,
        nz_estimates: qp.Ensemble,
        dataset_holder: RailDatasetHolder | None = None,
    ) -> RailPlotHolder:
        n_pdf = truth.npdf
        bin_edges = np.linspace(
            self.config.z_min, self.config.z_max, self.config.n_zbins + 1
        )
        truth_vals = truth.pdf(bin_edges)
        nz_vals = nz_estimates.pdf(bin_edges)

        # Compute means and variances
        z_centers = 0.5 * (bin_edges[1:] + bin_edges[:-1])
        truth_means = np.array(
            [
                np.sum(z_centers * truth_vals[i][:-1]) / np.sum(truth_vals[i][:-1])
                for i in range(n_pdf)
            ]
        )
        truth_vars = np.array(
            [
                np.sum((z_centers - truth_means[i]) ** 2 * truth_vals[i][:-1])
                / np.sum(truth_vals[i][:-1])
                for i in range(n_pdf)
            ]
        )

        est_means = np.array(
            [
                np.sum(z_centers * nz_vals[i][:-1]) / np.sum(nz_vals[i][:-1])
                for i in range(n_pdf)
            ]
        )
        est_vars = np.array(
            [
                np.sum((z_centers - est_means[i]) ** 2 * nz_vals[i][:-1])
                / np.sum(nz_vals[i][:-1])
                for i in range(n_pdf)
            ]
        )

        # Create subplots
        fig, axes = plt.subplots(n_pdf, 1, figsize=(8, 1.5 * n_pdf), sharex=True)
        if n_pdf == 1:  # pragma: no cover
            axes = [axes]  # Ensure iterable

        cmap = mpl.colormaps["plasma"]
        colors = cmap(np.linspace(0, 1, n_pdf))

        for i in range(n_pdf):
            ax = axes[i]
            color = colors[i]
            ax.plot(bin_edges, truth_vals[i], "-", color=color, label="True")
            ax.plot(bin_edges, nz_vals[i], "--", color=color, label="Estimate")

            d_mean = est_means[i] - truth_means[i]
            d_sigma = np.sqrt(est_vars[i] - truth_vars[i])

            ax.legend(
                title=f"$\\Delta \\mu_z$ = {d_mean:.3f}\n$\\Delta \\sigma_z$ = {d_sigma:.3f}",
                loc="upper right",
                fontsize="small",
                title_fontsize="small",
            )
            ax.set_ylabel(f"$n(z,{{{i}}})$")

        axes[-1].set_xlabel("z")
        fig.tight_layout()

        plot_name = self._make_full_plot_name(prefix, "")
        return RailPlotHolder(
            name=plot_name, figure=fig, plotter=self, dataset_holder=dataset_holder
        )

    def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
        find_only = kwargs.get("find_only", False)
        figtype = kwargs.get("figtype", "png")
        dataset_holder = kwargs.get("dataset_holder")
        out_dict: dict[str, RailPlotHolder] = {}
        truth: qp.Ensemble = kwargs["truth"]
        nz_estimates: qp.Ensemble = kwargs["nz_estimates"]
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
            plot = self._make_plot(
                prefix=prefix,
                truth=truth,
                nz_estimates=nz_estimates,
                dataset_holder=dataset_holder,
            )
        out_dict[plot.name] = plot
        return out_dict

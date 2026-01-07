import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure


def get_subplot_nrow_ncol(nfig: int) -> tuple[int, int]:
    """Get the number of rows and columns of sub-plots
    for a particular number of plots

    Parameters
    ----------
    nfig:
        Number of figures

    Returns
    -------
    Number of rows and columns as (nrow, ncol)
    """
    shape_dict = {
        1: (1, 1),
        2: (1, 2),
        3: (1, 3),
        4: (2, 2),
        5: (2, 3),
        6: (2, 3),
        7: (2, 4),
        8: (2, 4),
        9: (3, 3),
        10: (3, 4),
        11: (3, 4),
        12: (3, 4),
        13: (4, 4),
        14: (4, 4),
        15: (4, 4),
        16: (4, 4),
    }
    try:
        return shape_dict[nfig]
    except KeyError:  # pragma: no cover
        raise ValueError(
            f"Sorry, Phillipe.  I'm not going to put {nfig} subplots in one figure"
        ) from None


def plot_feature_histograms(
    data: np.ndarray,
    labels: list[str] | None = None,
    bins: int | np.ndarray = 100,
) -> Figure:
    """Plot Histograms of the features being used to train
    a ML algorithm on a single, busy figure

    Parameters
    ----------
    data:
        Input data

    labels:
        Labels for the various features

    bins:
        Bins for the histogram

    Returns
    -------
    Figure with requested plots
    """
    fig = plt.figure(figsize=(8, 8))
    n_features = data.shape[-1]
    nrow, ncol = get_subplot_nrow_ncol(n_features)
    axs = fig.subplots(nrow, ncol)

    for ifeature in range(n_features):
        icol = int(ifeature / ncol)
        irow = ifeature % ncol

        axs[icol][irow].hist(data[:, ifeature], bins=bins)
        if labels is not None:
            axs[icol][irow].set_xlabel(labels[ifeature])

    return fig


def plot_true_nz(targets: np.ndarray) -> Figure:
    """Plot the true NZ

    Parameters
    ----------
    targets:
        Input data

    Returns
    -------
    Figure with requested plot
    """
    fig = plt.figure(figsize=(8, 8))
    ax = fig.subplots(1, 1)
    ax.hist(targets, bins=100)
    return fig


def plot_feature_target_hist2d(
    data: np.ndarray,
    targets: np.ndarray,
    labels: list[str] | None = None,
    bins: tuple[int | np.ndarray, int | np.ndarray] = (100, 100),
) -> Figure:
    """Plot input data v. target redshift value as 2D histogram

    Parameters
    ----------
    data:
        Input data [N_objects, N_features]

    targets:
        Target redshifts [N_objects]

    labels:
        Labels for the data columns [N_features]

    bins:
        Bins for the histogram

    Returns
    -------
    Figure with requested plots

    Notes
    -----
    This will create N_features sub-plots
    """

    fig = plt.figure(figsize=(8, 8))
    n_features = data.shape[-1]
    nrow, ncol = get_subplot_nrow_ncol(n_features)
    axs = fig.subplots(nrow, ncol)

    for ifeature in range(n_features):
        icol = int(ifeature / ncol)
        irow = ifeature % ncol

        axs[icol][irow].hist2d(targets, data[:, ifeature], bins=bins)
        if labels is not None:
            axs[icol][irow].set_xlabel(labels[ifeature])

    return fig


def plot_colors_v_redshifts_with_templates(
    redshifts: np.ndarray,
    color_data: np.ndarray,
    zmax: float = 4.0,
    templates: dict | None = None,
    labels: list[str] | None = None,
) -> Figure:  # pragma: no cover

    fig = plt.figure(figsize=(8, 8))
    n_colors = color_data.shape[-1]
    nrow, ncol = get_subplot_nrow_ncol(n_colors)
    axs = fig.subplots(nrow, ncol)

    cmap = plt.get_cmap("rainbow")

    for icolor in range(n_colors):
        icol = int(icolor / ncol)
        irow = icolor % ncol
        axs[icol][irow].scatter(redshifts, color_data[:, icolor], color="black", s=1)
        axs[icol][irow].set_xlim(0, zmax)
        axs[icol][irow].set_ylim(-3.0, 3.0)
        if templates is not None:
            for key, val in templates.items():
                mask = val[0] < zmax
                _ = axs[icol][irow].plot(
                    val[0][mask],
                    val[2][icolor][mask],
                    label=key,
                    c=cmap(1.0 - val[3] / len(templates)),
                )
        # axs[icol][irow].legend()
        axs[icol][irow].set_xlabel("redshift")
        if labels is not None:
            axs[icol][irow].set_ylabel(labels[icolor])

    return fig


def plot_colors_v_colors_with_templates(
    color_data: np.ndarray,
    zmax: float = 4.0,
    templates: dict | None = None,
    labels: list[str] | None = None,
) -> Figure:  # pragma: no cover

    fig = plt.figure(figsize=(8, 8))
    n_colors = color_data.shape[-1]
    nrow, ncol = n_colors - 1, n_colors - 1
    axs = fig.subplots(nrow, ncol)

    cmap = plt.get_cmap("rainbow")

    for icol in range(n_colors - 1):
        for irow in range(n_colors - 1):
            axs[icol][irow].set_xlim(-3.0, 3.0)
            axs[icol][irow].set_ylim(-3.0, 3.0)
            if labels is not None:
                axs[icol][irow].set_ylabel(labels[icol])
                axs[icol][irow].set_xlabel(labels[irow + 1])
            if irow < icol:
                continue
            axs[icol][irow].scatter(
                color_data[:, icol], color_data[:, irow + 1], color="black", s=1
            )
            if templates is not None:
                for key, val in templates.items():
                    mask = val[0] < zmax
                    _ = axs[icol][irow].plot(
                        val[2][icol][mask],
                        val[2][irow + 1][mask],
                        label=key,
                        c=cmap(1.0 - val[3] / len(templates)),
                    )
            # axs[icol][irow].legend()
    return fig

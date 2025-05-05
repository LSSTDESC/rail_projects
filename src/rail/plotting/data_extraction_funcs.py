"""A set of utility functions to extract data for plotting from rail files"""

from __future__ import annotations

import glob
import os
from typing import Any

import numpy as np
import qp
import tables_io

from rail.projects import RailProject


def extract_z_true(
    filepath: str,
    colname: str = "redshift",
) -> np.ndarray:
    """Extract the true redshifts from a file

    Parameters
    ----------
    filepath: str
        Path to file with tabular data

    colname: str
        Name of the column with redshfits ['redshift']

    Returns
    -------
    redshifts: np.ndarray
        Redshifts in question

    Notes
    -----
    This assumes the redshifts are in a file that can be read by tables_io
    """
    truth_table = tables_io.read(filepath)
    return truth_table[colname]


def extract_z_point(
    filepath: str,
    colname: str = "zmode",
) -> np.ndarray:
    """Extract the point estimates of redshifts from a file

    Parameters
    ----------
    filepath: str
        Path to file with tabular data

    colname: str
        Name of the column with point estimates ['zmode']

    Returns
    -------
    z_estimates: np.ndarray
        Redshift estimates in question

    Notes
    -----
    This assumes the point estimates are in a qp file
    """
    qp_ens = qp.read(filepath)
    z_estimates = np.squeeze(qp_ens.ancil[colname])
    return z_estimates


def extract_mag(
    filepath: str,
    colname: str = "LSST_obs_i",
) -> np.ndarray:
    """Extract the i-mag from a file

    Parameters
    ----------
    filepath: str
        Path to file with tabular data

    colname: str
        Name of the column with redshfits ['redshift']

    Returns
    -------
    magnitude: np.ndarray
        Magnitude in question

    Notes
    -----
    This assumes the magnitude are in a file that can be read by tables_io
    """
    magnitude_table = tables_io.read(filepath)
    return magnitude_table[colname]


def extract_z_pdf(
    filepath: str,
) -> qp.ensemble:
    """Extract the pdf estimates of redshifts from a file

    Parameters
    ----------
    filepath: str
        Path to file with tabular data

    Returns
    -------
    z_pdf: qp.ensemble
        Redshift pdf in question

    Notes
    -----
    This assumes the point estimates are in a qp file
    """
    z_pdf = qp.read(filepath)
    return z_pdf


def extract_multiple_z_point(
    filepaths: dict[str, str],
    colname: str = "zmode",
) -> dict[str, np.ndarray]:  # pragma: no cover
    """Extract the point estimates of redshifts from several files

    Parameters
    ----------
    filepaths: dict[str, str]
        Path to file with tabular data, keys will be associatd with the various
        extracted point estimates

    colname: str
        Name of the column with point estimates ['zmode']

    Returns
    -------
    z_estimates: dict[str, np.ndarray]
        Redshift estimates in question, key by the key from input argument

    Notes
    -----
    This assumes the point estimates are in a qp file
    """
    ret_dict = {key: extract_z_point(val, colname) for key, val in filepaths.items()}
    return ret_dict


def make_z_true_z_point_dict(
    z_true: np.ndarray,
    z_estimate: np.ndarray,
    mags: np.ndarray,
) -> dict[str, np.ndarray]:
    """Build a dictionary with true redshifts and a point_estimates

    Parameters
    ----------
    z_true:
        True Redshifts

    z_estimate:
        Point estimates

    mags:
        Magnitdues

    Returns
    -------
    out_dict: dict[str, np.ndarray]
        Dictionary with true redshift and a point estimate of the redshift
    """
    out_dict: dict[str, Any] = dict(
        truth=z_true,
        pointEstimate=z_estimate,
        magnitude=mags,
    )
    return out_dict


def make_z_true_multi_z_point_dict(
    z_true: np.ndarray,
    z_estimates: dict[str, np.ndarray],
) -> dict[str, Any]:
    """Build a single dictionary with true redshifts and several point_estimates

    Parameters
    ----------
    z_true: np.ndarray
        True Redshifts

    z_estimates: dict[str, np.ndarray]
        Point estimates

    Returns
    -------
    out_dict: dict[str, Any]
        Dictionary with true redshift and all the point estimate of the redshift
    """
    out_dict: dict[str, Any] = dict(
        truth=z_true,
        pointEstimates=z_estimates,
    )
    return out_dict


def get_z_true_path(
    project: RailProject,
    selection: str,
    flavor: str,
    tag: str,
) -> str:
    """Get the path to the the file with true redshfits
    for a particualar analysis selection and flavor

    Parameters
    ----------
    project: RailProject
        Object with information about the structure of the current project

    selection: str
        Data selection in question, e.g., 'gold', or 'blended'

    flavor: str
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    tag: str
        File tag, e.g., 'test' or 'train', or 'train_zCosmos'

    Returns
    -------
    path: str
        Path to the file in question
    """
    return project.get_file_for_flavor(flavor, tag, selection=selection)


def get_ceci_pz_output_path(
    project: RailProject,
    selection: str,
    flavor: str,
    algo: str,
) -> str | None:
    """Get the paths to the file with redshfit estimates
    for a particualar analysis selection and flavor

    Parameters
    ----------
    project: RailProject
        Object with information about the structure of the current project

    selection: str
        Data selection in question, e.g., 'gold', or 'blended'

    flavor: str
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    algo: str
        Algorithm we want the estimates for, e.g., 'knn', 'bpz', etc..

    Returns
    -------
    path: str | None
        Path to the file in question, if it exists, otherwise None
    """
    outdir = project.get_path("ceci_output_dir", selection=selection, flavor=flavor)
    basename = f"output_estimate_{algo}.hdf5"
    outpath = os.path.join(outdir, basename)
    return outpath if os.path.exists(outpath) else None


def get_ceci_nz_output_paths(
    project: RailProject,
    selection: str,
    flavor: str,
    algo: str,
    classifier: str,
    summarizer: str,
) -> list[str]:
    """Get the paths to the file with n(z) estimates
    for a particualar analysis selection and flavor

    Parameters
    ----------
    project: RailProject
        Object with information about the structure of the current project

    selection: str
        Data selection in question, e.g., 'gold', or 'blended'

    flavor: str
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    algo: str
        Algorithm we want the estimates for, e.g., 'knn', 'bpz'], etc...

    classifier: str
        Algorithm we use to make tomograpic bin

    summarizer: str
        Algorithm we use to go from p(z) to n(z)

    Returns
    -------
    paths: list[str]
        Paths to data
    """
    outdir = project.get_path("ceci_output_dir", selection=selection, flavor=flavor)
    basename = f"single_NZ_summarize_{algo}_{classifier}_bin*_{summarizer}.hdf5"
    outpath = os.path.join(outdir, basename)
    paths = sorted(glob.glob(outpath))
    return paths


def get_ceci_true_nz_output_paths(
    project: RailProject,
    selection: str,
    flavor: str,
    algo: str,
    classifier: str,
) -> list[str]:
    """Get the paths to the file with n(z) estimates
    for a particualar analysis selection and flavor

    Parameters
    ----------
    project: RailProject
        Object with information about the structure of the current project

    selection: str
        Data selection in question, e.g., 'gold', or 'blended'

    flavor: str
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    algo: str
        Algorithm we want the estimates for, e.g., 'knn', 'bpz'], etc...

    classifier: str
        Algorithm we use to make tomograpic bin

    Returns
    -------
    paths: list[str]
        Paths to data
    """
    outdir = project.get_path("ceci_output_dir", selection=selection, flavor=flavor)
    basename = f"true_NZ_true_nz_{algo}_{classifier}_bin*.hdf5"
    outpath = os.path.join(outdir, basename)
    paths = sorted(glob.glob(outpath))
    return paths


def get_pz_point_estimate_data(
    project: RailProject,
    selection: str,
    flavor: str,
    tag: str,
    algo: str,
) -> dict[str, np.ndarray] | None:
    """Get the true redshifts and point estimates
    for a particualar analysis selection and flavor

    Parameters
    ----------
    project: RailProject
        Object with information about the structure of the current project

    selection: str
        Data selection in question, e.g., 'gold', or 'blended'

    flavor: str
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    algo: str
        Algorithm we want the estimates for, e.g., 'knn', 'bpz'], etc...

    tag: str
        File tag, e.g., 'test' or 'train', or 'train_zCosmos'

    Returns
    -------
    pz_data: dict[str, np.ndarray] | None
        Data in question or None if a file is missing
    """
    z_true_path = get_z_true_path(project, selection, flavor, tag)
    z_estimate_path = get_ceci_pz_output_path(project, selection, flavor, algo)
    if z_estimate_path is None:  # pragma: no cover
        return None
    z_true_data = extract_z_true(z_true_path)
    z_estimate_data = extract_z_point(z_estimate_path)
    mag_data = extract_mag(z_true_path)
    pz_data = make_z_true_z_point_dict(z_true_data, z_estimate_data, mag_data)
    return pz_data


def get_multi_pz_point_estimate_data(
    point_estimate_infos: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Get the true redshifts and point estimates

    for several analysis variants

    This checks that they all have the same redshifts
    Parameters
    ----------
    point_estimate_infos: dict[str, dict[str, Any]]
        Information about how to get point estimates

    Returns
    -------
    pz_data: dict[str, Any] | None
        Data in question or None
    """
    point_estimates: dict[str, np.ndarray] = {}
    ztrue_data: np.ndarray | None = None
    ztrue_key: str | None = None
    for key, val in point_estimate_infos.items():
        the_data = get_pz_point_estimate_data(**val)
        if the_data is None:  # pragma: no cover
            continue
        if ztrue_data is None:
            ztrue_data = the_data["truth"]
            ztrue_key = key
        else:
            if not np.allclose(ztrue_data, the_data["truth"]):  # pragma: no cover
                raise ValueError(
                    f"Mismatch in truth data. data({key}) != data({ztrue_key})"
                )
        point_estimates[key] = the_data["pointEstimate"]
    if ztrue_data is None:  # pragma: no cover
        return None
    pz_data = make_z_true_multi_z_point_dict(ztrue_data, point_estimates)
    return pz_data


def get_tomo_bins_nz_estimate_data(
    project: RailProject,
    selection: str,
    flavor: str,
    algo: str,
    classifier: str,
    summarizer: str,
) -> qp.Ensemble:
    """Get the tomographic bin n(z) estimates

    Parameters
    ----------
    project: RailProject
        Object with information about the structure of the current project

    selection: str
        Data selection in question, e.g., 'gold', or 'blended'

    flavor: str
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    algo: str
        Algorithm we want the estimates for, e.g., 'knn', 'bpz'], etc...

    classifier: str
        Algorithm we use to make tomograpic bin

    summarizer: str
        Algorithm we use to go from p(z) to n(z)

    Returns
    -------
    nz_data: qp.Ensemble
        Tomographic bin n(z) data
    """
    paths = get_ceci_nz_output_paths(
        project,
        selection,
        flavor,
        algo,
        classifier,
        summarizer,
    )

    data = qp.concatenate([extract_z_pdf(path_) for path_ in paths])
    return data


def get_tomo_bins_true_nz_data(
    project: RailProject,
    selection: str,
    flavor: str,
    algo: str,
    classifier: str,
) -> qp.Ensemble:
    """Get the tomographic bin true n(z)

    Parameters
    ----------
    project: RailProject
        Object with information about the structure of the current project

    selection: str
        Data selection in question, e.g., 'gold', or 'blended'

    flavor: str
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    algo: str
        Algorithm we want the estimates for, e.g., 'knn', 'bpz'], etc...

    classifier: str
        Algorithm we use to make tomograpic bin

    Returns
    -------
    nz_data: qp.Ensemble
        Tomographic bin n(z) data
    """
    paths = get_ceci_true_nz_output_paths(
        project,
        selection,
        flavor,
        algo,
        classifier,
    )

    data = qp.concatenate([extract_z_pdf(path_) for path_ in paths])
    return data

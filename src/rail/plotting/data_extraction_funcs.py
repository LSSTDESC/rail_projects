"""A set of utility functions to extract data for plotting from rail files"""

from __future__ import annotations

import os
from typing import Any

import numpy as np
import tables_io
import qp

from rail.projects import RailProject


def extract_z_true(
    filepath: str,
    colname: str='redshift',
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
    colname: str='zmode',
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


def extract_multiple_z_point(
    filepaths: dict[str, str],
    colname: str='zmode',
) -> dict[str, np.ndarray]:
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


def get_ceci_pz_output_paths(
    project: RailProject,
    selection: str,
    flavor: str,
    algos: list[str] | None = None,
) -> dict[str, str]:
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

    algos: list[str]
        Algorithms we want the estimates for, e.g., ['knn', 'bpz'], etc...

    Returns
    -------
    paths: dict[str, str]
        Paths to the file in question
    """
    if algos is None:  # pragma: no cover
        algos = ['all']
    if 'all' in algos:
        algos = list(project.get_pzalgorithms().keys())

    out_dict = {}
    outdir = project.get_path('ceci_output_dir', selection=selection, flavor=flavor)
    for algo_ in algos:
        basename = f"output_estimate_{algo_}.hdf5"
        outpath = os.path.join(outdir, basename)
        if os.path.exists(outpath):
            out_dict[algo_] = outpath
    return out_dict


def get_pz_point_estimate_data(
    project: RailProject,
    selection: str,
    flavor: str,
    tag: str,
    algos: list[str] | None = None,
) -> dict[str, Any]:
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

    algos: list[str]
        Algorithms we want the estimates for, e.g., ['knn', 'bpz'], etc...

    tag: str
        File tag, e.g., 'test' or 'train', or 'train_zCosmos'

    Returns
    -------
    paths: dict[str, Any]
        Data in question
    """
    z_true_path = get_z_true_path(project, selection, flavor, tag)
    z_estimate_paths = get_ceci_pz_output_paths(project, selection, flavor, algos)
    z_true_data = extract_z_true(z_true_path)
    z_estimate_data = extract_multiple_z_point(z_estimate_paths)
    pz_data = make_z_true_z_point_dict(z_true_data, z_estimate_data)
    return pz_data

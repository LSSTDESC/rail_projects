"""A set of utility functions to extract data for plotting from rail files"""

from __future__ import annotations

import glob
import os
from typing import Any

from rail.projects import RailProject


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


def get_ceci_pz_model_paths(
    project: RailProject,
    selection: str,
    flavor: str,
    algo: str|None = None,
) -> list[str]:
    """Get the paths to the file with pz model
    for a particualar analysis selection and flavor

    Parameters
    ----------
    project:
        Object with information about the structure of the current project

    selection:
        Data selection in question, e.g., 'gold', or 'blended'

    flavor:
        Analysis flavor in question, e.g., 'baseline' or 'zCosmos'

    algo:
        Algorithm we want the estimates for, e.g., 'knn', 'bpz', etc..
        None will find all the existing model files.

    Returns
    -------
    path: str | None
        Path to the file in question, if it exists, otherwise None
    """
    outdir = project.get_path("ceci_output_dir", selection=selection, flavor=flavor)
    if algo is None:
        basepath =  os.path.join(outdir, "model_inform_*.pkl")
        paths = sorted(glob.glob(basepath)) 
        return [output for output in paths if os.path.exists(output)]
    basename = f"model_inform_{algo}.pkl"
    outpath = os.path.join(outdir, basename)
    return [outpath] if os.path.exists(outpath) else []


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

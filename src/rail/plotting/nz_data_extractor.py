from __future__ import annotations

from typing import Any

from rail.projects import RailProject

from .data_extraction_funcs import (
    get_tomo_bins_nz_estimate_data,
    get_tomo_bins_true_nz_data,
)
from .data_extractor import RailProjectDataExtractor


class NZTomoBinDataExtractor(RailProjectDataExtractor):
    """Class to extract true redshifts and n(z) tomo bin estimates
    from a RailProject.

    This will return a dict:

    truth: np.ndarray
        True redshifts for each tomo bin

    nz_estimates: np.ndarray
        n(z) estimates for each tomo bin
    """

    inputs: dict = {
        "project": RailProject,
        "selection": str,
        "flavor": str,
        "algo": str,
        "classifier": str,
        "summarizer": str,
    }

    def _get_data(self, **kwargs: Any) -> dict[str, Any]:
        kwcopy = kwargs.copy()
        kwcopy.pop("summarizer")
        data = dict(
            nz_estimates=get_tomo_bins_nz_estimate_data(**kwargs),
            truth=get_tomo_bins_true_nz_data(**kwcopy),
        )
        return data

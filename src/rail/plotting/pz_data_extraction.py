from __future__ import annotations

from typing import Any

from rail.projects import RailProject

from .data_extraction import RailProjectDataExtractor
from .data_extraction_funcs import get_pz_point_estimate_data


class PZPointEstimateDataExtractor(RailProjectDataExtractor):
    """ Class to extract true redshifts and p(z) point estimates 
    from a RailProject.

    This will return a dict:

    truth: np.ndarray
        True redshifts

    pointEstimates: dict[str, np.ndarray]
         Dict mapping from the names for the various point estimates to the 
         estimates themselves
    """

    inputs: dict = {
        'project':RailProject,
        'selection':str,
        'flavor':str,
        'tag':str,
        'algos':list[str],
    }

    def _get_data(self, **kwargs: Any) -> dict[str, Any]:
        return get_pz_point_estimate_data(**kwargs)

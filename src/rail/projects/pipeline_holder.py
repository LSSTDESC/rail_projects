from __future__ import annotations

from typing import Any
from types import GenericAlias

from rail.plotting.configurable import Configurable


class RailPipelineHolder(Configurable):
    """Simple class for holding a pipeline configuraiton"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Pipeline name"),
        pipeline_cass=StageParameter(
            str, None, fmt="%s", required=True, msg="Full class name for Pipeline",
        ),
        kwargs=StageParameter(
            dict, {}, fmt="%s", msg="Keywords to provide Pipeline c'tor",
        ),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailPipelineHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

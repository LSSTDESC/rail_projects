from __future__ import annotations

from typing import Any

from ceci.config import StageParameter

from .configurable import Configurable


class RailPipelineTemplate(Configurable):
    """Simple class for holding a pipeline configuraiton"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Pipeline name"),
        pipeline_class=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Full class name for Pipeline",
        ),
        input_catalog_template=StageParameter(
            str,
            None,
            fmt="%s",
            msg="Template to use for input catalog",
        ),
        output_catalog_template=StageParameter(
            str,
            None,
            fmt="%s",
            msg="Template to use for output catalog",
        ),
        input_file_templates=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Templates to use for input files",
        ),
        kwargs=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Keywords to provide Pipeline c'tor",
        ),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailPipelineTemplate, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.pipeline_class}"

    
class RailPipelineInstance(Configurable):
    """Simple class for holding a pipeline configuraiton"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Pipeline name"),
        pipeline_template=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Name of PipelineTemplate to use",
        ),
        overrides=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Parameters to override from template",
        ),
        interpolants=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Parameters to interpolate from template",
        ),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailPipelineInstance, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.pipeline_template} {self.config.interpolants}"
        
    

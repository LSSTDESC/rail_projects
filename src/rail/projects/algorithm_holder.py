from __future__ import annotations

from typing import Any
from types import GenericAlias

from ceci.config import StageParameter

from rail.plotting.configurable import Configurable
from rail.plotting.dynamic_class import DynamicClass


class RailAlgorithmHolder(Configurable, DynamicClass):
    """Simple class for holding an algorithm by name"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Algorithm name"),
        module=StageParameter(
            str, None, fmt="%s", required=True, msg="Name of associated module",
        ),
    )
    sub_classes: dict[str, type[DynamicClass]] = {}

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailAlgorithmHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        DynamicClass.__init__(self)


class RailPZAlgorithmHolder(RailAlgorithmHolder):

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        estimate=StageParameter(str, None, fmt="%s", required=True, msg="Estimator Class"),
        inform=StageParameter(str, None, fmt="%s", required=True, msg="Informer Class"),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailSummarizerAlgorithmHolder(RailAlgorithmHolder):

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        summarize=StageParameter(str, None, fmt="%s", required=True, msg="Summarizer Class"),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailClassificationAlgorithmHolder(RailAlgorithmHolder):

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        classify=StageParameter(str, None, fmt="%s", required=True, msg="Classifier Class"),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailSpecSelectionAlgorithmHolder(RailAlgorithmHolder):

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        select=StageParameter(str, None, fmt="%s", required=True, msg="Selector Class"),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailErrorModelAlgorithmHolder(RailAlgorithmHolder):

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        error_model=StageParameter(str, None, fmt="%s", required=True, msg="Photometric Error Model Class"),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)

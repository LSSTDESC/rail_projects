from __future__ import annotations

from typing import Any

from ceci.config import StageParameter
from ceci.stage import PipelineStage

from .configurable import Configurable
from .dynamic_class import DynamicClass
from .subsampler import RailSubsampler
from .reducer import RailReducer


class RailAlgorithmHolder(Configurable, DynamicClass):
    """Simple class for holding an algorithm by name"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Algorithm name"),
        Module=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Name of associated module",
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

    def __call__(self, key: str) -> type:
        """Get the associated class one of the parts of the algorithm"""
        try:
            class_name = self.config[key]
        except KeyError as missing_key:
            raise KeyError(
                f"RailAlgorithmHolder does not have {key} in {self.config.to_dict().keys}"
            ) from missing_key
        return PipelineStage.get_stage(class_name, self.config.Module)

    def fill_dict(self, the_dict: dict[str, dict[str, str]]) -> None:
        """Fill a dict with infomation about the algorithm"""
        copy_dict = self.config.to_dict().copy()
        the_name = copy_dict.pop("name")
        the_dict[the_name] = copy_dict


class RailPZAlgorithmHolder(RailAlgorithmHolder):
    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Estimate=StageParameter(
            str, None, fmt="%s", required=True, msg="Estimator Class"
        ),
        Inform=StageParameter(str, None, fmt="%s", required=True, msg="Informer Class"),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailSummarizerAlgorithmHolder(RailAlgorithmHolder):
    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Summarize=StageParameter(
            str, None, fmt="%s", required=True, msg="Summarizer Class"
        ),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailClassificationAlgorithmHolder(RailAlgorithmHolder):
    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Classify=StageParameter(
            str, None, fmt="%s", required=True, msg="Classifier Class"
        ),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailSpecSelectionAlgorithmHolder(RailAlgorithmHolder):
    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Select=StageParameter(str, None, fmt="%s", required=True, msg="Selector Class"),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailErrorModelAlgorithmHolder(RailAlgorithmHolder):
    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        ErrorModel=StageParameter(
            str, None, fmt="%s", required=True, msg="Photometric Error Model Class"
        ),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailReducerAlgorithmHolder(RailAlgorithmHolder):
    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Reduce=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Data Reducer Class",
        ),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)

    def __call__(self, key: str) -> type:
        """Get the associated class one of the parts of the algorithm"""
        try:
            class_name = self.config[key]
        except KeyError as missing_key:
            raise KeyError(
                f"RailReducerAlgorithmHolder does not have {key} in {self.config.to_dict().keys}"
            ) from missing_key
        return RailReducer.get_sub_class(
            class_name, f"{self.config.Module}.{class_name}"
        )


class RailSubsamplerAlgorithmHolder(RailAlgorithmHolder):
    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Subsample=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Data Subsampler Class",
        ),
    )

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)

    def __call__(self, key: str) -> type:
        """Get the associated class one of the parts of the algorithm"""
        try:
            class_name = self.config[key]
        except KeyError as missing_key:
            raise KeyError(
                f"RailSubsamplerAlgorithmHolder does not have {key} in {self.config.to_dict().keys}"
            ) from missing_key
        return RailSubsampler.get_sub_class(
            class_name, f"{self.config.Module}.{class_name}"
        )

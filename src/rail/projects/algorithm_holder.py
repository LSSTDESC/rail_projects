from __future__ import annotations

from typing import Any

from ceci.config import StageParameter
from ceci.stage import PipelineStage

from .configurable import Configurable
from .dynamic_class import DynamicClass
from .reducer import RailReducer
from .subsampler import RailSubsampler


class RailAlgorithmHolder(Configurable, DynamicClass):
    """Simple class for holding an algorithm by name.

    This has the information needed to create the associated classes,
    namely the name of the python module in which they live, and the
    names of the classes themselves.
    """

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
    """Dictionary of sub_classes"""

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

    def __repr__(self) -> str:
        vals = self.config.to_dict().copy()
        vals.pop("name")
        vals.pop("Module")
        return f"{self.config.Module}.{vals}"

    def resolve(self, key: str) -> type:
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
    """Wrapper for algorithms that estimate per-object p(z).

    This wraps both the Inform and Estimate classes.

    The Inform class will typically be a `CatInformer` type `RailStage`,
    used to train the model for p(z) estimation.

    The Estimate class will typically be a `CatEstimator` type `RailStage`,
    which uses the trained model for p(z) estimation.

    A set of PZAlgorithm are used as inputs to several of the pipelines,
    specifying that the set of algorithms to run the pipeline with.
    """

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Estimate=StageParameter(
            str, None, fmt="%s", required=True, msg="Estimator Class"
        ),
        Inform=StageParameter(str, None, fmt="%s", required=True, msg="Informer Class"),
    )
    yaml_tag = "PZAlgorithm"

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailSummarizerAlgorithmHolder(RailAlgorithmHolder):
    """Wrapper for algorithms that make ensemble n(z) from a set of p(z).

    This wraps the Summarize class,
    which is typically a `PZToNZSummarizer` type `RailStage`.

    A set of Summarizer are used as inputs to the tomography-related pipelines,
    specifying that the set of algorithms to obtain n(z) information.
    """

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Summarize=StageParameter(
            str, None, fmt="%s", required=True, msg="Summarizer Class"
        ),
    )
    yaml_tag = "Summarizer"

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailClassificationAlgorithmHolder(RailAlgorithmHolder):
    """Wrapper for algorithms that assign objects to tomographic bins.

    This wraps the Classify class,
    which is typically a `Classifier` type `RailStage`.

    A set of Classifier are used as inputs to the tomography-related pipelines,
    specifying that the set of algorithms to assign objects to tomographic bins.
    """

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Classify=StageParameter(
            str, None, fmt="%s", required=True, msg="Classifier Class"
        ),
    )
    yaml_tag = "Classifier"

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailSpecSelectionAlgorithmHolder(RailAlgorithmHolder):
    """Wrapper for algorithms that emulate spectrosopic selections.

    This wraps the SpecSelection class,
    which is typically a `SpecSelector` type `RailStage`.

    A set of SpecSelection are used as inputs to the observation emulation pipelines,
    specifying that the set of algorithms to emulate spectrosopic selections.
    """

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        Select=StageParameter(str, None, fmt="%s", required=True, msg="Selector Class"),
    )
    yaml_tag = "SpecSelection"

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailErrorModelAlgorithmHolder(RailAlgorithmHolder):
    """Wrapper for algorithms that emulate photometric errors.

    This wraps the ErrorModel class,
    which is typically a `PhotoErrorModel` type RailStage

    A set of ErrorModel are used as inputs to the observation emulation pipelines,
    specifying that the set of algorithms to emulate photometric errors.
    """

    config_options = RailAlgorithmHolder.config_options.copy()
    config_options.update(
        ErrorModel=StageParameter(
            str, None, fmt="%s", required=True, msg="Photometric Error Model Class"
        ),
        Bands=StageParameter(
            list, ['u', 'g', 'r', 'i', 'z', 'y'], fmt="%s", required=False, msg="Bands to apply errors to",
        ),
    )
    yaml_tag = "ErrorModel"

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)


class RailReducerAlgorithmHolder(RailAlgorithmHolder):
    """Wrapper for algorithms that reduce data sets by applying selections
    and removing unneed columns.

    This wraps the Reduce class, which is typically a `RailReducer` object.

    Typically a single Reducer is used to prepare data for a particular project,
    possible apply a few different selections along the way.
    """

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
    yaml_tag = "Reducer"

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)

    def resolve(self, key: str) -> type:
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
    """Wrapper for algorithms that sumsample catalogs
    to provide testing and training data sets.

    This wraps the Subsample class, which is typically a `RailSubsampler` object.

    Typically a single Subsample is used to create a number of different
    test and training data sets for a particular project.
    """

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
    yaml_tag = "Subsampler"

    def __init__(self, **kwargs: Any):
        RailAlgorithmHolder.__init__(self, **kwargs)

    def resolve(self, key: str) -> type:
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

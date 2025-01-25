from __future__ import annotations

from typing import Any


from ceci.config import StageParameter

from rail.projects import RailProject
from .dataset_holder import RailDatasetHolder
from .data_extractor import RailProjectDataExtractor
from .dataset_factory import RailDatasetFactory


class RailProjectDatasetHolder(RailDatasetHolder):
    """Simple class for holding a dataset for plotting data that comes from a RailProject"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        extractor=StageParameter(
            str, None, fmt="%s", required=True, msg="Dataset extractor class name"
        ),
        project=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject name"
        ),
        selection=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject data selection"
        ),
        flavor=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject analysis flavor"
        ),
        tag=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject file tag"
        ),
        algo=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject algorithm"
        ),
    )

    extractor_inputs: dict = {
        "project": RailProject,
        "extractor": RailProjectDataExtractor,
        "selection": str,
        "flavor": str,
        "tag": str,
        "algo": str,
    }

    def __init__(self, **kwargs: Any):
        RailDatasetHolder.__init__(self, **kwargs)
        self._project: RailProject | None = None
        self._extractor: RailProjectDataExtractor | None = None

    def get_extractor_inputs(self) -> dict[str, Any]:
        if self._project is None:
            self._project = RailDatasetFactory.get_project(self.config.project)
        if self._extractor is None:
            self._extractor = RailProjectDataExtractor.create_from_dict(
                dict(name=self.config.name, class_name=self.config.extractor),
            )
        the_extractor_inputs = dict(
            project=self._project,
            extractor=self._extractor,
            selection=self.config.selection,
            flavor=self.config.flavor,
            tag=self.config.tag,
            algo=self.config.algo,
        )
        self._validate_extractor_inputs(**the_extractor_inputs)
        return the_extractor_inputs


class RailProjectMultiDatasetHolder(RailDatasetHolder):
    """Simple class for holding making a merged for plotting data that comes from a RailProject"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        extractor=StageParameter(
            str, None, fmt="%s", required=True, msg="Dataset extractor class name"
        ),
        datasets=StageParameter(
            list, None, fmt="%s", required=True, msg="Dataset name"
        ),
    )

    extractor_inputs: dict = {
        "extractor": RailProjectDataExtractor,
        "datasets": list[RailDatasetHolder],
    }

    def __init__(self, **kwargs: Any):
        RailDatasetHolder.__init__(self, **kwargs)
        self._extractor: RailProjectDataExtractor | None = None
        self._datasets: list[RailDatasetHolder] | None = None

    def get_extractor_inputs(self) -> dict[str, Any]:
        if self._extractor is None:
            self._extractor = RailProjectDataExtractor.create_from_dict(
                dict(name=self.config.name, class_name=self.config.extractor),
            )
        if self._datasets is None:
            self._datasets = [
                RailDatasetFactory.get_dataset(key) for key in self.config.datasets
            ]

        the_extractor_inputs = dict(
            extractor=self._extractor,
            datasets=self._datasets,
        )
        self._validate_extractor_inputs(**the_extractor_inputs)
        return the_extractor_inputs


class RailProjectNZDatasetHolder(RailDatasetHolder):
    """Simple class for holding a dataset for plotting data that comes from a RailProject"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        extractor=StageParameter(
            str, None, fmt="%s", required=True, msg="Dataset extractor class name"
        ),
        project=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject name"
        ),
        selection=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject data selection"
        ),
        flavor=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject analysis flavor"
        ),
        algo=StageParameter(
            str, None, fmt="%s", required=True, msg="RailProject algorithm"
        ),
        classifier=StageParameter(
            str, None, fmt="%s", required=True, msg="Tomographic bin classifier"
        ),
        summarizer=StageParameter(
            str, None, fmt="%s", required=True, msg="p(z) to n(z) summarizer"
        ),
   )

    extractor_inputs: dict = {
        "project": RailProject,
        "extractor": RailProjectDataExtractor,
        "selection": str,
        "flavor": str,
        "algo": str,
        "classifier": str,
        "summarizer": str,
    }

    def __init__(self, **kwargs: Any):
        RailDatasetHolder.__init__(self, **kwargs)
        self._project: RailProject | None = None
        self._extractor: RailProjectDataExtractor | None = None

    def get_extractor_inputs(self) -> dict[str, Any]:
        if self._project is None:
            self._project = RailDatasetFactory.get_project(self.config.project)
        if self._extractor is None:
            self._extractor = RailProjectDataExtractor.create_from_dict(
                dict(name=self.config.name, class_name=self.config.extractor),
            )
        the_extractor_inputs = dict(
            project=self._project,
            extractor=self._extractor,
            selection=self.config.selection,
            flavor=self.config.flavor,
            algo=self.config.algo,
            classifier=self.config.classifier,
            summarizer=self.config.summarizer,
        )
        self._validate_extractor_inputs(**the_extractor_inputs)
        return the_extractor_inputs

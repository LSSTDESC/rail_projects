from __future__ import annotations

from typing import Any

from ceci.config import StageParameter

from rail.projects import RailProject

from .data_extraction_funcs import (
    get_ceci_pz_output_path,
    get_multi_pz_point_estimate_data,
    get_pz_point_estimate_data,
)
from .dataset import RailDataset
from .dataset_factory import RailDatasetFactory
from .dataset_holder import (
    DatasetSplitMode,
    RailDatasetHolder,
    RailDatasetListHolder,
    RailProjectHolder,
)
from .pz_plotters import RailPZMultiPointEstimateDataset, RailPZPointEstimateDataset


class RailPZPointEstimateDataHolder(RailDatasetHolder):
    """Class to extract true redshifts and one p(z) point estimate
    from a RailProject.

    This will return a dict:

    truth: np.ndarray
        True redshifts

    pointEstimate: np.ndarray
        Point estimates of the true redshifts
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
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
        "selection": str,
        "flavor": str,
        "tag": str,
        "algo": str,
    }

    output_type: type[RailDataset] = RailPZPointEstimateDataset

    def __init__(self, **kwargs: Any):
        RailDatasetHolder.__init__(self, **kwargs)
        self._project: RailProject | None = None

    def __repr__(self) -> str:
        ret_str = (
            f"{self.__class__.__name__} "
            "( "
            f"{self.config.project}, "
            f"{self.config.selection}_{self.config.flavor}_{self.config.tag}_{self.config.algo}"
            ")"
        )
        return ret_str

    def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
        return get_pz_point_estimate_data(**kwargs)

    def get_extractor_inputs(self) -> dict[str, Any]:
        if self._project is None:
            self._project = RailDatasetFactory.get_project(
                self.config.project
            ).resolve()
        the_extractor_inputs = dict(
            project=self._project,
            selection=self.config.selection,
            flavor=self.config.flavor,
            tag=self.config.tag,
            algo=self.config.algo,
        )
        self._validate_extractor_inputs(**the_extractor_inputs)
        return the_extractor_inputs

    @classmethod
    def generate_dataset_dict(
        cls,
        **kwargs: Any,
    ) -> tuple[
        list[RailProjectHolder], list[RailDatasetHolder], list[RailDatasetListHolder]
    ]:
        """
        Parameters
        ----------
        **kwargs
            Set Notes

        Notes
        -----
        dataset_list_name: str
            Name for the resulting DatasetList

        project_file: str
            Config file for project to inspect

        selections: list[str]
            Selections to use

        flavors: list[str]
            Flavors to use

        Returns
        -------
        list[RailProjectHolder]
            Underlying RailProjects

        list[RailDatasetHolder]
            Extracted datasets

        list[RailDatasetListHolder]
            Extracted dataset lists
        """
        dataset_list_name: str | None = kwargs.get("dataset_list_name")
        project_file = kwargs["project_file"]
        project = RailProject.load_config(project_file)
        selections = kwargs.get("selections")
        flavors = kwargs.get("flavors")
        split_mode = kwargs.get("split_mode", DatasetSplitMode.by_algo)

        flavor_dict = project.get_flavors()
        if flavors is None or "all" in flavors:
            flavors = list(flavor_dict.keys())
        if selections is None or "all" in selections:
            selections = list(project.get_selections().keys())

        project_name = project.name
        if not dataset_list_name:
            dataset_list_name = f"{project_name}_nz_tomo"

        projects: list[RailProjectHolder] = []
        datasets: list[RailDatasetHolder] = []
        dataset_lists: list[RailDatasetListHolder] = []

        projects.append(
            RailProjectHolder(
                name=project_name,
                yaml_file=project_file,
            )
        )

        dataset_list_dict: dict[str, list[str]] = {}
        dataset_key = dataset_list_name
        if split_mode == DatasetSplitMode.no_split:
            dataset_list_dict[dataset_key] = []

        for key in flavors:
            val = flavor_dict[key]
            pipelines = val["pipelines"]
            if "all" not in pipelines and "pz" not in pipelines:  # pragma: no cover
                continue
            try:
                algos = val["pipeline_overrides"]["default"]["kwargs"]["algorithms"]
            except KeyError:
                algos = list(project.get_pzalgorithms().keys())

            for selection_ in selections:
                if split_mode == DatasetSplitMode.by_flavor:
                    dataset_key = f"{dataset_list_name}_{selection_}_{key}"
                    dataset_list_dict[dataset_key] = []

                for algo_ in algos:
                    if split_mode == DatasetSplitMode.by_algo:
                        dataset_key = f"{dataset_list_name}_{selection_}_{algo_}"
                        if dataset_key not in dataset_list_dict:
                            dataset_list_dict[dataset_key] = []

                    path = get_ceci_pz_output_path(
                        project,
                        selection=selection_,
                        flavor=key,
                        algo=algo_,
                    )
                    if path is None:
                        continue
                    dataset_name = f"{selection_}_{key}_{algo_}"
                    dataset = cls(
                        name=dataset_name,
                        project=project_name,
                        flavor=key,
                        algo=algo_,
                        tag="test",
                        selection=selection_,
                    )
                    datasets.append(dataset)
                    dataset_list_dict[dataset_key].append(dataset_name)

        for ds_name, ds_list in dataset_list_dict.items():
            # Skip empty lists
            if not ds_list:
                continue
            dataset_list = RailDatasetListHolder(
                name=ds_name,
                dataset_class=cls.output_type.full_class_name(),
                datasets=ds_list,
            )
            dataset_lists.append(dataset_list)

        return (projects, datasets, dataset_lists)


class RailPZMultiPointEstimateDataHolder(RailDatasetHolder):
    """Simple class for holding making a merged for plotting data that comes from a RailProject"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
        datasets=StageParameter(
            list, None, fmt="%s", required=True, msg="Dataset name"
        ),
    )

    extractor_inputs: dict = {
        "datasets": list[RailDatasetHolder],
    }

    output_type: type[RailDataset] = RailPZMultiPointEstimateDataset

    def __init__(self, **kwargs: Any):
        RailDatasetHolder.__init__(self, **kwargs)
        self._datasets: list[RailDatasetHolder] | None = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}( {self.config.datasets} )"

    def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
        the_datasets = kwargs.get("datasets", None)
        if the_datasets is None:  # pragma: no cover
            raise KeyError(f"Missed datasets {kwargs}")
        point_estimate_infos: dict[str, dict[str, Any]] = {}
        for dataset_ in the_datasets:
            the_name = dataset_.config.name
            point_estimate_infos[the_name] = dataset_.get_extractor_inputs()
        return get_multi_pz_point_estimate_data(point_estimate_infos)

    def get_extractor_inputs(self) -> dict[str, Any]:
        if self._datasets is None:
            self._datasets = [
                RailDatasetFactory.get_dataset(key) for key in self.config.datasets
            ]

        the_extractor_inputs = dict(
            datasets=self._datasets,
        )
        self._validate_extractor_inputs(**the_extractor_inputs)
        return the_extractor_inputs

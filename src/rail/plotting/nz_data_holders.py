from __future__ import annotations

from typing import Any

from ceci.config import StageParameter

from rail.projects import RailProject

from .data_extraction_funcs import (get_ceci_nz_output_paths,
                                    get_ceci_true_nz_output_paths,
                                    get_tomo_bins_nz_estimate_data,
                                    get_tomo_bins_true_nz_data)
from .dataset import RailDataset
from .dataset_factory import RailDatasetFactory
from .dataset_holder import RailDatasetHolder
from .nz_plotters import RailNZTomoBinsDataset


class RailNZTomoBinsDataHolder(RailDatasetHolder):
    """Simple class for holding a dataset for plotting data that comes from a RailProject"""

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
        "selection": str,
        "flavor": str,
        "algo": str,
        "classifier": str,
        "summarizer": str,
    }

    output_type: type[RailDataset] = RailNZTomoBinsDataset

    def __init__(self, **kwargs: Any):
        RailDatasetHolder.__init__(self, **kwargs)
        self._project: RailProject | None = None

    def __repr__(self) -> str:
        ret_str = (
            f"{self.__class__.__name__}"
            "( "
            f"{self.config.project}, "
            f"{self.config.selection}_{self.config.flavor}_{self.config.algo}_"
            f"{self.config.classifier}_{self.config.summarizer} "
            ")"
        )
        return ret_str

    def _get_data(self, **kwargs: Any) -> dict[str, Any]:
        kwcopy = kwargs.copy()
        kwcopy.pop("summarizer")
        data = dict(
            nz_estimates=get_tomo_bins_nz_estimate_data(**kwargs),
            truth=get_tomo_bins_true_nz_data(**kwcopy),
        )
        return data

    def get_extractor_inputs(self) -> dict[str, Any]:
        if self._project is None:
            self._project = RailDatasetFactory.get_project(self.config.project).resolve()
        the_extractor_inputs = dict(
            project=self._project,
            selection=self.config.selection,
            flavor=self.config.flavor,
            algo=self.config.algo,
            classifier=self.config.classifier,
            summarizer=self.config.summarizer,
        )
        self._validate_extractor_inputs(**the_extractor_inputs)
        return the_extractor_inputs

    @classmethod
    def generate_dataset_dict(
        cls,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Parameters
        ----------
        **kwargs
            Set Notes

        Notes
        -----
        dataset_list_name: str
            Name for the resulting DatasetList

        dataset_holder_class: str
            Class for the dataset holder

        project_file: str
            Config file for project to inspect

        selections: list[str]
            Selections to use

        flavors: list[str]
            Flavors to use

        classifiers: list[str]
            Flavors to use

        summarizers: list[str]
            Summarizers to use

        Returns
        -------
        list[dict[str, Any]]
            Dictionary of the extracted datasets
        """
        dataset_list_name: str | None = kwargs.get("dataset_list_name")
        dataset_holder_class: str | None = kwargs.get("dataset_holder_class")
        project_file = kwargs["project_file"]
        project = RailProject.load_config(project_file)

        selections = kwargs.get("selections")
        flavors = kwargs.get("flavors")
        split_by_flavor = kwargs.get("split_by_flavor", False)

        output: list[dict[str, Any]] = []

        flavor_dict = project.get_flavors()
        if flavors is None or "all" in flavors:
            flavors = list(flavor_dict.keys())
        if selections is None or "all" in selections:
            selections = list(project.get_selections().keys())

        project_name = project.name
        if not dataset_list_name:
            dataset_list_name = f"{project_name}_pz_point"

        project_block = dict(
            Project=dict(
                name=project_name,
                yaml_file=project_file,
            )
        )

        output.append(project_block)

        dataset_list_dict: dict[str, list[str]] = {}
        dataset_key = dataset_list_name
        if not split_by_flavor:
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

            try:
                classifiers = val["pipeline_overrides"]["default"]["kwargs"][
                    "classifiers"
                ]
            except KeyError:
                classifiers = list(project.get_classifiers().keys())

            try:
                summarizers = val["pipeline_overrides"]["default"]["kwargs"][
                    "summarizers"
                ]
            except KeyError:
                summarizers = list(project.get_summarizers().keys())

            for selection_ in selections:
                if split_by_flavor:
                    dataset_key = f"{dataset_list_name}_{selection_}_{key}"
                    dataset_list_dict[dataset_key] = []

                for algo_ in algos:
                    for classifier_ in classifiers:
                        nz_true_paths = get_ceci_true_nz_output_paths(
                            project,
                            selection=selection_,
                            flavor=key,
                            algo=algo_,
                            classifier=classifier_,
                        )

                        if not nz_true_paths:  # pragma: no cover
                            continue

                        for summarizer_ in summarizers:
                            nz_paths = get_ceci_nz_output_paths(
                                project,
                                selection=selection_,
                                flavor=key,
                                algo=algo_,
                                classifier=classifier_,
                                summarizer=summarizer_,
                            )

                            if not nz_paths:  # pragma: no cover
                                continue

                            dataset_name = f"{selection_}_{key}_{algo_}_{classifier_}_{summarizer_}"

                            dataset_dict = dict(
                                name=dataset_name,
                                class_name=dataset_holder_class,
                                project=project_name,
                                flavor=key,
                                algo=algo_,
                                selection=selection_,
                                classifier=classifier_,
                                summarizer=summarizer_,
                            )

                            dataset_list_dict[dataset_key].append(dataset_name)
                            output.append(dict(Dataset=dataset_dict))

        for ds_name, ds_list in dataset_list_dict.items():
            # Skip empty lists
            if not ds_list:
                continue
            dataset_list = dict(
                name=ds_name,
                datasets=ds_list,
            )
            output.append(dict(DatasetList=dataset_list))

        return output

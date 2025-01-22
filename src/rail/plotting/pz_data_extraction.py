from __future__ import annotations

from typing import Any

from rail.projects import RailProject

from .data_extraction import RailProjectDataExtractor
from .data_extraction_funcs import (
    get_pz_point_estimate_data,
    get_ceci_pz_output_path,
    get_multi_pz_point_estimate_data,
)


class PZPointEstimateDataExtractor(RailProjectDataExtractor):
    """Class to extract true redshifts and one p(z) point estimate
    from a RailProject.

    This will return a dict:

    truth: np.ndarray
        True redshifts

    pointEstimate: np.ndarray
        Point estimates of the true redshifts
    """

    inputs: dict = {
        "project": RailProject,
        "selection": str,
        "flavor": str,
        "tag": str,
        "algo": str,
    }

    def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
        return get_pz_point_estimate_data(**kwargs)

    @classmethod
    def generate_dataset_dict(
        cls,
        dataset_list_name: str,
        project: RailProject,
        selections: list[str] | None = None,
        flavors: list[str] | None = None,
        split_by_flavor: bool=False,
    ) -> list[dict[str, Any]]:
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
                yaml_file="dummy",
            )
        )

        output.append(project_block)

        dataset_list_dict: dict[str, list[str]] = {}
        dataset_key = dataset_list_name
        if not split_by_flavor:
            dataset_list_dict[dataset_key] = []
        
        for key in flavors:
            val = flavor_dict[key]
            pipelines = val["Pipelines"]
            if "all" not in pipelines and "pz" not in pipelines:  # pragma: no cover
                continue
            try:
                algos = val["PipelineOverrides"]["default"]["kwargs"]["PZAlgorithms"]
            except KeyError:
                algos = list(project.get_pzalgorithms().keys())

            for selection_ in selections:
                
                if split_by_flavor:
                    dataset_key = f"{dataset_list_name}_{selection_}_{key}"                
                    dataset_list_dict[dataset_key] = []
                    
                for algo_ in algos:
                    path = get_ceci_pz_output_path(
                        project,
                        selection=selection_,
                        flavor=key,
                        algo=algo_,
                    )
                    if path is None:
                        continue
                    dataset_name = f"{selection_}_{key}_{algo_}"
                    dataset_dict = dict(
                        name=dataset_name,
                        extractor="rail.plotting.pz_data_extraction.PZPointEstimateDataExtractor",
                        project=project_name,
                        flavor=key,
                        algo=algo_,
                        tag="test",
                        selection=selection_,
                    )

                    dataset_list_dict[dataset_key].append(dataset_name)
                    output.append(dict(Dataset=dataset_dict))

        for ds_name, ds_list in dataset_list_dict.items():
            dataset_list = dict(
                name=ds_name,
                datasets=ds_list,
            )
            output.append(dict(DatasetList=dataset_list))

        return output


class PZMultiPointEstimateDataExtractor(RailProjectDataExtractor):
    """Class to extract true redshifts and multiple p(z) point estimates
    from a RailProject.

    This will return a dict:

    truth: np.ndarray
        True redshifts

    pointEstimates: dict[str, np.ndarray]
         Dict mapping from the names for the various point estimates to the
         estimates themselves
    """

    inputs: dict = {
        "datasets": list[str],
    }

    def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
        the_datasets = kwargs.get("datasets", None)
        if the_datasets is None:  # pragma: no cover
            raise KeyError(f"Missed datasets {kwargs}")
        point_estimate_infos: dict[str, dict[str, Any]] = {}
        for dataset_ in the_datasets:
            the_name = dataset_.config.name
            point_estimate_infos[the_name] = dataset_.get_extractor_inputs()
            point_estimate_infos[the_name].pop("extractor")
        return get_multi_pz_point_estimate_data(point_estimate_infos)

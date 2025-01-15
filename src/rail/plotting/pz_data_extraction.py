from __future__ import annotations

from typing import Any

from rail.projects import RailProject

from .data_extraction import RailProjectDataExtractor
from .data_extraction_funcs import get_pz_point_estimate_data, get_ceci_pz_output_paths


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

    @classmethod
    def generate_dataset_dict(
        cls,
        dataset_list_name: str,
        project: RailProject,
        selections: list[str] | None=None,
        flavors: list[str] | None=None,    
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []

        flavor_dict = project.get_flavors()
        if flavors is None or 'all' in flavors:
            flavors = list(flavor_dict.keys())
        if selections is None or 'all' in selections:
            selections = list(project.get_selections().keys())

        project_name = project.name        
        if not dataset_list_name:
            dataset_list_name=f"{project_name}_pz_point"
            
        project_block = dict(
            Project=dict(
                name=project_name,
                yaml_file="dummy",
            )
        )
        
        output.append(project_block)

        datasets: list[str] = []
        
        for key in flavors:
            val = flavor_dict[key]
            pipelines = val['Pipelines']
            if not 'all' in pipelines and not 'pz' in pipelines:
                continue
            try:
                algos = val['PipelineOverrides']['default']['kwargs']['PZAlgorithms']
            except KeyError:
                algos = ['all']

            for selection_ in selections:
                algo_dict = get_ceci_pz_output_paths(
                    project,
                    selection=selection_,
                    flavor=key,
                    algos=algos,
                )
                if not algo_dict:
                    continue
                dataset_name = f"{selection_}_{key}"
                dataset_dict = dict(
                    name=dataset_name,
                    extractor="rail.plotting.pz_data_extraction.PZPointEstimateDataExtractor",
                    project=project_name,
                    flavor=key,
                    algos=list(algo_dict.keys()),
                    tag='test',
                    selection=selection_,
                )
                datasets.append(dataset_name)
                output.append(dict(Dataset=dataset_dict))

        dataset_list = dict(
            name=dataset_list_name,
            datasets=datasets,
        )

        output.append(dict(DatasetList=dataset_list))
            
        return output

        
        

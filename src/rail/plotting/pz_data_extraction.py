from __future__ import annotations

from typing import Any

from rail.projects import RailProject

from .data_extraction import RailProjectDataExtractor
from .data_extraction_funcs import get_pz_point_estimate_data


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
    def generate_dataset_dict(cls, project: RailProject) -> list[dict[str, Any]]:

        output: list[dict[str, Any]] = []
        flavors = project.get_flavors()

        project_name = project.name

        selections = list(project.get_selections().keys())
        
        project_block = dict(
            Project=dict(
                name=project_name,
                yaml_file="dummy",
            )
        )
        
        output.append(project_block)

        for key, val in flavors.items():
            pipelines = val['Pipelines']
            if not 'all' in pipelines and not 'pz' in pipelines:
                continue
            try:
                algos = val['PipelineOverrides']['default']['kwargs']['PZAlgorithms']
            except KeyError:
                algos = ['all']
            for selection_ in selections:
                dataset_dict = dict(
                    name: f"{selection_}_{flavor}",
                    extractor="rail.plotting.pz_data_extraction.PZPointEstimateDataExtractor",
                    project=project_namem,
                    flavor=key,
                    algos=algos,
                    selection=selection_
                )
                output.append(dict(Dataset=dataset_dict))
        return output

        
        

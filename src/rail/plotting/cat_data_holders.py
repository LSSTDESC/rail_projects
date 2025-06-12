from __future__ import annotations

from typing import Any

from ceci.config import StageParameter

from rail.projects import RailProject, path_funcs

from .dataset import RailDataset
from .dataset_factory import RailDatasetFactory
from .dataset_holder import (
    DatasetSplitMode,
    RailDatasetHolder,
    RailDatasetListHolder,
    RailProjectHolder,
)
from .cat_plotters import RailCatTruthAndMagnitudesDataset


class RailCatTruthAndMagntiduesDataHolder(RailDatasetHolder):
    """Class to extract true redshifts and observed magntidues
    for a catalog from a RailProject

    This will return a dict:

    truth: np.ndarray
        True redshifts

    magntidues: np.ndarray
        Magnitudes in the various filters
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
    )

    extractor_inputs: dict = {
        "project": RailProject,
        "selection": str,
        "flavor": str,
        "tag": str,
    }

    output_type: type[RailDataset] = RailCatTruthAndMagnitudesDataset

    def __init__(self, **kwargs: Any):
        RailDatasetHolder.__init__(self, **kwargs)
        self._project: RailProject | None = None

    def __repr__(self) -> str:
        ret_str = (
            f"{self.__class__.__name__} "
            "( "
            f"{self.config.project}, "
            f"{self.config.selection}_{self.config.flavor}_{self.config.tag}"
            ")"
        )
        return ret_str

    def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
        return get_ztrue_and_magntidues(**kwargs)

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

        tag: str
            File tag

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
            dataset_list_name = f"{project_name}_catalog"

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

            for selection_ in selections:
                dataset_key = f"{dataset_list_name}_{selection_}_{key}"
                dataset_list_dict[dataset_key] = []

                path = path_funcs.get_z_true_path(
                    project,
                    selection=selection_,
                    flavor=key,
                    tag=kwargs.get("tag", "test"),
                )
                if path is None:
                    continue
                dataset_name = f"{selection_}_{key}"
                dataset = cls(
                    name=dataset_name,
                    project=project_name,
                    flavor=key,
                    tag=kwargs.get("tag", "test"),
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

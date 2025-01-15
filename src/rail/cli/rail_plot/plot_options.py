from rail.cli.rail.options import (
    PartialOption,
)


__all__: list[str] = [
    "purge_plots",
    "save_plots",
    "dataset_list_name",
    "include_groups",
    "exclude_groups",
    "extractor_class",
]


extractor_class =  PartialOption(
    "--extractor_class",
    type=str,
    help="Class name for data extractor, e.g., PZPointEstimateDataExtractor",
)


exclude_groups = PartialOption(
    "--exclude_groups",
    help="Plot groups to exclue",
    multiple=True,
)


include_groups = PartialOption(
    "--include_groups",
    help="Plot groups to include",
    multiple=True,
)


dataset_list_name = PartialOption(
    "--dataset_list_name",
    help="Name for dataset list",
    type=str,
)


purge_plots = PartialOption(
    "--purge_plots",
    help="Purge plots from memory after saving",
    is_flag=True,
)

save_plots = PartialOption(
    "--save_plots",
    help="Save plots to disk",
    is_flag=True,
)

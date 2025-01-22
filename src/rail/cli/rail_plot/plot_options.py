from rail.cli.rail.options import (
    PartialOption,
)


__all__: list[str] = [
    "purge_plots",
    "save_plots",
    "dataset_holder_class",
    "dataset_list_name",
    "plotter_list_name",
    "output_prefix",
    "dataset_yaml_path",
    "plotter_yaml_path",
    "include_groups",
    "exclude_groups",
    "extractor_class",
    "split_by_flavor",
]


extractor_class = PartialOption(
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


dataset_holder_class = PartialOption(
    "--dataset_holder_class",
    help="Class for the dataset holder",
    type=str,
)


dataset_list_name = PartialOption(
    "--dataset_list_name",
    help="Name for dataset list",
    type=str,
)


dataset_yaml_path = PartialOption(
    "--dataset_yaml_path",
    help="Name for dataset list",
    type=str,
)


output_prefix = PartialOption(
    "--output_prefix",
    help="Name for dataset list",
    default="",
    type=str,
)


plotter_list_name = PartialOption(
    "--plotter_list_name",
    help="Name for plotter list",
    type=str,
)


plotter_yaml_path = PartialOption(
    "--plotter_yaml_path",
    help="Name for plotter list",
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

split_by_flavor = PartialOption(
    "--split_by_flavor",
    help="Split dataset organization by flavor",
    is_flag=True,
)

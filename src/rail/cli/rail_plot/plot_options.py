from rail.cli.rail.options import EnumChoice, PartialOption

from rail.plotting.dataset_holder import DatasetSplitMode

__all__: list[str] = [
    "purge_plots",
    "save_plots",
    "find_only",
    "make_html",
    "dataset_holder_class",
    "dataset_list_name",
    "plotter_list_name",
    "output_prefix",
    "output_yaml",
    "dataset_yaml_path",
    "plotter_yaml_path",
    "include_groups",
    "exclude_groups",
    "split_mode",
]


exclude_groups = PartialOption(
    "--exclude-groups",
    help="Plot groups to exclue",
    multiple=True,
)


include_groups = PartialOption(
    "--include-groups",
    help="Plot groups to include",
    multiple=True,
)


dataset_holder_class = PartialOption(
    "--dataset-holder-class",
    help="Class for the dataset holder",
    type=str,
)


dataset_list_name = PartialOption(
    "--dataset-list-name",
    help="Name for dataset list",
    type=str,
)


dataset_yaml_path = PartialOption(
    "--dataset-yaml-path",
    help="Name for dataset list",
    type=str,
)


output_prefix = PartialOption(
    "--output-prefix",
    help="Name for dataset list",
    default="",
    type=str,
)


output_yaml = PartialOption(
    "--output-yaml",
    help="Name for output yaml file",
    default="",
    type=str,
)


plotter_list_name = PartialOption(
    "--plotter-list-name",
    help="Name for plotter list",
    type=str,
)


plotter_yaml_path = PartialOption(
    "--plotter-yaml-path",
    help="Name for plotter list",
    type=str,
)


find_only = PartialOption(
    "--find-only",
    help="Find existing plots, do not create new ones",
    is_flag=True,
)


make_html = PartialOption(
    "--make-html",
    help="Make html files to help browse plots",
    is_flag=True,
)


purge_plots = PartialOption(
    "--purge-plots",
    help="Purge plots from memory after saving",
    is_flag=True,
)

save_plots = PartialOption(
    "--save-plots",
    help="Save plots to disk",
    is_flag=True,
)

split_mode = PartialOption(
    "--split-mode",
    help="How to split datasets within a project",
    type=EnumChoice(DatasetSplitMode),
    default="by_algo",
)

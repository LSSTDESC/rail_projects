from rail.cli.rail.options import (
    PartialOption,
)


__all__: list[str] = [
    "purge_plots",
    "save_plots",
]


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

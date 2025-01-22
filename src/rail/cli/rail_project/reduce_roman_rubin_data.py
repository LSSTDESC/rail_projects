import math
import itertools
import os
from typing import Any

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import acero

from rail.projects import RailProject
from .project_options import RunMode


COLUMNS = [
    "galaxy_id",
    "ra",
    "dec",
    "redshift",
    "LSST_obs_u",
    "LSST_obs_g",
    "LSST_obs_r",
    "LSST_obs_i",
    "LSST_obs_z",
    "LSST_obs_y",
    "ROMAN_obs_F184",
    "ROMAN_obs_J129",
    "ROMAN_obs_H158",
    "ROMAN_obs_W146",
    "ROMAN_obs_Z087",
    "ROMAN_obs_Y106",
    "ROMAN_obs_K213",
    "ROMAN_obs_R062",
    "totalEllipticity",
    "totalEllipticity1",
    "totalEllipticity2",
    "diskHalfLightRadiusArcsec",
    "spheroidHalfLightRadiusArcsec",
    "bulge_frac",
    # "healpix",
]

PROJECTIONS = [
    {
        "mag_u_lsst": pc.field("LSST_obs_u"),
        "mag_g_lsst": pc.field("LSST_obs_g"),
        "mag_r_lsst": pc.field("LSST_obs_r"),
        "mag_i_lsst": pc.field("LSST_obs_i"),
        "mag_z_lsst": pc.field("LSST_obs_z"),
        "mag_y_lsst": pc.field("LSST_obs_y"),
        "totalHalfLightRadiusArcsec": pc.add(
            pc.multiply(
                pc.field("diskHalfLightRadiusArcsec"),
                pc.subtract(pc.scalar(1), pc.field("bulge_frac")),
            ),
            pc.multiply(
                pc.field("spheroidHalfLightRadiusArcsec"),
                pc.field("bulge_frac"),
            ),
        ),
        "_orientationAngle": pc.atan2(
            pc.field("totalEllipticity2"), pc.field("totalEllipticity1")
        ),
    },
    {
        "major": pc.divide(
            pc.field("totalHalfLightRadiusArcsec"),
            pc.sqrt(pc.field("totalEllipticity")),
        ),
        "minor": pc.multiply(
            pc.field("totalHalfLightRadiusArcsec"),
            pc.sqrt(pc.field("totalEllipticity")),
        ),
        "orientationAngle": pc.multiply(
            pc.scalar(0.5),
            pc.subtract(
                pc.field("_orientationAngle"),
                pc.multiply(
                    pc.floor(
                        pc.divide(pc.field("_orientationAngle"), pc.scalar(2 * math.pi))
                    ),
                    pc.scalar(2 * math.pi),
                ),
            ),
        ),
    },
]


def run_reduction(
    source_catalog: str,
    predicate: Any,
    sink_dir: str,
    sink_catalog: str,
) -> None:  # pragma: no cover
    dataset = ds.dataset(
        source_catalog,
        format="parquet",
    )

    scan_node = acero.Declaration(
        "scan",
        acero.ScanNodeOptions(
            dataset,
            columns=COLUMNS,
            filter=predicate,
        ),
    )

    filter_node = acero.Declaration(
        "filter",
        acero.FilterNodeOptions(
            predicate,
        ),
    )

    column_projection = {k: pc.field(k) for k in COLUMNS}
    projection = column_projection
    project_nodes = []
    for _projection in PROJECTIONS:
        for k, v in _projection.items():
            projection[k] = v
        project_node = acero.Declaration(
            "project",
            acero.ProjectNodeOptions(
                [v for k, v in projection.items()],
                names=[k for k, v in projection.items()],
            ),
        )
        project_nodes.append(project_node)

    seq = [
        scan_node,
        filter_node,
        *project_nodes,
    ]
    plan = acero.Declaration.from_sequence(seq)
    print(plan)

    # batches = plan.to_reader(use_threads=True)
    table = plan.to_table(use_threads=True)
    print(f"writing dataset to {sink_catalog}")
    os.makedirs(sink_dir, exist_ok=True)
    pq.write_table(table, sink_catalog)


def reduce_roman_rubin_data(
    project: RailProject,
    input_tag: str,
    input_selection: str,
    selection: str | None,
    run_mode: RunMode = RunMode.bash,
) -> int:
    source_catalogs = []
    sink_catalogs = []
    catalogs = []
    predicates = []

    if selection is not None:
        selection_dict = project.get_selection(selection)
    else:  # pragma: no cover
        selection_dict = {}

    # FIXME is this how we want to get the Iteration variables
    iteration_vars = list(project.config.get("IterationVars", {}).keys())
    if iteration_vars is not None:
        iterations = itertools.product(
            *[
                project.config.get("IterationVars", {}).get(iteration_var, "")
                for iteration_var in iteration_vars
            ]
        )
        for iteration_args in iterations:
            iteration_kwargs = {
                iteration_vars[i]: iteration_args[i] for i in range(len(iteration_vars))
            }
            source_catalog = project.get_catalog(
                input_tag, selection=input_selection, **iteration_kwargs
            )
            sink_catalog = project.get_catalog(
                "reduced", selection=selection, **iteration_kwargs
            )
            sink_dir = os.path.dirname(sink_catalog)
            if selection_dict:
                predicate = pc.field("LSST_obs_i") < selection_dict["maglim_i"][1]
            else:  # pragma: no cover
                predicate = None

            if (
                not os.path.isfile(source_catalog) and run_mode != RunMode.dry_run
            ):  # pragma: no cover
                raise ValueError(f"Input file {source_catalog} not found")

            # FIXME properly warn about or protect pre-existing output files
            if os.path.isfile(sink_catalog):  # pragma: no cover
                # raise ValueError(f"Input file {source_catalog} not found")
                print(f"Warning: output file {sink_catalog} found; may be rewritten...")

            source_catalogs.append(source_catalog)
            sink_catalogs.append(sink_catalog)

            catalogs.append((source_catalog, sink_catalog))

            predicates.append(predicate)

            if run_mode == RunMode.slurm:
                raise NotImplementedError(
                    "run_mode == RunMode.slurm not implemented for reduce_roman_rubin"
                )
            if run_mode == RunMode.bash:  # pragma: no cover
                run_reduction(
                    source_catalog,
                    predicate,
                    sink_dir,
                    sink_catalog,
                )

        print("writing completed")

    return 0

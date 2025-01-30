from __future__ import annotations

import os
from typing import Any
import math

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import acero

from ceci.config import StageParameter

from rail.plotting.configurable import Configurable
from rail.plotting.dynamic_class import DynamicClass


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


class RailReducer(Configurable, DynamicClass):
    """Base class for subsampling ata

    The main function in this class is:
    __call__(...)

    This function will take the input files and make a single output file

    config_options: a dict[str, `ceci.StageParameter`] that
    will be used to configure things like the seed and the number of output objects,

    _inputs: a dict [str, type] that specifics the inputs
    that the sub-classes expect, this is used the check the kwargs
    that are passed to the __call__ function.

    A function:
    _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:

    That actually makes the plots.  It does not need to do the checking
    that the correct kwargs have been given.
    """

    config_options: dict[str, StageParameter] = {}
    sub_classes: dict[str, type[DynamicClass]] = {}

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this plotter, must match
            class.config_options data members
        """
        DynamicClass.__init__(self)
        Configurable.__init__(self, **kwargs)

    def __call__(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:
        """Subsample the data

        Parameters
        ----------
        input_catalog: str,
            Input files to subsamle

        output_catalog: str,
            Path to the output file
        """
        raise NotImplementedError()


class RomanRubinReducer(RailReducer):
    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Reducer Name"),
        cuts=StageParameter(dict, {}, fmt="%s", msg="Selections"),
    )

    def __call__(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:
        # FIXME, do this right
        if self.config.cuts:
            predicate = pc.field("LSST_obs_i") < self.config.cuts["maglim_i"][1]
        else:  # pragma: no cover
            predicate = None

        dataset = ds.dataset(
            input_catalog,
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
        print(f"writing dataset to {output_catalog}")

        output_dir = os.path.dirname(output_catalog)

        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(table, output_dir)

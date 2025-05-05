from __future__ import annotations

import math
import os
from typing import Any

import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from ceci.config import StageParameter
from pyarrow import acero

from .configurable import Configurable
from .dynamic_class import DynamicClass

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


COLUMNS_COM_CAM = [
    "objectId",
    "coord_ra",
    "coord_dec",
    "u_cModelFlux",
    "g_cModelFlux",
    "r_cModelFlux",
    "i_cModelFlux",
    "z_cModelFlux",
    "y_cModelFlux",
    "u_cModelFluxErr",
    "g_cModelFluxErr",
    "r_cModelFluxErr",
    "i_cModelFluxErr",
    "z_cModelFluxErr",
    "y_cModelFluxErr",
]

PROJECTIONS_COM_CAM = [
    {
        "ref_flux": pc.field("i_cModelFlux"),
    }
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
    """Base class for reducing data catalogs

    The main function in this class is:
    run(input_catalog, output_catalog)

    This function will files in the input_catalog, and reduce each one to make the
    output catalog
    """

    config_options: dict[str, StageParameter] = {}
    sub_classes: dict[str, type[DynamicClass]] = {}

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        **kwargs:
            Configuration parameters for this Reducer, must match
            class.config_options data members
        """
        DynamicClass.__init__(self)
        Configurable.__init__(self, **kwargs)

    def run(
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
    """Class to reduce the 'roman_rubin' simulation input files for pz analysis"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Reducer Name"),
        cuts=StageParameter(dict, {}, fmt="%s", msg="Selections"),
    )

    def run(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:
        # FIXME: do this right
        if self.config.cuts:
            if "maglim_i" in self.config.cuts:
                predicate = pc.field("LSST_obs_i") < self.config.cuts["maglim_i"][1]
            elif "maglim_Y" in self.config.cuts:
                predicate = pc.field("ROMAN_obs_Y106") < self.config.cuts["maglim_Y"][1]
            else:
                raise ValueError("No valid cut")
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

        # batches = plan.to_reader(use_threads=True)
        table = plan.to_table(use_threads=True)
        print(f"writing dataset to {output_catalog}")

        output_dir = os.path.dirname(output_catalog)

        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(table, output_catalog)


class ComCamReducer(RailReducer):
    """Class to reduce the 'com_cam' input files for pz analysis"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Reducer Name"),
        cuts=StageParameter(dict, {}, fmt="%s", msg="Selections"),
    )

    _mag_offset = 31.4

    def run(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:
        # FIXME: do this right
        mag_cut = self.config.cuts["maglim_i"][1]
        flux_cut = np.power(10, (self._mag_offset - mag_cut) / 2.5)

        if self.config.cuts:
            predicate = pc.field("i_cModelFlux") > flux_cut
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
                columns=COLUMNS_COM_CAM,
                filter=predicate,
            ),
        )

        filter_node = acero.Declaration(
            "filter",
            acero.FilterNodeOptions(
                predicate,
            ),
        )

        column_projection = {k: pc.field(k) for k in COLUMNS_COM_CAM}
        projection = column_projection
        project_nodes = []
        for _projection in PROJECTIONS_COM_CAM:
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

        # batches = plan.to_reader(use_threads=True)
        table = plan.to_table(use_threads=True)
        print(f"writing dataset to {output_catalog}")

        output_dir = os.path.dirname(output_catalog)

        os.makedirs(output_dir, exist_ok=True)
        pd = table.to_pandas()
        pd.to_parquet(output_catalog)
        # pq.write_table(table, output_catalog)

from __future__ import annotations

import math
import os
from typing import Any

import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import yaml
from ceci.config import StageParameter
from pyarrow import acero
from rail.core.configurable import Configurable

from .arrow_utils import parse_item
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
    "sod_halo_mass",
    "logsm_obs",
    "sfr"
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

COLUMNS_FLAGSHIP = [
    "halo_id",
    "galaxy_id",
    "observed_redshift_gal", # observed redshift incl. velocity
    "ra_mag_gal",  # observed galaxy ra/dec with lensing displacement field applied [degrees]
    "dec_mag_gal",
    "lsst_u_el_model3_ext",  # observed flux from the continuum + emission including internal attenuation in LSST bands
    "lsst_g_el_model3_ext",
    "lsst_r_el_model3_ext",
    "lsst_i_el_model3_ext",
    "lsst_z_el_model3_ext",
    "lsst_y_el_model3_ext",
    "euclid_nisp_h_el_model3_ext",  # euclid bands (noiseless)
    "euclid_nisp_j_el_model3_ext",
    "euclid_nisp_y_el_model3_ext",
    "euclid_vis_el_model3_ext",
    "bulge_r50",  # half light radius of the bulge [arcsec]
    "disk_r50",  # half light radius of the disk for an exponential profile (or Sersic profile with index n=1); disk_r50 = disk_scalelength * 1.678 [arcsec]
    "bulge_fraction",  # ratio of the flux in the bulge component to the total flux (often written B/T)
    "gamma1",  # shape contribution from lensing, not large but added for consistency
    "gamma2",
    "eps1_gal",  # intrinsic galaxy ellipticity component
    "eps2_gal",
    "log_sfr",
    "log_stellar_mass",
    "lm_halo",
]

COLUMNS_CARDINAL = [
    "galaxy_id",
    "ra",
    "dec",
    "sedid",
    "size",
    "Ellipticity_1",
    "Ellipticity_2",
    "mag_u_lsst",
    "mag_g_lsst",
    "mag_r_lsst",
    "mag_i_lsst",
    "mag_z_lsst",
    "mag_y_lsst",
    "Roman_Y106",
    "Roman_J129",
    "Roman_H158",
    "Roman_F184",
    "Roman_K213",
    "WISE_W1",
    "WISE_W2",
    "redshift",
    "true_redshift",
    "t_true_redshift",
    "Euclid_Y",
    "Euclid_J",
    "Euclid_H",
    "Euclid_redshift"
]

PROJECTIONS_COM_CAM = [
    {
        "ref_flux": pc.field("i_cModelFlux"),
    }
]


PROJECTIONS_DP1 = [
    {
        "ref_flux": pc.field("i_psfFlux"),
    }
]

PROJECTIONS_CARDINAL = [
    {
        #  "Roman_K213": pc.field("k213"),
        "shift_ra": pc.add(pc.field("ra"), -60.),
        "shift_dec": pc.multiply(pc.field("dec"), -1.),
        "totalEllipticity1": pc.field("Ellipticity_1"),
        "totalEllipticity2": pc.field("Ellipticity_2"),
        "mag_y_euclid": pc.field("Euclid_Y"),
        "mag_j_euclid": pc.field("Euclid_J"),
        "mag_h_euclid": pc.field("Euclid_H"),
        "mag_w1_wise": pc.field("WISE_W1"),
        "mag_w2_wise": pc.field("WISE_W2"),
        "mag_Y_roman": pc.field("Roman_Y106"),
        "mag_J_roman": pc.field("Roman_J129"),
        "mag_H_roman": pc.field("Roman_H158"),
        "mag_F_roman": pc.field("Roman_F184"),
        "mag_K_roman": pc.field("Roman_K213"),
        "totalHalfLightRadiusArcsec": pc.field("size"),
        "totalEllipticity": pc.sqrt(pc.add(pc.power(pc.field("Ellipticity_1"), 2),
                                           pc.power(pc.field("Ellipticity_2"), 2))),
    },
    {
        "major": pc.divide(pc.field("size"), pc.sqrt(pc.sqrt(pc.add(pc.power(pc.field("Ellipticity_1"), 2),
                                                                    pc.power(pc.field("Ellipticity_2"), 2))))),

        "minor": pc.multiply(pc.field("size"), pc.sqrt(pc.sqrt(pc.add(pc.power(pc.field("Ellipticity_1"), 2),
                                                                      pc.power(pc.field("Ellipticity_2"), 2))))),
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
        "mag_R_roman": pc.field("ROMAN_obs_R062"),
        "mag_Z_roman": pc.field("ROMAN_obs_Z087"),
        "mag_Y_roman": pc.field("ROMAN_obs_Y106"),
        "mag_J_roman": pc.field("ROMAN_obs_J129"),
        "mag_W_roman": pc.field("ROMAN_obs_W146"),
        "mag_H_roman": pc.field("ROMAN_obs_H158"),
        "mag_F_roman": pc.field("ROMAN_obs_F184"),
        "mag_K_roman": pc.field("ROMAN_obs_K213"),
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

PROJECTIONS_FLAGSHIP = [
    {
        "ra": pc.if_else(
                pc.greater(pc.add(pc.field("ra_mag_gal"), pc.scalar(180)), pc.scalar(360)),
                pc.subtract(pc.field("ra_mag_gal"), pc.scalar(180)),
                pc.add(pc.field("ra_mag_gal"), pc.scalar(180))
                ),
        "dec": pc.multiply(pc.scalar(-1), pc.field("dec_mag_gal")),
        "redshift": pc.field("observed_redshift_gal"),
        "mag_u_lsst": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("lsst_u_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_g_lsst": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("lsst_g_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_r_lsst": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("lsst_r_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_i_lsst": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("lsst_i_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_z_lsst": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("lsst_z_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_y_lsst": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("lsst_y_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_h_euclid": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("euclid_nisp_h_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_j_euclid": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("euclid_nisp_j_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_y_euclid": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("euclid_nisp_y_el_model3_ext"))), pc.scalar(48.6)
        ),
        "mag_vis_euclid": pc.subtract(
            pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("euclid_vis_el_model3_ext"))), pc.scalar(48.6)
        ),
        "totalHalfLightRadiusArcsec": pc.add(
            pc.multiply(
                pc.field("disk_r50"),
                pc.subtract(pc.scalar(1), pc.field("bulge_fraction")),
            ),
            pc.multiply(
                pc.field("bulge_r50"),
                pc.field("bulge_fraction"),
            ),
        ),
        "_orientationAngle": pc.atan2(
            pc.add(pc.field("eps2_gal"), pc.field("gamma2")),
            pc.add(pc.field("eps1_gal"), pc.field("gamma1"))
        ),
    },
    {
        "major": pc.divide(
            pc.field("totalHalfLightRadiusArcsec"),
            pc.sqrt(
                pc.sqrt(pc.add(pc.power(pc.add(pc.field("eps1_gal"), pc.field("gamma1")), 2),
                               pc.power(pc.add(pc.field("eps2_gal"), pc.field("gamma2")), 2)))
            ),
        ),
        "minor": pc.multiply(
            pc.field("totalHalfLightRadiusArcsec"),
            pc.sqrt(
                pc.sqrt(pc.add(pc.power(pc.add(pc.field("eps1_gal"), pc.field("gamma1")), 2),
                               pc.power(pc.add(pc.field("eps2_gal"), pc.field("gamma2")), 2)))
            ),
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

DROP_COLS: list[str] = [
    "LSST_obs_u",
    "LSST_obs_g",
    "LSST_obs_r",
    "LSST_obs_i",
    "LSST_obs_z",
    "LSST_obs_y",
    "ROMAN_obs_R062",
    "ROMAN_obs_Z087",
    "ROMAN_obs_Y106",
    "ROMAN_obs_J129",
    "ROMAN_obs_W146",
    "ROMAN_obs_H158",
    "ROMAN_obs_F184",
    "ROMAN_obs_K213",
    "bulge_frac",
    "spheroidHalfLightRadiusArcsec",
    "diskHalfLightRadiusArcsec",
]

DROP_COLS_FLAGSHIP: list[str] = [
    "lsst_u_el_model3_ext",
    "lsst_g_el_model3_ext",
    "lsst_r_el_model3_ext",
    "lsst_i_el_model3_ext",
    "lsst_z_el_model3_ext",
    "lsst_y_el_model3_ext",
    "euclid_nisp_h_el_model3_ext",
    "euclid_nisp_j_el_model3_ext",
    "euclid_nisp_y_el_model3_ext",
    "euclid_vis_el_model3_ext",
    "_orientationAngle",
    "bulge_fraction",
    "dec_mag_gal",
    "ra_mag_gal",
    "bulge_r50",
    "disk_r50",
]

DROP_COLS_CARDINAL: list[str] = [
    "shift_dec",
    "shift_ra",
    "Euclid_H",
    "Euclid_J",
    "Euclid_Y",
    "WISE_W1",
    "WISE_W2",
    "Ellipticity_1",
    "Ellipticity_2",
    "true_redshift",
    "size",
    "t_true_redshift",
    "Euclid_redshift",
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
        # Try to do this right
        try:
            parsed_filter = parse_item(self.config.cuts)
            predicate = pq.filters_to_expression(parsed_filter)
        except Exception as msg:
            # Fallback to old way.  FIXME, deprecate this
            if self.config.cuts:
                if "maglim_i" in self.config.cuts:
                    predicate = pc.field("LSST_obs_i") < self.config.cuts["maglim_i"][1]
                elif "maglim_Y" in self.config.cuts:
                    predicate = (
                        pc.field("ROMAN_obs_Y106") < self.config.cuts["maglim_Y"][1]
                    )
                else:
                    raise ValueError("No valid cut") from msg
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

        if DROP_COLS:
            table = table.drop_columns(DROP_COLS)

        print(f"writing dataset to {output_catalog}")

        output_dir = os.path.dirname(output_catalog)

        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(table, output_catalog)


class CardinalReducer(RailReducer):
    """Class to reduce the 'Cardinal' simulation input files for pz analysis
       Note that cardinal native files are fits files split into triplets, a
       preprocessing stage was performed to put them into pyarrow parquet
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Reducer Name"),
        cuts=StageParameter(dict, {}, fmt="%s", msg="Selections"),
    )

    def run(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:
        # Try to do this right
        try:
            parsed_filter = parse_item(self.config.cuts)
            predicate = pq.filters_to_expression(parsed_filter)
        except Exception as msg:
            # Fallback to old way.  FIXME, deprecate this
            if self.config.cuts:
                if "maglim_i" in self.config.cuts:
                    predicate = pc.field("mag_i_lsst") < self.config.cuts["maglim_i"][1]
                elif "maglim_Y" in self.config.cuts:
                    predicate = (
                        pc.field("Roman_Y106") < self.config.cuts["maglim_Y"][1]
                    )
                else:
                    raise ValueError("No valid cut") from msg
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
                columns=COLUMNS_CARDINAL,
                filter=predicate,
            ),
        )

        filter_node = acero.Declaration(
            "filter",
            acero.FilterNodeOptions(
                predicate,
            ),
        )

        column_projection = {k: pc.field(k) for k in COLUMNS_CARDINAL}
        projection = column_projection
        project_nodes = []
        for _projection in PROJECTIONS_CARDINAL:
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

        if DROP_COLS_CARDINAL:
            table = table.drop_columns(DROP_COLS_CARDINAL)

        print(f"writing dataset to {output_catalog}")

        output_dir = os.path.dirname(output_catalog)

        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(table, output_catalog)


class FlagshipReducer(RailReducer):
    """Class to reduce the 'flagship' simulation input files for pz analysis"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Reducer Name"),
        cuts=StageParameter(dict, {}, fmt="%s", msg="Selections"),
    )

    def run(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:
        # Try to do this right
        try:
            parsed_filter = parse_item(self.config.cuts)
            predicate = pq.filters_to_expression(parsed_filter)
        except Exception as msg:
            # Fallback to old way.  FIXME, deprecate this
            if self.config.cuts:
                if "maglim_i" in self.config.cuts:
                    predicate = pc.subtract(pc.multiply(pc.scalar(-2.5), pc.log10(pc.field("lsst_i_el_model3_ext"))), pc.scalar(48.6)) < self.config.cuts["maglim_i"][1]
                else:
                    raise ValueError("No valid cut") from msg
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
                columns=COLUMNS_FLAGSHIP,
                filter=predicate,
            ),
        )

        filter_node = acero.Declaration(
            "filter",
            acero.FilterNodeOptions(
                predicate,
            ),
        )

        column_projection = {k: pc.field(k) for k in COLUMNS_FLAGSHIP}
        projection = column_projection
        project_nodes = []
        for _projection in PROJECTIONS_FLAGSHIP:
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

        # batches = plan.to_reader(use_threadsx=True)
        table = plan.to_table(use_threads=True)

        if DROP_COLS_FLAGSHIP:
            table = table.drop_columns(DROP_COLS_FLAGSHIP)

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
    ) -> None:  # pragma: no cover

        # Try to do this right
        try:
            parsed_filter = parse_item(self.config.cuts)
            predicate = pq.filters_to_expression(parsed_filter)
        except Exception:
            # Fallback to old way.  FIXME, deprecate this
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


class DP1Reducer(RailReducer):
    """Class to reduce the 'DP1' input files for pz analysis"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Reducer Name"),
        cuts=StageParameter(dict, {}, fmt="%s", msg="Selections"),
    )

    _mag_offset = 31.4

    def run(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:  # pragma: no cover

        topdir = os.path.dirname(os.path.dirname(input_catalog))
        columns_file = os.path.join(topdir, "columns.yaml")
        with open(columns_file, "r", encoding="utf-8") as fin:
            columns = yaml.safe_load(fin)

        # Try to do this right
        try:
            parsed_filter = parse_item(self.config.cuts)
            predicate = pq.filters_to_expression(parsed_filter)
        except Exception:
            # Fallback to old way.  FIXME, deprecate this
            mag_cut = self.config.cuts["maglim_i"][1]
            flux_cut = np.power(10, (self._mag_offset - mag_cut) / 2.5)
            if self.config.cuts:
                predicate = (
                    (pc.field("i_psfFlux") > flux_cut)
                    & (pc.field("i_psfFlux") / pc.field("i_psfFluxErr") > 5)
                    & (
                        (pc.field("g_extendedness") > 0.5)
                        | (pc.field("r_extendedness") > 0.5)
                    )
                )

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
                columns=columns,
                filter=predicate,
            ),
        )

        filter_node = acero.Declaration(
            "filter",
            acero.FilterNodeOptions(
                predicate,
            ),
        )

        column_projection = {k: pc.field(k) for k in columns}
        projection = column_projection
        project_nodes = []
        for _projection in PROJECTIONS_DP1:
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

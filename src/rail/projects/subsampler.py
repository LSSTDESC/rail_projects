from __future__ import annotations

import os
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from ceci.config import StageParameter
from rail.core.configurable import Configurable

from .dynamic_class import DynamicClass
from .arrow_utils import parse_item, filter_dataset, inner_join_datasets


class RailSubsampler(Configurable, DynamicClass):
    """Base class for subsampling ata

    The main function in this class is:
    run(...)

    This function will take the input files and make a single output file

    """

    config_options: dict[str, StageParameter] = {}
    sub_classes: dict[str, type[DynamicClass]] = {}

    def __init__(self, **kwargs: Any) -> None:
        """C'tor

        Parameters
        ----------
        **kwargs:
            Configuration parameters for this plotter, must match
            class.config_options data members
        """
        DynamicClass.__init__(self)
        Configurable.__init__(self, **kwargs)

    def get_basename_dict(self, **kwargs: Any) -> dict[str, str]:
        """Retrun a map of the basenames of the files to pull data from

        Returns
        -------
        Map from tag to basename, used to organize input files
        """
        raise NotImplementedError()

    def run(
        self,
        input_files: dict[str, list[str]],
        output: str,
    ) -> None:
        """Subsample the data

        Parameters
        ----------
        input_files: list[str]
            Input files to subsamle

        output: str
            Path to the output file
        """
        raise NotImplementedError()


class RandomSubsampler(RailSubsampler):
    """Pick a random subsample of the data"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Subsampler Name"),
        seed=StageParameter(int, 1234, fmt="%i", msg="Random number seed"),
        num_objects=StageParameter(
            int, None, fmt="%i", required=True, msg="Number of output objects"
        ),
    )

    def get_basename_dict(self, **kwargs: Any) -> dict[str, str]:
        return dict(main=cast(str, kwargs.get("basename", "")))

    def run(
        self,
        input_files: dict[str, list[str]],
        output: str,
    ) -> None:
        assert "main" in input_files
        the_files = input_files["main"]

        dataset = ds.dataset(the_files)
        num_rows = dataset.count_rows()
        print("num rows", num_rows)
        rng = np.random.default_rng(self.config.seed)
        print("sampling", self.config.num_objects)

        size = min(self.config.num_objects, num_rows)
        indices = rng.choice(num_rows, size=size, replace=False)
        subset = dataset.take(indices)
        print("writing", output)

        output_dir = os.path.dirname(output)
        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(
            subset,
            output,
        )
        print("done")


class MultiCatalogSubsampler(RailSubsampler):
    """Pick a sample for multiple catalogs"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Subsampler Name"),
        seed=StageParameter(int, 1234, fmt="%i", msg="Random number seed"),
        num_objects=StageParameter(
            int, None, fmt="%i", required=True, msg="Number of output objects"
        ),
        weight_map=StageParameter(
            str, None, fmt="%s", msg="File with selection weights"
        ),
        object_id_col=StageParameter(
            str, "object_id", fmt="%s", msg="Object Id column name"
        ),
        cuts=StageParameter(dict, None, fmt="%s", msg="Selection cuts"),
        inputs=StageParameter(dict, None, fmt="%s", msg="Input catalog detatils"),
    )

    def get_basename_dict(self, **kwargs: Any) -> dict[str, str]:
        out_dict: dict[str, str] = {}
        for key, val in self.config.inputs.items():
            assert "basename" in val
            out_dict[key] = val.get("basename")
        return out_dict

    @staticmethod
    def _get_mag_columns(input_params: dict[str, Any]) -> list[str]:
        try:
            bands = input_params["bands"]
        except KeyError:  # pragma: no cover
            raise KeyError(
                f"Input paramters does not include 'bands' {list(input_params.keys())}"
            ) from None
        try:
            mag_band_name_template = input_params["mag_band_name_template"]
        except KeyError:  # pragma: no cover
            raise KeyError(
                f"Input paramters does not include 'mag_band_name_template' {list(input_params.keys())}"
            ) from None
        try:
            mag_err_band_name_template = input_params["mag_err_band_name_template"]
        except KeyError:  # pragma: no cover
            raise KeyError(
                f"Input paramters does not include 'mag_err_band_name_template' {list(input_params.keys())}"
            ) from None
        out_list: list[str] = [
            mag_band_name_template.format(band=band_) for band_ in bands
        ]
        out_list += [mag_err_band_name_template.format(band=band_) for band_ in bands]
        return out_list

    def _sub_selection(self, key: str, file_list: list[str]) -> ds.Dataset:
        sub_selection_params = self.config.inputs[key]
        if self.config.cuts is not None:
            all_cuts = self.config.cuts.copy()
        else:
            all_cuts = []
        sub_sel_cuts = sub_selection_params.get("cuts", [])
        if sub_sel_cuts:
            all_cuts += sub_sel_cuts
        parsed_cuts = parse_item(all_cuts)
        dataset = ds.dataset(file_list)
        save_cols: list[str] = [self.config.object_id_col]
        save_cols += self._get_mag_columns(sub_selection_params).copy()
        save_cols += sub_selection_params.get("extra_cols", [])
        filtered = filter_dataset(dataset, parsed_cuts, save_cols)  # type: ignore
        return filtered

    def _merge_selection(self, selected_data: dict[str, ds.Dataset]) -> ds.Dataset:
        return inner_join_datasets(selected_data, self.config.object_id_col)

    def run(
        self,
        input_files: dict[str, list[str]],
        output: str,
    ) -> None:

        selected_data = {
            key: self._sub_selection(key, val) for key, val in input_files.items()
        }
        subset = self._merge_selection(selected_data)
        num_rows = subset.count_rows()

        print("num rows selected", num_rows)

        rng = np.random.default_rng(self.config.seed)
        print("sampling", self.config.num_objects)

        size = min(self.config.num_objects, num_rows)
        indices = rng.choice(num_rows, size=size, replace=False)
        subset = subset.take(indices)
        print("writing", output)

        output_dir = os.path.dirname(output)
        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(
            subset,
            output,
        )
        print("done")


class SpecAreaSubsampler(RailSubsampler):
    """Combine spectroscopic surveys with per-survey area cuts, then join photometric data.

    Unlike MultiCatalogSubsampler (which uses a single pre-merged spec file),
    this class takes individual per-survey spec selection files, applies optional
    area cuts (square RA/Dec boxes) to specified surveys, unions the results,
    then inner-joins with photometric inputs (e.g., Roman).  All selected
    objects are written to the output — no random sub-sampling.

    Area cut convention
    -------------------
    For a survey entry that contains an ``area_cut`` key with sub-keys
    ``ra_center``, ``dec_center``, and ``area_sq_deg``, the selection is::

        |RA  - ra_center|  <= sqrt(area_sq_deg)/2 / cos(dec_center)
        |Dec - dec_center| <= sqrt(area_sq_deg)/2

    This produces a box that is square in projected (RA*cos(dec), Dec) space
    and covers approximately ``area_sq_deg`` square degrees.
    Surveys without an ``area_cut`` entry are included in their entirety.

    Configuration
    -------------
    spec_inputs : dict
        Mapping of survey label to input parameters dict.  Each entry must
        contain ``basename``, ``bands``, ``mag_band_name_template``, and
        ``mag_err_band_name_template``.  Optional keys: ``extra_cols``
        (list), ``cuts`` (list of 3-element filter specs), and ``area_cut``
        (dict with ``ra_center``, ``dec_center``, ``area_sq_deg``).
    photometric_inputs : dict
        Mapping of catalog label to input parameters dict (same structure as
        ``spec_inputs`` but without ``area_cut``).  These catalogs are
        inner-joined with the unioned spec catalog on ``object_id_col``.
    object_id_col : str
        Column used as the join key (default: ``"object_id"``).
    ra_col : str
        RA column name used for area cuts (default: ``"ra"``).
    dec_col : str
        Dec column name used for area cuts (default: ``"dec"``).

    Example YAML (Subsample entry)
    -------------------------------
    .. code-block:: yaml

      - Subsample:
          name: "taskset_2_spec_train_10yr"
          object_id_col: "object_id"
          ra_col: "ra"
          dec_col: "dec"
          spec_inputs:
            zCOSMOS:
              basename: output_select_lsst_obs_cond_10yr_zCOSMOS.pq
              bands: ['u', 'g', 'r', 'i', 'z', 'y']
              mag_band_name_template: "mag_{band}_lsst"
              mag_err_band_name_template: "mag_{band}_lsst_err"
              extra_cols: ['redshift', 'ra', 'dec']
              cuts:
                - ['mag_i_lsst', '<', 25.4]
              area_cut:
                ra_center: 9.0
                dec_center: -42.0
                area_sq_deg: 2.0
            VVDSf02:
              basename: output_select_lsst_obs_cond_10yr_VVDSf02.pq
              bands: ['u', 'g', 'r', 'i', 'z', 'y']
              mag_band_name_template: "mag_{band}_lsst"
              mag_err_band_name_template: "mag_{band}_lsst_err"
              extra_cols: ['redshift', 'ra', 'dec']
              cuts:
                - ['mag_i_lsst', '<', 25.4]
              area_cut:
                ra_center: 14.0
                dec_center: -42.0
                area_sq_deg: 0.6
            DEEP2_LSST:
              basename: output_select_lsst_obs_cond_10yr_DEEP2_LSST.pq
              bands: ['u', 'g', 'r', 'i', 'z', 'y']
              mag_band_name_template: "mag_{band}_lsst"
              mag_err_band_name_template: "mag_{band}_lsst_err"
              extra_cols: ['redshift', 'ra', 'dec']
              cuts:
                - ['mag_i_lsst', '<', 25.4]
              area_cut:
                ra_center: 9.0
                dec_center: -46.0
                area_sq_deg: 2.0
            DESI_BGS:
              basename: output_select_lsst_obs_cond_10yr_DESI_BGS_color.pq
              bands: ['u', 'g', 'r', 'i', 'z', 'y']
              mag_band_name_template: "mag_{band}_lsst"
              mag_err_band_name_template: "mag_{band}_lsst_err"
              extra_cols: ['redshift', 'ra', 'dec']
              cuts:
                - ['mag_i_lsst', '<', 25.4]
            # DESI_LRG, DESI_ELG_LOP etc. follow same pattern (no area_cut = full sky)
          photometric_inputs:
            roman:
              basename: output_deredden_roman_medium.pq
              bands: ['Y', 'J', 'H']
              mag_band_name_template: "mag_{band}_roman"
              mag_err_band_name_template: "mag_{band}_roman_err"
              extra_cols: []
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Subsampler Name"),
        object_id_col=StageParameter(
            str, "object_id", fmt="%s", msg="Object Id column name"
        ),
        ra_col=StageParameter(str, "ra", fmt="%s", msg="RA column name for area cuts"),
        dec_col=StageParameter(
            str, "dec", fmt="%s", msg="Dec column name for area cuts"
        ),
        spec_inputs=StageParameter(
            dict,
            None,
            fmt="%s",
            msg="Per-survey spec selection inputs (with optional area_cut)",
        ),
        photometric_inputs=StageParameter(
            dict,
            None,
            fmt="%s",
            msg="Photometric catalog inputs to inner-join with the spec union",
        ),
    )

    def get_basename_dict(self, **kwargs: Any) -> dict[str, str]:
        out_dict: dict[str, str] = {}
        for key, val in (self.config.spec_inputs or {}).items():
            assert "basename" in val, f"spec_inputs['{key}'] missing 'basename'"
            out_dict[key] = val["basename"]
        for key, val in (self.config.photometric_inputs or {}).items():
            assert "basename" in val, f"photometric_inputs['{key}'] missing 'basename'"
            out_dict[key] = val["basename"]
        return out_dict

    @staticmethod
    def _get_mag_columns(input_params: dict[str, Any]) -> list[str]:
        try:
            bands = input_params["bands"]
        except KeyError:  # pragma: no cover
            raise KeyError(
                f"Input parameters does not include 'bands'"
                f" {list(input_params.keys())}"
            ) from None
        try:
            mag_band_name_template = input_params["mag_band_name_template"]
        except KeyError:  # pragma: no cover
            raise KeyError(
                f"Input parameters does not include 'mag_band_name_template'"
                f" {list(input_params.keys())}"
            ) from None
        try:
            mag_err_band_name_template = input_params["mag_err_band_name_template"]
        except KeyError:  # pragma: no cover
            raise KeyError(
                f"Input parameters does not include 'mag_err_band_name_template'"
                f" {list(input_params.keys())}"
            ) from None
        out_list: list[str] = [
            mag_band_name_template.format(band=band_) for band_ in bands
        ]
        out_list += [mag_err_band_name_template.format(band=band_) for band_ in bands]
        return out_list

    def _make_area_cut_filters(self, input_params: dict[str, Any]) -> list[list]:
        """Return RA/Dec box filter entries for this input, or [] if no area_cut.

        The box is square in projected (RA*cos(dec), Dec) space:
          RA  half-width = sqrt(area) / 2 / cos(dec_center)
          Dec half-width = sqrt(area) / 2
        so that the covered sky area is approximately area_sq_deg.
        """
        area_cut = input_params.get("area_cut")
        if not area_cut:
            return []
        ra0 = float(area_cut["ra_center"])
        dec0 = float(area_cut["dec_center"])
        area = float(area_cut["area_sq_deg"])
        half_side_dec = np.sqrt(area) / 2.0
        half_side_ra = half_side_dec / np.cos(np.radians(dec0))
        ra_col = self.config.ra_col
        dec_col = self.config.dec_col
        return [
            [ra_col, ">=", ra0 - half_side_ra],
            [ra_col, "<=", ra0 + half_side_ra],
            [dec_col, ">=", dec0 - half_side_dec],
            [dec_col, "<=", dec0 + half_side_dec],
        ]

    def _select_input(
        self,
        input_params: dict[str, Any],
        file_list: list[str],
        apply_area_cut: bool = False,
    ) -> pa.Table:
        """Filter dataset, apply optional area cut, project to required columns."""
        all_cuts: list = []
        sub_sel_cuts = input_params.get("cuts", [])
        if sub_sel_cuts:
            all_cuts += list(sub_sel_cuts)
        if apply_area_cut:
            all_cuts += self._make_area_cut_filters(input_params)

        parsed_cuts = parse_item(all_cuts) if all_cuts else []
        dataset = ds.dataset(file_list)

        save_cols: list[str] = [self.config.object_id_col]
        save_cols += self._get_mag_columns(input_params)
        save_cols += input_params.get("extra_cols", [])

        filtered = filter_dataset(dataset, parsed_cuts, save_cols)  # type: ignore
        return filtered.to_table()

    def run(
        self,
        input_files: dict[str, list[str]],
        output: str,
    ) -> None:
        spec_inputs = self.config.spec_inputs or {}
        photo_inputs = self.config.photometric_inputs or {}

        # 1. Process each spec survey, applying area cut where configured
        spec_tables: list[pa.Table] = []
        for key, input_params in spec_inputs.items():
            if key not in input_files:
                print(f"Warning: spec key '{key}' not found in input_files, skipping")
                continue
            table = self._select_input(
                input_params, input_files[key], apply_area_cut=True
            )
            has_area_cut = bool(input_params.get("area_cut"))
            print(
                f"{key}: {table.num_rows} rows"
                f" (area cut {'applied' if has_area_cut else 'not applied'})"
            )
            spec_tables.append(table)

        if not spec_tables:
            raise ValueError("No spec survey inputs produced any data")

        # 2. Union all spec tables; deduplicate by object_id
        #    (an object may pass color cuts for multiple surveys)
        spec_combined = pa.concat_tables(spec_tables, promote_options="default")
        print(f"Combined spec rows before dedup: {spec_combined.num_rows}")

        object_id_col = self.config.object_id_col
        seen: set = set()
        keep_indices: list[int] = []
        for i, oid in enumerate(spec_combined.column(object_id_col).to_pylist()):
            if oid not in seen:
                seen.add(oid)
                keep_indices.append(i)
        if len(keep_indices) < spec_combined.num_rows:
            spec_combined = spec_combined.take(keep_indices)
        print(f"Combined spec rows after dedup: {spec_combined.num_rows}")

        # 3. Inner-join with each photometric input on object_id
        result: pa.Table = spec_combined
        for key, input_params in photo_inputs.items():
            if key not in input_files:
                print(
                    f"Warning: photo key '{key}' not found in input_files, skipping"
                )
                continue
            photo_table = self._select_input(
                input_params, input_files[key], apply_area_cut=False
            )
            print(f"{key}: {photo_table.num_rows} rows")
            result = result.join(
                photo_table,
                keys=object_id_col,
                join_type="inner",
            )

        print(f"Total objects after join: {result.num_rows}")
        print(f"writing {output}")

        output_dir = os.path.dirname(output)
        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(result, output)
        print("done")

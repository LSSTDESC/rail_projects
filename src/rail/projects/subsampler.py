from __future__ import annotations

import os
from typing import Any, cast

import numpy as np
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

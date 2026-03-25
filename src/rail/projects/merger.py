from __future__ import annotations

import math
import os
from typing import Any

import numpy as np
import yaml
from ceci.config import StageParameter
from rail.core.configurable import Configurable

from .panda_utils import union_dataframes_deduplicated
from .dynamic_class import DynamicClass


class RailMergerAlgo(Configurable, DynamicClass):
    """Base class for merging output files

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
        input_basenames: list[str]
        output_basename: str,
    ) -> None:
        """Subsample the data

        Parameters
        ----------
        input_catalog:
            Input files to subsamle

        output_catalog:
            Path to the output file
            
        input_basenames:
            Basenames for input files that will be merged

        output_basename:
            Basename for output file

        """
        raise NotImplementedError()


class SpecSelectionMergerAlgo(RailMergerAlgo):
    """Class to merge different spec selections"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Merger Name"),
        merge_col=StageParameter(str, "object_id", fmt="%s", required=True, msg="Merge column name")
        inputs=StageParameter(dict, None, fmt="%s", msg="Input catalog detatils"),
        output_basename=StageParameter(dict, None, fmt="%s", msg="Input catalog detatils"),
    )

    def run(
        self,
        input_catalog: str,
        output_catalog: str,
    ) -> None:

        for input_key, input_basename in self.config.inputs.items():
            input_fullname = os.path.join(input_catalog, input_basename)
            input_dataframe = tables_io.read(input_fullname)
            input_dataframe[input_key] = True

        merged = union_dataframes_deduplicated(input_dataframes, self.config.merge_col)
        output_file = os.path.join(output_catalog, output_basename)

        tables_io.write(merged, output_file)

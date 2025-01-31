from __future__ import annotations

import os
from typing import Any

import numpy as np

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from ceci.config import StageParameter

from .configurable import Configurable
from .dynamic_class import DynamicClass


class RailSubsampler(Configurable, DynamicClass):
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
        input_files: list[str],
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
    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Subsampler Name"),
        seed=StageParameter(int, 1234, fmt="%i", msg="Random number seed"),
        num_objects=StageParameter(
            int, None, fmt="%i", required=True, msg="Number of output objects"
        ),
    )

    def __call__(
        self,
        input_files: list[str],
        output: str,
    ) -> None:
        dataset = ds.dataset(input_files)
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

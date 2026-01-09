from __future__ import annotations

import os
from typing import Any

import numpy as np
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from ceci.config import StageParameter
from rail.core.configurable import Configurable

from .dynamic_class import DynamicClass


class RailSplitter(Configurable, DynamicClass):
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
        input_file: str,
        output_train: str,
        output_test: str,
    ) -> None:
        """Subsample the data

        Parameters
        ----------
        input_file:
            Input files to spllit

        output_train: str
            Path to the output training file

        output_test: str
            Path to the output testing file
        """
        raise NotImplementedError()


class RandomSplitter(RailSplitter):
    """Pick a random subsample of the data"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Splitter Name"),
        seed=StageParameter(int, 1234, fmt="%i", msg="Random number seed"),
        train_fraction=StageParameter(
            float,
            0.70,
            fmt="%s",
            msg="Training fraction",
        ),
    )

    def run(
        self,
        input_file: str,
        output_train: str,
        output_test: str,
    ) -> None:
        dataset = ds.dataset([input_file])
        num_rows = dataset.count_rows()
        print("num rows", num_rows)
        rng = np.random.default_rng(self.config.seed)

        num_train = int(self.config.train_fraction * num_rows)
        print("sampling", num_train)

        mask = np.zeros(num_rows, dtype=bool)
        indices = rng.choice(num_rows, size=num_train, replace=False)
        mask[indices] = True
        subset_train = dataset.take(mask)
        subset_test = dataset.take(~mask)

        print("writing", output_train)

        output_dir = os.path.dirname(output_train)
        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(
            subset_train,
            output_train,
        )

        print("writing", output_test)
        output_dir = os.path.dirname(output_test)
        os.makedirs(output_dir, exist_ok=True)
        pq.write_table(
            subset_test,
            output_test,
        )
        print("done")

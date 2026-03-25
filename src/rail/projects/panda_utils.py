from __future__ import annotations
from typing import Any, List, Literal, Optional

import pandas as pd


def union_dataframes_deduplicated(
    dataframes: List[pd.DataFrame],
    dedup_column: str,
    keep: Literal['first', 'last'] = 'first',
) -> pd.DataFrame:  # pragma: no cover
    """
    Create a union of multiple dataframes and remove duplicates based on a key column.
    
    Parameters
    ----------
    dataframes : List[pd.DataFrame]
        List of pandas DataFrames with identical columns
    dedup_column : str
        Column name to use for identifying and removing duplicate rows
    keep : {'first', 'last'}, default 'first'
        Which duplicate to keep:
        - 'first': keep first occurrence across all dataframes
        - 'last': keep last occurrence across all dataframes
    
    Returns
    -------
    pd.DataFrame
        Unified dataframe with duplicates removed
        
    Examples
    --------
    >>> df1 = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
    >>> df2 = pd.DataFrame({'id': [2, 3, 4], 'value': ['x', 'y', 'z']})
    >>> result = union_dataframes_deduplicated([df1, df2], dedup_column='id')
    >>> # Returns rows with id=[1, 2, 3, 4], keeping first occurrence of 2 and 3
    """
    
    if not dataframes:
        raise ValueError("dataframes list cannot be empty")
    
    if len(dataframes) == 1:
        return dataframes[0].drop_duplicates(subset=dedup_column, keep=keep).reset_index(drop=True)
    
    # Verify all dataframes have the same columns
    base_columns = set(dataframes[0].columns)
    
    # Verify dedup_column exists
    if dedup_column not in base_columns:
        raise ValueError(f"dedup_column '{dedup_column}' not found in dataframes")

    # Concatenate all dataframes
    combined = pd.concat(dataframes, ignore_index=True)

    # Remove duplicates
    result = combined.drop_duplicates(subset=dedup_column, keep=keep)
    
    return result.reset_index(drop=True)

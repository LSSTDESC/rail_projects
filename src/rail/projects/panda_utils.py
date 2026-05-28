from __future__ import annotations
from typing import List, Literal

import pandas as pd


def union_dataframes_deduplicated(
    dataframes: List[pd.DataFrame],
    dedup_column: str,
    keep: Literal["first", "last"] = "first",
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
        return (
            dataframes[0]
            .drop_duplicates(subset=dedup_column, keep=keep)
            .reset_index(drop=True)
        )

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


def outer_join_deduplicate_columns(
    dataframes: List[pd.DataFrame],
    join_column: str,
    keep: Literal["first", "last"] = "first",
) -> pd.DataFrame:  # pragma: no cover
    """
    Outer join multiple dataframes, keeping only one copy of overlapping columns.

    Parameters
    ----------
    dataframes : List[pd.DataFrame]
        List of pandas DataFrames to join
    join_column : str
        Column name to use as the join key (must exist in all dataframes)
    keep : {'first', 'last'}, default 'first'
        Which dataframe's values to keep for overlapping columns:
        - 'first': prioritize values from earlier dataframes in the list
        - 'last': prioritize values from later dataframes in the list

    Returns
    -------
    pd.DataFrame
        Result of outer join with all rows from all dataframes and deduplicated columns

    Examples
    --------
    >>> df1 = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b'], 'score': [10, 20]})
    >>> df2 = pd.DataFrame({'id': [2, 3], 'value': ['x', 'y'], 'category': ['A', 'B']})
    >>> df3 = pd.DataFrame({'id': [3, 4], 'value': ['m', 'n'], 'flag': [True, False]})
    >>> result = outer_join_deduplicate_columns([df1, df2, df3], join_column='id', keep='first')
    >>> # Returns: id=[1, 2, 3, 4] with columns: id, value, score, category, flag
    >>> # For id=2: uses 'value' from df1 ('b')
    >>> # For id=3: uses 'value' from df2 ('y') since df1 doesn't have id=3
    """

    if not dataframes:
        raise ValueError("dataframes list cannot be empty")

    if len(dataframes) == 1:
        return dataframes[0].reset_index(drop=True)

    # Verify join_column exists in all dataframes
    for i, df in enumerate(dataframes):
        if join_column not in df.columns:
            raise ValueError(
                f"join_column '{join_column}' not found in dataframe at index {i}"
            )

    # Start with the first dataframe
    result = dataframes[0].copy()

    # Sequentially join with remaining dataframes
    for i, df in enumerate(dataframes[1:], start=1):
        # Find overlapping columns (excluding join column)
        result_cols = set(result.columns)
        df_cols = set(df.columns)
        overlapping_cols = (result_cols & df_cols) - {join_column}

        # Perform outer join with suffixes for overlapping columns
        merged = result.merge(
            df,
            on=join_column,
            how="outer",
            suffixes=("_existing", "_new"),
        )

        # Deduplicate overlapping columns
        for col in overlapping_cols:
            existing_col = f"{col}_existing"
            new_col = f"{col}_new"

            if keep == "first":
                # Keep existing values, fill with new where existing is NaN
                merged[col] = merged[existing_col].fillna(merged[new_col])
            else:  # keep == "last"
                # Keep new values, fill with existing where new is NaN
                merged[col] = merged[new_col].fillna(merged[existing_col])

            # Drop the suffixed columns
            merged = merged.drop(columns=[existing_col, new_col])

        result = merged

    return result.reset_index(drop=True)

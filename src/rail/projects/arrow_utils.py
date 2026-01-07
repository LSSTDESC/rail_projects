from __future__ import annotations
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow.parquet import filters_to_expression


def filter_dataset(
    dataset: ds.Dataset,
    filter_conditions: list[tuple[str, str, Any]] | list[list[tuple[str, str, Any]]],
    columns: list[str] | dict[str, str],
) -> _ProjectedDataset:
    """
    Filter a PyArrow dataset and select specific columns.

    This is a convenience wrapper around PyArrow's native filtering and
    column projection functionality.

    Parameters
    ----------
    dataset
        The PyArrow dataset to filter.
    filter_conditions
        Filter specification using PyArrow's standard filter format:

        - List of tuples: [(column, op, value), ...] for AND logic
        - List of lists: [[(col, op, val), ...], [(col, op, val), ...]]
          for OR logic between groups

        Supported operators: '==', '!=', '<', '<=', '>', '>=', 'in', 'not in'

        Examples:
            [('category', '==', 'A'), ('value', '>', 10)]  # AND
            [[('category', '==', 'A')], [('value', '>=', 50)]]  # OR
    columns
        Column specification for projection:

        - List of column names to retain
        - Dict mapping new names to original names for renaming

        Examples:
            ['id', 'name', 'value']
            {'user_id': 'id', 'user_name': 'name'}

    Returns
    -------
        Filtered dataset with column projection applied. The dataset remains
        lazy and will be materialized only when explicitly converted.

    Raises
    ------
    ValueError
        If filter conditions are invalid or columns don't exist in schema.

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.dataset as ds
    >>>
    >>> table = pa.table({
    ...     'id': [1, 2, 3, 4],
    ...     'category': ['A', 'B', 'A', 'C'],
    ...     'value': [10, 20, 30, 40]
    ... })
    >>> dataset = ds.dataset(table)

    Simple AND filter:
    >>> filtered = filter_dataset(
    ...     dataset,
    ...     [('category', '==', 'A'), ('value', '>', 15)],
    ...     ['id', 'value']
    ... )

    OR logic:
    >>> filtered = filter_dataset(
    ...     dataset,
    ...     [[('category', '==', 'A')], [('value', '>=', 40)]],
    ...     ['id', 'category', 'value']
    ... )

    Column renaming:
    >>> filtered = filter_dataset(
    ...     dataset,
    ...     [('category', '==', 'A')],
    ...     {'identifier': 'id', 'amount': 'value'}
    ... )

    Notes
    -----
    - Uses PyArrow's native `filters_to_expression` for filter construction
    - Column projection is applied during scanning, maintaining lazy evaluation
    - See PyArrow documentation for full filter syntax details
    """
    # Build filter expression using PyArrow's built-in function
    if filter_conditions:
        filter_expr = filters_to_expression(filter_conditions)
        filtered_dataset = dataset.filter(filter_expr)
    else:
        filtered_dataset = dataset

    # Validate columns exist in schema
    schema_names = set(filtered_dataset.schema.names)

    if isinstance(columns, dict):
        # Dict: {new_name: old_name}
        missing_cols = set(columns.values()) - schema_names
        if missing_cols:
            raise ValueError(f"Columns not found in dataset schema: {missing_cols}")
        column_spec: dict[str, str] | list[str] = columns
    else:
        # List: [col1, col2, ...]
        missing_cols = set(columns) - schema_names
        if missing_cols:
            raise ValueError(f"Columns not found in dataset schema: {missing_cols}")
        column_spec = columns

    # Return a wrapper that applies column projection during scanning
    return _ProjectedDataset(filtered_dataset, column_spec)


class _ProjectedDataset:
    """
    Wrapper for a dataset with column projection applied lazily.

    This class wraps a PyArrow dataset and applies column projection
    (selection and/or renaming) only when the dataset is scanned or
    materialized.
    """

    def __init__(
        self,
        dataset: ds.Dataset,
        columns: list[str] | dict[str, str],
    ):
        """
        Initialize the projected dataset.

        Parameters
        ----------
        dataset
            Source dataset to wrap.
        columns
            Column specification (list for selection, dict for renaming).
        """
        self._dataset = dataset
        self._columns = columns

    @property
    def schema(self) -> pa.Schema:
        """Get the schema of the underlying dataset."""
        return self._dataset.schema

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying dataset."""
        return getattr(self._dataset, name)

    def scanner(self, **kwargs: Any) -> ds.Scanner:
        """Create a scanner with column projection applied."""
        if "columns" not in kwargs:
            if isinstance(self._columns, dict):
                # For dict, we need to use expressions
                kwargs["columns"] = {
                    new_name: pc.field(old_name)
                    for new_name, old_name in self._columns.items()
                }
            else:
                kwargs["columns"] = self._columns
        return self._dataset.scanner(**kwargs)

    def to_table(self, **kwargs: Any) -> pa.Table:
        """Materialize the dataset with column projection applied."""
        return self.scanner(**kwargs).to_table()


def inner_join_datasets(
    datasets: dict[str, ds.Dataset | _ProjectedDataset],
    join_key: str,
    use_threads: bool = True,
) -> ds.Dataset:
    """
    Perform an inner join on multiple datasets using a specified join key.

    This is a thin wrapper around PyArrow's native join functionality.
    Column name collisions are automatically handled by PyArrow using
    suffixes based on dataset names.

    Parameters
    ----------
    datasets
        Dictionary mapping dataset names to PyArrow datasets. Names are used
        as suffixes for conflicting column names. Must contain at least one
        dataset.
    join_key
        Name of the column to use as the join key. This column must exist in
        all datasets.
    use_threads
        Whether to use multiple threads for the join operation.

    Returns
    -------
        Materialized table containing the inner join result. The join key
        column appears only once. Conflicting column names are suffixed with
        dataset names (e.g., 'value_users', 'value_orders').

    Raises
    ------
    ValueError
        If no datasets are provided or if the join key is missing from any
        dataset.

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.dataset as ds
    >>>
    >>> users_ds = ds.dataset(pa.table({
    ...     'id': [1, 2, 3],
    ...     'name': ['Alice', 'Bob', 'Charlie'],
    ...     'age': [25, 30, 35]
    ... }))
    >>> orders_ds = ds.dataset(pa.table({
    ...     'id': [2, 3, 4],
    ...     'total': [100, 200, 150]
    ... }))
    >>>
    >>> result = inner_join_datasets(
    ...     {'users': users_ds, 'orders': orders_ds},
    ...     'id'
    ... )
    >>> result.column_names
    ['id', 'name', 'age', 'total']

    With column conflicts:
    >>> users_ds = ds.dataset(pa.table({
    ...     'id': [1, 2, 3],
    ...     'value': [10, 20, 30]
    ... }))
    >>> orders_ds = ds.dataset(pa.table({
    ...     'id': [2, 3, 4],
    ...     'value': [100, 200, 150]
    ... }))
    >>> result = inner_join_datasets(
    ...     {'users': users_ds, 'orders': orders_ds},
    ...     'id'
    ... )
    >>> result.column_names
    ['id', 'value_users', 'value_orders']

    Notes
    -----
    - Uses PyArrow's native Table.join() method with automatic suffix handling
    - For multiple datasets, joins are performed sequentially
    - Column name conflicts are resolved using dataset names as suffixes
    """
    if not datasets:
        raise ValueError("At least one dataset must be provided")

    # Validate join key exists in all datasets
    for name, dataset in datasets.items():
        if join_key not in dataset.schema.names:
            raise ValueError(
                f"Join key '{join_key}' not found in dataset '{name}' schema"
            )

    dataset_items = list(datasets.items())

    if len(dataset_items) == 1:
        # Single dataset
        _, dataset = dataset_items[0]
        if isinstance(dataset, _ProjectedDataset):
            return ds.dataset(dataset.to_table())
        return dataset

    # Perform sequential joins using PyArrow's native join with suffixes
    first_name, first_dataset = dataset_items[0]
    if isinstance(first_dataset, _ProjectedDataset):
        result = ds.dataset(first_dataset.to_table())
    else:
        result = first_dataset

    for name, dataset in dataset_items[1:]:
        if isinstance(dataset, _ProjectedDataset):
            right_table = ds.dataset(dataset.to_table())
        else:
            right_table = dataset

        # Perform inner join with automatic suffix handling
        result = result.join(
            right_table,
            keys=join_key,
            join_type="inner",
            left_suffix=f"_{first_name}",
            right_suffix=f"_{name}",
            use_threads=use_threads,
        )

        # Update the "left" name for subsequent joins
        # (this accumulates previous dataset names)
        first_name = f"{first_name}+{name}"

    return result

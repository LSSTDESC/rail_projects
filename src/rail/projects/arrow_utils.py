from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc


def filter_dataset(
    dataset: ds.Dataset,
    filter_conditions: dict[str, Any] | list[dict[str, Any]],
    columns: list[str],
) -> ds.Dataset:
    """
    Filter a PyArrow dataset and select specific columns.

    This function applies filter conditions to a PyArrow dataset and retains
    only the specified columns. The dataset remains lazy and is not
    materialized.

    Parameters
    ----------
    dataset
        The PyArrow dataset to filter.
    filter_conditions
        Filter specification in one of two formats:
        
        - Single dict: Conditions are combined with AND logic. Each value can
          be a scalar (equality), a tuple of (operator, value) for
          comparisons, or a tuple of (operator, value1, value2) for BETWEEN.
        - List of dicts: Each dict represents a group of AND conditions.
          Groups are combined with OR logic.
        
        Supported operators: '==', '!=', '<', '<=', '>', '>=', 'between'.
    columns
        List of column names to retain in the filtered dataset. All other
        columns will be discarded.

    Returns
    -------
        Filtered lazy dataset containing only the specified columns.

    Raises
    ------
    ValueError
        If any column in `columns` or `filter_conditions` does not exist in
        the dataset schema, or if an unsupported operator is provided.

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.dataset as ds
    >>> table = pa.table({
    ...     'id': [1, 2, 3, 4],
    ...     'category': ['A', 'B', 'A', 'C'],
    ...     'value': [10, 20, 30, 40]
    ... })
    >>> dataset = ds.dataset(table)
    
    Simple equality filter (AND logic):
    >>> filtered = filter_dataset(
    ...     dataset,
    ...     {'category': 'A', 'value': ('>', 15)},
    ...     ['id', 'value']
    ... )
    
    OR logic between condition groups:
    >>> filtered = filter_dataset(
    ...     dataset,
    ...     [{'category': 'A'}, {'value': ('>=', 40)}],
    ...     ['id', 'category', 'value']
    ... )
    
    Range query with BETWEEN:
    >>> filtered = filter_dataset(
    ...     dataset,
    ...     {'value': ('between', 15, 35)},
    ...     ['id', 'value']
    ... )

    Notes
    -----
    - The returned dataset is lazy and will not be materialized until
      explicitly converted (e.g., via `.to_table()`).
    - Column projection is applied during scanning, not immediately.
    - Supported comparison operators: ==, !=, <, <=, >, >=, between.
    """
    schema = dataset.schema

    # Normalize filter_conditions to list of dicts for uniform processing
    if isinstance(filter_conditions, dict):
        condition_groups = [filter_conditions]
    else:
        condition_groups = filter_conditions

    # Validate columns
    schema_names = set(schema.names)
    requested_cols = set(columns)
    
    # Collect all filter columns from all condition groups
    filter_cols = set()
    for group in condition_groups:
        filter_cols.update(group.keys())

    missing_cols = requested_cols - schema_names
    if missing_cols:
        raise ValueError(f"Columns not found in dataset schema: {missing_cols}")

    missing_filter_cols = filter_cols - schema_names
    if missing_filter_cols:
        raise ValueError(
            f"Filter columns not found in dataset schema: {missing_filter_cols}"
        )

    # Build filter expression
    filter_expr = _build_filter_expression(condition_groups)

    # Apply filter if conditions exist
    if filter_expr is not None:
        filtered_dataset = dataset.filter(filter_expr)
    else:
        filtered_dataset = dataset

    # Return dataset with column projection applied via scanner
    # We need to wrap it to maintain lazy evaluation with column selection
    return _create_projected_dataset(filtered_dataset, columns)


def _build_filter_expression(
    condition_groups: list[dict[str, Any]]
) -> pc.Expression | None:
    """
    Build a PyArrow filter expression from condition groups.

    Parameters
    ----------
    condition_groups
        List of condition dictionaries. Each dict's conditions are combined
        with AND, and groups are combined with OR.

    Returns
    -------
        Combined filter expression, or None if no conditions.

    Raises
    ------
    ValueError
        If an unsupported operator is provided.
    """
    or_expressions = []

    for group in condition_groups:
        and_expr = None
        
        for column_name, condition in group.items():
            field = pc.field(column_name)
            
            # Parse condition
            if isinstance(condition, tuple):
                operator = condition[0]
                
                if operator == 'between':
                    if len(condition) != 3:
                        raise ValueError(
                            f"'between' operator requires 2 values, got "
                            f"{len(condition) - 1}"
                        )
                    expr = (field >= condition[1]) & (field <= condition[2])
                elif operator == '==':
                    expr = field == condition[1]
                elif operator == '!=':
                    expr = field != condition[1]
                elif operator == '<':
                    expr = field < condition[1]
                elif operator == '<=':
                    expr = field <= condition[1]
                elif operator == '>':
                    expr = field > condition[1]
                elif operator == '>=':
                    expr = field >= condition[1]
                else:
                    raise ValueError(f"Unsupported operator: {operator}")
            else:
                # Scalar value means equality
                expr = field == condition
            
            # Combine with AND
            if and_expr is None:
                and_expr = expr
            else:
                and_expr = and_expr & expr
        
        if and_expr is not None:
            or_expressions.append(and_expr)
    
    # Combine groups with OR
    if not or_expressions:
        return None
    
    final_expr = or_expressions[0]
    for expr in or_expressions[1:]:
        final_expr = final_expr | expr
    
    return final_expr


def _create_projected_dataset(
    dataset: ds.Dataset, columns: list[str]
) -> ds.Dataset:
    """
    Create a dataset wrapper that projects only specified columns.

    Parameters
    ----------
    dataset
        Source dataset.
    columns
        Columns to retain.

    Returns
    -------
        Dataset that will project columns when scanned.
    """
    # Create a new dataset with a custom scanner that projects columns
    # PyArrow datasets don't directly support column projection, so we use
    # a factory pattern
    class ProjectedDataset:
        def __init__(self, source_dataset: ds.Dataset, column_list: list[str]):
            self._dataset = source_dataset
            self._columns = column_list
        
        def __getattr__(self, name: str) -> Any:
            # Delegate all other attributes to the underlying dataset
            return getattr(self._dataset, name)
        
        def scanner(self, **kwargs: Any) -> ds.Scanner:
            # Override scanner to include column projection
            if 'columns' not in kwargs:
                kwargs['columns'] = self._columns
            return self._dataset.scanner(**kwargs)
        
        def to_table(self, **kwargs: Any) -> pa.Table:
            # Convenience method for materialization with projection
            if 'columns' not in kwargs:
                kwargs['columns'] = self._columns
            return self._dataset.to_table(**kwargs)
    
    return ProjectedDataset(dataset, columns)


def inner_join_datasets(
    datasets: dict[str, Any],
    join_key: str,
) -> pa.Table:
    """
    Perform an inner join on multiple named datasets using a specified join key.

    This function joins multiple PyArrow datasets (or ProjectedDataset objects)
    on a common column. All datasets must contain the join key column. The
    result is materialized as a PyArrow table with column names prefixed by
    their dataset names.

    Parameters
    ----------
    datasets
        Dictionary mapping dataset names to PyArrow datasets or
        ProjectedDataset objects. Names are used as column prefixes for
        non-join-key columns. Must contain at least one dataset.
    join_key
        Name of the column to use as the join key. This column must exist in
        all datasets.

    Returns
    -------
        Materialized table containing the inner join result. Columns from
        different datasets (excluding the join key) are prefixed with their
        dataset name and an underscore (e.g., 'users_name', 'orders_total').

    Raises
    ------
    ValueError
        If no datasets are provided, if the join key is missing from any
        dataset, or if dataset names contain underscores (reserved for the
        naming scheme).

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.dataset as ds
    >>> 
    >>> # Create sample datasets
    >>> users_ds = ds.dataset(pa.table({
    ...     'id': [1, 2, 3],
    ...     'name': ['Alice', 'Bob', 'Charlie'],
    ...     'age': [25, 30, 35]
    ... }))
    >>> orders_ds = ds.dataset(pa.table({
    ...     'id': [2, 3, 4],
    ...     'dept': ['Engineering', 'Sales', 'HR'],
    ...     'total': [700, 600, 550]
    ... }))
    >>> 
    >>> # Perform inner join
    >>> result = inner_join_datasets(
    ...     {'users': users_ds, 'orders': orders_ds},
    ...     'id'
    ... )
    >>> result.to_pydict()
    {'id': [2, 3],
     'users_name': ['Bob', 'Charlie'],
     'users_age': [30, 35],
     'orders_dept': ['Engineering', 'Sales'],
     'orders_total': [700, 600]}

    Notes
    -----
    - The function materializes datasets only when necessary for joining.
    - Only rows with matching join key values across all datasets are retained
      (inner join semantics).
    - For two datasets, PyArrow's native join is used. For more than two,
      sequential joins are performed.
    - The join key column appears only once in the result without a prefix.
    - Dataset names should not contain underscores to avoid ambiguity in
      column naming.
    """
    if not datasets:
        raise ValueError("At least one dataset must be provided")

    # Validate dataset names don't contain underscores
    invalid_names = [name for name in datasets.keys() if '_' in name]
    if invalid_names:
        raise ValueError(
            f"Dataset names cannot contain underscores: {invalid_names}. "
            "Underscores are reserved for the column naming scheme."
        )

    dataset_items = list(datasets.items())

    if len(dataset_items) == 1:
        # Single dataset - verify join key exists and return with prefixes
        name, dataset = dataset_items[0]
        table = dataset.to_table()
        if join_key not in table.column_names:
            raise ValueError(
                f"Join key '{join_key}' not found in dataset '{name}' schema"
            )
        # Add prefix to all columns except join key
        return _add_column_prefix(table, name, join_key)

    # Validate all datasets have the join key (lazy - don't materialize yet)
    for name, dataset in dataset_items:
        schema = dataset.schema
        if join_key not in schema.names:
            raise ValueError(
                f"Join key '{join_key}' not found in dataset '{name}' schema"
            )

    # Perform sequential inner joins
    # Materialize only as needed during joins
    first_name, first_dataset = dataset_items[0]
    result = _add_column_prefix(first_dataset.to_table(), first_name, join_key)

    for name, dataset in dataset_items[1:]:
        right_table = _add_column_prefix(dataset.to_table(), name, join_key)
        result = _inner_join_two_tables(result, right_table, join_key)

    return result


def _inner_join_two_tables(
    left: pa.Table,
    right: pa.Table,
    join_key: str,
) -> pa.Table:
    """
    Perform an inner join on two PyArrow tables.

    Parameters
    ----------
    left
        Left table for the join.
    right
        Right table for the join.
    join_key
        Column name to join on.

    Returns
    -------
        Joined table.

    Notes
    -----
    Assumes column names are already prefixed and don't conflict.
    Uses PyArrow's hash join implementation for efficiency.
    """
    result = left.join(
        right,
        keys=join_key,
        join_type="inner",
    )

    return result


def _add_column_prefix(
    table: pa.Table,
    prefix: str,
    exclude_column: str,
) -> pa.Table:
    """
    Add a prefix to all column names except the specified exclusion.

    Parameters
    ----------
    table
        Table whose columns will be prefixed.
    prefix
        Prefix to add to column names.
    exclude_column
        Column name to exclude from prefixing (typically the join key).

    Returns
    -------
        New table with prefixed column names.

    Examples
    --------
    >>> table = pa.table({'id': [1, 2], 'name': ['a', 'b']})
    >>> prefixed = _add_column_prefix(table, 'users', 'id')
    >>> prefixed.column_names
    ['id', 'users_name']
    """
    new_names = []
    for name in table.column_names:
        if name == exclude_column:
            new_names.append(name)
        else:
            new_names.append(f"{prefix}_{name}")

    return table.rename_columns(new_names)

import pytest
import pyarrow as pa
import pyarrow.dataset as ds

from rail.projects.arrow_utils import (
    filter_dataset,
    _build_filter_expression,
    inner_join_datasets,
    _inner_join_two_tables,
    _add_column_prefix,
)


class TestBuildFilterExpression:
    """Tests for the _build_filter_expression helper function."""

    def test_empty_conditions(self):
        """Test that empty condition groups return None."""
        result = _build_filter_expression([])
        assert result is None

    def test_empty_dict_in_conditions(self):
        """Test that a list with an empty dict returns None."""
        result = _build_filter_expression([{}])
        assert result is None

    def test_single_equality_condition(self):
        """Test a single equality condition."""
        expr = _build_filter_expression([{"category": "A"}])
        assert expr is not None
        # Expression should be evaluable (we can't easily test the exact
        # structure, but we can verify it's valid)

    def test_multiple_and_conditions(self):
        """Test multiple conditions combined with AND logic."""
        expr = _build_filter_expression([{"category": "A", "value": 10}])
        assert expr is not None

    def test_comparison_operators(self):
        """Test all supported comparison operators."""
        operators = [
            ("==", 10),
            ("!=", 10),
            ("<", 10),
            ("<=", 10),
            (">", 10),
            (">=", 10),
        ]
        
        for op, val in operators:
            expr = _build_filter_expression([{"value": (op, val)}])
            assert expr is not None, f"Operator {op} failed"

    def test_between_operator(self):
        """Test the BETWEEN operator."""
        expr = _build_filter_expression([{"value": ("between", 10, 20)}])
        assert expr is not None

    def test_between_operator_wrong_arg_count(self):
        """Test that BETWEEN with wrong number of args raises ValueError."""
        with pytest.raises(ValueError, match="'between' operator requires 2 values"):
            _build_filter_expression([{"value": ("between", 10)}])

    def test_unsupported_operator(self):
        """Test that unsupported operators raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported operator: invalid"):
            _build_filter_expression([{"value": ("invalid", 10)}])

    def test_or_logic_multiple_groups(self):
        """Test OR logic with multiple condition groups."""
        expr = _build_filter_expression([
            {"category": "A"},
            {"category": "B"}
        ])
        assert expr is not None

    def test_complex_or_and_combination(self):
        """Test complex combination of OR and AND logic."""
        expr = _build_filter_expression([
            {"category": "A", "value": (">", 10)},
            {"category": "B", "value": ("<", 5)}
        ])
        assert expr is not None


class TestFilterDataset:
    """Tests for the main filter_dataset function."""

    @pytest.fixture
    def sample_dataset(self):
        """Create a sample PyArrow dataset for testing."""
        table = pa.table({
            "id": [1, 2, 3, 4, 5],
            "category": ["A", "B", "A", "C", "B"],
            "value": [10, 20, 30, 40, 50],
            "extra": ["x", "y", "z", "w", "v"]
        })
        return ds.dataset(table)

    def test_simple_equality_filter(self, sample_dataset):
        """Test simple equality filtering."""
        filtered = filter_dataset(
            sample_dataset,
            {"category": "A"},
            ["id", "category", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 2
        assert result["id"].to_pylist() == [1, 3]
        assert result["category"].to_pylist() == ["A", "A"]

    def test_column_projection(self, sample_dataset):
        """Test that only specified columns are retained."""
        filtered = filter_dataset(
            sample_dataset,
            {"category": "A"},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.column_names == ["id", "value"]
        assert "category" not in result.column_names
        assert "extra" not in result.column_names

    def test_and_logic_multiple_conditions(self, sample_dataset):
        """Test AND logic with multiple conditions."""
        filtered = filter_dataset(
            sample_dataset,
            {"category": "A", "value": (">", 15)},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 1
        assert result["id"].to_pylist() == [3]
        assert result["value"].to_pylist() == [30]

    def test_or_logic_condition_groups(self, sample_dataset):
        """Test OR logic with multiple condition groups."""
        filtered = filter_dataset(
            sample_dataset,
            [{"category": "A"}, {"value": (">=", 50)}],
            ["id", "category", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 3
        assert set(result["id"].to_pylist()) == {1, 3, 5}

    def test_greater_than_filter(self, sample_dataset):
        """Test greater than comparison."""
        filtered = filter_dataset(
            sample_dataset,
            {"value": (">", 25)},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 3
        assert result["id"].to_pylist() == [3, 4, 5]

    def test_less_than_or_equal_filter(self, sample_dataset):
        """Test less than or equal comparison."""
        filtered = filter_dataset(
            sample_dataset,
            {"value": ("<=", 20)},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 2
        assert result["id"].to_pylist() == [1, 2]

    def test_between_filter(self, sample_dataset):
        """Test BETWEEN range query."""
        filtered = filter_dataset(
            sample_dataset,
            {"value": ("between", 20, 40)},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 3
        assert result["id"].to_pylist() == [2, 3, 4]
        assert result["value"].to_pylist() == [20, 30, 40]

    def test_not_equal_filter(self, sample_dataset):
        """Test not equal comparison."""
        filtered = filter_dataset(
            sample_dataset,
            {"category": ("!=", "A")},
            ["id", "category"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 3
        assert "A" not in result["category"].to_pylist()

    def test_empty_filter_conditions(self, sample_dataset):
        """Test that empty filter conditions return all rows."""
        filtered = filter_dataset(
            sample_dataset,
            {},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 5

    def test_no_matching_rows(self, sample_dataset):
        """Test filter that matches no rows returns empty table."""
        filtered = filter_dataset(
            sample_dataset,
            {"value": (">", 100)},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 0
        assert result.column_names == ["id", "value"]

    def test_missing_output_column(self, sample_dataset):
        """Test that requesting non-existent column raises ValueError."""
        with pytest.raises(ValueError, match="Columns not found in dataset schema"):
            filter_dataset(
                sample_dataset,
                {"category": "A"},
                ["id", "nonexistent"]
            )

    def test_missing_filter_column(self, sample_dataset):
        """Test that filtering on non-existent column raises ValueError."""
        with pytest.raises(ValueError, match="Filter columns not found"):
            filter_dataset(
                sample_dataset,
                {"nonexistent": "value"},
                ["id", "value"]
            )

    def test_complex_or_and_combination(self, sample_dataset):
        """Test complex combination of OR and AND logic."""
        filtered = filter_dataset(
            sample_dataset,
            [
                {"category": "A", "value": ("<", 20)},
                {"category": "B", "value": (">", 40)}
            ],
            ["id", "category", "value"]
        )
        
        result = filtered.to_table()
        # Should match: (category=A AND value<20) OR (category=B AND value>40)
        # Row 1: A, 10 ✓
        # Row 5: B, 50 ✓
        assert result.num_rows == 2
        assert set(result["id"].to_pylist()) == {1, 5}

    def test_lazy_evaluation(self, sample_dataset):
        """Test that the returned dataset is lazy (not materialized)."""
        filtered = filter_dataset(
            sample_dataset,
            {"category": "A"},
            ["id", "value"]
        )
        
        # The filtered object should be a dataset-like object
        assert hasattr(filtered, "scanner")
        assert hasattr(filtered, "to_table")
        
        # Should be able to scan multiple times
        result1 = filtered.to_table()
        result2 = filtered.to_table()
        
        assert result1.equals(result2)

    def test_scanner_preserves_column_projection(self, sample_dataset):
        """Test that using scanner() directly respects column projection."""
        filtered = filter_dataset(
            sample_dataset,
            {"category": "A"},
            ["id", "value"]
        )
        
        # Use scanner directly
        scanner = filtered.scanner()
        result = scanner.to_table()
        
        assert result.column_names == ["id", "value"]
        assert result.num_rows == 2

    def test_all_columns_retained(self, sample_dataset):
        """Test filtering with all columns retained."""
        filtered = filter_dataset(
            sample_dataset,
            {"value": (">", 30)},
            ["id", "category", "value", "extra"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 2
        assert len(result.column_names) == 4

    def test_single_column_output(self, sample_dataset):
        """Test filtering with only one output column."""
        filtered = filter_dataset(
            sample_dataset,
            {"category": "B"},
            ["id"]
        )
        
        result = filtered.to_table()
        assert result.column_names == ["id"]
        assert result["id"].to_pylist() == [2, 5]


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_empty_dataset(self):
        """Test filtering an empty dataset."""
        empty_table = pa.table({
            "id": pa.array([], type=pa.int64()),
            "value": pa.array([], type=pa.int64())
        })
        empty_ds = ds.dataset(empty_table)
        
        filtered = filter_dataset(
            empty_ds,
            {"value": (">", 0)},
            ["id"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 0

    def test_single_row_dataset(self):
        """Test filtering a single-row dataset."""
        single_table = pa.table({
            "id": [1],
            "value": [42]
        })
        single_ds = ds.dataset(single_table)
        
        filtered = filter_dataset(
            single_ds,
            {"value": 42},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 1
        assert result["id"].to_pylist() == [1]

    def test_multiple_data_types(self):
        """Test filtering with various data types."""
        table = pa.table({
            "int_col": [1, 2, 3],
            "float_col": [1.5, 2.5, 3.5],
            "string_col": ["a", "b", "c"],
            "bool_col": [True, False, True]
        })
        dataset = ds.dataset(table)
        
        # Test with different types
        filtered = filter_dataset(
            dataset,
            {"float_col": (">", 2.0), "bool_col": True},
            ["int_col", "string_col"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 1
        assert result["int_col"].to_pylist() == [3]

    def test_boundary_values(self):
        """Test filtering with boundary values for BETWEEN."""
        table = pa.table({
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50]
        })
        dataset = ds.dataset(table)
        
        # Test inclusive boundaries
        filtered = filter_dataset(
            dataset,
            {"value": ("between", 20, 40)},
            ["id", "value"]
        )
        
        result = filtered.to_table()
        assert result.num_rows == 3
        assert result["value"].to_pylist() == [20, 30, 40]


class TestAddColumnPrefix:
    """Tests for the _add_column_prefix helper function."""

    def test_simple_prefix(self):
        """Test adding prefix to columns."""
        table = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        result = _add_column_prefix(table, 'users', 'id')
        
        assert result.column_names == ['id', 'users_name', 'users_age']
        assert result['id'].to_pylist() == [1, 2, 3]
        assert result['users_name'].to_pylist() == ['Alice', 'Bob', 'Charlie']

    def test_exclude_column_not_prefixed(self):
        """Test that excluded column is not prefixed."""
        table = pa.table({
            'key': [1, 2],
            'value': [10, 20]
        })
        
        result = _add_column_prefix(table, 'data', 'key')
        
        assert 'key' in result.column_names
        assert 'data_key' not in result.column_names
        assert 'data_value' in result.column_names

    def test_all_columns_except_one(self):
        """Test prefixing with multiple columns."""
        table = pa.table({
            'id': [1],
            'col1': [10],
            'col2': [20],
            'col3': [30]
        })
        
        result = _add_column_prefix(table, 'test', 'id')
        
        assert result.column_names == ['id', 'test_col1', 'test_col2', 'test_col3']

    def test_single_column_table(self):
        """Test table with only the excluded column."""
        table = pa.table({'id': [1, 2, 3]})
        
        result = _add_column_prefix(table, 'prefix', 'id')
        
        assert result.column_names == ['id']

    def test_empty_table(self):
        """Test prefixing columns in empty table."""
        table = pa.table({
            'id': pa.array([], type=pa.int64()),
            'value': pa.array([], type=pa.int64())
        })
        
        result = _add_column_prefix(table, 'empty', 'id')
        
        assert result.column_names == ['id', 'empty_value']
        assert result.num_rows == 0


class TestInnerJoinTwoTables:
    """Tests for the _inner_join_two_tables helper function."""

    def test_simple_join(self):
        """Test basic inner join of two tables."""
        left = pa.table({
            'id': [1, 2, 3],
            'left_value': [10, 20, 30]
        })
        right = pa.table({
            'id': [2, 3, 4],
            'right_value': [200, 300, 400]
        })
        
        result = _inner_join_two_tables(left, right, 'id')
        
        assert result.num_rows == 2
        assert set(result.column_names) == {'id', 'left_value', 'right_value'}
        assert sorted(result['id'].to_pylist()) == [2, 3]

    def test_no_matching_rows(self):
        """Test join with no overlapping keys."""
        left = pa.table({
            'id': [1, 2],
            'value': [10, 20]
        })
        right = pa.table({
            'id': [3, 4],
            'value': [30, 40]
        })
        
        result = _inner_join_two_tables(left, right, 'id')
        
        assert result.num_rows == 0

    def test_all_rows_match(self):
        """Test join where all keys match."""
        left = pa.table({
            'id': [1, 2, 3],
            'left_col': ['a', 'b', 'c']
        })
        right = pa.table({
            'id': [1, 2, 3],
            'right_col': ['x', 'y', 'z']
        })
        
        result = _inner_join_two_tables(left, right, 'id')
        
        assert result.num_rows == 3
        assert sorted(result['id'].to_pylist()) == [1, 2, 3]

    def test_duplicate_keys(self):
        """Test join with duplicate keys (cartesian product expected)."""
        left = pa.table({
            'id': [1, 1, 2],
            'left_val': [10, 11, 20]
        })
        right = pa.table({
            'id': [1, 1, 2],
            'right_val': [100, 101, 200]
        })
        
        result = _inner_join_two_tables(left, right, 'id')
        
        # Each duplicate on left matches each duplicate on right
        # id=1: 2 left × 2 right = 4 rows
        # id=2: 1 left × 1 right = 1 row
        assert result.num_rows == 5


class TestInnerJoinDatasets:
    """Tests for the main inner_join_datasets function."""

    @pytest.fixture
    def users_dataset(self):
        """Create a users dataset."""
        table = pa.table({
            'user_id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Charlie', 'David'],
            'age': [25, 30, 35, 40]
        })
        return ds.dataset(table)

    @pytest.fixture
    def orders_dataset(self):
        """Create an orders dataset."""
        table = pa.table({
            'user_id': [2, 3, 4, 5],
            'order_total': [100.0, 200.0, 150.0, 300.0],
            'order_count': [1, 2, 1, 3]
        })
        return ds.dataset(table)

    @pytest.fixture
    def products_dataset(self):
        """Create a products dataset."""
        table = pa.table({
            'user_id': [1, 2, 3],
            'product_name': ['Widget', 'Gadget', 'Doohickey'],
            'price': [9.99, 19.99, 14.99]
        })
        return ds.dataset(table)

    def test_two_datasets_simple_join(self, users_dataset, orders_dataset):
        """Test simple join of two datasets."""
        result = inner_join_datasets(
            {'users': users_dataset, 'orders': orders_dataset},
            'user_id'
        )
        
        assert result.num_rows == 3  # user_id 2, 3, 4
        assert 'user_id' in result.column_names
        assert 'users_name' in result.column_names
        assert 'users_age' in result.column_names
        assert 'orders_order_total' in result.column_names
        assert 'orders_order_count' in result.column_names
        
        # Verify join key appears only once
        assert result.column_names.count('user_id') == 1

    def test_column_prefixes_correct(self, users_dataset, orders_dataset):
        """Test that column names are correctly prefixed."""
        result = inner_join_datasets(
            {'users': users_dataset, 'orders': orders_dataset},
            'user_id'
        )
        
        # Check all non-join columns are prefixed
        expected_columns = {
            'user_id',
            'users_name',
            'users_age',
            'orders_order_total',
            'orders_order_count'
        }
        assert set(result.column_names) == expected_columns

    def test_three_datasets_join(
        self, users_dataset, orders_dataset, products_dataset
    ):
        """Test joining three datasets."""
        result = inner_join_datasets(
            {
                'users': users_dataset,
                'orders': orders_dataset,
                'products': products_dataset
            },
            'user_id'
        )
        
        # Only user_id 2 and 3 appear in all three datasets
        assert result.num_rows == 2
        assert 'user_id' in result.column_names
        assert 'users_name' in result.column_names
        assert 'orders_order_total' in result.column_names
        assert 'products_product_name' in result.column_names

    def test_single_dataset(self, users_dataset):
        """Test behavior with a single dataset."""
        result = inner_join_datasets(
            {'users': users_dataset},
            'user_id'
        )
        
        assert result.num_rows == 4
        assert 'user_id' in result.column_names
        assert 'users_name' in result.column_names
        assert 'users_age' in result.column_names

    def test_empty_datasets_dict(self):
        """Test that empty datasets dict raises ValueError."""
        with pytest.raises(ValueError, match="At least one dataset must be provided"):
            inner_join_datasets({}, 'id')

    def test_missing_join_key_in_dataset(self, users_dataset, orders_dataset):
        """Test error when join key missing from a dataset."""
        with pytest.raises(
            ValueError,
            match="Join key 'nonexistent' not found in dataset 'users' schema"
        ):
            inner_join_datasets(
                {'users': users_dataset, 'orders': orders_dataset},
                'nonexistent'
            )

    def test_dataset_name_with_underscore(self, users_dataset):
        """Test that dataset names with underscores are rejected."""
        with pytest.raises(
            ValueError,
            match="Dataset names cannot contain underscores"
        ):
            inner_join_datasets(
                {'user_data': users_dataset},
                'user_id'
            )

    def test_multiple_dataset_names_with_underscores(
        self, users_dataset, orders_dataset
    ):
        """Test that multiple invalid dataset names are all reported."""
        with pytest.raises(
            ValueError,
            match="Dataset names cannot contain underscores.*user_data.*order_data"
        ):
            inner_join_datasets(
                {'user_data': users_dataset, 'order_data': orders_dataset},
                'user_id'
            )

    def test_no_matching_rows(self):
        """Test join with no matching keys across datasets."""
        ds1 = ds.dataset(pa.table({
            'id': [1, 2],
            'value': [10, 20]
        }))
        ds2 = ds.dataset(pa.table({
            'id': [3, 4],
            'value': [30, 40]
        }))
        
        result = inner_join_datasets(
            {'first': ds1, 'second': ds2},
            'id'
        )
        
        assert result.num_rows == 0
        # Columns should still be present
        assert 'id' in result.column_names
        assert 'first_value' in result.column_names
        assert 'second_value' in result.column_names

    def test_duplicate_join_keys(self):
        """Test join with duplicate keys in datasets."""
        ds1 = ds.dataset(pa.table({
            'id': [1, 1, 2],
            'value': [10, 11, 20]
        }))
        ds2 = ds.dataset(pa.table({
            'id': [1, 2, 2],
            'value': [100, 200, 201]
        }))
        
        result = inner_join_datasets(
            {'left': ds1, 'right': ds2},
            'id'
        )
        
        # id=1: 2 × 1 = 2 rows
        # id=2: 1 × 2 = 2 rows
        assert result.num_rows == 4

    def test_join_preserves_data_types(self, users_dataset, orders_dataset):
        """Test that data types are preserved after join."""
        result = inner_join_datasets(
            {'users': users_dataset, 'orders': orders_dataset},
            'user_id'
        )
        
        assert result['user_id'].type == pa.int64()
        assert result['users_name'].type == pa.string()
        assert result['users_age'].type == pa.int64()
        assert result['orders_order_total'].type == pa.float64()

    def test_join_order_deterministic(
        self, users_dataset, orders_dataset, products_dataset
    ):
        """Test that join results are deterministic."""
        result1 = inner_join_datasets(
            {
                'users': users_dataset,
                'orders': orders_dataset,
                'products': products_dataset
            },
            'user_id'
        )
        
        result2 = inner_join_datasets(
            {
                'users': users_dataset,
                'orders': orders_dataset,
                'products': products_dataset
            },
            'user_id'
        )
        
        assert result1.equals(result2)

    def test_different_column_names_same_join_key(self):
        """Test datasets with different columns but same join key."""
        ds1 = ds.dataset(pa.table({
            'id': [1, 2, 3],
            'alpha': ['a', 'b', 'c']
        }))
        ds2 = ds.dataset(pa.table({
            'id': [2, 3, 4],
            'beta': ['x', 'y', 'z']
        }))
        ds3 = ds.dataset(pa.table({
            'id': [1, 2, 3],
            'gamma': [10, 20, 30]
        }))
        
        result = inner_join_datasets(
            {'first': ds1, 'second': ds2, 'third': ds3},
            'id'
        )
        
        assert result.num_rows == 2  # id 2 and 3 in all three
        assert set(result.column_names) == {
            'id',
            'first_alpha',
            'second_beta',
            'third_gamma'
        }

    def test_single_row_datasets(self):
        """Test join with single-row datasets."""
        ds1 = ds.dataset(pa.table({
            'key': [1],
            'value1': [100]
        }))
        ds2 = ds.dataset(pa.table({
            'key': [1],
            'value2': [200]
        }))
        
        result = inner_join_datasets(
            {'a': ds1, 'b': ds2},
            'key'
        )
        
        assert result.num_rows == 1
        assert result['key'].to_pylist() == [1]
        assert result['a_value1'].to_pylist() == [100]
        assert result['b_value2'].to_pylist() == [200]

    def test_empty_dataset_join(self):
        """Test joining when one dataset is empty."""
        ds1 = ds.dataset(pa.table({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        }))
        ds2 = ds.dataset(pa.table({
            'id': pa.array([], type=pa.int64()),
            'value': pa.array([], type=pa.int64())
        }))
        
        result = inner_join_datasets(
            {'first': ds1, 'second': ds2},
            'id'
        )
        
        # Inner join with empty dataset should return empty result
        assert result.num_rows == 0
        assert set(result.column_names) == {'id', 'first_value', 'second_value'}

    def test_column_name_conflicts_resolved(self):
        """Test that column name conflicts are resolved by prefixing."""
        # Both datasets have a 'value' column
        ds1 = ds.dataset(pa.table({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        }))
        ds2 = ds.dataset(pa.table({
            'id': [2, 3, 4],
            'value': [200, 300, 400]
        }))
        
        result = inner_join_datasets(
            {'first': ds1, 'second': ds2},
            'id'
        )
        
        # Both value columns should be present with prefixes
        assert 'first_value' in result.column_names
        assert 'second_value' in result.column_names
        assert 'value' not in result.column_names
        
        # Verify data is correct
        assert result.num_rows == 2
        result_dict = result.to_pydict()
        id_2_idx = result_dict['id'].index(2)
        assert result_dict['first_value'][id_2_idx] == 20
        assert result_dict['second_value'][id_2_idx] == 200


class TestIntegrationWithProjectedDataset:
    """Integration tests with ProjectedDataset from filter_dataset."""

    @pytest.fixture
    def mock_projected_dataset(self):
        """Create a mock ProjectedDataset-like object."""
        class MockProjectedDataset:
            def __init__(self, table, columns):
                self._table = table
                self._columns = columns
            
            @property
            def schema(self):
                return self._table.schema
            
            def to_table(self):
                return self._table.select(self._columns)
        
        return MockProjectedDataset

    def test_join_with_projected_datasets(self, mock_projected_dataset):
        """Test joining ProjectedDataset objects."""
        table1 = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'extra': ['x', 'y', 'z']
        })
        table2 = pa.table({
            'id': [2, 3, 4],
            'salary': [70000, 60000, 55000],
            'dept': ['Eng', 'Sales', 'HR'],
            'extra': ['a', 'b', 'c']
        })
        
        # Create projected datasets (simulating filter_dataset output)
        proj1 = mock_projected_dataset(table1, ['id', 'name', 'age'])
        proj2 = mock_projected_dataset(table2, ['id', 'salary', 'dept'])
        
        result = inner_join_datasets(
            {'employees': proj1, 'payroll': proj2},
            'id'
        )
        
        assert result.num_rows == 2
        assert set(result.column_names) == {
            'id',
            'employees_name',
            'employees_age',
            'payroll_salary',
            'payroll_dept'
        }
        # 'extra' columns should not be present
        assert 'employees_extra' not in result.column_names
        assert 'payroll_extra' not in result.column_names

    def test_join_validates_schema_lazily(self, mock_projected_dataset):
        """Test that schema validation doesn't materialize the dataset."""
        table = pa.table({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        })
        
        # Track if to_table was called
        to_table_called = []
        
        class TrackedProjectedDataset:
            @property
            def schema(self):
                return table.schema
            
            def to_table(self):
                to_table_called.append(True)
                return table
        
        ds1 = TrackedProjectedDataset()
        ds2 = TrackedProjectedDataset()
        
        # This should validate schemas without calling to_table
        # (though it will call to_table for the actual join)
        result = inner_join_datasets(
            {'first': ds1, 'second': ds2},
            'id'
        )
        
        # to_table should be called for joining, but not for validation
        assert len(to_table_called) == 2  # Once per dataset for joining


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_very_large_column_count(self):
        """Test join with many columns."""
        # Create datasets with many columns
        num_cols = 50
        data1 = {'id': [1, 2, 3]}
        data2 = {'id': [2, 3, 4]}
        
        for i in range(num_cols):
            data1[f'col{i}'] = [i, i+1, i+2]
            data2[f'col{i}'] = [i*10, i*10+1, i*10+2]
        
        ds1 = ds.dataset(pa.table(data1))
        ds2 = ds.dataset(pa.table(data2))
        
        result = inner_join_datasets(
            {'left': ds1, 'right': ds2},
            'id'
        )
        
        assert result.num_rows == 2
        # 1 join key + 50*2 prefixed columns
        assert len(result.column_names) == 1 + num_cols * 2

    def test_join_key_with_nulls(self):
        """Test join behavior with NULL values in join key."""
        ds1 = ds.dataset(pa.table({
            'id': [1, 2, None],
            'value': [10, 20, 30]
        }))
        ds2 = ds.dataset(pa.table({
            'id': [2, None, 4],
            'value': [200, 300, 400]
        }))
        
        result = inner_join_datasets(
            {'left': ds1, 'right': ds2},
            'id'
        )
        
        # PyArrow join behavior: NULLs don't match
        # Only id=2 should match
        assert result.num_rows == 1
        assert result['id'].to_pylist() == [2]

    def test_numeric_dataset_names(self):
        """Test that numeric-only dataset names work."""
        ds1 = ds.dataset(pa.table({
            'id': [1, 2],
            'value': [10, 20]
        }))
        ds2 = ds.dataset(pa.table({
            'id': [2, 3],
            'value': [200, 300]
        }))
        
        # Numeric names should work (they're strings)
        result = inner_join_datasets(
            {'123': ds1, '456': ds2},
            'id'
        )
        
        assert result.num_rows == 1
        assert '123_value' in result.column_names
        assert '456_value' in result.column_names

    def test_special_characters_in_dataset_names(self):
        """Test dataset names with special characters (except underscore)."""
        ds1 = ds.dataset(pa.table({
            'id': [1, 2],
            'value': [10, 20]
        }))
        ds2 = ds.dataset(pa.table({
            'id': [2, 3],
            'value': [200, 300]
        }))
        
        # Special characters should work
        result = inner_join_datasets(
            {'data-source': ds1, 'data.backup': ds2},
            'id'
        )
        
        assert result.num_rows == 1
        assert 'data-source_value' in result.column_names
        assert 'data.backup_value' in result.column_names

    def test_join_on_string_key(self):
        """Test join using string column as key."""
        ds1 = ds.dataset(pa.table({
            'username': ['alice', 'bob', 'charlie'],
            'score': [100, 200, 300]
        }))
        ds2 = ds.dataset(pa.table({
            'username': ['bob', 'charlie', 'david'],
            'level': [5, 10, 15]
        }))
        
        result = inner_join_datasets(
            {'scores': ds1, 'levels': ds2},
            'username'
        )
        
        assert result.num_rows == 2
        assert set(result['username'].to_pylist()) == {'bob', 'charlie'}

    def test_join_preserves_row_order_deterministically(self):
        """Test that join produces consistent row ordering."""
        ds1 = ds.dataset(pa.table({
            'id': [3, 1, 2],
            'value': [30, 10, 20]
        }))
        ds2 = ds.dataset(pa.table({
            'id': [2, 3, 1],
            'value': [200, 300, 100]
        }))
        
        result1 = inner_join_datasets({'a': ds1, 'b': ds2}, 'id')
        result2 = inner_join_datasets({'a': ds1, 'b': ds2}, 'id')
        
        assert result1.equals(result2)

    def test_four_dataset_join(self):
        """Test joining four datasets."""
        datasets = {}
        for i in range(4):
            datasets[f'ds{i}'] = ds.dataset(pa.table({
                'id': [1, 2, 3],
                f'value{i}': [i*10, i*20, i*30]
            }))
        
        result = inner_join_datasets(datasets, 'id')
        
        assert result.num_rows == 3
        assert 'id' in result.column_names
        for i in range(4):
            assert f'ds{i}_value{i}' in result.column_names

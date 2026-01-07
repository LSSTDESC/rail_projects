from typing import Any
import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from rail.projects.arrow_utils import (
    LEGAL_OPERATIONS,
    check_list_as_tuple,
    parse_item,
    _ProjectedDataset,
    filter_dataset,
    inner_join_datasets,
)


class TestListAsTuple:
    """Tests for the check_list_as_tuple function."""

    def test_valid_three_element_list_equality(self) -> None:
        """Test valid 3-element list with equality operator."""
        assert check_list_as_tuple(["age", "==", 25]) is True

    def test_valid_three_element_list_greater_than(self) -> None:
        """Test valid 3-element list with greater than operator."""
        assert check_list_as_tuple(["value", ">", 100]) is True

    def test_valid_three_element_list_in_operator(self) -> None:
        """Test valid 3-element list with 'in' operator."""
        assert check_list_as_tuple(["category", "in", ["A", "B", "C"]]) is True

    def test_valid_three_element_list_not_in_operator(self) -> None:
        """Test valid 3-element list with 'not in' operator."""
        assert (
            check_list_as_tuple(["status", "not in", ["inactive", "deleted"]]) is True
        )

    def test_all_legal_operators(self) -> None:
        """Test that all legal operators are accepted."""
        for op in LEGAL_OPERATIONS:
            assert check_list_as_tuple(["field", op, "value"]) is True

    def test_two_element_list_returns_false(self) -> None:
        """Test that a 2-element list returns False."""
        assert check_list_as_tuple(["age", ">"]) is False

    def test_four_element_list_returns_false(self) -> None:
        """Test that a 4-element list returns False."""
        assert check_list_as_tuple(["age", ">", 25, "extra"]) is False

    def test_empty_list_returns_false(self) -> None:
        """Test that an empty list returns False."""
        assert check_list_as_tuple([]) is False

    def test_first_element_not_string_returns_false(self) -> None:
        """Test that non-string first element returns False."""
        assert check_list_as_tuple([123, "==", 25]) is False

    def test_invalid_operator_raises_value_error(self) -> None:
        """Test that invalid operator raises ValueError."""
        with pytest.raises(ValueError, match="is not in"):
            check_list_as_tuple(["age", "invalid_op", 25])

    def test_invalid_operator_error_message(self) -> None:
        """Test the error message for invalid operator."""
        with pytest.raises(ValueError) as exc_info:
            check_list_as_tuple(["age", "~~~", 25])

        error_msg = str(exc_info.value)
        assert "Expected three item list" in error_msg
        assert "~~~" in error_msg
        assert "is not in" in error_msg

    def test_operator_case_sensitive(self) -> None:
        """Test that operator matching is case-sensitive."""
        with pytest.raises(ValueError):
            check_list_as_tuple(["age", "IN", ["A", "B"]])

    def test_with_none_value(self) -> None:
        """Test that None as value is valid."""
        assert check_list_as_tuple(["field", "==", None]) is True

    def test_with_boolean_value(self) -> None:
        """Test that boolean value is valid."""
        assert check_list_as_tuple(["active", "==", True]) is True


class TestParseItem:
    """Tests for the parse_item function."""

    def test_simple_filter_top_level(self) -> None:
        """Test parsing a simple filter at top level."""
        result = parse_item(["age", ">", 25])
        assert result == [("age", ">", 25)]

    def test_simple_filter_nested(self) -> None:
        """Test parsing a simple filter as nested item."""
        result = parse_item(["age", ">", 25], top=False)
        assert result == ("age", ">", 25)

    def test_and_logic_two_filters(self) -> None:
        """Test AND logic with two filters."""
        result = parse_item([["age", ">", 25], ["category", "==", "A"]])
        assert result == [("age", ">", 25), ("category", "==", "A")]

    def test_and_logic_three_filters(self) -> None:
        """Test AND logic with three filters."""
        result = parse_item(
            [["age", ">", 25], ["category", "==", "A"], ["status", "!=", "inactive"]]
        )
        assert result == [
            ("age", ">", 25),
            ("category", "==", "A"),
            ("status", "!=", "inactive"),
        ]

    def test_or_group_simple(self) -> None:
        """Test OR_GROUP with simple filters."""
        result = parse_item({"OR_GROUP": [["age", ">", 50], ["status", "==", "VIP"]]})
        assert result == [("age", ">", 50), ("status", "==", "VIP")]

    def test_or_group_nested_and_conditions(self) -> None:
        """Test OR_GROUP with nested AND conditions."""
        result = parse_item(
            {
                "OR_GROUP": [
                    [["age", ">", 25], ["category", "==", "A"]],
                    [["age", ">", 50], ["category", "==", "B"]],
                ]
            }
        )
        assert result == [
            [("age", ">", 25), ("category", "==", "A")],
            [("age", ">", 50), ("category", "==", "B")],
        ]

    def test_nested_or_groups(self) -> None:
        """Test nested OR groups without OR_GROUP dict."""
        result = parse_item([[["age", ">", 25]], [["category", "==", "A"]]])
        assert result == [[("age", ">", 25)], [("category", "==", "A")]]

    def test_complex_nested_structure(self) -> None:
        """Test complex nested structure with multiple levels."""
        result = parse_item(
            [
                [["age", ">", 25], ["status", "==", "active"]],
                [["category", "in", ["VIP", "Premium"]]],
                [["country", "==", "US"], ["value", ">=", 1000]],
            ]
        )
        assert result == [
            [("age", ">", 25), ("status", "==", "active")],
            [("category", "in", ["VIP", "Premium"])],
            [("country", "==", "US"), ("value", ">=", 1000)],
        ]

    def test_single_filter_in_list(self) -> None:
        """Test single filter wrapped in a list."""
        result = parse_item([["age", ">", 25]])
        assert result == [("age", ">", 25)]

    def test_deeply_nested_lists(self) -> None:
        """Test deeply nested list structures."""
        result = parse_item([[[["age", ">", 25]]]])
        assert result == [[[("age", ">", 25)]]]

    def test_or_group_single_condition(self) -> None:
        """Test OR_GROUP with single condition."""
        result = parse_item({"OR_GROUP": [["age", ">", 25]]})
        assert result == [("age", ">", 25)]

    def test_various_operators(self) -> None:
        """Test parsing with various operators."""
        filters = [
            ["age", "==", 25],
            ["value", "!=", 100],
            ["score", "<", 50],
            ["rank", "<=", 10],
            ["count", ">", 5],
            ["total", ">=", 1000],
            ["category", "in", ["A", "B"]],
            ["status", "not in", ["inactive"]],
        ]
        result = parse_item(filters)
        assert len(result) == 8
        assert all(isinstance(item, tuple) for item in result)

    def test_various_value_types(self) -> None:
        """Test parsing with various value types."""
        result = parse_item(
            [
                ["int_field", "==", 42],
                ["float_field", ">", 3.14],
                ["str_field", "==", "value"],
                ["bool_field", "==", True],
                ["null_field", "==", None],
                ["list_field", "in", [1, 2, 3]],
            ]
        )
        assert result == [
            ("int_field", "==", 42),
            ("float_field", ">", 3.14),
            ("str_field", "==", "value"),
            ("bool_field", "==", True),
            ("null_field", "==", None),
            ("list_field", "in", [1, 2, 3]),
        ]

    def test_invalid_operator_raises_value_error(self) -> None:
        """Test that invalid operator raises ValueError."""
        with pytest.raises(ValueError, match="is not in"):
            parse_item(["age", "bad_op", 25])

    def test_dict_without_or_group_raises_assertion(self) -> None:
        """Test that dict without OR_GROUP raises AssertionError."""
        with pytest.raises(AssertionError, match="must contain 'OR_GROUP' key"):
            parse_item({"WRONG_KEY": [["age", ">", 25]]})

    def test_empty_dict_raises_assertion(self) -> None:
        """Test that empty dict raises AssertionError."""
        with pytest.raises(AssertionError, match="must contain 'OR_GROUP' key"):
            parse_item({})

    def test_dict_with_multiple_keys_but_has_or_group(self) -> None:
        """Test dict with OR_GROUP plus extra keys works."""
        result = parse_item({"OR_GROUP": [["age", ">", 25]], "extra_key": "ignored"})
        assert result == [("age", ">", 25)]

    def test_non_list_non_dict_raises_assertion(self) -> None:
        """Test that non-list, non-dict input raises AssertionError."""
        with pytest.raises(AssertionError, match="neither a list nor a dict"):
            parse_item("invalid_string")

    def test_integer_input_raises_assertion(self) -> None:
        """Test that integer input raises AssertionError."""
        with pytest.raises(AssertionError, match="neither a list nor a dict"):
            parse_item(42)

    def test_none_input_raises_assertion(self) -> None:
        """Test that None input raises AssertionError."""
        with pytest.raises(AssertionError, match="neither a list nor a dict"):
            parse_item(None)

    def test_empty_list_returns_empty_list(self) -> None:
        """Test that empty list returns empty list."""
        result = parse_item([])
        assert result == []

    def test_top_false_with_nested_structure(self) -> None:
        """Test top=False with nested structure."""
        result = parse_item([["age", ">", 25], ["category", "==", "A"]], top=False)
        assert result == [("age", ">", 25), ("category", "==", "A")]

    def test_mixed_nesting_depths(self) -> None:
        """Test structure with mixed nesting depths."""
        result = parse_item(
            [["simple", "==", 1], [["nested", ">", 2]], [[["deeply_nested", "<", 3]]]]
        )
        assert result == [
            ("simple", "==", 1),
            [("nested", ">", 2)],
            [[("deeply_nested", "<", 3)]],
        ]


class TestParseItemEdgeCases:
    """Edge case tests for parse_item function."""

    def test_unicode_column_names(self) -> None:
        """Test parsing with Unicode column names."""
        result = parse_item([["名前", "==", "value"]])
        assert result == [("名前", "==", "value")]

    def test_column_names_with_spaces(self) -> None:
        """Test column names with spaces."""
        result = parse_item([["column name", ">", 25]])
        assert result == [("column name", ">", 25)]

    def test_very_long_list_values(self) -> None:
        """Test 'in' operator with very long list."""
        long_list = list(range(1000))
        result = parse_item([["id", "in", long_list]])
        assert result == [("id", "in", long_list)]

    def test_nested_empty_lists(self) -> None:
        """Test nested empty lists."""
        result = parse_item([[]])
        assert result == [[]]

    def test_or_group_with_empty_list(self) -> None:
        """Test OR_GROUP with empty list."""
        result = parse_item({"OR_GROUP": []})
        assert result == []

    def test_special_characters_in_values(self) -> None:
        """Test values with special characters."""
        result = parse_item([["path", "==", "/usr/bin/python"]])
        assert result == [("path", "==", "/usr/bin/python")]

    def test_tuple_as_value(self) -> None:
        """Test that tuple can be used as value."""
        result = parse_item([["coords", "==", (1, 2, 3)]])
        assert result == [("coords", "==", (1, 2, 3))]


class TestParseItemIntegration:
    """Integration tests showing realistic usage patterns."""

    def test_simple_user_query(self) -> None:
        """Test a simple user filtering query."""
        # Find users older than 25 in category A
        result = parse_item([["age", ">", 25], ["category", "==", "A"]])
        assert result == [("age", ">", 25), ("category", "==", "A")]

    def test_complex_e_commerce_query(self) -> None:
        """Test complex e-commerce filtering."""
        # Find: (VIP customers) OR (high spenders in US/UK)
        result = parse_item(
            [
                [["tier", "==", "VIP"]],
                [["total_spent", ">=", 10000], ["country", "in", ["US", "UK"]]],
            ]
        )
        assert result == [
            [("tier", "==", "VIP")],
            [("total_spent", ">=", 10000), ("country", "in", ["US", "UK"])],
        ]

    def test_multi_tier_or_conditions(self) -> None:
        """Test multiple OR conditions with OR_GROUP."""
        result = parse_item(
            {
                "OR_GROUP": [
                    [["status", "==", "active"], ["verified", "==", True]],
                    [["status", "==", "pending"], ["priority", "==", "high"]],
                    [["status", "==", "trial"], ["days_left", ">", 10]],
                ]
            }
        )
        assert result == [
            [("status", "==", "active"), ("verified", "==", True)],
            [("status", "==", "pending"), ("priority", "==", "high")],
            [("status", "==", "trial"), ("days_left", ">", 10)],
        ]

    def test_inventory_filtering(self) -> None:
        """Test inventory filtering with multiple conditions."""
        # Find: low stock items OR discontinued items in specific categories
        result = parse_item(
            [
                [["stock", "<=", 10], ["category", "in", ["electronics", "furniture"]]],
                [["discontinued", "==", True]],
            ]
        )
        assert result == [
            [("stock", "<=", 10), ("category", "in", ["electronics", "furniture"])],
            [("discontinued", "==", True)],
        ]

    def test_date_range_query(self) -> None:
        """Test date range filtering pattern."""
        # Find records in date range and specific status
        result = parse_item(
            [
                ["date", ">=", "2024-01-01"],
                ["date", "<=", "2024-12-31"],
                ["status", "!=", "cancelled"],
            ]
        )
        assert result == [
            ("date", ">=", "2024-01-01"),
            ("date", "<=", "2024-12-31"),
            ("status", "!=", "cancelled"),
        ]

    def test_exclusion_pattern(self) -> None:
        """Test pattern excluding multiple values."""
        result = parse_item(
            [
                ["category", "not in", ["archived", "deleted", "spam"]],
                ["active", "==", True],
            ]
        )
        assert result == [
            ("category", "not in", ["archived", "deleted", "spam"]),
            ("active", "==", True),
        ]


class TestParseItemRecursion:
    """Tests for recursive behavior of parse_item."""

    def test_single_level_recursion(self) -> None:
        """Test single level of recursion."""
        result = parse_item([["field", "==", "value"]])
        assert result == [("field", "==", "value")]

    def test_double_level_recursion(self) -> None:
        """Test two levels of recursion."""
        result = parse_item([[["field", "==", "value"]]])
        assert result == [[("field", "==", "value")]]

    def test_triple_level_recursion(self) -> None:
        """Test three levels of recursion."""
        result = parse_item([[[["field", "==", "value"]]]])
        assert result == [[[("field", "==", "value")]]]

    def test_mixed_recursion_depths(self) -> None:
        """Test mixed recursion depths in same structure."""
        result = parse_item(
            [["shallow", "==", 1], [["medium", "==", 2]], [[["deep", "==", 3]]]]
        )
        assert result == [
            ("shallow", "==", 1),
            [("medium", "==", 2)],
            [[("deep", "==", 3)]],
        ]

    def test_or_group_recursive_processing(self) -> None:
        """Test OR_GROUP triggers recursive processing."""
        result = parse_item({"OR_GROUP": [[["a", "==", 1]], [[["b", "==", 2]]]]})
        assert result == [[("a", "==", 1)], [[("b", "==", 2)]]]

    def test_nested_or_groups(self) -> None:
        """Test nested OR_GROUP dictionaries."""
        result = parse_item(
            {
                "OR_GROUP": [
                    {"OR_GROUP": [["a", "==", 1], ["b", "==", 2]]},
                    ["c", "==", 3],
                ]
            }
        )
        # The inner OR_GROUP gets processed recursively
        assert result == [[("a", "==", 1), ("b", "==", 2)], ("c", "==", 3)]


class TestParseItemTypeCoercion:
    """Tests for type handling in parse_item."""

    def test_numeric_values(self) -> None:
        """Test various numeric value types."""
        result = parse_item(
            [
                ["int_val", "==", 42],
                ["float_val", ">", 3.14],
                ["negative", "<", -10],
                ["zero", "==", 0],
            ]
        )
        assert result == [
            ("int_val", "==", 42),
            ("float_val", ">", 3.14),
            ("negative", "<", -10),
            ("zero", "==", 0),
        ]

    def test_string_values(self) -> None:
        """Test various string value types."""
        result = parse_item(
            [
                ["empty", "==", ""],
                ["normal", "==", "value"],
                ["quoted", "==", "value with 'quotes'"],
                ["multiline", "==", "line1\nline2"],
            ]
        )
        assert result == [
            ("empty", "==", ""),
            ("normal", "==", "value"),
            ("quoted", "==", "value with 'quotes'"),
            ("multiline", "==", "line1\nline2"),
        ]

    def test_boolean_values(self) -> None:
        """Test boolean values."""
        result = parse_item([["is_active", "==", True], ["is_deleted", "==", False]])
        assert result == [("is_active", "==", True), ("is_deleted", "==", False)]

    def test_none_values(self) -> None:
        """Test None/null values."""
        result = parse_item(
            [["nullable_field", "==", None], ["another_field", "!=", None]]
        )
        assert result == [("nullable_field", "==", None), ("another_field", "!=", None)]

    def test_list_values_for_in_operator(self) -> None:
        """Test list values with 'in' operator."""
        result = parse_item(
            [
                ["id", "in", [1, 2, 3, 4, 5]],
                ["category", "in", ["A", "B", "C"]],
                ["mixed", "in", [1, "two", 3.0, None]],
            ]
        )
        assert result == [
            ("id", "in", [1, 2, 3, 4, 5]),
            ("category", "in", ["A", "B", "C"]),
            ("mixed", "in", [1, "two", 3.0, None]),
        ]


class TestParseItemErrorMessages:
    """Tests for error message quality."""

    def test_invalid_operator_shows_available_options(self) -> None:
        """Test that error message shows available operators."""
        with pytest.raises(ValueError) as exc_info:
            parse_item(["field", "EQUALS", "value"])

        error_msg = str(exc_info.value)
        assert "EQUALS" in error_msg
        assert str(LEGAL_OPERATIONS) in error_msg

    def test_missing_or_group_shows_keys(self) -> None:
        """Test that error message shows actual keys in dict."""
        with pytest.raises(AssertionError) as exc_info:
            parse_item({"WRONG_KEY": [], "ANOTHER": []})

        error_msg = str(exc_info.value)
        assert "OR_GROUP" in error_msg
        assert "WRONG_KEY" in error_msg or "dict_keys" in error_msg

    def test_invalid_type_shows_actual_type(self) -> None:
        """Test that error message shows the actual invalid type."""
        with pytest.raises(AssertionError) as exc_info:
            parse_item(12345)

        error_msg = str(exc_info.value)
        assert "12345" in error_msg
        assert "neither a list nor a dict" in error_msg


class TestParseItemBoundaryConditions:
    """Tests for boundary conditions and limits."""

    def test_very_long_column_name(self) -> None:
        """Test parsing with very long column name."""
        long_name = "a" * 1000
        result = parse_item([[long_name, "==", "value"]])
        assert result == [(long_name, "==", "value")]

    def test_very_large_numeric_value(self) -> None:
        """Test parsing with very large numbers."""
        large_num = 10**100
        result = parse_item([["field", ">", large_num]])
        assert result == [("field", ">", large_num)]

    def test_very_small_numeric_value(self) -> None:
        """Test parsing with very small numbers."""
        small_num = 1e-100
        result = parse_item([["field", "<", small_num]])
        assert result == [("field", "<", small_num)]

    def test_many_filters_in_and_group(self) -> None:
        """Test many filters in single AND group."""
        filters = [[f"field{i}", "==", i] for i in range(100)]
        result = parse_item(filters)
        assert len(result) == 100
        assert all(isinstance(f, tuple) for f in result)

    def test_many_or_groups(self) -> None:
        """Test many OR groups."""
        or_groups = [[[f"field{i}", "==", i]] for i in range(50)]
        result = parse_item(or_groups)
        assert len(result) == 50
        assert all(isinstance(group, list) for group in result)

    def test_deeply_nested_structure(self) -> None:
        """Test very deep nesting."""
        # Create a deeply nested structure
        deep = ["field", "==", "value"]
        for _ in range(10):
            deep = [deep]  # type: ignore

        result = parse_item(deep)

        # Unwrap to check the innermost value
        current: Any = result
        for _ in range(10):
            assert isinstance(current, list)
            assert len(current) == 1
            current = current[0]
        assert current == ("field", "==", "value")


class TestParseItemDocstringExamples:
    """Tests based on examples from docstrings."""

    def test_docstring_example_simple_filter_top(self) -> None:
        """Test docstring example: simple filter at top level."""
        result = parse_item(["age", ">", 25])
        assert result == [("age", ">", 25)]

    def test_docstring_example_simple_filter_nested(self) -> None:
        """Test docstring example: simple filter nested."""
        result = parse_item(["age", ">", 25], top=False)
        assert result == ("age", ">", 25)

    def test_docstring_example_and_logic(self) -> None:
        """Test docstring example: AND logic."""
        result = parse_item([["age", ">", 25], ["category", "==", "A"]])
        assert result == [("age", ">", 25), ("category", "==", "A")]

    def test_docstring_example_or_group(self) -> None:
        """Test docstring example: OR_GROUP."""
        result = parse_item({"OR_GROUP": [["age", ">", 50], ["status", "==", "VIP"]]})
        assert result == [("age", ">", 50), ("status", "==", "VIP")]

    def test_docstring_example_nested_or(self) -> None:
        """Test docstring example: nested OR groups."""
        result = parse_item([[["age", ">", 25]], [["category", "==", "A"]]])
        assert result == [[("age", ">", 25)], [("category", "==", "A")]]


class TestFilterDataset:
    """Tests for the filter_dataset function."""

    @pytest.fixture
    def sample_dataset(self) -> ds.Dataset:
        """Create a sample PyArrow dataset for testing."""
        table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "category": ["A", "B", "A", "C", "B"],
                "value": [10, 20, 30, 40, 50],
                "extra": ["x", "y", "z", "w", "v"],
            }
        )
        return ds.dataset(table)

    def test_simple_equality_filter(self, sample_dataset: ds.Dataset) -> None:
        """Test simple equality filtering."""
        filtered = filter_dataset(
            sample_dataset, [("category", "==", "A")], ["id", "category", "value"]
        )

        result = filtered.to_table()
        assert result.num_rows == 2
        assert result["id"].to_pylist() == [1, 3]
        assert result["category"].to_pylist() == ["A", "A"]

    def test_column_projection_list(self, sample_dataset: ds.Dataset) -> None:
        """Test that only specified columns are retained with list."""
        filtered = filter_dataset(
            sample_dataset, [("category", "==", "A")], ["id", "value"]
        )

        result = filtered.to_table()
        assert result.column_names == ["id", "value"]
        assert "category" not in result.column_names
        assert "extra" not in result.column_names

    def test_column_projection_dict(self, sample_dataset: ds.Dataset) -> None:
        """Test column renaming with dict."""
        filtered = filter_dataset(
            sample_dataset,
            [("category", "==", "A")],
            {"identifier": "id", "amount": "value"},
        )

        result = filtered.to_table()
        assert set(result.column_names) == {"identifier", "amount"}
        assert result.num_rows == 2

    def test_and_logic_multiple_conditions(self, sample_dataset: ds.Dataset) -> None:
        """Test AND logic with multiple conditions."""
        filtered = filter_dataset(
            sample_dataset,
            [("category", "==", "A"), ("value", ">", 15)],
            ["id", "value"],
        )

        result = filtered.to_table()
        assert result.num_rows == 1
        assert result["id"].to_pylist() == [3]
        assert result["value"].to_pylist() == [30]

    def test_or_logic_condition_groups(self, sample_dataset: ds.Dataset) -> None:
        """Test OR logic with multiple condition groups."""
        filtered = filter_dataset(
            sample_dataset,
            [[("category", "==", "A")], [("value", ">=", 50)]],
            ["id", "category", "value"],
        )

        result = filtered.to_table()
        assert result.num_rows == 3
        assert set(result["id"].to_pylist()) == {1, 3, 5}

    def test_greater_than_filter(self, sample_dataset: ds.Dataset) -> None:
        """Test greater than comparison."""
        filtered = filter_dataset(sample_dataset, [("value", ">", 25)], ["id", "value"])

        result = filtered.to_table()
        assert result.num_rows == 3
        assert result["id"].to_pylist() == [3, 4, 5]

    def test_less_than_or_equal_filter(self, sample_dataset: ds.Dataset) -> None:
        """Test less than or equal comparison."""
        filtered = filter_dataset(
            sample_dataset, [("value", "<=", 20)], ["id", "value"]
        )

        result = filtered.to_table()
        assert result.num_rows == 2
        assert result["id"].to_pylist() == [1, 2]

    def test_not_equal_filter(self, sample_dataset: ds.Dataset) -> None:
        """Test not equal comparison."""
        filtered = filter_dataset(
            sample_dataset, [("category", "!=", "A")], ["id", "category"]
        )

        result = filtered.to_table()
        assert result.num_rows == 3
        assert "A" not in result["category"].to_pylist()

    def test_in_operator(self, sample_dataset: ds.Dataset) -> None:
        """Test 'in' operator for list membership."""
        filtered = filter_dataset(
            sample_dataset, [("category", "in", ["A", "C"])], ["id", "category"]
        )

        result = filtered.to_table()
        assert result.num_rows == 3
        assert set(result["category"].to_pylist()) == {"A", "C"}

    def test_not_in_operator(self, sample_dataset: ds.Dataset) -> None:
        """Test 'not in' operator."""
        filtered = filter_dataset(
            sample_dataset, [("category", "not in", ["A", "C"])], ["id", "category"]
        )

        result = filtered.to_table()
        assert result.num_rows == 2
        assert set(result["category"].to_pylist()) == {"B"}

    def test_empty_filter_conditions(self, sample_dataset: ds.Dataset) -> None:
        """Test that empty filter conditions return all rows."""
        filtered = filter_dataset(sample_dataset, [], ["id", "value"])

        result = filtered.to_table()
        assert result.num_rows == 5

    def test_no_matching_rows(self, sample_dataset: ds.Dataset) -> None:
        """Test filter that matches no rows returns empty table."""
        filtered = filter_dataset(
            sample_dataset, [("value", ">", 100)], ["id", "value"]
        )

        result = filtered.to_table()
        assert result.num_rows == 0
        assert result.column_names == ["id", "value"]

    def test_missing_output_column_list(self, sample_dataset: ds.Dataset) -> None:
        """Test that requesting non-existent column raises ValueError."""
        with pytest.raises(ValueError, match="Columns not found in dataset schema"):
            filter_dataset(
                sample_dataset, [("category", "==", "A")], ["id", "nonexistent"]
            )

    def test_missing_output_column_dict(self, sample_dataset: ds.Dataset) -> None:
        """Test that renaming non-existent column raises ValueError."""
        with pytest.raises(ValueError, match="Columns not found in dataset schema"):
            filter_dataset(
                sample_dataset, [("category", "==", "A")], {"new_name": "nonexistent"}
            )

    def test_complex_or_and_combination(self, sample_dataset: ds.Dataset) -> None:
        """Test complex combination of OR and AND logic."""
        filtered = filter_dataset(
            sample_dataset,
            [
                [("category", "==", "A"), ("value", "<", 20)],
                [("category", "==", "B"), ("value", ">", 40)],
            ],
            ["id", "category", "value"],
        )

        result = filtered.to_table()
        # Should match: (category=A AND value<20) OR (category=B AND value>40)
        # Row 1: A, 10 ✓
        # Row 5: B, 50 ✓
        assert result.num_rows == 2
        assert set(result["id"].to_pylist()) == {1, 5}

    def test_lazy_evaluation(self, sample_dataset: ds.Dataset) -> None:
        """Test that the returned dataset is lazy (not materialized)."""
        filtered = filter_dataset(
            sample_dataset, [("category", "==", "A")], ["id", "value"]
        )

        # The filtered object should be a dataset-like object
        assert isinstance(filtered, _ProjectedDataset)
        assert hasattr(filtered, "scanner")
        assert hasattr(filtered, "to_table")

        # Should be able to scan multiple times
        result1 = filtered.to_table()
        result2 = filtered.to_table()

        assert result1.equals(result2)

    def test_scanner_preserves_column_projection(
        self, sample_dataset: ds.Dataset
    ) -> None:
        """Test that using scanner() directly respects column projection."""
        filtered = filter_dataset(
            sample_dataset, [("category", "==", "A")], ["id", "value"]
        )

        # Use scanner directly
        scanner = filtered.scanner()
        result = scanner.to_table()

        assert result.column_names == ["id", "value"]
        assert result.num_rows == 2

    def test_all_columns_retained(self, sample_dataset: ds.Dataset) -> None:
        """Test filtering with all columns retained."""
        filtered = filter_dataset(
            sample_dataset, [("value", ">", 30)], ["id", "category", "value", "extra"]
        )

        result = filtered.to_table()
        assert result.num_rows == 2
        assert len(result.column_names) == 4

    def test_single_column_output(self, sample_dataset: ds.Dataset) -> None:
        """Test filtering with only one output column."""
        filtered = filter_dataset(sample_dataset, [("category", "==", "B")], ["id"])

        result = filtered.to_table()
        assert result.column_names == ["id"]
        assert result["id"].to_pylist() == [2, 5]

    def test_column_rename_preserves_data(self, sample_dataset: ds.Dataset) -> None:
        """Test that column renaming preserves data correctly."""
        filtered = filter_dataset(
            sample_dataset,
            [("category", "==", "A")],
            {"user_id": "id", "user_name": "category", "score": "value"},
        )

        result = filtered.to_table()
        assert result.num_rows == 2
        assert result["user_id"].to_pylist() == [1, 3]
        assert result["user_name"].to_pylist() == ["A", "A"]
        assert result["score"].to_pylist() == [10, 30]


class TestProjectedDataset:
    """Tests for the _ProjectedDataset wrapper class."""

    def test_schema_property(self) -> None:
        """Test that schema property delegates to underlying dataset."""
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        dataset = ds.dataset(table)
        projected = _ProjectedDataset(dataset, ["id"])

        assert projected.schema == dataset.schema

    def test_getattr_delegation(self) -> None:
        """Test that unknown attributes delegate to underlying dataset."""
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        dataset = ds.dataset(table)
        projected = _ProjectedDataset(dataset, ["id"])

        # Access an attribute that should be delegated
        assert hasattr(projected, "schema")
        assert projected.schema == dataset.schema

    def test_scanner_with_list_columns(self) -> None:
        """Test scanner with list of columns."""
        table = pa.table({"id": [1, 2], "value": [10, 20], "extra": [100, 200]})
        dataset = ds.dataset(table)
        projected = _ProjectedDataset(dataset, ["id", "value"])

        scanner = projected.scanner()
        result = scanner.to_table()

        assert result.column_names == ["id", "value"]

    def test_scanner_with_dict_columns(self) -> None:
        """Test scanner with dict for column renaming."""
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        dataset = ds.dataset(table)
        projected = _ProjectedDataset(dataset, {"identifier": "id", "amount": "value"})

        scanner = projected.scanner()
        result = scanner.to_table()

        assert set(result.column_names) == {"identifier", "amount"}

    def test_to_table_with_list_columns(self) -> None:
        """Test to_table with list of columns."""
        table = pa.table({"id": [1, 2], "value": [10, 20], "extra": [100, 200]})
        dataset = ds.dataset(table)
        projected = _ProjectedDataset(dataset, ["id", "value"])

        result = projected.to_table()

        assert result.column_names == ["id", "value"]

    def test_to_table_with_dict_columns(self) -> None:
        """Test to_table with dict for column renaming."""
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        dataset = ds.dataset(table)
        projected = _ProjectedDataset(dataset, {"new_id": "id", "new_value": "value"})

        result = projected.to_table()

        assert set(result.column_names) == {"new_id", "new_value"}


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_multiple_data_types(self) -> None:
        """Test filtering with various data types."""
        table = pa.table(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.5, 2.5, 3.5],
                "string_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )
        dataset = ds.dataset(table)

        # Test with different types
        filtered = filter_dataset(
            dataset,
            [("float_col", ">", 2.0), ("bool_col", "==", True)],
            ["int_col", "string_col"],
        )

        result = filtered.to_table()
        assert result.num_rows == 1
        assert result["int_col"].to_pylist() == [3]

    def test_null_values_in_data(self) -> None:
        """Test filtering with NULL values present."""
        table = pa.table({"id": [1, 2, 3, 4], "value": [10, None, 30, None]})
        dataset = ds.dataset(table)

        # Filter for non-null values
        filtered = filter_dataset(dataset, [("value", ">", 0)], ["id", "value"])

        result = filtered.to_table()
        # Should only include non-null values > 0
        assert result.num_rows == 2
        assert result["id"].to_pylist() == [1, 3]


class TestInnerJoinDatasets:
    """Tests for the inner_join_datasets function."""

    @pytest.fixture
    def users_dataset(self) -> ds.Dataset:
        """Create a users dataset."""
        table = pa.table(
            {
                "user_id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "David"],
                "age": [25, 30, 35, 40],
            }
        )
        return ds.dataset(table)

    @pytest.fixture
    def orders_dataset(self) -> ds.Dataset:
        """Create an orders dataset."""
        table = pa.table(
            {
                "user_id": [2, 3, 4, 5],
                "order_total": [100.0, 200.0, 150.0, 300.0],
                "order_count": [1, 2, 1, 3],
            }
        )
        return ds.dataset(table)

    @pytest.fixture
    def products_dataset(self) -> ds.Dataset:
        """Create a products dataset."""
        table = pa.table(
            {
                "user_id": [1, 2, 3],
                "product_name": ["Widget", "Gadget", "Doohickey"],
                "price": [9.99, 19.99, 14.99],
            }
        )
        return ds.dataset(table)

    def test_two_datasets_simple_join_no_conflicts(
        self, users_dataset: ds.Dataset, orders_dataset: ds.Dataset
    ) -> None:
        """Test simple join of two datasets with no column conflicts."""
        result = inner_join_datasets(
            {"users": users_dataset, "orders": orders_dataset}, "user_id"
        ).to_table()

        assert result.num_rows == 3  # user_id 2, 3, 4
        assert "user_id" in result.column_names
        assert "name" in result.column_names
        assert "age" in result.column_names
        assert "order_total" in result.column_names
        assert "order_count" in result.column_names

        # Verify join key appears only once
        assert result.column_names.count("user_id") == 1

        # No suffixes since there are no conflicts
        assert "name_users" not in result.column_names
        assert "order_total_orders" not in result.column_names

    def test_column_conflicts_get_suffixes(self) -> None:
        """Test that column name conflicts are resolved with suffixes."""
        # Both datasets have a 'value' column
        ds1 = ds.dataset(pa.table({"id": [1, 2, 3], "value": [10, 20, 30]}))
        ds2 = ds.dataset(pa.table({"id": [2, 3, 4], "value": [200, 300, 400]}))

        result = inner_join_datasets({"users": ds1, "orders": ds2}, "id").to_table()

        # Both value columns should be present with suffixes
        assert "value_users" in result.column_names
        assert "value_orders" in result.column_names
        assert "value" not in result.column_names

        # Verify data is correct
        assert result.num_rows == 2
        result_dict = result.to_pydict()
        id_2_idx = result_dict["id"].index(2)
        assert result_dict["value_users"][id_2_idx] == 20
        assert result_dict["value_orders"][id_2_idx] == 200

    def test_partial_column_conflicts(self) -> None:
        """Test join where only some columns conflict."""
        ds1 = ds.dataset(
            pa.table(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "value": [10, 20, 30],
                }
            )
        )
        ds2 = ds.dataset(
            pa.table(
                {"id": [2, 3, 4], "value": [200, 300, 400], "category": ["A", "B", "C"]}
            )
        )

        result = inner_join_datasets({"left": ds1, "right": ds2}, "id").to_table()

        # Non-conflicting columns should not have suffixes
        assert "name" in result.column_names
        assert "category" in result.column_names

        # Conflicting column should have suffixes
        assert "value_left" in result.column_names
        assert "value_right" in result.column_names
        assert "value" not in result.column_names

    def test_three_datasets_join(
        self,
        users_dataset: ds.Dataset,
        orders_dataset: ds.Dataset,
        products_dataset: ds.Dataset,
    ) -> None:
        """Test joining three datasets."""
        result = inner_join_datasets(
            {
                "users": users_dataset,
                "orders": orders_dataset,
                "products": products_dataset,
            },
            "user_id",
        ).to_table()

        # Only user_id 2 and 3 appear in all three datasets
        assert result.num_rows == 2
        assert "user_id" in result.column_names
        assert "name" in result.column_names
        assert "order_total" in result.column_names
        assert "product_name" in result.column_names

    def test_three_datasets_with_conflicts(self) -> None:
        """Test three-way join with column conflicts."""
        ds1 = ds.dataset(pa.table({"id": [1, 2, 3], "value": [10, 20, 30]}))
        ds2 = ds.dataset(pa.table({"id": [2, 3, 4], "value": [100, 200, 300]}))
        ds3 = ds.dataset(pa.table({"id": [2, 3, 5], "value": [1000, 2000, 3000]}))

        result = inner_join_datasets(
            {"first": ds1, "second": ds2, "third": ds3}, "id"
        ).to_table()

        # Only id 2 and 3 in all three
        assert result.num_rows == 2

        # Check that suffixes are applied to conflicting columns
        assert "id" in result.column_names
        # After multiple joins, suffix patterns may vary
        # Just verify we have three value columns with suffixes
        value_cols = [col for col in result.column_names if "value" in col]
        assert len(value_cols) == 3

    def test_single_dataset(self, users_dataset: ds.Dataset) -> None:
        """Test behavior with a single dataset."""
        result = inner_join_datasets({"users": users_dataset}, "user_id").to_table()

        assert result.num_rows == 4
        assert "user_id" in result.column_names
        assert "name" in result.column_names
        assert "age" in result.column_names

        # No suffixes for single dataset
        assert "name_users" not in result.column_names

    def test_empty_datasets_dict(self) -> None:
        """Test that empty datasets dict raises ValueError."""
        with pytest.raises(ValueError, match="At least one dataset must be provided"):
            inner_join_datasets({}, "id")

    def test_missing_join_key_in_dataset(
        self, users_dataset: ds.Dataset, orders_dataset: ds.Dataset
    ) -> None:
        """Test error when join key missing from a dataset."""
        with pytest.raises(
            ValueError,
            match="Join key 'nonexistent' not found in dataset 'users' schema",
        ):
            inner_join_datasets(
                {"users": users_dataset, "orders": orders_dataset}, "nonexistent"
            ).to_table()

    def test_no_matching_rows(self) -> None:
        """Test join with no matching keys across datasets."""
        ds1 = ds.dataset(pa.table({"id": [1, 2], "value": [10, 20]}))
        ds2 = ds.dataset(pa.table({"id": [3, 4], "value": [30, 40]}))

        result = inner_join_datasets({"first": ds1, "second": ds2}, "id").to_table()

        assert result.num_rows == 0
        # Columns should still be present
        assert "id" in result.column_names

    def test_duplicate_join_keys(self) -> None:
        """Test join with duplicate keys in datasets."""
        ds1 = ds.dataset(pa.table({"id": [1, 1, 2], "value": [10, 11, 20]}))
        ds2 = ds.dataset(pa.table({"id": [1, 2, 2], "value": [100, 200, 201]}))

        result = inner_join_datasets({"left": ds1, "right": ds2}, "id").to_table()

        # id=1: 2 × 1 = 2 rows
        # id=2: 1 × 2 = 2 rows
        assert result.num_rows == 4

    def test_join_preserves_data_types(
        self, users_dataset: ds.Dataset, orders_dataset: ds.Dataset
    ) -> None:
        """Test that data types are preserved after join."""
        result = inner_join_datasets(
            {"users": users_dataset, "orders": orders_dataset}, "user_id"
        ).to_table()

        assert result["user_id"].type == pa.int64()
        assert result["name"].type == pa.string()
        assert result["age"].type == pa.int64()
        assert result["order_total"].type == pa.float64()

    def test_join_order_deterministic(
        self,
        users_dataset: ds.Dataset,
        orders_dataset: ds.Dataset,
        products_dataset: ds.Dataset,
    ) -> None:
        """Test that join results are deterministic."""
        result1 = inner_join_datasets(
            {
                "users": users_dataset,
                "orders": orders_dataset,
                "products": products_dataset,
            },
            "user_id",
        ).to_table()

        result2 = inner_join_datasets(
            {
                "users": users_dataset,
                "orders": orders_dataset,
                "products": products_dataset,
            },
            "user_id",
        ).to_table()

        assert result1.equals(result2)

    def test_different_column_names_same_join_key(self) -> None:
        """Test datasets with different columns but same join key."""
        ds1 = ds.dataset(pa.table({"id": [1, 2, 3], "alpha": ["a", "b", "c"]}))
        ds2 = ds.dataset(pa.table({"id": [2, 3, 4], "beta": ["x", "y", "z"]}))
        ds3 = ds.dataset(pa.table({"id": [1, 2, 3], "gamma": [10, 20, 30]}))

        result = inner_join_datasets(
            {"first": ds1, "second": ds2, "third": ds3}, "id"
        ).to_table()

        assert result.num_rows == 2  # id 2 and 3 in all three
        assert "id" in result.column_names
        assert "alpha" in result.column_names
        assert "beta" in result.column_names
        assert "gamma" in result.column_names

    def test_single_row_datasets(self) -> None:
        """Test join with single-row datasets."""
        ds1 = ds.dataset(pa.table({"key": [1], "value1": [100]}))
        ds2 = ds.dataset(pa.table({"key": [1], "value2": [200]}))

        result = inner_join_datasets({"a": ds1, "b": ds2}, "key").to_table()

        assert result.num_rows == 1
        assert result["key"].to_pylist() == [1]
        assert result["value1"].to_pylist() == [100]
        assert result["value2"].to_pylist() == [200]

    def test_empty_dataset_join(self) -> None:
        """Test joining when one dataset is empty."""
        ds1 = ds.dataset(pa.table({"id": [1, 2, 3], "value": [10, 20, 30]}))
        ds2 = ds.dataset(
            pa.table(
                {
                    "id": pa.array([], type=pa.int64()),
                    "value": pa.array([], type=pa.int64()),
                }
            )
        )

        result = inner_join_datasets({"first": ds1, "second": ds2}, "id").to_table()

        # Inner join with empty dataset should return empty result
        assert result.num_rows == 0

    def test_join_key_with_nulls(self) -> None:
        """Test join behavior with NULL values in join key."""
        ds1 = ds.dataset(pa.table({"id": [1, 2, None], "value": [10, 20, 30]}))
        ds2 = ds.dataset(pa.table({"id": [2, None, 4], "value": [200, 300, 400]}))

        result = inner_join_datasets({"left": ds1, "right": ds2}, "id").to_table()

        # PyArrow join behavior: NULLs don't match
        # Only id=2 should match
        assert result.num_rows == 1
        assert result["id"].to_pylist() == [2]

    def test_string_join_key(self) -> None:
        """using string column as key."""
        ds1 = ds.dataset(
            pa.table(
                {"username": ["alice", "bob", "charlie"], "score": [100, 200, 300]}
            )
        )
        ds2 = ds.dataset(
            pa.table({"username": ["bob", "charlie", "david"], "level": [5, 10, 15]})
        )

        result = inner_join_datasets(
            {"scores": ds1, "levels": ds2}, "username"
        ).to_table()

        assert result.num_rows == 2
        assert set(result["username"].to_pylist()) == {"bob", "charlie"}

    def test_four_dataset_join(self) -> None:
        """Test joining four datasets."""
        datasets: dict[str, ds.Dataset] = {}
        for i in range(4):
            datasets[f"ds{i}"] = ds.dataset(
                pa.table({"id": [1, 2, 3], f"value{i}": [i * 10, i * 20, i * 30]})
            )

        result = inner_join_datasets(datasets, "id").to_table()

        assert result.num_rows == 3
        assert "id" in result.column_names
        # All value columns should be present (no conflicts with unique names)
        for i in range(4):
            assert f"value{i}" in result.column_names

    def test_dataset_names_with_special_chars(self) -> None:
        """Test dataset names with special characters (hyphens, dots)."""
        ds1 = ds.dataset(pa.table({"id": [1, 2], "value": [10, 20]}))
        ds2 = ds.dataset(pa.table({"id": [2, 3], "value": [200, 300]}))

        # Special characters should work in dataset names
        result = inner_join_datasets(
            {"data-source": ds1, "data.backup": ds2}, "id"
        ).to_table()

        assert result.num_rows == 1
        # Check that suffixes contain the dataset names
        value_cols = [col for col in result.column_names if "value" in col]
        assert len(value_cols) == 2

    def test_use_threads_parameter(self) -> None:
        """Test that use_threads parameter is accepted."""
        ds1 = ds.dataset(pa.table({"id": [1, 2, 3], "value": [10, 20, 30]}))
        ds2 = ds.dataset(pa.table({"id": [2, 3, 4], "value": [200, 300, 400]}))

        # Should work with use_threads=False
        result = inner_join_datasets(
            {"left": ds1, "right": ds2}, "id", use_threads=False
        ).to_table()

        assert result.num_rows == 2

    def test_all_columns_unique_no_suffixes(self) -> None:
        """Test that no suffixes are added when all columns are unique."""
        ds1 = ds.dataset(
            pa.table(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
        )
        ds2 = ds.dataset(
            pa.table(
                {
                    "id": [2, 3, 4],
                    "salary": [50000, 60000, 70000],
                    "department": ["IT", "Sales", "HR"],
                }
            )
        )

        result = inner_join_datasets(
            {"employees": ds1, "payroll": ds2}, "id"
        ).to_table()

        # All column names should be as-is (no suffixes)
        assert "name" in result.column_names
        assert "age" in result.column_names
        assert "salary" in result.column_names
        assert "department" in result.column_names

        # No suffixed versions
        assert "name_employees" not in result.column_names
        assert "salary_payroll" not in result.column_names

    def test_complex_multi_dataset_conflicts(self) -> None:
        """Test complex scenario with multiple datasets and mixed conflicts."""
        ds1 = ds.dataset(
            pa.table(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "score": [100, 200, 300],
                }
            )
        )
        ds2 = ds.dataset(
            pa.table(
                {
                    "id": [2, 3, 4],
                    "level": [5, 10, 15],
                    "score": [1000, 2000, 3000],  # Conflicts with ds1
                }
            )
        )
        ds3 = ds.dataset(
            pa.table(
                {
                    "id": [2, 3, 5],
                    "category": ["A", "B", "C"],
                    "score": [10000, 20000, 30000],  # Conflicts with ds1 and ds2
                }
            )
        )

        result = inner_join_datasets(
            {"users": ds1, "levels": ds2, "categories": ds3}, "id"
        ).to_table()

        # Only id 2 and 3 in all three
        assert result.num_rows == 2

        # Non-conflicting columns should be unsuffixed
        assert "name" in result.column_names
        assert "level" in result.column_names
        assert "category" in result.column_names

        # Conflicting 'score' columns should have suffixes
        score_cols = [col for col in result.column_names if "score" in col]
        assert len(score_cols) == 3


class TestIntegrationFilterAndJoin:
    """Integration tests combining filter_dataset and inner_join_datasets."""

    def test_filter_then_join(self) -> None:
        """Test filtering datasets before joining."""
        users_table = pa.table(
            {
                "user_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "active": [True, True, False, True, False],
            }
        )
        orders_table = pa.table(
            {
                "user_id": [1, 2, 3, 4, 5],
                "total": [100, 200, 300, 400, 500],
                "status": [
                    "completed",
                    "completed",
                    "pending",
                    "completed",
                    "cancelled",
                ],
            }
        )

        users_ds = ds.dataset(users_table)
        orders_ds = ds.dataset(orders_table)

        # Filter both datasets first
        active_users = filter_dataset(
            users_ds, [("active", "==", True)], ["user_id", "name"]
        )
        completed_orders = filter_dataset(
            orders_ds, [("status", "==", "completed")], ["user_id", "total"]
        )

        # Then join
        result = inner_join_datasets(
            {"users": active_users, "orders": completed_orders}, "user_id"
        ).to_table()

        # Should only have active users with completed orders (user_id 1, 2, 4)
        assert result.num_rows == 3
        assert set(result["user_id"].to_pylist()) == {1, 2, 4}
        assert "name" in result.column_names
        assert "total" in result.column_names

    def test_filter_with_rename_then_join(self) -> None:
        """Test filtering with column renaming before joining."""
        users_table = pa.table(
            {
                "id": [1, 2, 3],
                "user_name": ["Alice", "Bob", "Charlie"],
                "country": ["US", "UK", "US"],
            }
        )
        orders_table = pa.table({"id": [1, 2, 3], "order_value": [100, 200, 300]})

        users_ds = ds.dataset(users_table)
        orders_ds = ds.dataset(orders_table)

        # Filter US users and rename columns
        # Note: Keep 'id' as 'id' so we can join on it
        us_users = filter_dataset(
            users_ds,
            [("country", "==", "US")],
            {"id": "id", "name": "user_name"},  # Rename user_name but keep id
        )

        # Filter high-value orders
        high_orders = filter_dataset(
            orders_ds, [("order_value", ">", 150)], ["id", "order_value"]
        )

        # Join on 'id' which exists in both datasets
        result = inner_join_datasets(
            {"users": us_users, "orders": high_orders}, "id"
        ).to_table()

        # Should have US users (1, 3) with high-value orders (2, 3)
        # Only user 3 matches both conditions
        assert result.num_rows == 1
        assert result["id"].to_pylist() == [3]
        assert "name" in result.column_names
        assert "order_value" in result.column_names

    def test_multiple_filters_then_multi_join(self) -> None:
        """Test complex workflow with multiple filters and joins."""
        # Create three datasets
        users_table = pa.table(
            {
                "user_id": [1, 2, 3, 4],
                "tier": ["gold", "silver", "gold", "bronze"],
                "name": ["Alice", "Bob", "Charlie", "David"],
            }
        )
        orders_table = pa.table(
            {"user_id": [1, 2, 3, 4], "amount": [1000, 500, 1500, 200]}
        )
        rewards_table = pa.table({"user_id": [1, 2, 3], "points": [100, 50, 150]})

        users_ds = ds.dataset(users_table)
        orders_ds = ds.dataset(orders_table)
        rewards_ds = ds.dataset(rewards_table)

        # Filter each dataset
        gold_users = filter_dataset(
            users_ds, [("tier", "==", "gold")], ["user_id", "name"]
        )
        high_orders = filter_dataset(
            orders_ds, [("amount", ">=", 1000)], ["user_id", "amount"]
        )
        high_rewards = filter_dataset(
            rewards_ds, [("points", ">=", 100)], ["user_id", "points"]
        )

        # Join all three
        result = inner_join_datasets(
            {"users": gold_users, "orders": high_orders, "rewards": high_rewards},
            "user_id",
        ).to_table()

        # Should only have gold users with high orders and high rewards
        # (user_id 1 and 3)
        assert result.num_rows == 2
        assert set(result["user_id"].to_pylist()) == {1, 3}


class TestEdgeCasesJoin:
    """Additional edge case tests for joins."""

    def test_very_large_column_count(self) -> None:
        """Test join with many columns."""
        num_cols = 50
        data1 = {"id": [1, 2, 3]}
        data2 = {"id": [2, 3, 4]}

        for i in range(num_cols):
            data1[f"col{i}"] = [i, i + 1, i + 2]
            data2[f"col{i}_right"] = [i * 10, i * 10 + 1, i * 10 + 2]

        ds1 = ds.dataset(pa.table(data1))
        ds2 = ds.dataset(pa.table(data2))

        result = inner_join_datasets({"left": ds1, "right": ds2}, "id").to_table()

        assert result.num_rows == 2
        # Should have 1 id column + 50 from left + 50 from right
        assert len(result.column_names) == 1 + num_cols + num_cols

    def test_dataset_dict_insertion_order(self) -> None:
        """Test that dataset dictionary order affects join order."""
        ds1 = ds.dataset(pa.table({"id": [1, 2], "a": [10, 20]}))
        ds2 = ds.dataset(pa.table({"id": [2, 3], "b": [100, 200]}))
        ds3 = ds.dataset(pa.table({"id": [1, 2], "c": [1000, 2000]}))

        # Try different orders
        result1 = inner_join_datasets(
            {"first": ds1, "second": ds2, "third": ds3}, "id"
        ).to_table()
        result2 = inner_join_datasets(
            {"third": ds3, "first": ds1, "second": ds2}, "id"
        ).to_table()

        # Both should produce valid results
        assert result1.num_rows > 0
        assert result2.num_rows > 0

import pandas as pd
import pytest
from typing import List

from rail.projects.panda_utils import union_dataframes_deduplicated

# Unit Tests
class TestUnionDataframesDeduplicated:
    """Test suite for union_dataframes_deduplicated function."""
    
    def test_empty_list_raises_error(self):
        """Test that empty dataframes list raises ValueError."""
        with pytest.raises(ValueError, match="dataframes list cannot be empty"):
            union_dataframes_deduplicated([], dedup_column='id')
    
    def test_single_dataframe(self):
        """Test that single dataframe is returned with duplicates removed."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'value': ['a', 'b', 'c', 'd']
        })
        
        result = union_dataframes_deduplicated([df], dedup_column='id')
        
        expected = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'd']
        })
        
        pd.testing.assert_frame_equal(result, expected)
    
    def test_two_dataframes_no_overlap(self):
        """Test union of two dataframes with no overlapping keys."""
        df1 = pd.DataFrame({
            'id': [1, 2],
            'value': ['a', 'b']
        })
        
        df2 = pd.DataFrame({
            'id': [3, 4],
            'value': ['c', 'd']
        })
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='id')
        
        expected = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'value': ['a', 'b', 'c', 'd']
        })
        
        pd.testing.assert_frame_equal(result, expected)
    
    def test_two_dataframes_with_overlap_keep_first(self):
        """Test union with overlapping keys, keeping first occurrence."""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        
        df2 = pd.DataFrame({
            'id': [2, 3, 4],
            'value': ['x', 'y', 'z']
        })
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='id', keep='first')
        
        expected = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'value': ['a', 'b', 'c', 'z']
        })
        
        pd.testing.assert_frame_equal(result, expected)
    
    def test_two_dataframes_with_overlap_keep_last(self):
        """Test union with overlapping keys, keeping last occurrence."""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        
        df2 = pd.DataFrame({
            'id': [2, 3, 4],
            'value': ['x', 'y', 'z']
        })
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='id', keep='last')
        
        expected = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'value': ['a', 'x', 'y', 'z']
        })
        
        pd.testing.assert_frame_equal(result, expected)
    
    def test_three_dataframes_keep_first(self):
        """Test union of three dataframes with multiple overlaps, keep first."""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        df2 = pd.DataFrame({
            'id': [2, 3, 4],
            'name': ['Bob2', 'Charlie2', 'David']
        })
        
        df3 = pd.DataFrame({
            'id': [3, 4, 5],
            'name': ['Charlie3', 'David3', 'Eve']
        })
        
        result = union_dataframes_deduplicated([df1, df2, df3], dedup_column='id', keep='first')
        
        expected = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })
        
        pd.testing.assert_frame_equal(result, expected)
    
    def test_three_dataframes_keep_last(self):
        """Test union of three dataframes with multiple overlaps, keep last."""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        df2 = pd.DataFrame({
            'id': [2, 3, 4],
            'name': ['Bob2', 'Charlie2', 'David']
        })
        
        df3 = pd.DataFrame({
            'id': [3, 4, 5],
            'name': ['Charlie3', 'David3', 'Eve']
        })
        
        result = union_dataframes_deduplicated([df1, df2, df3], dedup_column='id', keep='last')
        
        expected = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob2', 'Charlie3', 'David3', 'Eve']
        })
        
        pd.testing.assert_frame_equal(result, expected)
        
    def test_missing_dedup_column_raises_error(self):
        """Test that missing dedup_column raises ValueError."""
        df1 = pd.DataFrame({
            'id': [1, 2],
            'value': ['a', 'b']
        })
        
        df2 = pd.DataFrame({
            'id': [3, 4],
            'value': ['c', 'd']
        })
        
        with pytest.raises(ValueError, match="dedup_column 'missing' not found"):
            union_dataframes_deduplicated([df1, df2], dedup_column='missing')
    
    def test_multiple_columns(self):
        """Test with dataframes containing multiple columns."""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'score': [85, 90, 75],
            'grade': ['B', 'A', 'C']
        })
        
        df2 = pd.DataFrame({
            'id': [2, 3, 4],
            'name': ['Bob', 'Charlie', 'David'],
            'score': [92, 78, 88],
            'grade': ['A', 'C', 'B']
        })
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='id', keep='first')
        
        assert len(result) == 4
        assert list(result['id']) == [1, 2, 3, 4]
        assert result.loc[result['id'] == 2, 'score'].values[0] == 90  # First occurrence
        assert result.loc[result['id'] == 3, 'name'].values[0] == 'Charlie'  # First occurrence
    
    def test_string_dedup_column(self):
        """Test deduplication with string-type key column."""
        df1 = pd.DataFrame({
            'code': ['A', 'B', 'C'],
            'value': [1, 2, 3]
        })
        
        df2 = pd.DataFrame({
            'code': ['B', 'C', 'D'],
            'value': [20, 30, 40]
        })
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='code', keep='first')
        
        expected = pd.DataFrame({
            'code': ['A', 'B', 'C', 'D'],
            'value': [1, 2, 3, 40]
        })
        
        pd.testing.assert_frame_equal(result, expected)
        
    def test_all_duplicates(self):
        """Test when all rows across dataframes are duplicates."""
        df1 = pd.DataFrame({
            'id': [1, 2],
            'value': ['a', 'b']
        })
        
        df2 = pd.DataFrame({
            'id': [1, 2],
            'value': ['x', 'y']
        })
        
        df3 = pd.DataFrame({
            'id': [1, 2],
            'value': ['m', 'n']
        })
        
        result = union_dataframes_deduplicated([df1, df2, df3], dedup_column='id', keep='first')
        
        expected = pd.DataFrame({
            'id': [1, 2],
            'value': ['a', 'b']
        })
        
        pd.testing.assert_frame_equal(result, expected)
    
    def test_preserves_column_order(self):
        """Test that column order is preserved from first dataframe."""
        df1 = pd.DataFrame({
            'z': [1, 2],
            'a': ['x', 'y'],
            'b': [10, 20]
        })
        
        df2 = pd.DataFrame({
            'z': [3, 4],
            'a': ['m', 'n'],
            'b': [30, 40]
        })
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='z')
        
        assert list(result.columns) == ['z', 'a', 'b']
    
    def test_numeric_dedup_column_with_floats(self):
        """Test deduplication with float-type key column."""
        df1 = pd.DataFrame({
            'id': [1.0, 2.5, 3.7],
            'value': ['a', 'b', 'c']
        })
        
        df2 = pd.DataFrame({
            'id': [2.5, 3.7, 4.2],
            'value': ['x', 'y', 'z']
        })
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='id', keep='last')
        
        expected = pd.DataFrame({
            'id': [1.0, 2.5, 3.7, 4.2],
            'value': ['a', 'x', 'y', 'z']
        })
        
        pd.testing.assert_frame_equal(result, expected)
    
    def test_reset_index_properly(self):
        """Test that result index is properly reset to 0, 1, 2, ..."""
        df1 = pd.DataFrame({
            'id': [5, 10, 15],
            'value': ['a', 'b', 'c']
        }, index=[100, 200, 300])
        
        df2 = pd.DataFrame({
            'id': [20, 25],
            'value': ['d', 'e']
        }, index=[400, 500])
        
        result = union_dataframes_deduplicated([df1, df2], dedup_column='id')
        
        assert list(result.index) == [0, 1, 2, 3, 4]


# Run tests with pytest
if __name__ == "__main__":
    pytest.main([__file__, '-v'])

# frozen_string_literal: true

require "test_helper"

# Test for basic operations like select, drop, unique, head, tail, sample
class TestOperations < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
    @df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
  end

  def test_select_columns
    result = TabularTool::Operations.select(@df, columns: ["name", "age"])
    assert_equal 2, result.width
    assert_equal ["name", "age"], result.columns
    assert_equal 10, result.height
  end

  def test_drop_columns
    result = TabularTool::Operations.drop(@df, columns: ["city", "status"])
    assert_equal 3, result.width
    assert_equal ["name", "age", "score"], result.columns
    assert_equal 10, result.height
  end

  def test_unique_removes_duplicates
    # Create a dataframe with duplicates
    df_with_dups = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations.unique(df_with_dups)

    # Should remove the duplicate Alice row
    assert result.height < df_with_dups.height
  end

  def test_unique_on_specific_columns
    # Create dataframe with duplicate cities
    result = TabularTool::Operations.unique(@df, columns: ["city"])

    # Should keep only first occurrence of each city
    cities = result["city"].to_a
    assert_equal cities.uniq, cities
  end

  def test_head_returns_first_n_rows
    result = TabularTool::Operations.head(@df, n: 5)
    assert_equal 5, result.height
    assert_equal "Alice", result["name"][0]
  end

  def test_head_default_is_10
    result = TabularTool::Operations.head(@df)
    assert_equal 10, result.height  # Our test data has exactly 10 rows
  end

  def test_head_more_than_available
    result = TabularTool::Operations.head(@df, n: 100)
    assert_equal 10, result.height  # Can't return more than exists
  end

  def test_tail_returns_last_n_rows
    result = TabularTool::Operations.tail(@df, n: 3)
    assert_equal 3, result.height
    assert_equal "Jack", result["name"][-1]
  end

  def test_tail_default_is_10
    result = TabularTool::Operations.tail(@df)
    assert_equal 10, result.height  # Our test data has exactly 10 rows
  end

  def test_sample_returns_n_rows
    result = TabularTool::Operations.sample(@df, n: 5)
    assert_equal 5, result.height
    assert_equal 5, result.width
  end

  def test_sample_returns_fraction_of_rows
    result = TabularTool::Operations.sample(@df, fraction: 0.5)
    assert_equal 5, result.height  # 50% of 10 = 5
  end

  def test_sample_fraction_rounds_appropriately
    result = TabularTool::Operations.sample(@df, fraction: 0.15)
    # 15% of 10 = 1.5
    assert_operator result.height, :>, 0
    assert_operator result.height, :<=, @df.height
  end

  def test_sample_small_fraction_can_return_zero
    result = TabularTool::Operations.sample(@df, fraction: 0.01)
    # 1% of 10 = 0.1, Polars may return 0 rows (no minimum enforced)
    assert_operator result.height, :>=, 0
    assert_operator result.height, :<=, @df.height
  end

  def test_chained_operations
    # Test that operations can be chained
    result = @df
    result = TabularTool::Operations.select(result, columns: ["name", "age", "score"])
    result = TabularTool::Operations::Filter.call(result, expression: "age > 28")
    result = TabularTool::Operations::Sort.call(result, keys: ["score"], reverse: true)
    result = TabularTool::Operations.head(result, n: 3)

    assert_equal 3, result.height
    assert_equal 3, result.width
    assert_equal ["name", "age", "score"], result.columns

    # Should be top 3 by score, filtered by age > 28
    scores = result["score"].to_a
    assert_equal scores.sort.reverse, scores  # Descending order
  end
end

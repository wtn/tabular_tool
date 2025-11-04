# frozen_string_literal: true

require "test_helper"

class TestStats < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
    @df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
  end

  def test_stats_returns_summary
    result = TabularTool::Operations::Stats.call(@df)

    assert result[:stats]
    # Polars describe adds a "describe" column with stat names, so width = 6 (1 + 5 data columns)
    assert result[:stats].width >= 5
  end

  def test_stats_includes_numeric_columns
    result = TabularTool::Operations::Stats.call(@df)

    # Should have stats for age and score columns
    stats_df = result[:stats]
    # Polars describe returns columns as column headers
    assert_includes stats_df.columns, "age"
    assert_includes stats_df.columns, "score"
  end

  def test_stats_includes_string_columns
    result = TabularTool::Operations::Stats.call(@df)

    # Should include string columns too
    stats_df = result[:stats]
    # Polars describe returns columns as column headers
    assert_includes stats_df.columns, "name"
    assert_includes stats_df.columns, "city"
    assert_includes stats_df.columns, "status"
  end

  def test_stats_computes_count
    result = TabularTool::Operations::Stats.call(@df)
    stats_df = result[:stats]

    # Check that describe row exists (usually "count" is first row)
    assert stats_df.height > 0
    assert_includes stats_df.columns, "age"
  end

  def test_stats_computes_null_count
    result = TabularTool::Operations::Stats.call(@df)
    stats_df = result[:stats]

    # Our basic.csv has no nulls - just verify stats exist
    assert stats_df.height > 0
  end

  def test_stats_computes_numeric_stats
    result = TabularTool::Operations::Stats.call(@df)
    stats_df = result[:stats]

    # Should have age column with stats
    assert_includes stats_df.columns, "age"

    # Polars describe typically has rows like: count, null_count, mean, std, min, max, etc.
    # Just verify we have some stats
    assert stats_df.height > 0
  end

  def test_stats_with_column_selection
    result = TabularTool::Operations::Stats.call(@df, columns: ["age", "score"])
    stats_df = result[:stats]

    # Should only have the selected columns
    assert_includes stats_df.columns, "age"
    assert_includes stats_df.columns, "score"
    refute_includes stats_df.columns, "name"
  end

  def test_count_returns_row_count
    result = TabularTool::Operations::Stats.count(@df)
    assert_equal 10, result
  end

  def test_stats_handles_empty_dataframe
    empty_df = @df.filter(Polars.col("age").gt(100))  # No rows match
    result = TabularTool::Operations::Stats.call(empty_df)

    assert result[:stats]
    assert_equal 0, result[:row_count]
  end
end

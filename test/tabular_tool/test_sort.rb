# frozen_string_literal: true

require "test_helper"

class TestSort < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
    @df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
  end

  def test_sort_by_single_column_ascending
    sorted = TabularTool::Operations::Sort.call(@df, keys: ["age"])
    ages = sorted["age"].to_a
    assert_equal [25, 26, 27, 28, 29, 30, 31, 32, 33, 35], ages
  end

  def test_sort_by_single_column_descending
    sorted = TabularTool::Operations::Sort.call(@df, keys: ["age"], reverse: true)
    ages = sorted["age"].to_a
    assert_equal [35, 33, 32, 31, 30, 29, 28, 27, 26, 25], ages
  end

  def test_sort_by_string_column
    sorted = TabularTool::Operations::Sort.call(@df, keys: ["name"])
    names = sorted["name"].to_a
    assert_equal "Alice", names.first
    assert_equal "Jack", names.last
  end

  def test_sort_by_multiple_columns
    # Sort by status, then by score
    sorted = TabularTool::Operations::Sort.call(
      @df,
      keys: ["status", "score"],
      reverse: false,
    )

    # "active" comes before "inactive" alphabetically
    assert_equal "active", sorted["status"][0]

    # Check that within each status group, records are sorted by score
    statuses = sorted["status"].to_a
    scores = sorted["score"].to_a

    # Find where status changes from active to inactive
    active_count = statuses.count("active")
    active_scores = scores[0...active_count]
    inactive_scores = scores[active_count..-1]

    assert_equal active_scores.sort, active_scores
    assert_equal inactive_scores.sort, inactive_scores
  end

  def test_sort_with_reverse_multiple_columns
    sorted = TabularTool::Operations::Sort.call(
      @df,
      keys: ["status", "score"],
      reverse: true,
    )

    # With reverse, we get descending sort for both columns
    # "inactive" comes before "active" in descending order
    assert_equal "inactive", sorted["status"][0]

    # Scores should also be in descending order within each group
    scores = sorted["score"].to_a

    # Check that scores are descending
    assert scores[0] >= scores[1]
  end

  def test_sort_numeric_vs_string
    # Numeric column should sort numerically
    sorted = TabularTool::Operations::Sort.call(@df, keys: ["age"])
    ages = sorted["age"].to_a
    assert_equal ages.sort, ages

    # String column should sort lexicographically
    sorted = TabularTool::Operations::Sort.call(@df, keys: ["name"])
    names = sorted["name"].to_a
    assert_equal names.sort, names
  end

  def test_sort_preserves_other_columns
    sorted = TabularTool::Operations::Sort.call(@df, keys: ["age"])

    # Verify all columns are still present
    assert_equal ["name", "age", "city", "score", "status"], sorted.columns

    # Verify row count is preserved
    assert_equal 10, sorted.height
  end

  def test_sort_case_insensitive
    # Create a dataframe with mixed case names
    df = Polars::DataFrame.new({
      "name" => ["alice", "Bob", "CHARLIE", "diana", "Eve"],
      "value" => [1, 2, 3, 4, 5],
    })

    # Case-sensitive sort (default) - uppercase comes before lowercase in ASCII
    # Expected order: Bob, CHARLIE, Eve, alice, diana
    sorted_sensitive = TabularTool::Operations::Sort.call(df, keys: ["name"], ignore_case: false)
    names_sensitive = sorted_sensitive["name"].to_a
    assert_equal ["Bob", "CHARLIE", "Eve", "alice", "diana"], names_sensitive

    # Case-insensitive sort - should be alphabetical regardless of case
    # Expected order: alice, Bob, CHARLIE, diana, Eve
    sorted_insensitive = TabularTool::Operations::Sort.call(df, keys: ["name"], ignore_case: true)
    names_insensitive = sorted_insensitive["name"].to_a
    assert_equal ["alice", "Bob", "CHARLIE", "diana", "Eve"], names_insensitive,
                 "Case-insensitive sort should order: alice, Bob, CHARLIE, diana, Eve"
  end
end

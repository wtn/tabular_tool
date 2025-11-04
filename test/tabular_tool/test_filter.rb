# frozen_string_literal: true

require "test_helper"

class TestFilter < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
    @df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
  end

  def test_filter_greater_than
    filtered = TabularTool::Operations::Filter.call(@df, expression: "age > 30")
    ages = filtered["age"].to_a
    assert ages.all? { |a| a > 30 }
    assert_equal 4, filtered.height  # Charlie(35), Eve(32), Henry(31), Jack(33)
  end

  def test_filter_less_than
    filtered = TabularTool::Operations::Filter.call(@df, expression: "age < 28")
    ages = filtered["age"].to_a
    assert ages.all? { |a| a < 28 }
    assert_equal 3, filtered.height  # Bob(25), Frank(27), Ivy(26)
  end

  def test_filter_greater_than_or_equal
    filtered = TabularTool::Operations::Filter.call(@df, expression: "age >= 30")
    ages = filtered["age"].to_a
    assert ages.all? { |a| a >= 30 }
    assert_equal 5, filtered.height
  end

  def test_filter_less_than_or_equal
    filtered = TabularTool::Operations::Filter.call(@df, expression: "score <= 85.5")
    scores = filtered["score"].to_a
    assert scores.all? { |s| s <= 85.5 }
  end

  def test_filter_equality
    filtered = TabularTool::Operations::Filter.call(@df, expression: "status == 'active'")
    statuses = filtered["status"].to_a
    assert statuses.all? { |s| s == "active" }
    assert_equal 7, filtered.height
  end

  def test_filter_inequality
    filtered = TabularTool::Operations::Filter.call(@df, expression: "status != 'active'")
    statuses = filtered["status"].to_a
    assert statuses.all? { |s| s == "inactive" }
    assert_equal 3, filtered.height
  end

  def test_filter_and_condition
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "age > 28 && status == 'active'",
    )
    assert filtered.height > 0
    filtered["age"].to_a.each do |age|
      assert age > 28
    end
    filtered["status"].to_a.each do |status|
      assert_equal "active", status
    end
  end

  def test_filter_or_condition
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "age < 26 || age > 33",
    )
    ages = filtered["age"].to_a
    assert ages.all? { |a| a < 26 || a > 33 }
  end

  def test_filter_contains
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "city.contains('San')",
    )
    cities = filtered["city"].to_a
    assert cities.all? { |c| c.include?("San") }
  end

  def test_filter_starts_with
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "name.starts_with('A')",
    )
    names = filtered["name"].to_a
    assert names.all? { |n| n.start_with?("A") }
    assert_equal 1, filtered.height  # Only Alice
  end

  def test_filter_ends_with
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "name.ends_with('e')",
    )
    names = filtered["name"].to_a
    assert names.all? { |n| n.end_with?("e") }
  end

  def test_filter_preserves_columns
    filtered = TabularTool::Operations::Filter.call(@df, expression: "age > 30")
    assert_equal ["name", "age", "city", "score", "status"], filtered.columns
  end

  def test_filter_empty_result
    filtered = TabularTool::Operations::Filter.call(@df, expression: "age > 100")
    assert_equal 0, filtered.height
    assert_equal 5, filtered.width  # Columns still present
  end

  def test_filter_with_parentheses
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "(age < 27 || age > 32) && status == 'active'",
    )
    ages = filtered["age"].to_a
    statuses = filtered["status"].to_a

    # Should have ages < 27 or > 32, and all active
    assert ages.all? { |a| a < 27 || a > 32 }
    assert statuses.all? { |s| s == "active" }
  end

  def test_filter_with_not_operator
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "!(status == 'inactive')",
    )
    statuses = filtered["status"].to_a
    assert statuses.all? { |s| s == "active" }
    assert_equal 7, filtered.height
  end

  def test_filter_complex_nested_expression
    # (age > 28 && score > 85) || (age < 27 && score > 90)
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "(age > 28 && score > 85.0) || (age < 27 && score > 90.0)",
    )

    # Should get rows matching either condition
    assert filtered.height > 0
    filtered.height.times do |i|
      age = filtered["age"][i]
      score = filtered["score"][i]
      assert((age > 28 && score > 85.0) || (age < 27 && score > 90.0))
    end
  end

  def test_filter_with_null_check
    # Create a dataframe with null values
    df_with_nulls = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))

    filtered = TabularTool::Operations::Filter.call(
      df_with_nulls,
      expression: "email.is_not_null()",
    )

    # Should filter out rows where email is null
    assert filtered.height < df_with_nulls.height
  end

  def test_filter_double_quotes
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: 'status == "active"',
    )
    statuses = filtered["status"].to_a
    assert statuses.all? { |s| s == "active" }
    assert_equal 7, filtered.height
  end

  def test_filter_multiple_string_methods
    filtered = TabularTool::Operations::Filter.call(
      @df,
      expression: "name.starts_with('A') || name.ends_with('e')",
    )
    names = filtered["name"].to_a
    assert names.all? { |n| n.start_with?("A") || n.end_with?("e") }
  end
end

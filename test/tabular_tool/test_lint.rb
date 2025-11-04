# frozen_string_literal: true

require "test_helper"

class TestLint < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
  end

  def test_lint_clean_file
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
    result = TabularTool::Operations::Lint.call(df)

    assert_equal true, result[:passed]
    assert_empty result[:errors]
    assert_empty result[:warnings]
  end

  def test_lint_detects_blank_values
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df)

    # Should detect blank email in row 3 (Charlie) and blank city in row 5 (Eve)
    blank_issues = result[:warnings].select { |w| w[:type] == :blank_values }
    refute_empty blank_issues
  end

  def test_lint_detects_duplicate_rows
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df)

    # Row 1 (Alice) is duplicated at row 9
    dup_issues = result[:errors].select { |e| e[:type] == :duplicate_rows }
    refute_empty dup_issues
  end

  def test_lint_detects_whitespace
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df)

    # Should detect trailing whitespace in age "29 " and leading in " 31"
    ws_issues = result[:warnings].select { |w| w[:type] == :whitespace }
    refute_empty ws_issues
  end

  def test_lint_detects_duplicate_column_values_when_requested
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df, unique_columns: ["email"])

    # Should detect duplicate email "bob@example.com"
    dup_col_issues = result[:errors].select { |e| e[:type] == :duplicate_column_values }
    refute_empty dup_col_issues

    email_issue = dup_col_issues.find { |e| e[:column] == "email" }
    assert email_issue
    assert email_issue[:lines].include?(3)  # Bob (line 3)
    assert email_issue[:lines].include?(7)  # Frank (line 7)
  end

  def test_lint_check_all_columns_for_duplicates
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df, unique_columns: :all)

    # Should check all columns for duplicates
    dup_col_issues = result[:errors].select { |e| e[:type] == :duplicate_column_values }
    refute_empty dup_col_issues
  end

  def test_lint_reports_row_and_column_count
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
    result = TabularTool::Operations::Lint.call(df)

    assert_equal 10, result[:row_count]
    assert_equal 5, result[:column_count]
  end

  def test_lint_returns_issues_with_line_numbers
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df)

    # All issues should have line numbers
    (result[:errors] + result[:warnings]).each do |issue|
      assert issue[:lines].is_a?(Array)
      assert issue[:lines].all? { |line| line.is_a?(Integer) }
    end
  end

  def test_lint_error_vs_warning_distinction
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df)

    # Errors: duplicate rows, duplicate column values (when checked)
    # Warnings: blank values, whitespace
    assert result[:errors].any? { |e| e[:type] == :duplicate_rows }
    assert result[:warnings].any? { |w| w[:type] == :blank_values || w[:type] == :whitespace }
  end

  def test_lint_no_unique_check_by_default
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df)

    # Without specifying unique_columns, should not check for duplicate column values
    dup_col_issues = result[:errors].select { |e| e[:type] == :duplicate_column_values }
    assert_empty dup_col_issues
  end

  def test_lint_includes_duplicate_values
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df, unique_columns: ["email"])

    email_issue = result[:errors].find { |e| e[:type] == :duplicate_column_values && e[:column] == "email" }
    assert email_issue
    assert email_issue[:values]
    assert email_issue[:values].include?("bob@example.com")
    assert email_issue[:values].include?("alice@example.com")
  end

  def test_lint_excludes_null_duplicates_from_duplicate_check
    # Create a dataframe with multiple null values in a column
    df = Polars::DataFrame.new({
      "name" => ["Alice", "Bob", "Charlie"],
      "email" => [nil, nil, "alice@example.com"],
    })
    result = TabularTool::Operations::Lint.call(df, unique_columns: ["email"])

    # Should not report null values as duplicates
    dup_col_issues = result[:errors].select { |e| e[:type] == :duplicate_column_values }
    assert_empty dup_col_issues

    # But should still report them as blank values
    blank_issues = result[:warnings].select { |w| w[:type] == :blank_values && w[:column] == "email" }
    refute_empty blank_issues
    assert_equal 2, blank_issues.first[:count]
  end

  def test_lint_duplicate_rows_include_line_numbers
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df)

    dup_row_issues = result[:errors].select { |e| e[:type] == :duplicate_rows }
    refute_empty dup_row_issues

    # Should include line numbers for duplicate rows
    assert dup_row_issues.first[:lines]
    refute_empty dup_row_issues.first[:lines]
    # Alice appears on lines 2 and 10
    assert dup_row_issues.first[:lines].include?(2)
    assert dup_row_issues.first[:lines].include?(10)
  end

  def test_lint_line_numbers_are_correct
    df = TabularTool::Formats.read(File.join(@fixtures_path, "lint_test.csv"))
    result = TabularTool::Operations::Lint.call(df, unique_columns: ["email"])

    # Check that line numbers match actual file line numbers (1-indexed, accounting for header)
    email_issue = result[:errors].find { |e| e[:type] == :duplicate_column_values && e[:column] == "email" }

    # Bob is on line 3, Frank on line 7, Alice on lines 2 and 10
    assert email_issue[:lines].include?(2)  # Alice
    assert email_issue[:lines].include?(3)  # Bob
    assert email_issue[:lines].include?(7)  # Frank
    assert email_issue[:lines].include?(10) # Alice duplicate
  end
end

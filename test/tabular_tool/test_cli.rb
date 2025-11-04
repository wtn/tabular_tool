# frozen_string_literal: true

require "test_helper"
require "tempfile"

class TestCLI < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
  end

  def test_parse_basic_file_argument
    args = TabularTool::CLI.parse_args(["file.csv"])
    assert_equal "file.csv", args[:file]
    assert_equal :cat, args[:command]
  end

  def test_parse_unknown_command_raises_error
    error = assert_raises(TabularTool::Error) do
      TabularTool::CLI.parse_args(["more", "file.csv"])
    end
    assert_match(/Unknown command: more/, error.message)
    assert_match(/Valid commands:/, error.message)
  end

  def test_parse_unknown_command_with_flag_before_raises_error
    error = assert_raises(TabularTool::Error) do
      TabularTool::CLI.parse_args(["--pretty", "more", "file.csv"])
    end
    assert_match(/Unexpected argument/, error.message)
  end

  def test_parse_leftover_arguments_raises_error
    error = assert_raises(TabularTool::Error) do
      TabularTool::CLI.parse_args(["head", "tail", "file.csv"])
    end
    assert_match(/Unexpected argument/, error.message)
  end

  def test_parse_head_command
    args = TabularTool::CLI.parse_args(["head", "file.csv"])
    assert_equal :head, args[:command]
    assert_equal "file.csv", args[:file]
    assert_equal 10, args[:limit]
  end

  def test_parse_head_with_count
    args = TabularTool::CLI.parse_args(["head", "20", "file.csv"])
    assert_equal :head, args[:command]
    assert_equal 20, args[:limit]
  end

  def test_parse_tail_command
    args = TabularTool::CLI.parse_args(["tail", "file.csv"])
    assert_equal :tail, args[:command]
    assert_equal 10, args[:limit]
  end

  def test_parse_sample_command
    args = TabularTool::CLI.parse_args(["sample", "100", "file.csv"])
    assert_equal :sample, args[:command]
    assert_equal 100, args[:sample_n]
  end

  def test_parse_sample_fraction
    args = TabularTool::CLI.parse_args(["sample", "0.5", "file.csv"])
    assert_equal :sample, args[:command]
    assert_equal 0.5, args[:sample_fraction]
  end

  def test_parse_sample_fraction_leading_dot
    args = TabularTool::CLI.parse_args(["sample", ".7", "file.csv"])
    assert_equal :sample, args[:command]
    assert_equal 0.7, args[:sample_fraction]
  end

  def test_parse_sample_decimal_as_integer
    # 12.8 should truncate to 12 rows
    args = TabularTool::CLI.parse_args(["sample", "12.8", "file.csv"])
    assert_equal :sample, args[:command]
    assert_equal 12, args[:sample_n]
    assert_nil args[:sample_fraction]
  end

  def test_parse_sample_no_argument_defaults_to_10
    args = TabularTool::CLI.parse_args(["sample", "file.csv"])
    assert_equal :sample, args[:command]
    assert_equal 10, args[:sample_n]
  end

  def test_parse_lint_command
    args = TabularTool::CLI.parse_args(["lint", "file.csv"])
    assert_equal :lint, args[:command]
  end

  def test_parse_stats_command
    args = TabularTool::CLI.parse_args(["stats", "file.csv"])
    assert_equal :stats, args[:command]
  end

  def test_parse_count_command
    args = TabularTool::CLI.parse_args(["count", "file.csv"])
    assert_equal :count, args[:command]
  end

  def test_parse_less_command
    args = TabularTool::CLI.parse_args(["less", "file.csv"])
    assert_equal :less, args[:command]
    assert_equal "file.csv", args[:file]
  end

  def test_execute_less_not_tty_behaves_like_cat
    # When not TTY (like in test environment), less should output raw CSV like cat
    result = TabularTool::CLI.execute(
      command: :less,
      file: File.join(@fixtures_path, "basic.csv"),
      pretty: false,
    )

    # Should output raw CSV
    assert result.is_a?(String)
    lines = result.split("\n")
    assert lines[0].include?("name,age,city"), "Should have CSV header"
    assert lines[1].include?("Alice"), "Should have data"
  end

  def test_parse_sort_key
    args = TabularTool::CLI.parse_args(["-k", "age", "file.csv"])
    assert_equal ["age"], args[:sort_keys]
  end

  def test_parse_multiple_sort_keys
    args = TabularTool::CLI.parse_args(["-k", "city", "-k", "age", "file.csv"])
    assert_equal ["city", "age"], args[:sort_keys]
  end

  def test_parse_reverse_flag
    args = TabularTool::CLI.parse_args(["-r", "file.csv"])
    assert args[:reverse]
  end

  def test_parse_filter_expression
    args = TabularTool::CLI.parse_args(["--where", "age > 30", "file.csv"])
    assert_equal "age > 30", args[:where]
  end

  def test_parse_select_columns
    args = TabularTool::CLI.parse_args(["--select", "name,age", "file.csv"])
    assert_equal ["name", "age"], args[:select]
  end

  def test_parse_only_columns
    args = TabularTool::CLI.parse_args(["--only", "name,age", "file.csv"])
    assert_equal ["name", "age"], args[:select]
  end

  def test_parse_drop_columns
    args = TabularTool::CLI.parse_args(["--drop", "city,status", "file.csv"])
    assert_equal ["city", "status"], args[:drop]
  end

  def test_parse_unique_flag
    args = TabularTool::CLI.parse_args(["--unique", "file.csv"])
    assert args[:unique]
  end

  def test_parse_unique_on_columns
    args = TabularTool::CLI.parse_args(["--unique-on", "email,user_id", "file.csv"])
    assert_equal ["email", "user_id"], args[:unique_on]
  end

  def test_parse_output_file
    args = TabularTool::CLI.parse_args(["-o", "output.csv", "file.csv"])
    assert_equal "output.csv", args[:output]
  end

  def test_parse_delimiter
    args = TabularTool::CLI.parse_args(["-d", "|", "file.csv"])
    assert_equal "|", args[:delimiter]
  end

  def test_parse_output_delimiter
    args = TabularTool::CLI.parse_args(["--output-delimiter", "\t", "file.csv"])
    assert_equal "\t", args[:output_delimiter]
  end

  def test_parse_lint_with_unique_columns
    args = TabularTool::CLI.parse_args(["lint", "--unique", "email,user_id", "file.csv"])
    assert_equal :lint, args[:command]
    assert_equal ["email", "user_id"], args[:unique_columns]
  end

  def test_parse_lint_with_unique_all
    args = TabularTool::CLI.parse_args(["lint", "--unique", "*", "file.csv"])
    assert_equal :all, args[:unique_columns]
  end

  def test_parse_lint_with_unique_no_args_defaults_to_all
    args = TabularTool::CLI.parse_args(["lint", "--unique", "file.csv"])
    assert_equal :all, args[:unique_columns]
    assert_equal false, args[:unique]
  end

  def test_parse_pretty_flags
    args = TabularTool::CLI.parse_args(["--pretty", "file.csv"])
    assert_equal true, args[:pretty]

    args = TabularTool::CLI.parse_args(["--no-pretty", "file.csv"])
    assert_equal false, args[:pretty]
  end

  def test_parse_parquet_compression
    args = TabularTool::CLI.parse_args(["-c", "gzip", "file.csv"])
    assert_equal "gzip", args[:compression]
  end

  def test_parse_complex_command
    args = TabularTool::CLI.parse_args([
      "head", "20",
      "-k", "score",
      "-r",
      "--where", "age > 25",
      "--select", "name,age,score",
      "file.csv"
    ])

    assert_equal :head, args[:command]
    assert_equal 20, args[:limit]
    assert_equal ["score"], args[:sort_keys]
    assert args[:reverse]
    assert_equal "age > 25", args[:where]
    assert_equal ["name", "age", "score"], args[:select]
    assert_equal "file.csv", args[:file]
  end

  def test_execute_cat_command
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv"),
    )

    assert result.is_a?(String)
    assert result.include?("Alice")
  end

  def test_execute_head_command
    result = TabularTool::CLI.execute(
      command: :head,
      file: File.join(@fixtures_path, "basic.csv"),
      limit: 5,
      pretty: false,  # Force raw output for consistent testing
    )

    lines = result.split("\n")
    # Should have 5 data rows + header
    assert_equal 6, lines.length
  end

  def test_execute_with_sort
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv"),
      sort_keys: ["age"],
    )

    # Should be sorted by age
    assert result.include?("Bob")  # Age 25, should be first
  end

  def test_execute_with_filter
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv"),
      where: "status == 'active'",
    )

    # Should only include active status
    assert result.include?("Alice")
    refute result.include?("Charlie")  # Charlie is inactive
  end

  def test_execute_lint_command
    result = TabularTool::CLI.execute(
      command: :lint,
      file: File.join(@fixtures_path, "basic.csv"),
    )

    assert result.is_a?(String)
    assert result.include?("Row count")
  end

  def test_execute_stats_command
    result = TabularTool::CLI.execute(
      command: :stats,
      file: File.join(@fixtures_path, "basic.csv"),
    )

    assert result.is_a?(String)
    assert result.include?("age") || result.include?("column")
  end

  def test_execute_count_command
    result = TabularTool::CLI.execute(
      command: :count,
      file: File.join(@fixtures_path, "basic.csv"),
    )

    assert result.is_a?(String)
    assert result.include?("10")
  end

  def test_parse_no_header_flag
    args = TabularTool::CLI.parse_args(["--no-header", "file.csv"])
    assert_equal true, args[:no_header]
  end

  def test_execute_with_no_header
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "headerless.csv"),
      no_header: true,
      pretty: false,
    )

    # Should have auto-generated column names
    assert result.include?("column_1")
    assert result.include?("column_2")
    assert result.include?("Alice")
  end

  def test_execute_head_with_no_header
    result = TabularTool::CLI.execute(
      command: :head,
      file: File.join(@fixtures_path, "headerless.csv"),
      no_header: true,
      limit: 3,
      pretty: false,
    )

    lines = result.split("\n")
    # Should have 3 data rows + header
    assert_equal 4, lines.length
    assert lines[0].include?("column_1")
  end

  def test_execute_sort_with_no_header
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "headerless.csv"),
      no_header: true,
      sort_keys: ["column_2"],
      reverse: true,
      pretty: false,
    )

    # Should be sorted by column_2 (age) descending
    # Charlie (35) should be first
    lines = result.split("\n")
    assert lines[1].include?("Charlie")
  end

  def test_csv_with_leading_empty_line_duplicates_header
    # Bug: When a CSV starts with an empty line, Polars treats the empty line as header
    # and includes the actual header line as data, causing it to appear twice
    Tempfile.create(["leading_empty", ".csv"]) do |file|
      file.write("\n")  # Empty line
      file.write("header\n")  # Header (single column)
      file.write("data1\n")  # Data
      file.write("data2\n")  # Data
      file.close

      result = TabularTool::CLI.execute(
        command: :cat,
        file: file.path,
        pretty: false,
      )

      lines = result.split("\n")

      # After fix, should have 3 lines (header + 2 data rows)
      assert_equal 3, lines.length, "Should have header + 2 data rows"
      assert_equal "header", lines[0], "First line should be header"
      assert_equal "data1", lines[1], "Second line should be first data row"
      assert_equal "data2", lines[2], "Third line should be second data row"
    end
  end

  # Test for flexible input file position (file before flags)
  def test_parse_file_before_output_flag
    args = TabularTool::CLI.parse_args(["input.csv", "-o", "output.parquet"])
    assert_equal "input.csv", args[:file]
    assert_equal "output.parquet", args[:output]
    assert_equal :cat, args[:command]
  end

  def test_parse_file_before_multiple_flags
    args = TabularTool::CLI.parse_args(["data.csv", "-k", "age", "-r", "-o", "sorted.csv"])
    assert_equal "data.csv", args[:file]
    assert_equal "sorted.csv", args[:output]
    assert_equal ["age"], args[:sort_keys]
    assert args[:reverse]
  end

  def test_parse_file_with_path_not_treated_as_unknown_command
    # File paths with / or . should not be treated as unknown commands
    args = TabularTool::CLI.parse_args(["./data/file.csv", "-o", "output.csv"])
    assert_equal "./data/file.csv", args[:file]
    assert_equal "output.csv", args[:output]
  end

  def test_parse_absolute_path_file_before_flags
    args = TabularTool::CLI.parse_args(["/tmp/test.csv", "--select", "name,age"])
    assert_equal "/tmp/test.csv", args[:file]
    assert_equal ["name", "age"], args[:select]
  end

  # Test that Parquet files output CSV to stdout (raw format)
  def test_execute_parquet_to_stdout_outputs_csv
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.parquet"),
      pretty: false,  # Explicitly request raw CSV output
    )

    # Result should be CSV format (comma-separated)
    assert result.is_a?(String)
    lines = result.split("\n")

    # First line should be CSV header
    assert lines[0].include?(","), "Output should be CSV format with commas"
    assert lines[0].include?("name"), "Should include header fields"

    # Data lines should also be CSV
    assert lines[1].include?(","), "Data rows should be comma-separated"
    assert lines[1].include?("Alice"), "Should include data from Parquet file"
  end

  # Test that cat command with pretty: true outputs formatted table
  def test_execute_cat_with_pretty_flag
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv"),
      pretty: true,  # Explicitly request pretty table output
    )

    # Result should be a formatted table (Polars DataFrame string representation)
    assert result.is_a?(String)
    # Pretty output has box-drawing characters and aligned columns
    # It won't have raw CSV commas on every line (just separators)
    assert result.include?("Alice"), "Should include data"
    assert result.include?("name"), "Should include column headers"
    # Pretty format has the shape info at the top (e.g., "shape: (10, 5)")
    assert result.include?("shape:"), "Pretty output shows shape"
    # Pretty format uses box-drawing characters
    assert result.match?(/[┌┬┐├┼┤└┴┘─│]/), "Pretty output has table borders"
  end

  def test_execute_parquet_head_to_stdout_outputs_csv
    result = TabularTool::CLI.execute(
      command: :head,
      file: File.join(@fixtures_path, "basic.parquet"),
      limit: 3,
      pretty: false,
    )

    lines = result.split("\n")

    # Should have header + 3 data rows = 4 lines
    assert_equal 4, lines.length, "Should have header + 3 data rows"

    # All lines should be CSV format
    assert lines[0].include?(","), "Header should be CSV format"
    assert lines[1].include?(","), "Data should be CSV format"
  end
end

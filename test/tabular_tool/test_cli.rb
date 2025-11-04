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
    # Stub tty? to simulate non-TTY environment and avoid opening interactive pager
    $stdout.stub :tty?, false do
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
  end

  def test_execute_less_compressed_file_not_tty
    # Regression test: compressed files should work in non-TTY mode without hanging
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv.gz"),
        pretty: false,
      )

      # Should decompress and output CSV
      assert result.is_a?(String)
      assert result.include?("Alice"), "Should have decompressed data"
    end
  end

  # Comprehensive less command test coverage

  def test_less_regular_file_not_tty_no_pretty_flag
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv"),
      )

      assert result.is_a?(String)
      lines = result.split("\n")
      assert_equal "name,age,city,score,status", lines[0], "Should output CSV header"
      assert lines[1].include?("Alice"), "Should have data"
    end
  end

  def test_less_regular_file_not_tty_with_pretty_true
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv"),
        pretty: true,
      )

      assert result.is_a?(String)
      assert result.include?("shape:"), "Pretty mode should show shape"
      assert result.match?(/[┌┬┐├┼┤└┴┘]/), "Pretty mode should have box-drawing"
    end
  end

  def test_less_regular_file_not_tty_with_pretty_false
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv"),
        pretty: false,
      )

      assert result.is_a?(String)
      lines = result.split("\n")
      assert_equal "name,age,city,score,status", lines[0]
      refute result.include?("shape:"), "No-pretty mode should not show shape"
    end
  end

  def test_less_compressed_file_not_tty_no_pretty_flag
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv.gz"),
      )

      assert result.is_a?(String)
      assert result.include?("Alice"), "Should decompress data"
      lines = result.split("\n")
      assert lines[0].include?("name"), "Should have header"
    end
  end

  def test_less_compressed_file_not_tty_with_pretty_true
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv.gz"),
        pretty: true,
      )

      assert result.is_a?(String)
      assert result.include?("Alice"), "Should have data"
      assert result.include?("shape:"), "Should have pretty format even for compressed in non-TTY"
    end
  end

  def test_less_compressed_file_not_tty_with_pretty_false
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv.gz"),
        pretty: false,
      )

      assert result.is_a?(String)
      lines = result.split("\n")
      assert lines[0].include?("name"), "Should have CSV header"
      assert result.include?("Alice"), "Should have data"
      refute result.include?("shape:"), "Should be raw CSV"
    end
  end

  def test_less_with_output_file
    $stdout.stub :tty?, false do
      Tempfile.create(["output", ".csv"]) do |tmp|
        result = TabularTool::CLI.execute(
          command: :less,
          file: File.join(@fixtures_path, "basic.csv"),
          output: tmp.path,
        )

        assert_nil result, "Should return nil when writing to file"
        assert File.exist?(tmp.path), "Output file should exist"
        content = File.read(tmp.path)
        assert content.include?("Alice"), "Output file should have data"
      end
    end
  end

  def test_less_with_select_transformation_not_tty
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv"),
        select: ["name", "age"],
        pretty: false,
      )

      assert result.is_a?(String)
      lines = result.split("\n")
      assert_equal "name,age", lines[0], "Should only have selected columns"
      assert lines[1].include?("Alice"), "Should have data"
      refute lines[0].include?("city"), "Should not include dropped columns"
    end
  end

  def test_less_compressed_with_select_not_tty
    $stdout.stub :tty?, false do
      result = TabularTool::CLI.execute(
        command: :less,
        file: File.join(@fixtures_path, "basic.csv.gz"),
        select: ["name", "age"],
        pretty: false,
      )

      assert result.is_a?(String)
      lines = result.split("\n")
      assert_equal "name,age", lines[0], "Should only have selected columns"
      assert result.include?("Alice"), "Should have data"
    end
  end

  # Test cat command with compressed files
  def test_cat_compressed_file
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv.gz"),
      pretty: false,
    )

    assert result.is_a?(String)
    assert result.include?("Alice"), "Should decompress and output data"
  end

  def test_cat_compressed_with_transformations
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv.gz"),
      select: ["name", "age"],
      sort_keys: ["age"],
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal "name,age", lines[0], "Should have selected columns"
    assert lines[1].include?("25"), "Should be sorted by age (Bob, age 25, first)"
  end

  # Test head command with compressed files
  def test_head_compressed_file
    result = TabularTool::CLI.execute(
      command: :head,
      file: File.join(@fixtures_path, "basic.csv.gz"),
      limit: 3,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal 4, lines.length, "Should have header + 3 rows"
  end

  def test_head_compressed_with_select
    result = TabularTool::CLI.execute(
      command: :head,
      file: File.join(@fixtures_path, "basic.csv.gz"),
      limit: 5,
      select: ["name"],
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal "name", lines[0], "Should only show selected column"
    assert_equal 6, lines.length, "Should have header + 5 rows"
  end

  # Test tail command with compressed files
  def test_tail_compressed_file
    result = TabularTool::CLI.execute(
      command: :tail,
      file: File.join(@fixtures_path, "basic.csv.gz"),
      limit: 3,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal 4, lines.length, "Should have header + 3 rows"
    assert lines[-1].include?("Jack"), "Last row should be Jack"
  end

  # Test parquet files
  def test_cat_parquet_file_outputs_csv
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.parquet"),
      pretty: false,
    )

    assert result.is_a?(String)
    assert result.include?(","), "Should output CSV format"
    assert result.include?("Alice"), "Should have data"
  end

  def test_head_parquet_file
    result = TabularTool::CLI.execute(
      command: :head,
      file: File.join(@fixtures_path, "basic.parquet"),
      limit: 3,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal 4, lines.length, "Should have header + 3 rows"
  end

  # Test transformation combinations
  def test_filter_and_sort_combination
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv"),
      where: "status == 'active'",
      sort_keys: ["age"],
      pretty: false,
    )

    lines = result.split("\n")
    refute lines.join("\n").include?("Charlie"), "Should filter out inactive"
    assert lines[1].include?("25"), "Should sort by age (Bob, 25, first among active)"
  end

  def test_select_and_unique_combination
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv"),
      select: ["status"],
      unique: true,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal "status", lines[0], "Should have selected column"
    assert_equal 3, lines.length, "Should have header + 2 unique values (active, inactive)"
  end

  # Test sample command variations
  def test_sample_compressed_file
    result = TabularTool::CLI.execute(
      command: :sample,
      file: File.join(@fixtures_path, "basic.csv.gz"),
      sample_n: 3,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal 4, lines.length, "Should have header + 3 sampled rows"
  end

  def test_sample_with_fraction
    result = TabularTool::CLI.execute(
      command: :sample,
      file: File.join(@fixtures_path, "basic.csv"),
      sample_fraction: 0.3,
      pretty: false,
    )

    lines = result.split("\n")
    assert lines.length >= 2, "Should have at least header + 1 row"
    assert lines.length <= 5, "Should have at most header + 3-4 rows (30% of 10)"
  end

  def test_sample_parquet_file
    result = TabularTool::CLI.execute(
      command: :sample,
      file: File.join(@fixtures_path, "basic.parquet"),
      sample_n: 5,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal 6, lines.length, "Should have header + 5 sampled rows"
    assert lines[0].include?(","), "Output should be CSV format"
  end

  def test_sample_parquet_with_fraction
    result = TabularTool::CLI.execute(
      command: :sample,
      file: File.join(@fixtures_path, "basic.parquet"),
      sample_fraction: 0.5,
      pretty: false,
    )

    lines = result.split("\n")
    assert lines.length >= 2, "Should have at least header + 1 row"
    assert lines.length <= 7, "Should have at most header + 5-6 rows (50% of 10)"
  end

  def test_sample_parquet_with_streaming
    # Test that sample works efficiently even with streaming enabled
    # This would previously hang because it tried to collect the entire file
    result = TabularTool::CLI.execute(
      command: :sample,
      file: File.join(@fixtures_path, "basic.parquet"),
      sample_n: 3,
      streaming: true,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal 4, lines.length, "Should have header + 3 sampled rows"
  end

  def test_tail_parquet_with_streaming
    # Test that tail works efficiently with streaming enabled
    # This would previously hang because it tried to collect the entire file
    result = TabularTool::CLI.execute(
      command: :tail,
      file: File.join(@fixtures_path, "basic.parquet"),
      limit: 3,
      streaming: true,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal 4, lines.length, "Should have header + 3 tail rows"
  end

  # Test count command
  def test_count_compressed_file
    result = TabularTool::CLI.execute(
      command: :count,
      file: File.join(@fixtures_path, "basic.csv.gz"),
    )

    assert_equal "10", result, "Should count 10 rows"
  end

  def test_count_with_filter
    result = TabularTool::CLI.execute(
      command: :count,
      file: File.join(@fixtures_path, "basic.csv"),
      where: "status == 'active'",
    )

    assert_equal "7", result, "Should count 7 active rows"
  end

  # Test lint command
  def test_lint_compressed_file
    result = TabularTool::CLI.execute(
      command: :lint,
      file: File.join(@fixtures_path, "basic.csv.gz"),
    )

    assert result.is_a?(String)
    assert result.include?("Row count"), "Should show row count"
  end

  def test_lint_with_check_unique
    result = TabularTool::CLI.execute(
      command: :lint,
      file: File.join(@fixtures_path, "basic.csv"),
      check_unique_columns: ["name"],
    )

    assert result.is_a?(String)
    assert result.include?("Row count"), "Should show lint results"
  end

  # Test stats command
  def test_stats_compressed_file
    result = TabularTool::CLI.execute(
      command: :stats,
      file: File.join(@fixtures_path, "basic.csv.gz"),
    )

    assert result.is_a?(String)
    assert result.include?("age") || result.include?("column"), "Should show stats"
  end

  # Test edge cases
  def test_output_file_with_different_format
    Tempfile.create(["output", ".parquet"]) do |tmp|
      result = TabularTool::CLI.execute(
        command: :cat,
        file: File.join(@fixtures_path, "basic.csv"),
        output: tmp.path,
      )

      assert_nil result, "Should return nil when writing to file"
      assert File.exist?(tmp.path), "Parquet file should be created"
      assert File.size(tmp.path) > 0, "File should have content"
    end
  end

  def test_in_place_preserves_format
    Tempfile.create(["temp", ".csv"]) do |tmp|
      FileUtils.cp(File.join(@fixtures_path, "basic.csv"), tmp.path)

      TabularTool::CLI.execute(
        command: :cat,
        file: tmp.path,
        sort_keys: ["age"],
        in_place: true,
      )

      content = File.read(tmp.path)
      lines = content.split("\n")
      assert lines[1].include?("Bob") || lines[1].include?("25"), "Should be sorted"
    end
  end

  # Error cases
  def test_execute_missing_file_raises_error
    error = assert_raises(TabularTool::Error) do
      TabularTool::CLI.execute(
        command: :cat,
        file: "/nonexistent/file.csv",
      )
    end
    assert_match(/File not found/, error.message)
  end

  def test_execute_no_file_raises_error
    error = assert_raises(TabularTool::Error) do
      TabularTool::CLI.execute(
        command: :cat,
        file: nil,
      )
    end
    assert_match(/No input file specified/, error.message)
  end

  def test_conflicting_output_flags
    Tempfile.create(["temp", ".csv"]) do |tmp|
      Tempfile.create(["output", ".csv"]) do |out|
        FileUtils.cp(File.join(@fixtures_path, "basic.csv"), tmp.path)

        # Both --in-place and -o specified (in-place wins)
        TabularTool::CLI.execute(
          command: :cat,
          file: tmp.path,
          in_place: true,
          output: out.path,
          sort_keys: ["age"],
        )

        # in-place should win (file at tmp.path is modified)
        content = File.read(tmp.path)
        assert content.include?("Bob"), "In-place file should be modified"
      end
    end
  end

  # Test multiple transformations in sequence
  def test_complex_transformation_pipeline
    result = TabularTool::CLI.execute(
      command: :cat,
      file: File.join(@fixtures_path, "basic.csv"),
      where: "age > 25",
      select: ["name", "age", "status"],
      sort_keys: ["age"],
      reverse: true,
      pretty: false,
    )

    lines = result.split("\n")
    assert_equal "name,age,status", lines[0], "Should have selected columns"
    refute lines.join("\n").include?("Bob"), "Should filter out age 25"
    assert lines[1].include?("35") || lines[1].include?("Charlie"), "Should have oldest first (reverse sort)"
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

  def test_parse_lint_with_check_unique_columns
    args = TabularTool::CLI.parse_args(["lint", "--check-unique", "email,user_id", "file.csv"])
    assert_equal :lint, args[:command]
    assert_equal ["email", "user_id"], args[:check_unique_columns]
  end

  def test_parse_lint_with_check_unique_all
    args = TabularTool::CLI.parse_args(["lint", "--check-unique", "*", "file.csv"])
    assert_equal :all, args[:check_unique_columns]
  end

  def test_parse_lint_without_check_unique
    args = TabularTool::CLI.parse_args(["lint", "file.csv"])
    assert_equal :lint, args[:command]
    assert_nil args[:check_unique_columns]
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

# frozen_string_literal: true

require "test_helper"
require "tempfile"

class TestFormatWrite < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
    @df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
  end

  def test_write_csv
    Tempfile.create(["output", ".csv"]) do |f|
      TabularTool::Formats.write(@df, f.path)

      # Read it back
      df_read = TabularTool::Formats.read(f.path)
      assert_equal @df.height, df_read.height
      assert_equal @df.width, df_read.width
      assert_equal @df.columns, df_read.columns
    end
  end

  def test_write_tsv
    Tempfile.create(["output", ".tsv"]) do |f|
      TabularTool::Formats.write(@df, f.path)

      # Read it back
      df_read = TabularTool::Formats.read(f.path)
      assert_equal @df.height, df_read.height
      assert_equal @df.columns, df_read.columns
    end
  end

  def test_write_json
    Tempfile.create(["output", ".json"]) do |f|
      TabularTool::Formats.write(@df, f.path)

      # Read it back
      df_read = TabularTool::Formats.read(f.path)
      assert_equal @df.height, df_read.height
      assert_equal @df.columns, df_read.columns
    end
  end

  def test_write_jsonl
    Tempfile.create(["output", ".jsonl"]) do |f|
      TabularTool::Formats.write(@df, f.path)

      # Read it back
      df_read = TabularTool::Formats.read(f.path)
      assert_equal @df.height, df_read.height
      assert_equal @df.columns, df_read.columns
    end
  end

  def test_write_parquet
    Tempfile.create(["output", ".parquet"]) do |f|
      TabularTool::Formats.write(@df, f.path)

      # Read it back
      df_read = TabularTool::Formats.read(f.path)
      assert_equal @df.height, df_read.height
      assert_equal @df.columns, df_read.columns
    end
  end

  def test_format_conversion_csv_to_json
    Tempfile.create(["output", ".json"]) do |f|
      # Read CSV
      csv_df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))

      # Write as JSON
      TabularTool::Formats.write(csv_df, f.path)

      # Read JSON back
      json_df = TabularTool::Formats.read(f.path)

      assert_equal csv_df.height, json_df.height
      assert_equal csv_df.columns, json_df.columns
    end
  end

  def test_format_conversion_json_to_parquet
    Tempfile.create(["output", ".parquet"]) do |f|
      # Read JSON
      json_df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.json"))

      # Write as Parquet
      TabularTool::Formats.write(json_df, f.path)

      # Read Parquet back
      parquet_df = TabularTool::Formats.read(f.path)

      assert_equal json_df.height, parquet_df.height
      assert_equal json_df.columns, parquet_df.columns
    end
  end

  def test_write_with_custom_delimiter
    Tempfile.create(["output", ".csv"]) do |f|
      TabularTool::Formats.write(@df, f.path, delimiter: "|")

      # Read it back with custom delimiter
      df_read = TabularTool::Formats.read(f.path, delimiter: "|")
      assert_equal @df.height, df_read.height
      assert_equal @df.columns, df_read.columns
    end
  end

  def test_write_parquet_with_compression
    Tempfile.create(["output", ".parquet"]) do |f|
      TabularTool::Formats.write(@df, f.path, compression: "gzip")

      # Read it back
      df_read = TabularTool::Formats.read(f.path)
      assert_equal @df.height, df_read.height
    end
  end

  def test_write_parquet_default_compression_is_zstd
    # Create test data with enough rows to show compression difference
    large_df = Polars::DataFrame.new({
      "a" => (1..1000).to_a,
      "b" => (1..1000).map { |i| "test string #{i}" * 10 },
    })

    Tempfile.create(["default", ".parquet"]) do |f1|
      Tempfile.create(["zstd", ".parquet"]) do |f2|
        # Write with default compression
        TabularTool::Formats.write(large_df, f1.path)

        # Write with explicit zstd
        TabularTool::Formats.write(large_df, f2.path, compression: "zstd")

        # Sizes should be similar (default should use zstd)
        size_default = File.size(f1.path)
        size_zstd = File.size(f2.path)

        # They should be equal or very close
        assert_equal size_zstd, size_default, "Default compression should be zstd"

        # And both should be readable
        df_read = TabularTool::Formats.read(f1.path)
        assert_equal large_df.height, df_read.height
      end
    end
  end

  def test_write_to_stdout
    # Test writing to stdout (should return string)
    output = TabularTool::Formats.write_to_stdout(@df, format: :csv)
    assert output.is_a?(String)
    assert output.include?("name,age,city,score,status")
    assert output.include?("Alice")
  end

  def test_write_to_stdout_preserves_format
    csv_output = TabularTool::Formats.write_to_stdout(@df, format: :csv)
    assert csv_output.include?(",")  # CSV uses commas

    tsv_output = TabularTool::Formats.write_to_stdout(@df, format: :tsv)
    assert tsv_output.include?("\t")  # TSV uses tabs
  end
end

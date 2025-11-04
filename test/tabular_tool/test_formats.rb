# frozen_string_literal: true

require "test_helper"

class TestFormats < Minitest::Test
  def setup
    @fixtures_path = File.join(__dir__, "..", "fixtures")
  end

  def test_detect_format_csv
    assert_equal :csv, TabularTool::Formats.detect_format("file.csv")
    assert_equal :csv, TabularTool::Formats.detect_format("file.txt")
  end

  def test_detect_format_tsv
    assert_equal :tsv, TabularTool::Formats.detect_format("file.tsv")
  end

  def test_detect_format_parquet
    assert_equal :parquet, TabularTool::Formats.detect_format("file.parquet")
    assert_equal :parquet, TabularTool::Formats.detect_format("file.pq")
  end

  def test_detect_format_json
    assert_equal :json, TabularTool::Formats.detect_format("file.json")
  end

  def test_detect_format_jsonl
    assert_equal :jsonl, TabularTool::Formats.detect_format("file.jsonl")
    assert_equal :jsonl, TabularTool::Formats.detect_format("file.ndjson")
  end

  def test_detect_format_with_compression
    assert_equal :csv, TabularTool::Formats.detect_format("file.csv.gz")
    assert_equal :csv, TabularTool::Formats.detect_format("file.csv.zst")
    assert_equal :parquet, TabularTool::Formats.detect_format("file.parquet.gz")
  end

  def test_read_csv
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
    assert_equal 10, df.height
    assert_equal 5, df.width
    assert_equal ["name", "age", "city", "score", "status"], df.columns
  end

  def test_read_tsv
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.tsv"))
    assert_equal 10, df.height
    assert_equal 5, df.width
    assert_equal ["name", "age", "city", "score", "status"], df.columns
  end

  def test_read_json
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.json"))
    assert_equal 10, df.height
    assert_equal 5, df.width
    assert_equal ["name", "age", "city", "score", "status"], df.columns
  end

  def test_read_jsonl
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.jsonl"))
    assert_equal 10, df.height
    assert_equal 5, df.width
    assert_equal ["name", "age", "city", "score", "status"], df.columns
  end

  def test_read_with_custom_delimiter
    df = TabularTool::Formats.read(
      File.join(@fixtures_path, "basic.tsv"),
      delimiter: "\t",
    )
    assert_equal 10, df.height
  end

  def test_read_csv_gzip
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv.gz"))
    assert_equal 10, df.height
    assert_equal 5, df.width
    assert_equal ["name", "age", "city", "score", "status"], df.columns
  end

  def test_read_csv_zstd
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv.zst"))
    assert_equal 10, df.height
    assert_equal 5, df.width
    assert_equal ["name", "age", "city", "score", "status"], df.columns
  end

  def test_read_tsv_gzip
    df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.tsv.gz"))
    assert_equal 10, df.height
    assert_equal 5, df.width
    assert_equal ["name", "age", "city", "score", "status"], df.columns
  end

  def test_compressed_data_matches_uncompressed
    csv_df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv"))
    gz_df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv.gz"))
    zst_df = TabularTool::Formats.read(File.join(@fixtures_path, "basic.csv.zst"))

    # All should have same dimensions
    assert_equal csv_df.height, gz_df.height
    assert_equal csv_df.height, zst_df.height
    assert_equal csv_df.width, gz_df.width
    assert_equal csv_df.width, zst_df.width

    # All should have same columns
    assert_equal csv_df.columns, gz_df.columns
    assert_equal csv_df.columns, zst_df.columns
  end

  def test_read_headerless_csv
    df = TabularTool::Formats.read(
      File.join(@fixtures_path, "headerless.csv"),
      has_header: false,
    )
    assert_equal 5, df.height
    assert_equal 5, df.width
    # Polars auto-generates column names for headerless files
    assert_equal ["column_1", "column_2", "column_3", "column_4", "column_5"], df.columns
  end

  def test_headerless_csv_data_integrity
    df = TabularTool::Formats.read(
      File.join(@fixtures_path, "headerless.csv"),
      has_header: false,
    )
    # Check first row data
    first_row = df.row(0)
    assert_equal "Alice", first_row[0]
    assert_equal 30, first_row[1]
    assert_equal "New York", first_row[2]
  end

  def test_should_stream_compressed_files_with_lower_threshold
    require 'tempfile'

    # Create a temp file to test streaming threshold
    Tempfile.create(['test', '.csv.gz']) do |f|
      # Write enough data to make it 60 MB compressed
      # With 10x multiplier, this should trigger streaming (60 * 10 = 600 MB > 500 MB threshold)
      f.write("x" * 60 * 1024 * 1024)
      f.flush

      # Should trigger streaming for compressed file
      assert TabularTool::Formats.should_stream?(f.path), "60 MB .gz file should trigger streaming (60*10=600 > 500)"
    end

    # Test regular file doesn't trigger at same size
    Tempfile.create(['test', '.csv']) do |f|
      f.write("x" * 60 * 1024 * 1024)
      f.flush

      # Should NOT trigger streaming for regular file
      refute TabularTool::Formats.should_stream?(f.path), "60 MB .csv file should NOT trigger streaming (60 < 500)"
    end
  end
end

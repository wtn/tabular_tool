# frozen_string_literal: true

require "test_helper"
require "tempfile"
require "zlib"

class TestCompressedOptimization < Minitest::Test
  def setup
    @tmpdir = File.join(__dir__, "..", "..", "tmp")
    Dir.mkdir(@tmpdir) unless Dir.exist?(@tmpdir)
  end

  def teardown
    # Clean up test files if needed
  end

  def create_test_csv_gz(path:, rows: 100)
    # Create a gzipped CSV file for testing
    Zlib::GzipWriter.open(path) do |gz|
      gz.puts "id,name,value"
      rows.times do |i|
        gz.puts "#{i},row_#{i},#{i * 10}"
      end
    end
  end

  def test_compressed_file_detection
    assert TabularTool::CLI.send(:compressed_file?, "file.csv.gz")
    assert TabularTool::CLI.send(:compressed_file?, "file.csv.GZ")
    assert TabularTool::CLI.send(:compressed_file?, "file.tsv.zst")
    refute TabularTool::CLI.send(:compressed_file?, "file.csv")
    refute TabularTool::CLI.send(:compressed_file?, "file.parquet")
  end

  def test_should_use_shell_decompression_for_head
    assert TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :head,
      {},
    )
  end

  def test_should_use_shell_decompression_for_tail
    # tail is faster with shell decompression (2x faster than Polars)
    assert TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :tail,
      {},
    )
  end

  def test_should_not_use_shell_decompression_for_uncompressed
    refute TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv",
      :head,
      {},
    )
  end

  def test_should_not_use_shell_decompression_with_sort
    refute TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :head,
      { sort_keys: ["name"] },
    )
  end

  def test_should_not_use_shell_decompression_with_unique
    refute TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :head,
      { unique: true },
    )
  end

  def test_should_not_use_shell_decompression_with_where
    refute TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :head,
      { where: "age > 30" },
    )
  end

  def test_should_not_use_shell_decompression_with_streaming
    refute TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :head,
      { streaming: true },
    )
  end

  def test_should_use_shell_decompression_with_select
    # select is OK, doesn't prevent optimization
    assert TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :head,
      { select: ["name", "age"] },
    )
  end

  def test_should_use_shell_decompression_with_drop
    # drop is OK, doesn't prevent optimization
    assert TabularTool::CLI.send(
      :should_use_shell_decompression?,
      "file.csv.gz",
      :head,
      { drop: ["unwanted_col"] },
    )
  end

  def test_detect_format_without_compression
    assert_equal :csv, TabularTool::CLI.send(:detect_format_without_compression, "file.csv.gz")
    assert_equal :tsv, TabularTool::CLI.send(:detect_format_without_compression, "file.tsv.gz")
    assert_equal :csv, TabularTool::CLI.send(:detect_format_without_compression, "file.txt.zst")
  end

  def test_head_compressed_basic
    test_file = File.join(@tmpdir, "test_head.csv.gz")
    create_test_csv_gz(path: test_file, rows: 100)

    result = TabularTool::CLI.execute(
      command: :head,
      file: test_file,
      limit: 10,
      pretty: false,
    )

    lines = result.split("\n")
    # Should have 10 data rows + header
    assert_equal 11, lines.length, "Should have exactly 11 lines (1 header + 10 data rows)"
    assert lines[0].include?("id"), "First line should be header"
    assert lines[1].include?("0,row_0,0"), "Second line should be first data row"
    assert lines[10].include?("9,row_9,90"), "Last line should be 10th data row"
  ensure
    File.delete(test_file) if test_file && File.exist?(test_file)
  end

  def test_head_compressed_with_select
    test_file = File.join(@tmpdir, "test_head_select.csv.gz")
    create_test_csv_gz(path: test_file, rows: 100)

    result = TabularTool::CLI.execute(
      command: :head,
      file: test_file,
      limit: 5,
      select: ["id", "name"],
      pretty: false,
    )

    lines = result.split("\n")
    # Should have 5 data rows + header, with only 2 columns
    assert_equal 6, lines.length
    assert lines[0].include?("id"), "Should have id column"
    assert lines[0].include?("name"), "Should have name column"
    refute lines[0].include?("value"), "Should not have value column"
  ensure
    File.delete(test_file) if test_file && File.exist?(test_file)
  end

  def test_head_compressed_with_drop
    test_file = File.join(@tmpdir, "test_head_drop.csv.gz")
    create_test_csv_gz(path: test_file, rows: 100)

    result = TabularTool::CLI.execute(
      command: :head,
      file: test_file,
      limit: 5,
      drop: ["value"],
      pretty: false,
    )

    lines = result.split("\n")
    # Should have 5 data rows + header, without value column
    assert_equal 6, lines.length
    assert lines[0].include?("id"), "Should have id column"
    assert lines[0].include?("name"), "Should have name column"
    refute lines[0].include?("value"), "Should not have value column"
  ensure
    File.delete(test_file) if test_file && File.exist?(test_file)
  end

  def test_tail_compressed_basic
    # tail uses shell decompression optimization (2x faster than Polars native)
    test_file = File.join(@tmpdir, "test_tail.csv.gz")
    create_test_csv_gz(path: test_file, rows: 100)

    result = TabularTool::CLI.execute(
      command: :tail,
      file: test_file,
      limit: 10,
      pretty: false,
    )

    lines = result.split("\n")
    # Should have 10 data rows + header
    assert_equal 11, lines.length
    assert lines[0].include?("id"), "First line should be header"
    # Last 10 rows should be rows 90-99
    assert lines[1].include?("90,row_90,900"), "First data line should be row 90"
    assert lines[10].include?("99,row_99,990"), "Last line should be row 99"
  ensure
    File.delete(test_file) if test_file && File.exist?(test_file)
  end

  def test_tail_compressed_with_select
    # tail uses shell decompression optimization (2x faster than Polars native)
    test_file = File.join(@tmpdir, "test_tail_select.csv.gz")
    create_test_csv_gz(path: test_file, rows: 100)

    result = TabularTool::CLI.execute(
      command: :tail,
      file: test_file,
      limit: 5,
      select: ["id", "name"],
      pretty: false,
    )

    lines = result.split("\n")
    # Should have 5 data rows + header, with only 2 columns
    assert_equal 6, lines.length
    assert lines[0].include?("id"), "Should have id column"
    assert lines[0].include?("name"), "Should have name column"
    refute lines[0].include?("value"), "Should not have value column"
  ensure
    File.delete(test_file) if test_file && File.exist?(test_file)
  end

  def test_head_compressed_with_sort_uses_full_read
    # With sort, should NOT use optimization
    # Instead it should read the full file through normal Polars read path
    test_file = File.join(@tmpdir, "test_head_sort.csv.gz")
    create_test_csv_gz(path: test_file, rows: 50)

    result = TabularTool::CLI.execute(
      command: :head,
      file: test_file,
      limit: 10,
      sort_keys: ["name"],
      pretty: false,
    )

    lines = result.split("\n")
    # Should still work, just not using optimization
    assert_equal 11, lines.length
    assert lines[0].include?("id"), "First line should be header"
  ensure
    File.delete(test_file) if test_file && File.exist?(test_file)
  end

  def test_formats_read_from_io_csv
    # Test that Formats.read_from_io works with CSV
    csv_data = "id,name\n1,Alice\n2,Bob\n"
    io = StringIO.new(csv_data)

    df = TabularTool::Formats.read_from_io(io, format: :csv)

    assert_equal 2, df.height
    assert_equal ["id", "name"], df.columns
  end

  def test_formats_read_from_io_tsv
    # Test that Formats.read_from_io works with TSV
    tsv_data = "id\tname\n1\tAlice\n2\tBob\n"
    io = StringIO.new(tsv_data)

    df = TabularTool::Formats.read_from_io(io, format: :tsv)

    assert_equal 2, df.height
    assert_equal ["id", "name"], df.columns
  end

  def test_formats_read_from_io_unsupported_format
    # Test that unsupported formats raise an error
    io = StringIO.new("{}")

    assert_raises(TabularTool::Error) do
      TabularTool::Formats.read_from_io(io, format: :json)
    end
  end
end

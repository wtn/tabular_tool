# frozen_string_literal: true

require "polars"

module TabularTool
  module Formats
    # Default streaming threshold: 500 MiB
    STREAMING_THRESHOLD = 500 * 1024 * 1024

    class << self
      def should_stream?(path, threshold: STREAMING_THRESHOLD)
        size = File.size(path)

        # Compressed files: use lower threshold since compressed size << actual size
        # Assume ~10x compression ratio for text files
        if path.match?(/\.(gz|zst)$/i)
          size * 10 >= threshold
        else
          size >= threshold
        end
      rescue Errno::ENOENT
        false
      end

      def detect_format(path)
        base_path = path.sub(/\.(gz|zst)$/, "")
        ext = File.extname(base_path).downcase

        case ext
        when ".csv", ".txt"
          :csv
        when ".tsv"
          :tsv
        when ".parquet", ".pq"
          :parquet
        when ".json"
          :json
        when ".jsonl", ".ndjson"
          :jsonl
        else
          :csv
        end
      end

      def read(path, format: nil, delimiter: nil, has_header: true, streaming: nil, **options)
        format ||= detect_format(path)

        streaming = should_stream?(path) if streaming.nil?

        case format
        when :csv
          read_csv(path, delimiter: delimiter || ",", has_header: has_header, streaming: streaming, **options)
        when :tsv
          read_csv(path, delimiter: delimiter || "\t", has_header: has_header, streaming: streaming, **options)
        when :parquet
          if streaming
            Polars.scan_parquet(path, **options)
          else
            Polars.read_parquet(path, **options)
          end
        when :json
          # JSON doesn't support streaming in Polars
          Polars.read_json(path, **options)
        when :jsonl
          if streaming
            Polars.scan_ndjson(path, **options)
          else
            Polars.read_ndjson(path, **options)
          end
        else
          raise Error, "Unsupported format: #{format}"
        end
      end

      def write(df, path, format: nil, delimiter: nil, compression: nil, **options)
        format ||= detect_format(path)

        # For LazyFrames, use streaming sink methods when possible
        if df.is_a?(Polars::LazyFrame)
          case format
          when :csv
            return df.sink_csv(path, separator: delimiter || ",", **options)
          when :tsv
            return df.sink_csv(path, separator: delimiter || "\t", **options)
          when :parquet
            return df.sink_parquet(path, compression: compression || "zstd", **options)
          when :jsonl
            return df.sink_ndjson(path, **options)
          when :json
            # JSON doesn't have a sink method, need to collect
            df = df.collect
          else
            raise Error, "Unsupported format: #{format}"
          end
        end

        # For DataFrames or formats without sink methods
        case format
        when :csv
          write_csv(df, path, delimiter: delimiter || ",", **options)
        when :tsv
          write_csv(df, path, delimiter: delimiter || "\t", **options)
        when :parquet
          df.write_parquet(path, compression: compression || "zstd", **options)
        when :json
          df.write_json(path, **options)
        when :jsonl
          df.write_ndjson(path, **options)
        else
          raise Error, "Unsupported format: #{format}"
        end
      end

      def write_to_stdout(df, format:, delimiter: nil, **options)
        case format
        when :csv
          df.write_csv(nil, separator: delimiter || ",")
        when :tsv
          df.write_csv(nil, separator: delimiter || "\t")
        when :json
          df.write_json
        when :jsonl
          df.write_ndjson
        when :parquet
          # For stdout, we need to write to memory buffer
          # This is tricky with Polars, may need to write to temp file
          raise Error, "Writing Parquet to stdout not yet supported"
        else
          raise Error, "Unsupported format: #{format}"
        end
      end

      def read_from_io(io, format:, delimiter: nil, has_header: true, **options)
        case format
        when :csv
          read_csv_from_io(io, delimiter: delimiter || ",", has_header: has_header, **options)
        when :tsv
          read_csv_from_io(io, delimiter: delimiter || "\t", has_header: has_header, **options)
        else
          raise Error, "Shell decompression only supports CSV/TSV, got: #{format}"
        end
      end

      private

      def read_csv(path, delimiter:, has_header: true, streaming: false, **options)
        # Work around Polars bug with leading empty lines by preprocessing
        # Only for uncompressed files (compressed files are handled differently)
        # https://github.com/pola-rs/polars/issues/xxxxx
        if !compressed_file?(path) && has_leading_empty_lines?(path)
          # Read file, skip leading empty lines, then pass to Polars
          File.open(path, 'r') do |file|
            read_csv_from_io(file, delimiter: delimiter, has_header: has_header, streaming: streaming, **options)
          end
        else
          if streaming
            Polars.scan_csv(path, separator: delimiter, has_header: has_header, **options)
          else
            Polars.read_csv(path, separator: delimiter, has_header: has_header, **options)
          end
        end
      end

      def write_csv(df, path, delimiter:, **options)
        df.write_csv(path, separator: delimiter, **options)
      end

      def read_csv_from_io(io, delimiter:, has_header: true, streaming: false, **options)
        lines = []

        io.each_line do |line|
          if line.strip.empty? && lines.empty?
            next
          else
            lines << line
          end
        end

        require 'stringio'
        cleaned_io = StringIO.new(lines.join)

        Polars.read_csv(cleaned_io, separator: delimiter, has_header: has_header, **options)
      end

      def has_leading_empty_lines?(path)
        File.open(path, 'r') do |file|
          first_line = file.gets
          return false if first_line.nil?
          return first_line.strip.empty?
        end
      end

      def compressed_file?(path)
        path.match?(/\.(gz|zst)$/i)
      end
    end
  end
end

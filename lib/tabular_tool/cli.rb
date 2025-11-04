# frozen_string_literal: true

require "optparse"

module TabularTool
  module CLI
    class << self
      def parse_args(argv)
        args = {
          command: :cat,
          limit: 10,
          sort_keys: [],
          reverse: false,
          pretty: nil,  # nil means auto-detect TTY
          streaming: nil,  # nil means auto-detect based on file size
        }

        parser = build_parser(args)

        parse_command_and_args(argv, args)
        parser.parse!(argv)
        args[:file] = argv.pop if argv.any?
        raise Error, "Unexpected argument(s): #{argv.join(' ')}" if argv.any?

        args
      end

      def execute(command:, file: nil, **options)
        df, use_shell_optimization = read_input(file, command, options)

        result = case command
        when :cat    then execute_cat(df, file, options, use_shell_optimization)
        when :head   then execute_head(df, file, options, use_shell_optimization)
        when :tail   then execute_tail(df, file, options, use_shell_optimization)
        when :sample then execute_sample(df, file, options)
        when :less   then execute_less(df, file, options, use_shell_optimization)
        when :lint   then execute_lint(collect_if_lazy(df), options)
        when :stats  then execute_stats(collect_if_lazy(df), options)
        when :count  then execute_count(df, options)
        else raise Error, "Unknown command: #{command}"
        end

        result
      rescue Interrupt
        raise
      rescue TypeError => e
        # TypeErrors during interrupt likely mean interrupted IO left nil values
        raise Interrupt if e.message.include?("no implicit conversion of nil")
        raise
      end

      private

      def parse_command_and_args(argv, args)
        return unless argv.first && !argv.first.start_with?("-")

        first_arg = argv.first

        case first_arg
        when "cat", "head", "tail", "sample", "less", "lint", "stats", "count"
          args[:command] = argv.shift.to_sym
          parse_numeric_args(argv, args)
        else
          # Heuristic: paths contain . or /, command names don't
          # If multiple args remain and first_arg doesn't look like a path, it's likely a typo
          if argv.length > 1 && !first_arg.include?(".") && !first_arg.include?("/")
            raise Error, "Unknown command: #{first_arg}. Valid commands: cat, head, tail, sample, less, lint, stats, count"
          end
        end
      end

      def parse_numeric_args(argv, args)
        if [:head, :tail].include?(args[:command]) && argv.first && argv.first.match?(/^\d+$/)
          args[:limit] = argv.shift.to_i
        elsif args[:command] == :sample
          if argv.first && argv.first.match?(/^\.?\d+\.?\d*$/)
            value = argv.shift
            num = value.to_f

            # Values < 1.0 are fractions (0.1 = 10%), >= 1.0 are row counts
            if num < 1.0
              args[:sample_fraction] = num
            else
              args[:sample_n] = num.to_i
            end
          else
            args[:sample_n] = 10
          end
        end
      end

      def read_input(file, command, options)
        raise Error, "No input file specified" unless file
        raise Error, "File not found: #{file}" unless File.exist?(file)

        use_shell_optimization = should_use_shell_decompression?(file, command, options)

        df = nil
        unless use_shell_optimization
          df = Formats.read(
            file,
            delimiter: options[:delimiter],
            has_header: !options[:no_header],
            streaming: options[:streaming],
          )

          df = apply_transformations(df, options)
        end

        [df, use_shell_optimization]
      end

      def execute_cat(df, file, options, use_shell_optimization)
        df = read_compressed_full(file, options: options) if use_shell_optimization
        output_dataframe(df, file, options, default_pretty: true)
      end

      def execute_head(df, file, options, use_shell_optimization)
        if use_shell_optimization
          df = read_compressed_partial(file, command: :head, limit: options[:limit] || 10, options: options)
        else
          df = Operations.head(df, n: options[:limit] || 10)
        end
        output_dataframe(df, file, options, default_pretty: true)
      end

      def execute_tail(df, file, options, use_shell_optimization)
        if use_shell_optimization
          df = read_compressed_partial(file, command: :tail, limit: options[:limit] || 10, options: options)
        else
          # Operations.tail now handles lazy frames efficiently, no need to collect first
          df = Operations.tail(df, n: options[:limit] || 10)
        end
        output_dataframe(df, file, options, default_pretty: true)
      end

      def execute_sample(df, file, options)
        # Operations.sample now handles lazy frames efficiently, no need to collect first
        if options[:sample_fraction]
          df = Operations.sample(df, fraction: options[:sample_fraction])
        else
          df = Operations.sample(df, n: options[:sample_n])
        end
        output_dataframe(df, file, options, default_pretty: true)
      end

      def execute_less(df, file, options, use_shell_optimization)
        output_file = options[:in_place] ? file : options[:output]

        if output_file
          df = read_compressed_full(file, options: options) if use_shell_optimization
          df = collect_if_lazy(df)
          Formats.write(
            df,
            output_file,
            delimiter: options[:output_delimiter],
            compression: options[:compression],
          )
          nil
        elsif $stdout.tty?
          # For compressed files: always use shell bypass for performance
          # Loading giant compressed files through Polars is slow and blocks in Rust (uninterruptible)
          # Trade-off: --pretty flag is ignored for compressed files (outputs raw CSV)
          if compressed_file?(file) && can_bypass_dataframe?(options)
            page_compressed_file_directly(file, options)
          else
            df = read_compressed_full(file, options: options) if use_shell_optimization
            df = collect_if_lazy(df)
            use_pretty = options[:pretty] != false
            output_to_pager(df, pretty: use_pretty)
          end
          nil
        else
          df = read_compressed_full(file, options: options) if use_shell_optimization
          output_dataframe(df, file, options, default_pretty: false)
        end
      end

      def execute_count(df, options)
        count = df.is_a?(Polars::LazyFrame) ? df.collect.height : df.height
        if options[:output]
          File.write(options[:output], "#{count}\n")
          nil
        else
          "#{count}"
        end
      end

      def can_bypass_dataframe?(options)
        options[:sort_keys]&.empty? != false &&
          !options[:where] &&
          !options[:select] &&
          !options[:drop] &&
          !options[:unique] &&
          !options[:unique_on]
      end

      def page_compressed_file_directly(file, options)
        require 'shellwords'

        decompressor = case file
                       when /\.gz$/i then "gzip -dc"
                       when /\.zst$/i then "zstd -dc"
                       end

        # Fork and exec to let less properly control the terminal
        pid = fork do
          exec("bash", "-c", "#{decompressor} #{Shellwords.escape(file)} | less -SRX")
        end
        Process.wait(pid)
      rescue Errno::ENOENT => e
        raise Error, "Decompression tool not found: #{e.message}. Please install gzip or zstd."
      end

      def collect_if_lazy(df)
        df.is_a?(Polars::LazyFrame) ? df.collect : df
      end

      def compressed_file?(file)
        file.match?(/\.(gz|zst)$/i)
      end

      def should_use_shell_decompression?(file, command, options)
        return false unless compressed_file?(file)

        # Shell-based decompression is 2-100x faster and interruptible (Polars blocks in Rust)
        return false unless [:cat, :head, :tail, :less].include?(command)
        return false if options[:sort_keys]&.any?
        return false if options[:unique] || options[:unique_on]
        return false if options[:where]
        return false if options[:streaming]

        true
      end

      def detect_format_without_compression(file)
        file.sub(/\.(gz|zst)$/i, '').then { |base| Formats.detect_format(base) }
      end

      def read_compressed_full(file, options:)
        require 'shellwords'

        decompressor = case file
                       when /\.gz$/i then "gzip -dc"
                       when /\.zst$/i then "zstd -dc"
                       end

        cmd = "#{decompressor} #{Shellwords.escape(file)}"

        IO.popen(cmd) do |io|
          df = Formats.read_from_io(io,
            format: detect_format_without_compression(file),
            delimiter: options[:delimiter],
            has_header: !options[:no_header],
          )

          df = Operations.select(df, columns: options[:select]) if options[:select]
          df = Operations.drop(df, columns: options[:drop]) if options[:drop]

          df
        end
      rescue Interrupt
        raise
      rescue Errno::ENOENT => e
        raise Error, "Decompression tool not found: #{e.message}. Please install gzip or zstd."
      end

      def read_compressed_partial(file, command:, limit:, options:)
        require 'shellwords'
        require 'tempfile'

        decompressor = case file
                       when /\.gz$/i then "gzip -dc"
                       when /\.zst$/i then "zstd -dc"
                       end

        case command
        when :head
          # +100 buffer handles edge cases like CSV with embedded newlines
          cmd = "#{decompressor} #{Shellwords.escape(file)} | head -n #{limit + 1 + 100}"

          IO.popen(cmd) do |io|
            df = Formats.read_from_io(io,
              format: detect_format_without_compression(file),
              delimiter: options[:delimiter],
              has_header: !options[:no_header],
            )

            df = Operations.select(df, columns: options[:select]) if options[:select]
            df = Operations.drop(df, columns: options[:drop]) if options[:drop]
            df.head(limit)
          end

        when :tail
          has_header = !options[:no_header]

          cmd = if has_header
                  "bash -c '#{decompressor} #{Shellwords.escape(file)} | (read -r header; echo \"$header\"; tail -n #{limit})'"
                else
                  "#{decompressor} #{Shellwords.escape(file)} | tail -n #{limit}"
                end

          IO.popen(cmd) do |io|
            df = Formats.read_from_io(io,
              format: detect_format_without_compression(file),
              delimiter: options[:delimiter],
              has_header: has_header,
            )

            df = Operations.select(df, columns: options[:select]) if options[:select]
            df = Operations.drop(df, columns: options[:drop]) if options[:drop]
            df.tail(limit)
          end
        end
      rescue Interrupt
        raise
      rescue Errno::ENOENT => e
        raise Error, "Decompression tool not found: #{e.message}. Please install gzip or zstd."
      end

      def build_parser(args)
        OptionParser.new do |opts|
          opts.banner = "Usage: tt [COMMAND] [OPTIONS] <file>"
          opts.separator ""
          opts.separator "Supported Formats:"
          opts.separator "  CSV        .csv, .txt"
          opts.separator "  TSV        .tsv"
          opts.separator "  Parquet    .parquet, .pq"
          opts.separator "  JSON       .json"
          opts.separator "  JSONL      .jsonl, .ndjson"
          opts.separator ""
          opts.separator "Compressed Files:"
          opts.separator "  Zstandard  .zst"
          opts.separator "  Gzip       .gz"
          opts.separator ""
          opts.separator "Commands:"
          opts.separator "  cat              Pass-through (default)"
          opts.separator "  head [N]         Show first N rows (default: 10)"
          opts.separator "  tail [N]         Show last N rows (default: 10)"
          opts.separator "  sample [N]       Random sample: N rows or 0.N ratio (e.g., 0.1 = 10% of rows)"
          opts.separator "  less             Page through data interactively"
          opts.separator "  lint             Data quality checks"
          opts.separator "  stats            Statistics"
          opts.separator "  count            Row count only"
          opts.separator ""
          opts.separator "Transformation Options:"

          opts.on("-k", "--key COLUMN", "Sort by column (repeatable)") do |col|
            args[:sort_keys] << col
          end

          opts.on("-r", "--reverse", "Sort in descending order") do
            args[:reverse] = true
          end

          opts.on("-i", "--ignore-case", "Case-insensitive sort") do
            args[:ignore_case] = true
          end

          # Filtering
          opts.on("--where EXPRESSION", "Filter rows by expression") do |expr|
            args[:where] = expr
          end

          # Column selection
          opts.on("--select COLUMNS", "--only COLUMNS", "Select specific columns (comma-separated)") do |cols|
            args[:select] = cols.split(",").map(&:strip)
          end

          opts.on("--drop COLUMNS", "Drop specific columns (comma-separated)") do |cols|
            args[:drop] = cols.split(",").map(&:strip)
          end

          # Deduplication
          opts.on("--unique", "Remove duplicate rows") do
            args[:unique] = true
          end

          opts.on("--unique-on COLUMNS", "Remove duplicates based on columns") do |cols|
            args[:unique_on] = cols.split(",").map(&:strip)
          end

          opts.on("--check-unique COLUMNS", "Check columns for duplicate values (lint command only)") do |cols|
            if cols == "*"
              args[:check_unique_columns] = :all
            else
              args[:check_unique_columns] = cols.split(",").map(&:strip)
            end
          end

          opts.separator ""
          opts.separator "I/O Options:"

          opts.on("-o", "--output FILE", "Output file (format detected by extension)") do |file|
            args[:output] = file
          end

          opts.on("--in-place", "Modify input file") do
            args[:in_place] = true
          end

          opts.on("-d", "--delimiter CHAR", "Input delimiter (auto: comma for CSV, tab for TSV)") do |delim|
            args[:delimiter] = delim
          end

          opts.on("--output-delimiter CHAR", "Output delimiter (default: same as input)") do |delim|
            args[:output_delimiter] = delim
          end

          opts.on("--no-header", "Treat input as headerless (use column_1, column_2, etc.)") do
            args[:no_header] = true
          end

          opts.on("--streaming", "Force streaming mode (for large files)") do
            args[:streaming] = true
          end

          opts.on("--no-streaming", "Disable streaming mode") do
            args[:streaming] = false
          end

          opts.separator ""
          opts.separator "Display Options:"

          opts.on("--pretty", "Force pretty table output") do
            args[:pretty] = true
          end

          opts.on("--no-pretty", "Force raw output (even on TTY)") do
            args[:pretty] = false
          end

          opts.separator ""
          opts.separator "Parquet Options:"

          opts.on("-c", "--compression CODEC", "Compression: snappy|gzip|zstd|lz4|brotli|uncompressed (default: zstd)") do |codec|
            args[:compression] = codec
          end

          opts.separator ""

          opts.on("-h", "--help", "Show help") do
            puts opts, ?\n
            exit
          end

          opts.on("--version", "Show version") do
            puts TabularTool::VERSION
            exit
          end

          opts.separator ""
          gemspec = Gem.loaded_specs["tabular_tool"]
          homepage = gemspec&.homepage || "https://github.com/wtn/tabular_tool"
          opts.separator "Project: #{homepage}"
        end
      end

      def apply_transformations(df, options)
        df = Operations::Filter.call(df, expression: options[:where]) if options[:where]

        df = Operations.select(df, columns: options[:select]) if options[:select]
        df = Operations.drop(df, columns: options[:drop]) if options[:drop]

        # Unique (requires DataFrame, not LazyFrame)
        if options[:unique] || options[:unique_on]
          df = collect_if_lazy(df)

          if options[:unique]
            df = Operations.unique(df)
          elsif options[:unique_on]
            df = Operations.unique(df, columns: options[:unique_on])
          end
        end

        if options[:sort_keys] && options[:sort_keys].any?
          df = Operations::Sort.call(
            df,
            keys: options[:sort_keys],
            reverse: options[:reverse],
            ignore_case: options[:ignore_case],
          )
        end

        df
      end

      def output_dataframe(df, file, options, default_pretty: false)
        output_file = options[:in_place] ? file : options[:output]

        if output_file
          # Formats.write now handles LazyFrames efficiently using sink methods
          Formats.write(
            df,
            output_file,
            delimiter: options[:output_delimiter],
            compression: options[:compression],
          )
          nil
        else
          # For stdout output, we need to collect the LazyFrame
          df = collect_if_lazy(df)
          should_pretty = options[:pretty].nil? ? default_pretty && $stdout.tty? : options[:pretty]

          if should_pretty
            df.to_s
          else
            # CSV is most universal format for stdout (works for all input types including Parquet)
            Formats.write_to_stdout(df, format: :csv, delimiter: options[:output_delimiter])
          end
        end
      end

      def output_to_pager(df, pretty: true)
        original_rows = nil

        content = if pretty
                    # Show all rows with box-drawing format (not truncated summary)
                    original_rows = Polars::Config.set_tbl_rows(-1)
                    result = df.to_s
                    Polars::Config.set_tbl_rows(original_rows)
                    result
                  else
                    require 'stringio'
                    sio = StringIO.new
                    df.write_csv(sio)
                    sio.string
                  end

        IO.popen("less -SRX", "w") { |io| io.write(content) }
      rescue Errno::EPIPE
        nil
      rescue Errno::ENOENT
        warn "Warning: 'less' command not found. Install less for pagination."
        puts content
      ensure
        Polars::Config.set_tbl_rows(original_rows) if original_rows
      end

      def execute_lint(df, options)
        unique_cols = options[:check_unique_columns]

        result = Operations::Lint.call(
          df,
          unique_columns: unique_cols,
        )

        output = []
        output << "* Row count: #{result[:row_count]}"
        output << "* Column count: #{result[:column_count]}"
        output << ""

        if result[:passed] && result[:warnings].empty?
          output << "✓ All checks passed:"
          output << "  - No blank values"
          output << "  - No duplicate rows"
          output << "  - No whitespace issues"
          if unique_cols
            cols_desc = unique_cols == :all ? "all columns" : unique_cols.join(", ")
            output << "  - No duplicate values in #{cols_desc}"
          end
        else
          output << "Issues found:"
          output << ""

          result[:errors].each do |error|
            output << format_lint_issue("✗", error, df)
            output << ""
          end

          result[:warnings].each do |warning|
            output << format_lint_issue("⚠", warning, df)
            output << ""
          end
        end

        output.join("\n") + "\n"
      end

      def execute_stats(df, options)
        result = Operations::Stats.call(df, columns: options[:select])
        result[:stats].to_s
      end

      def format_lint_issue(symbol, issue, df)
        lines = issue[:lines] || []

        message = case issue[:type]
        when :blank_values
          "#{symbol} Column '#{issue[:column]}': #{issue[:count]} blank/null values\n  Lines: #{lines.join(', ')}"
        when :duplicate_rows
          "#{symbol} #{issue[:count]} duplicate rows found\n  Lines: #{lines.join(', ')}"
        when :duplicate_column_values
          values_str = issue[:values].map { |v| v.nil? ? "(null)" : v.inspect }.join(", ")
          "#{symbol} Column '#{issue[:column]}': #{issue[:count]} duplicate values\n  Values: #{values_str}\n  Lines: #{lines.join(', ')}"
        when :whitespace
          "#{symbol} Column '#{issue[:column]}': trailing/leading whitespace in #{issue[:count]} values\n  Lines: #{lines.join(', ')}"
        else
          "#{symbol} #{issue[:type]}: #{issue[:message]}"
        end

        # Add row data for issues with line numbers (limit to first 5 to avoid overwhelming output)
        if lines.any?
          display_lines = lines.take(5)
          message += "\n"
          display_lines.each do |line_num|
            row_index = line_num - 2  # Convert file line number to 0-based row index (accounting for header)
            if row_index >= 0 && row_index < df.height
              row = df.slice(row_index, 1)
              row_data = df.columns.map { |col| "#{col}=#{row[col][0].inspect}" }.join(", ")
              message += "\n  Row #{line_num}: #{row_data}"
            end
          end
          if lines.length > 5
            message += "\n  ... and #{lines.length - 5} more rows"
          end
        end

        message
      end
    end
  end
end

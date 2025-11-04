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

        # Handle commands first
        if argv.first && !argv.first.start_with?("-")
          first_arg = argv.first

          case first_arg
          when "cat", "head", "tail", "sample", "less", "lint", "stats", "count"
            args[:command] = argv.shift.to_sym

            # Handle numeric argument for head/tail/sample
            if [:head, :tail].include?(args[:command]) && argv.first && argv.first.match?(/^\d+$/)
              args[:limit] = argv.shift.to_i
            elsif args[:command] == :sample
              if argv.first && argv.first.match?(/^\.?\d+\.?\d*$/)
                # Validate that the argument is numeric (supports .7 or 0.7)
                value = argv.shift
                num = value.to_f

                # Determine if it's a ratio or row count
                # Decimals < 1.0 are ratios (0.1 = 10% of rows), >= 1.0 are row counts (truncated)
                if num < 1.0
                  args[:sample_fraction] = num
                else
                  args[:sample_n] = num.to_i  # Truncate decimals like 12.8 → 12
                end
              else
                # Default to 10 rows like head/tail
                args[:sample_n] = 10
              end
            end
          else
            # Not a known command. Check if this is a typo or just a file path.
            # If it looks like a file path (contains . or /), treat as file
            # Otherwise, if multiple args remain, it's probably a command typo
            if argv.length > 1 && !first_arg.include?(".") && !first_arg.include?("/")
              raise Error, "Unknown command: #{first_arg}. Valid commands: cat, head, tail, sample, less, lint, stats, count"
            end
            # Leave the file path in argv to be processed later
          end
        end

        # Parse remaining arguments
        parser.parse!(argv)

        # Special handling for lint --unique COLUMNS
        # After parsing, if command is lint and --unique was set,
        # check if there's a non-file argument (COLUMNS)
        if args[:command] == :lint && args[:unique]
          if argv.length > 1
            # The last argv is the file, anything before that is the columns argument
            args[:file] = argv.pop
            columns_arg = argv.pop
            if columns_arg == "*"
              args[:unique_columns] = :all
            else
              args[:unique_columns] = columns_arg.split(",").map(&:strip)
            end
          else
            # No columns argument provided, default to all columns
            args[:file] = argv.pop if argv.any?
            args[:unique_columns] = :all
          end
          args[:unique] = false  # Don't apply as transformation
        else
          # Last argument should be the file
          args[:file] = argv.pop if argv.any?
        end

        # After parsing, argv should be empty - all args should have been consumed
        if argv.any?
          raise Error, "Unexpected argument(s): #{argv.join(' ')}"
        end

        args
      end

      def execute(command:, file: nil, **options)
        # Validate file argument
        raise Error, "No input file specified" unless file
        raise Error, "File not found: #{file}" unless File.exist?(file)

        # Check if we can use shell decompression optimization
        # If so, skip the full file read and handle it in the command execution
        use_shell_optimization = should_use_shell_decompression?(file, command, options)

        df = nil
        unless use_shell_optimization
          # Read the file
          df = Formats.read(
            file,
            delimiter: options[:delimiter],
            has_header: !options[:no_header],
            streaming: options[:streaming],
          )

          # For inspection commands (lint, stats, count), --unique has special meaning
          # Don't apply it as a transformation
          inspection_commands = [:lint, :stats, :count]
          if inspection_commands.include?(command)
            # Apply transformations but skip --unique (it's a lint option)
            df = apply_transformations(df, options.merge(unique: false, unique_columns: nil))
          else
            # Apply all transformations including --unique
            df = apply_transformations(df, options)
          end
        end

        # Execute command
        result = case command
        when :cat
          if use_shell_optimization
            df = read_compressed_full(file, options: options)
          end
          output_dataframe(df, file, options, default_pretty: true)
        when :head
          if use_shell_optimization
            df = read_compressed_partial(file, command: :head, limit: options[:limit] || 10, options: options)
          else
            df = Operations.head(df, n: options[:limit] || 10)
          end
          output_dataframe(df, file, options, default_pretty: true)
        when :tail
          if use_shell_optimization
            df = read_compressed_partial(file, command: :tail, limit: options[:limit] || 10, options: options)
          else
            # tail requires full data, collect if lazy
            df = collect_if_lazy(df)
            df = Operations.tail(df, n: options[:limit] || 10)
          end
          output_dataframe(df, file, options, default_pretty: true)
        when :sample
          # sample requires full data, collect if lazy
          df = collect_if_lazy(df)
          if options[:sample_fraction]
            df = Operations.sample(df, fraction: options[:sample_fraction])
          else
            df = Operations.sample(df, n: options[:sample_n])
          end
          output_dataframe(df, file, options, default_pretty: true)
        when :less
          # less pages through data interactively
          if use_shell_optimization
            df = read_compressed_full(file, options: options)
          end

          # Check if we're writing to a file
          output_file = options[:in_place] ? file : options[:output]

          if output_file
            # Writing to file - use normal file output, no pagination
            df = collect_if_lazy(df)
            Formats.write(
              df,
              output_file,
              delimiter: options[:output_delimiter],
              compression: options[:compression],
            )
            nil
          elsif $stdout.tty?
            # On TTY: ALWAYS paginate (that's why they used 'less')
            # The --pretty flag controls the format being paginated
            df = collect_if_lazy(df)
            use_pretty = options[:pretty] != false  # Default to pretty unless --no-pretty
            output_to_pager(df, pretty: use_pretty)
            nil
          else
            # Not a TTY (piped output) - behave like cat for pipeline compatibility
            output_dataframe(df, file, options, default_pretty: false)
          end
        when :lint
          # lint needs full data
          df = collect_if_lazy(df)
          execute_lint(df, options)
        when :stats
          # stats needs full data
          df = collect_if_lazy(df)
          execute_stats(df, options)
        when :count
          # count can work on lazy frames
          count = df.is_a?(Polars::LazyFrame) ? df.collect.height : df.height
          if options[:in_place]
            File.write(file, "#{count}\n")
            nil
          elsif options[:output]
            File.write(options[:output], "#{count}\n")
            nil
          else
            "#{count}"
          end
        else
          raise Error, "Unknown command: #{command}"
        end

        result
      rescue Interrupt
        # Clean interrupt handling - don't try to output partial results
        raise
      rescue TypeError => e
        # TypeErrors during interrupt likely mean interrupted IO left nil values
        # Check if this might be interrupt-related
        if e.message.include?("no implicit conversion of nil")
          raise Interrupt
        else
          raise
        end
      end

      private

      def collect_if_lazy(df)
        # Collect LazyFrame to DataFrame if needed
        df.is_a?(Polars::LazyFrame) ? df.collect : df
      end

      def compressed_file?(file)
        file.match?(/\.(gz|zst)$/i)
      end

      def should_use_shell_decompression?(file, command, options)
        # Only for compressed files
        return false unless compressed_file?(file)

        # For cat, head, and tail commands
        # Shell-based decompression is 2-100x faster and interruptible (Polars blocks in Rust)
        return false unless [:cat, :head, :tail].include?(command)

        # Not if we have complex operations that need full DataFrame
        return false if options[:sort_keys]&.any?
        return false if options[:unique] || options[:unique_on]
        return false if options[:where]  # Phase 1: skip filters

        # Not if streaming mode is explicitly set
        return false if options[:streaming]

        # Simple operations are OK
        # --select, --drop can be applied after decompression
        true
      end

      def detect_format_without_compression(file)
        # Strip .gz, .zst and detect underlying format
        base = file.sub(/\.(gz|zst)$/i, '')
        Formats.detect_format(base)
      end

      def read_compressed_full(file, options:)
        require 'shellwords'

        decompressor = case file
                       when /\.gz$/i then "gzip -dc"
                       when /\.zst$/i then "zstd -dc"
                       end

        # Decompress entire file through shell pipe (interruptible)
        cmd = "#{decompressor} #{Shellwords.escape(file)}"

        # Read from pipe into Polars
        IO.popen(cmd) do |io|
          df = Formats.read_from_io(io,
            format: detect_format_without_compression(file),
            delimiter: options[:delimiter],
            has_header: !options[:no_header],
          )

          # Apply simple column operations
          df = Operations.select(df, columns: options[:select]) if options[:select]
          df = Operations.drop(df, columns: options[:drop]) if options[:drop]

          df
        end
      rescue Interrupt
        # Re-raise interrupt so it propagates up
        raise
      rescue Errno::ENOENT => e
        # gzip/zstd not installed, fall back to Polars
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
          # Decompress first N+1 lines (header + N rows)
          # Use N+1+buffer to handle edge cases like CSV with embedded newlines
          cmd = "#{decompressor} #{Shellwords.escape(file)} | head -n #{limit + 1 + 100}"

          # Read from pipe into Polars
          IO.popen(cmd) do |io|
            df = Formats.read_from_io(io,
              format: detect_format_without_compression(file),
              delimiter: options[:delimiter],
              has_header: !options[:no_header],
            )

            # Apply simple column operations
            df = Operations.select(df, columns: options[:select]) if options[:select]
            df = Operations.drop(df, columns: options[:drop]) if options[:drop]

            # Ensure we have exactly N rows (head may give us buffer)
            df.head(limit)
          end

        when :tail
          # For tail, we need header + last N rows
          # Use bash to read header, then tail for last N rows
          # bash -c '...' allows us to use read and other shell features
          has_header = !options[:no_header]

          if has_header
            # With header: save first line, then tail -n N for data rows
            cmd = "bash -c '#{decompressor} #{Shellwords.escape(file)} | (read -r header; echo \"$header\"; tail -n #{limit})'"
          else
            # Without header: just tail -n N
            cmd = "#{decompressor} #{Shellwords.escape(file)} | tail -n #{limit}"
          end

          # Read from pipe into Polars
          IO.popen(cmd) do |io|
            df = Formats.read_from_io(io,
              format: detect_format_without_compression(file),
              delimiter: options[:delimiter],
              has_header: has_header,
            )

            # Apply simple column operations
            df = Operations.select(df, columns: options[:select]) if options[:select]
            df = Operations.drop(df, columns: options[:drop]) if options[:drop]

            # Ensure we have exactly N rows
            df.tail(limit)
          end
        end
      rescue Interrupt
        # Re-raise interrupt so it propagates up
        raise
      rescue Errno::ENOENT => e
        # gzip/zstd not installed, fall back to Polars
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

          # Sorting
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

          opts.separator ""
          opts.separator "I/O Options:"

          # I/O
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

          # Display
          opts.on("--pretty", "Force pretty table output") do
            args[:pretty] = true
          end

          opts.on("--no-pretty", "Force raw output (even on TTY)") do
            args[:pretty] = false
          end

          opts.separator ""
          opts.separator "Parquet Options:"

          # Parquet options
          opts.on("-c", "--compression CODEC", "Compression: snappy|gzip|zstd|lz4|brotli|uncompressed (default: zstd)") do |codec|
            args[:compression] = codec
          end

          opts.separator ""

          # Help
          opts.on("-h", "--help", "Show help") do
            puts opts, ?\n
            exit
          end

          opts.on("--version", "Show version") do
            puts TabularTool::VERSION
            exit
          end

          opts.separator ""
          # Read homepage from gemspec if available
          gemspec = Gem.loaded_specs["tabular_tool"]
          homepage = gemspec&.homepage || "https://github.com/wtn/tabular_tool"
          opts.separator "Project: #{homepage}"
        end
      end

      def apply_transformations(df, options)
        # 1. Filter
        df = Operations::Filter.call(df, expression: options[:where]) if options[:where]

        # 2. Select/Drop
        df = Operations.select(df, columns: options[:select]) if options[:select]
        df = Operations.drop(df, columns: options[:drop]) if options[:drop]

        # 3. Unique (requires DataFrame, not LazyFrame)
        if options[:unique] || options[:unique_columns] || options[:unique_on]
          # Collect LazyFrame before unique operation
          df = collect_if_lazy(df)

          if options[:unique]
            df = Operations.unique(df)
          elsif options[:unique_columns]
            # --unique COLUMNS in transformation mode (not lint)
            df = Operations.unique(df, columns: options[:unique_columns])
          elsif options[:unique_on]
            df = Operations.unique(df, columns: options[:unique_on])
          end
        end

        # 4. Sort
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
        # Collect LazyFrame before output
        df = collect_if_lazy(df)

        # Determine output destination
        output_file = if options[:in_place]
                        file
                      else
                        options[:output]
                      end

        if output_file
          # Write to file
          Formats.write(
            df,
            output_file,
            delimiter: options[:output_delimiter],
            compression: options[:compression],
          )
          nil
        else
          # Write to stdout
          should_pretty = options[:pretty].nil? ? default_pretty && $stdout.tty? : options[:pretty]

          if should_pretty
            df.to_s
          else
            # Output as CSV for stdout (most universal format, works for all input types including Parquet)
            format = :csv
            Formats.write_to_stdout(df, format: format, delimiter: options[:output_delimiter])
          end
        end
      end

      def output_to_pager(df, pretty: true)
        # Generate content based on format preference
        original_rows = nil

        if pretty
          # Configure Polars to show ALL rows (not truncated)
          # Use the same beautiful box-drawing format as head/tail/sample
          original_rows = Polars::Config.set_tbl_rows(-1)
          content = df.to_s
          Polars::Config.set_tbl_rows(original_rows)
        else
          # Output raw CSV for easier searching/grepping within less
          require 'stringio'
          sio = StringIO.new
          df.write_csv(sio)
          content = sio.string
        end

        # Use less with sensible defaults:
        # -S: chop long lines (enable horizontal scrolling for wide tables)
        # -R: allow ANSI color codes (for box-drawing characters)
        # -X: don't clear screen on exit
        IO.popen("less -SRX", "w") do |io|
          io.write(content)
        end
      rescue Errno::EPIPE
        # User quit before content finished - that's OK
        nil
      rescue Errno::ENOENT
        # less not found, fall back to stdout
        warn "Warning: 'less' command not found. Install less for pagination."
        puts content
      ensure
        # Make sure we restore the setting even if there's an error
        Polars::Config.set_tbl_rows(original_rows) if original_rows
      end

      def execute_lint(df, options)
        # In lint mode, --unique COLUMNS means check those columns for duplicates
        unique_cols = options[:unique_columns] || options[:unique_on]

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

        # Build the main issue message
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

# frozen_string_literal: true

module TabularTool
  module Operations
    module Lint
      def self.call(df, unique_columns: nil)
        errors = []
        warnings = []

        check_blank_values(df, warnings)
        check_duplicate_rows(df, errors)
        check_whitespace(df, warnings)

        if unique_columns
          if unique_columns == :all
            df.columns.each do |col|
              check_duplicate_column_values(df, col, errors)
            end
          else
            Array(unique_columns).each do |col|
              check_duplicate_column_values(df, col, errors)
            end
          end
        end

        {
          passed: errors.empty?,
          row_count: df.height,
          column_count: df.width,
          errors: errors,
          warnings: warnings,
        }
      end

      private

      def self.check_blank_values(df, warnings)
        df.columns.each do |col|
          null_indices = df.with_row_index.filter(Polars.col(col).is_null).select(["index"])["index"].to_a

          if null_indices.any?
            warnings << {
              type: :blank_values,
              column: col,
              count: null_indices.length,
              lines: null_indices.map { |i| i + 2 },  # +2 because index is 0-based and we need to account for header line
            }
          end
        end
      end

      def self.check_duplicate_rows(df, errors)
        original_height = df.height
        df_with_index = df.with_row_index

        if original_height == 0
          return
        end

        grouped = df_with_index.group_by(df.columns).agg(
          Polars.col("index").alias("indices"),
        )

        duplicates = grouped.filter(Polars.col("indices").list.len.gt(1))

        if duplicates.height > 0
          all_dup_indices = []
          duplicates["indices"].to_a.each do |indices_list|
            all_dup_indices.concat(indices_list)
          end

          # Convert to line numbers (sorted)
          lines = all_dup_indices.map { |i| i + 2 }.sort  # +2 to account for 0-based index and header line

          errors << {
            type: :duplicate_rows,
            count: all_dup_indices.length - duplicates.height,  # Number of duplicate rows (not counting first occurrence)
            lines: lines,
          }
        end
      end

      def self.check_whitespace(df, warnings)
        df.columns.each do |col|
          begin
            df_indexed = df.with_row_index

            ws_rows = df_indexed.filter(
              Polars.col(col).is_not_null &
              (Polars.col(col).cast(Polars::String).str.strip_chars != Polars.col(col).cast(Polars::String)),
            )

            if ws_rows.height > 0
              lines = ws_rows["index"].to_a.map { |i| i + 2 }  # +2 to account for 0-based index and header line
              warnings << {
                type: :whitespace,
                column: col,
                count: lines.length,
                lines: lines,
              }
            end
          rescue Polars::Error
          end
        end
      end

      def self.check_duplicate_column_values(df, col, errors)
        df_indexed = df.with_row_index

        # Excluding nulls (they're reported separately as blank values)
        value_counts = df_indexed.filter(Polars.col(col).is_not_null).group_by(col).agg(
          Polars.col("index").count.alias("count"),
        )
        duplicates = value_counts.filter(Polars.col("count").gt(1))

        if duplicates.height > 0
          dup_values = duplicates[col].to_a

          lines = []
          dup_values.each do |val|
            indices = df_indexed.filter(Polars.col(col).eq(val))["index"].to_a
            lines.concat(indices.map { |i| i + 2 })  # +2 to account for 0-based index and header line
          end

          errors << {
            type: :duplicate_column_values,
            column: col,
            count: duplicates["count"].sum - duplicates.height,
            lines: lines.sort,
            values: dup_values,
          }
        end
      end
    end
  end
end

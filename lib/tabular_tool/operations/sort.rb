# frozen_string_literal: true

module TabularTool
  module Operations
    module Sort
      def self.call(df, keys:, reverse: false, ignore_case: false)
        # Convert keys to array if single string
        keys = Array(keys)

        # For case-insensitive sort, we need to sort by lowercase version
        if ignore_case
          # Create a copy with temporary lowercase columns for sorting
          df_with_lower = df.clone

          keys.each do |key|
            # Try to add a lowercase version of string columns
            # If it's not a string column, this will fail silently
            begin
              df_with_lower = df_with_lower.with_columns(
                Polars.col(key).cast(Polars::String).str.to_lowercase.alias("__#{key}_lower__"),
              )
            rescue Polars::Error
              # Column is not a string, skip case-insensitive for this key
            end
          end

          # Build sort keys: use lowercase versions where available, original otherwise
          sort_keys = keys.map do |key|
            df_with_lower.columns.include?("__#{key}_lower__") ? "__#{key}_lower__" : key
          end

          # Sort and then drop the temporary columns
          sorted = if reverse
                     df_with_lower.sort(sort_keys, reverse: true)
                   else
                     df_with_lower.sort(sort_keys)
                   end
          temp_cols = df_with_lower.columns.select { |c| c.start_with?("__") && c.end_with?("_lower__") }
          sorted.drop(temp_cols)
        else
          # Regular sort
          if reverse
            df.sort(keys, reverse: true)
          else
            df.sort(keys)
          end
        end
      end
    end
  end
end

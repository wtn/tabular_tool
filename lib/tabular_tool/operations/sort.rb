# frozen_string_literal: true

module TabularTool
  module Operations
    module Sort
      def self.call(df, keys:, reverse: false, ignore_case: false)
        keys = Array(keys)

        if ignore_case
          df_with_lower = df.clone

          keys.each do |key|
            begin
              df_with_lower = df_with_lower.with_columns(
                Polars.col(key).cast(Polars::String).str.to_lowercase.alias("__#{key}_lower__"),
              )
            rescue Polars::Error
            end
          end

          sort_keys = keys.map do |key|
            df_with_lower.columns.include?("__#{key}_lower__") ? "__#{key}_lower__" : key
          end

          sorted = reverse ? df_with_lower.sort(sort_keys, descending: true) : df_with_lower.sort(sort_keys)
          temp_cols = df_with_lower.columns.select { |c| c.start_with?("__") && c.end_with?("_lower__") }
          sorted.drop(temp_cols)
        else
          if reverse
            df.sort(keys, descending: true)
          else
            df.sort(keys)
          end
        end
      end
    end
  end
end

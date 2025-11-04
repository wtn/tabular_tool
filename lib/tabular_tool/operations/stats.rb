# frozen_string_literal: true

module TabularTool
  module Operations
    module Stats
      def self.call(df, columns: nil)
        df = df.select(columns) if columns

        stats_df = df.describe

        {
          row_count: df.height,
          column_count: df.width,
          stats: stats_df,
        }
      end

      def self.count(df)
        df.height
      end
    end
  end
end

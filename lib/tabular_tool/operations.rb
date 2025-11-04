# frozen_string_literal: true

require_relative "operations/sort"
require_relative "operations/filter"
require_relative "operations/lint"
require_relative "operations/stats"

module TabularTool
  module Operations
    def self.select(df, columns:)
      df.select(columns)
    end

    def self.drop(df, columns:)
      df.drop(columns)
    end

    def self.unique(df, columns: nil)
      if columns
        df.unique(subset: columns, maintain_order: true)
      else
        df.unique(maintain_order: true)
      end
    end

    def self.head(df, n: 10)
      result = df.head(n)
      # If the input is a LazyFrame, head returns a LazyFrame, so we need to collect it
      result.is_a?(Polars::LazyFrame) ? result.collect : result
    end

    def self.tail(df, n: 10)
      result = df.tail(n)
      # If the input is a LazyFrame, tail returns a LazyFrame, so we need to collect it
      result.is_a?(Polars::LazyFrame) ? result.collect : result
    end

    def self.sample(df, n: nil, fraction: nil)
      # Handle lazy frames efficiently by sampling across the entire dataset
      if df.is_a?(Polars::LazyFrame)
        # Get total row count (fast operation)
        total_rows = df.select(Polars.len.alias("count")).collect["count"][0]

        # Calculate sample size
        sample_size = if fraction
          [(total_rows * fraction).round, total_rows].min
        elsif n
          [n, total_rows].min
        else
          raise Error, "Must specify either n or fraction for sample"
        end

        # Generate random indices across the entire range
        random_indices = (0...sample_size).map { rand(total_rows) }.sort

        # Fetch rows at random indices using slice
        samples = random_indices.map { |idx| df.slice(idx, 1).collect }

        # Combine all samples into one DataFrame
        return Polars.concat(samples)
      end

      # For DataFrames, use built-in sample method
      if fraction
        df.sample(fraction: fraction)
      elsif n
        df.sample(n: [n, df.height].min)
      else
        raise Error, "Must specify either n or fraction for sample"
      end
    end
  end
end

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
      df.head(n)
    end

    def self.tail(df, n: 10)
      df.tail(n)
    end

    def self.sample(df, n: nil, fraction: nil)
      if fraction
        df.sample(frac: fraction)
      elsif n
        df.sample(n: [n, df.height].min)
      else
        raise Error, "Must specify either n or fraction for sample"
      end
    end
  end
end

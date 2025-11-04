# frozen_string_literal: true

require_relative "dsl_to_sql"

module TabularTool
  module Operations
    module Filter
      def self.call(df, expression:)
        # Translate our DSL syntax to SQL, then use Polars' battle-tested SQL parser
        sql = DslToSql.new(expression).translate
        df.filter(Polars.sql_expr(sql))
      end
    end
  end
end

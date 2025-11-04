# frozen_string_literal: true

module TabularTool
  module Operations
    # Translates our DSL filter syntax to SQL WHERE clause syntax
    # Leverages Polars' battle-tested SQL parser instead of maintaining our own
    class DslToSql
      def initialize(expression)
        @expression = expression
      end

      def translate
        sql = @expression.dup

        sql = translate_quotes(sql)
        sql = translate_method_calls(sql)
        sql = translate_operators(sql)

        sql
      end

      private

      def translate_quotes(sql)
        # Convert double-quoted string literals to single-quoted (SQL standard)
        # This must be done before operator translation to avoid conflicts with ==
        sql.gsub(/"([^"]+)"/) { "'#{::Regexp.last_match(1)}'" }
      end

      def translate_method_calls(sql)
        sql = sql.gsub(/(\w+)\.contains\(['"]([^'"]+)['"]\)/) do
          column = ::Regexp.last_match(1)
          value = ::Regexp.last_match(2)
          "#{column} LIKE '%#{escape_like(value)}%'"
        end

        sql = sql.gsub(/(\w+)\.starts_with\(['"]([^'"]+)['"]\)/) do
          column = ::Regexp.last_match(1)
          value = ::Regexp.last_match(2)
          "#{column} LIKE '#{escape_like(value)}%'"
        end

        sql = sql.gsub(/(\w+)\.ends_with\(['"]([^'"]+)['"]\)/) do
          column = ::Regexp.last_match(1)
          value = ::Regexp.last_match(2)
          "#{column} LIKE '%#{escape_like(value)}'"
        end

        sql = sql.gsub(/(\w+)\.is_null\(\)/, '\1 IS NULL')
        sql = sql.gsub(/(\w+)\.is_not_null\(\)/, '\1 IS NOT NULL')

        sql
      end

      def translate_operators(sql)
        sql = sql.gsub("&&", " AND ")
        sql = sql.gsub("||", " OR ")
        sql = sql.gsub("!=", "<>")
        sql = sql.gsub("==", "=")
        sql = sql.gsub(/!\s*\(/, "NOT (")

        sql
      end

      def escape_like(value)
        value.gsub("%", "\\%").gsub("_", "\\_")
      end
    end
  end
end

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

        # First, convert double-quoted strings to single-quoted (SQL standard)
        sql = translate_quotes(sql)

        # Second, handle method calls (before operator replacements to avoid conflicts)
        sql = translate_method_calls(sql)

        # Finally, translate operators (in specific order to avoid conflicts)
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
        # .contains('value') -> LIKE '%value%'
        sql = sql.gsub(/(\w+)\.contains\(['"]([^'"]+)['"]\)/) do
          column = ::Regexp.last_match(1)
          value = ::Regexp.last_match(2)
          "#{column} LIKE '%#{escape_like(value)}%'"
        end

        # .starts_with('value') -> LIKE 'value%'
        sql = sql.gsub(/(\w+)\.starts_with\(['"]([^'"]+)['"]\)/) do
          column = ::Regexp.last_match(1)
          value = ::Regexp.last_match(2)
          "#{column} LIKE '#{escape_like(value)}%'"
        end

        # .ends_with('value') -> LIKE '%value'
        sql = sql.gsub(/(\w+)\.ends_with\(['"]([^'"]+)['"]\)/) do
          column = ::Regexp.last_match(1)
          value = ::Regexp.last_match(2)
          "#{column} LIKE '%#{escape_like(value)}'"
        end

        # .is_null() -> IS NULL
        sql = sql.gsub(/(\w+)\.is_null\(\)/, '\1 IS NULL')

        # .is_not_null() -> IS NOT NULL
        sql = sql.gsub(/(\w+)\.is_not_null\(\)/, '\1 IS NOT NULL')

        sql
      end

      def translate_operators(sql)
        # Replace logical operators (must be done before == to avoid conflict)
        sql = sql.gsub("&&", " AND ")
        sql = sql.gsub("||", " OR ")

        # Replace comparison operators
        # Handle != before == to avoid partial matches
        sql = sql.gsub("!=", "<>")
        sql = sql.gsub("==", "=")

        # Handle NOT operator (! followed by optional whitespace and opening paren)
        sql = sql.gsub(/!\s*\(/, "NOT (")

        sql
      end

      def escape_like(value)
        # Escape special LIKE characters
        value.gsub("%", "\\%").gsub("_", "\\_")
      end
    end
  end
end

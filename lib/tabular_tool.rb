# frozen_string_literal: true

require_relative "tabular_tool/version"
require_relative "tabular_tool/formats"
require_relative "tabular_tool/operations"
require_relative "tabular_tool/cli"

module TabularTool
  class Error < StandardError; end
end

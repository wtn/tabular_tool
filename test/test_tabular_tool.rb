# frozen_string_literal: true

require "test_helper"

class TestTabularTool < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::TabularTool::VERSION
  end

  def test_it_does_something_useful
    # Basic smoke test
    assert_respond_to TabularTool::Formats, :read
    assert_respond_to TabularTool::Operations, :select
    assert_respond_to TabularTool::CLI, :parse_args
  end
end

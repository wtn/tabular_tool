# frozen_string_literal: true

require_relative "lib/tabular_tool/version"

Gem::Specification.new do |spec|
  spec.name = "tabular_tool"
  spec.version = TabularTool::VERSION
  spec.authors = ["William T. Nelson"]
  spec.email = ["35801+wtn@users.noreply.github.com"]

  spec.summary = "Command-line tool for tabular data operations (CSV, TSV, Parquet, JSON, JSONL)"
  spec.description = "tt is a command-line tool powered by ruby-polars for working with tabular data. Supports sorting, filtering, linting, statistics, and format conversion."
  spec.homepage = "https://github.com/wtn/tabular_tool"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.0.0"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ Gemfile .gitignore test/])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "polars-df", "~> 0.22"
end

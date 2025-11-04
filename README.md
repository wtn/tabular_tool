# Tabular Tool

Command-line tool for working with tabular data files. Built on [ruby-polars](https://github.com/ankane/ruby-polars).

Supports: CSV, TSV, Parquet, JSON, JSONL (including .gz and .zst compressed files)

## Installation

```bash
gem install tabular_tool
```

## Usage

```bash
# Preview first 10 rows
tt head data.csv

# Sort by column and re-write file in-place
tt -k age --in-place data.csv

# Filter rows
tt --where "age > 30" data.csv

# Convert formats (Parquet uses zstd compression by default)
tt data.csv -o output.parquet

# Data quality checks (detects duplicates, blanks, whitespace)
tt lint --unique data.csv

# View all options
tt --help
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/wtn/tabular_tool.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

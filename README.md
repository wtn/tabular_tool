# Tabular Tool

Command-line tool for working with tabular data files. Built on [ruby-polars](https://github.com/ankane/ruby-polars).

Supports: CSV, TSV, Parquet, JSON, JSONL (including Gzip and Zstandard compressed files)

## Installation

```bash
gem install tabular_tool
```

## Usage

```bash
# Preview first/last rows
tt head data.csv.gz
tt tail data.parquet

# Sort by column and modify file in-place
tt -k age --in-place data.csv

# Filter rows with expressions
tt --where "age > 30" data.csv

# Convert formats (auto-detects by extension)
tt data.csv -o output.parquet

# Data quality checks
tt lint data.csv

# View all options
tt --help
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/wtn/tabular_tool.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

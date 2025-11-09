# Tabular Tool

CLI for tabular data files. Built on [Polars](https://github.com/pola-rs/polars).

Supports: CSV, Parquet, JSON, JSONL (including Gzip and Zstandard compressed files)

## Installation

```bash
cargo install tabular_tool
```

## Usage

```bash
# Preview first/last rows
tt head data.csv.gz
tt tail data.parquet

# Sort by column and modify file in-place
tt --key age --in-place data.csv

# Filter rows with expressions
tt --where "age > 30" data.csv

# Convert formats
tt data.csv --output output.parquet

# Data quality checks
tt lint data.csv

# View all options
tt --help
```

## Notes

- `tt file.csv` (bare, no transforms, no `-o`) is a zero-copy pass-through -- byte-identical
  to `cat(1)`, even on a terminal. For a pretty preview, use `tt head file.csv`.
- `.gz` / `.zst` files *are* decompressed when the tool has to parse them (filter, head,
  sort, stat, etc.) or when you write to `-o something.csv`. Only the bare `cat` fast
  path copies bytes verbatim -- and that path skips compressed files, routing them through
  Polars so the output is decompressed CSV.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/wtn/tabular_tool.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

# Tabular Tool

CLI for tabular data files. Built on [Polars](https://github.com/pola-rs/polars).

Supports: CSV, TSV, Parquet, JSON, JSONL (including Gzip and Zstandard compressed files)

## Installation

```bash
cargo install tabular-tool
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

# Convert formats
tt data.csv -o output.parquet

# Data quality checks
tt lint data.csv

# View all options
tt --help
```

## Development

### Building

```bash
# Development build (fast compilation, unoptimized)
cargo build

# Release build (optimized, slower compilation)
cargo build --release

# The binary will be at:
# - Development: target/debug/tt
# - Release: target/release/tt
```

### Running Tests

```bash
# Run all tests
cargo test

# Run tests with release optimizations
cargo test --release

# Run specific test
cargo test test_streaming_sink_parquet

# Run tests with output
cargo test -- --nocapture
```

### Memory Profiling

```bash
# Monitor memory usage during operations
/usr/bin/time -l target/release/tt cat large_file.parquet -o output.parquet

# Key metrics:
# - maximum resident set size: Peak RAM usage
# - peak memory footprint: Virtual memory (may include paging)
```

### Performance Tips

- Use `--select` to reduce columns when working with wide Parquet files (198+ columns)
- Type inference scans entire CSV files for accuracy (uses streaming internally)
- All operations use Polars' streaming engine for memory efficiency
- Default Parquet compression: zstd level 3 (good speed/size balance)

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/wtn/tabular_tool.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

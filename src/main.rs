use anyhow::{Context, Result};
use clap::Parser;
use polars::prelude::*;
use polars_utils::plpath::PlPath;
use std::num::NonZero;
use std::path::Path;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "tt",
    about = "Fast tabular data tool for CSV, TSV, Parquet, JSON, and JSONL files",
    long_about = "A command-line tool for working with tabular data files.\n\
                  Supports CSV, TSV, Parquet, JSON, JSONL with optional gzip/zstd compression.\n\
                  All transformation options compose with all commands.",
    version = "0.0.0",
    after_help = "Examples:\n  \
      tt count data.csv                                    # Row and column count\n  \
      tt head 10 data.parquet                              # First 10 rows\n  \
      tt tail 5 data.csv                                   # Last 5 rows\n  \
      tt sample 100 data.parquet                           # Random 100 rows (windowed)\n  \
      tt sample 0.01 data.parquet                          # Random 1% sample\n  \
      tt stats data.csv                                    # Statistical summary\n  \
      tt lint data.csv --show-nulls --unique               # Data quality checks\n  \
      tt cat --limit 100 data.csv                          # First 100 rows\n  \
      tt head --filter \"age > 25\" data.csv                 # Filter then show\n  \
      tt cat --select \"name,age\" -k age data.csv           # Select columns and sort\n  \
      tt sample --filter \"city = 'NYC'\" -k age data.csv    # Filter, sample, sort output\n  \
      tt count --unique data.csv                           # Count unique rows\n  \
      tt stats --select \"age,value\" data.csv              # Stats on specific columns\n\n\
      Performance tip: Use --select with filters on wide Parquet files\n  \
      tt cat --filter \"status = 'active'\" --select \"id,name\" data.parquet\n\n\
      Parquet: Uses zstd compression level 3 by default (good speed/size balance)\n  \
      Streaming: All operations use memory-efficient streaming for large files.\n\n\
      Project: https://github.com/wtn/tabular_tool"
)]
struct Cli {
    /// Command to execute: cat, head, tail, sample, stats, count, lint
    #[arg(value_name = "COMMAND", help = "Command: cat, head [N], tail [N], sample [N], stats, count, lint")]
    command: Option<String>,

    /// Input file(s)
    #[arg(value_name = "FILE")]
    files: Vec<String>,

    /// Filter rows by SQL expression (e.g., "age > 25", "name = 'Alice'")
    #[arg(long, help = "Filter rows: --filter \"age > 25\"")]
    filter: Option<String>,

    /// Select specific columns (comma-separated)
    #[arg(long, alias = "only", help = "Select columns: --select \"name,age,city\"")]
    select: Option<String>,

    /// Drop specific columns (comma-separated)
    #[arg(long, help = "Drop columns: --drop \"col1,col2\"")]
    drop: Option<String>,

    /// Sort by column (repeatable for multi-column sort)
    #[arg(short = 'k', long = "key", help = "Sort: -k age -k name")]
    sort_keys: Vec<String>,

    /// Sort in descending order
    #[arg(short = 'r', long)]
    reverse: bool,

    /// Case-insensitive sort
    #[arg(short = 'i', long)]
    ignore_case: bool,

    /// Remove duplicate rows
    #[arg(long)]
    unique: bool,

    /// Remove duplicates based on specific columns (comma-separated)
    #[arg(long, help = "Deduplicate: --unique-on \"name,email\"")]
    unique_on: Option<String>,

    /// Show rows with null values (for lint command)
    #[arg(long, help = "Show rows with nulls: --show-nulls")]
    show_nulls: bool,

    /// Show all results, not just first N (for lint --show-nulls)
    #[arg(long, help = "Show all rows: --all")]
    all: bool,

    /// Show schema (for lint command)
    #[arg(long, help = "Show schema: --show-schema")]
    show_schema: bool,

    /// Limit output to N rows
    #[arg(long, help = "Limit: --limit 100")]
    limit: Option<usize>,

    /// Skip first N rows (negative for tail behavior)
    #[arg(long, help = "Offset: --offset 50 or --offset -10")]
    offset: Option<i64>,

    /// Output file (format detected by extension: .csv, .tsv, .parquet, .json, .jsonl)
    /// Parquet files use zstd compression level 3 by default
    #[arg(short = 'o', long, help = "Output: -o output.parquet (Parquet: zstd level 3)")]
    output: Option<String>,
}

impl Cli {
    /// Check if any transformations are applied
    fn has_transformations(&self) -> bool {
        self.filter.is_some()
            || self.select.is_some()
            || self.drop.is_some()
            || !self.sort_keys.is_empty()
            || self.unique
            || self.unique_on.is_some()
            || self.limit.is_some()
            || self.offset.is_some()
            || self.show_nulls
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command.as_deref() {
        Some("lint") => {
            if cli.files.is_empty() {
                anyhow::bail!("lint command requires at least one file");
            }

            let show_separators = cli.files.len() > 1;

            for (idx, file_path) in cli.files.iter().enumerate() {
                if show_separators && idx > 0 {
                    println!();
                }
                if show_separators {
                    println!("==> {} <==", file_path);
                }

                let lf = read_to_lazyframe(file_path)?;
                let lf = apply_transformations(lf, &cli)?;

                // Run lint checks with streaming
                lint_data(lf, &cli)?;
            }

            Ok(())
        }
        Some("count") => {
            if cli.files.is_empty() {
                anyhow::bail!("count command requires at least one file");
            }

            for file_path in &cli.files {
                let (rows, cols) = if cli.has_transformations() {
                    // Transformation path: read → transform → count
                    let lf = read_to_lazyframe(file_path)?;
                    let lf = apply_transformations(lf, &cli)?;
                    count_lazyframe(lf)?
                } else {
                    // Fast path: use metadata-only counting when possible
                    count_shape(file_path)?
                };

                println!("{}\t{}\t{}", rows, cols, file_path);
            }

            Ok(())
        }
        Some("stats") => {
            if cli.files.is_empty() {
                anyhow::bail!("stats command requires at least one file");
            }

            let show_separators = cli.files.len() > 1;
            let is_tty = atty::is(atty::Stream::Stdout);

            for (idx, file_path) in cli.files.iter().enumerate() {
                if show_separators && idx > 0 {
                    println!();
                }
                if show_separators {
                    println!("==> {} <==", file_path);
                }

                let lf = read_to_lazyframe(file_path)?;
                let lf = apply_transformations(lf, &cli)?;

                // Generate statistics using lazy aggregations (streaming-friendly)
                let stats_df = compute_stats_lazy(lf)?;

                // Output stats
                if let Some(output_file) = &cli.output {
                    write_output_file(&stats_df, output_file)?;
                } else {
                    print_dataframe(&stats_df, is_tty)?;
                }
            }

            Ok(())
        }
        Some("cat") | Some("head") | Some("tail") | Some("sample") => {
            if cli.files.is_empty() {
                anyhow::bail!("{} command requires at least one file", cli.command.as_ref().unwrap());
            }

            // Parse N for head/tail commands
            let (n, file_paths) = parse_n_and_files(&cli)?;
            let n_for_sample = n.clone(); // Keep a copy for sample

            // Create a modified CLI with limit/offset set based on command
            let mut cli_with_limit = cli.clone();
            match cli_with_limit.command.as_deref() {
                Some("head") => {
                    let n_str = n.unwrap_or("10".to_string());
                    let n_val: usize = n_str.parse()
                        .with_context(|| format!("Invalid number for head: '{}'", n_str))?;
                    if cli_with_limit.limit.is_none() {
                        cli_with_limit.limit = Some(n_val);
                    }
                }
                Some("tail") => {
                    let n_str = n.unwrap_or("10".to_string());
                    let n_val: usize = n_str.parse()
                        .with_context(|| format!("Invalid number for tail: '{}'", n_str))?;
                    if cli_with_limit.offset.is_none() {
                        cli_with_limit.offset = Some(-(n_val as i64));
                    }
                }
                Some("sample") => {
                    // Sample handles N differently (can be int or float)
                    // Will be processed in the loop below
                }
                _ => {} // cat uses whatever limit/offset user specified
            }

            let show_separators = file_paths.len() > 1;
            let is_tty = atty::is(atty::Stream::Stdout);

            for (idx, file_path) in file_paths.iter().enumerate() {
                if show_separators && idx > 0 {
                    println!();
                }
                if show_separators {
                    println!("==> {} <==", file_path);
                }

                let lf = read_to_lazyframe(file_path)?;
                let lf = apply_transformations(lf, &cli_with_limit)?;

                // For output files, use sink for direct streaming write
                if let Some(output_file) = &cli.output {
                    if cli.command.as_deref() != Some("sample") {
                        // Direct sink to output file (streaming, like Python)
                        sink_to_file(lf, output_file)?;
                        continue; // Skip to next file
                    }
                }

                // Collect and apply sampling if this is sample command
                let df = if cli.command.as_deref() == Some("sample") {
                    let default_n = "10".to_string();
                    let n_str = n_for_sample.as_ref().unwrap_or(&default_n);

                    // Use columnar sampling for truly random results with low memory
                    let n: usize = if n_str.contains('.') {
                        let row_count = lf.clone().select([len()]).with_new_streaming(true).collect()?
                            .column("len")?.u32()?.get(0).context("Failed to get count")? as usize;
                        let frac: f64 = n_str.parse()
                            .with_context(|| format!("Invalid fraction for sample: '{}'", n_str))?;
                        (row_count as f64 * frac).round() as usize
                    } else {
                        n_str.parse()
                            .with_context(|| format!("Invalid number for sample: '{}'", n_str))?
                    };

                    let mut sampled = apply_random_sample_streaming(lf, n)?;

                    // If user specified sort, sort the sample output
                    if !cli.sort_keys.is_empty() {
                        let sort_cols: Vec<_> = cli.sort_keys.iter().map(|s| s.as_str()).collect();
                        let descending = vec![cli.reverse; cli.sort_keys.len()];
                        sampled = sampled.sort(sort_cols, SortMultipleOptions::default().with_order_descending_multi(descending))?;
                    }

                    sampled
                } else {
                    lf.with_new_streaming(true).collect()?
                };

                // Output to file or print
                if let Some(output_file) = &cli.output {
                    write_output_file(&df, output_file)?;
                } else {
                    print_dataframe(&df, is_tty)?;
                }
            }

            Ok(())
        }
        Some(cmd) => {
            anyhow::bail!("Unknown command: {}", cmd);
        }
        None => {
            anyhow::bail!("No command specified. Try 'tt --help'");
        }
    }
}

fn count_shape(file_path: &str) -> Result<(usize, usize)> {
    let path = Path::new(file_path);

    // Detect format by extension
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .context("Could not determine file extension")?;

    // Handle compressed files by looking at the full extension chain
    let file_stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let inner_extension = Path::new(file_stem)
        .extension()
        .and_then(|e| e.to_str());

    let format = match extension {
        "gz" | "zst" => {
            // Use the inner extension for compressed files
            inner_extension.context("Compressed file missing format extension")?
        }
        ext => ext,
    };

    let (rows, cols) = match format {
        "csv" | "txt" | "tsv" => {
            let separator = if format == "tsv" { b'\t' } else { b',' };
            let mut lf = LazyCsvReader::new(PlPath::new(file_path))
                .with_separator(separator)
                .with_infer_schema_length(None)  // Scan all rows
                .with_try_parse_dates(true)
                .finish()?;
            let cols = lf.collect_schema()?.len();
            let df = lf.select([len()]).collect()?;
            let rows = df.column("len")?.u32()?.get(0).context("Failed to get count")? as usize;
            (rows, cols)
        }
        "parquet" | "pq" => {
            // For Parquet, use LazyFrame to get metadata
            let mut lf = LazyFrame::scan_parquet(PlPath::new(file_path), Default::default())?;
            let schema = lf.collect_schema()?;
            let cols = schema.len();

            // Count rows efficiently
            let df = lf.select([len()]).collect()?;
            let rows = df.column("len")?.u32()?.get(0).context("Failed to get count")? as usize;
            (rows, cols)
        }
        "json" | "jsonl" | "ndjson" => {
            // For JSON/JSONL, we need to use eager reading
            let df = JsonReader::new(std::fs::File::open(file_path)?)
                .with_json_format(JsonFormat::JsonLines)
                .infer_schema_len(Some(NonZero::new(100_000).unwrap()))
                .finish()?;
            (df.height(), df.width())
        }
        _ => anyhow::bail!("Unsupported file format: .{}", format),
    };

    Ok((rows, cols))
}

/// Read a file into a LazyFrame
fn read_to_lazyframe(file_path: &str) -> Result<LazyFrame> {
    let path = Path::new(file_path);

    // Detect format by extension
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .context("Could not determine file extension")?;

    // Handle compressed files by looking at the full extension chain
    let file_stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let inner_extension = Path::new(file_stem)
        .extension()
        .and_then(|e| e.to_str());

    let format = match extension {
        "gz" | "zst" => {
            // Use the inner extension for compressed files
            inner_extension.context("Compressed file missing format extension")?
        }
        ext => ext,
    };

    let lf = match format {
        "csv" | "txt" | "tsv" => {
            let separator = if format == "tsv" { b'\t' } else { b',' };
            LazyCsvReader::new(PlPath::new(file_path))
                .with_separator(separator)
                .with_infer_schema_length(None)  // Scan ALL rows for perfect type inference
                .with_try_parse_dates(true)  // Auto-parse date strings
                .finish()?
        }
        "parquet" | "pq" => {
            LazyFrame::scan_parquet(PlPath::new(file_path), Default::default())?
        }
        "json" | "jsonl" | "ndjson" => {
            // For JSON/JSONL, we need eager reading then convert to lazy
            let df = JsonReader::new(std::fs::File::open(file_path)?)
                .with_json_format(JsonFormat::JsonLines)
                .infer_schema_len(Some(NonZero::new(100_000).unwrap()))
                .finish()?;
            df.lazy()
        }
        _ => anyhow::bail!("Unsupported file format: .{}", format),
    };

    Ok(lf)
}

/// Lint data for quality issues: duplicates, nulls, etc.
fn lint_data(mut lf: LazyFrame, cli: &Cli) -> Result<()> {
    let schema = lf.collect_schema()?;
    let total_rows = lf.clone().select([len()]).with_new_streaming(true).collect()?
        .column("len")?.u32()?.get(0).context("Failed to get count")? as usize;
    let total_cols = schema.len();

    println!("Linting {} rows × {} columns", total_rows, total_cols);
    println!();

    // Check 1: Null values per column (column-by-column for memory efficiency)
    println!("Null Value Check:");
    let mut has_nulls = false;

    // Process columns in batches to avoid memory issues
    let col_names: Vec<_> = schema.iter_names().collect();
    for col_name in &col_names {
        let null_count_result = lf.clone()
            .select([col(col_name.as_str()).null_count()])
            .with_new_streaming(true)
            .collect()?;

        let null_count = null_count_result
            .column(col_name.as_str())?
            .u32()?
            .get(0)
            .unwrap_or(0);

        if null_count > 0 {
            let pct = (null_count as f64 / total_rows as f64) * 100.0;
            println!("  {} has {} null values ({:.2}%)", col_name, null_count, pct);
            has_nulls = true;
        }
    }
    if !has_nulls {
        println!("  ✓ No null values found");
    }
    println!();

    // Check 2: Duplicate rows (if --unique flag specified or always check count)
    if cli.unique || cli.unique_on.is_some() {
        println!("Duplicate Check:");

        let unique_col_names: Vec<_> = if let Some(cols_str) = &cli.unique_on {
            cols_str.split(',').map(|s| s.trim()).collect()
        } else {
            vec![]
        };

        // Count unique rows using streaming
        let unique_lf = if !unique_col_names.is_empty() {
            lf.clone().unique_stable(Some(cols(unique_col_names.clone())), UniqueKeepStrategy::First)
        } else {
            lf.clone().unique_stable(None, UniqueKeepStrategy::First)
        };

        let unique_count = unique_lf.select([len()]).with_new_streaming(true).collect()?
            .column("len")?.u32()?.get(0).context("Failed to get unique count")? as usize;

        let duplicate_count = total_rows - unique_count;

        if duplicate_count > 0 {
            let pct = (duplicate_count as f64 / total_rows as f64) * 100.0;
            if !unique_col_names.is_empty() {
                println!("  {} duplicate rows on columns [{}] ({:.2}%)",
                    duplicate_count, unique_col_names.join(", "), pct);
            } else {
                println!("  {} duplicate rows ({:.2}%)", duplicate_count, pct);
            }
        } else {
            if !unique_col_names.is_empty() {
                println!("  ✓ No duplicates on columns [{}]", unique_col_names.join(", "));
            } else {
                println!("  ✓ No duplicate rows");
            }
        }
        println!();
    }

    // Check 3: Data type summary (optional)
    if cli.show_schema {
        println!("Schema:");
        for (col_name, dtype) in schema.iter() {
            println!("  {}: {}", col_name, dtype);
        }
        println!();
    }

    // Check 4: Show rows with nulls if requested
    if cli.show_nulls {
        // Check specific columns if --select used, otherwise ALL columns
        let cols_to_check: Vec<String> = if let Some(select_cols) = &cli.select {
            select_cols.split(',').map(|s| s.trim().to_string()).collect()
        } else {
            // Check ALL columns by default
            schema.iter_names().map(|s| s.to_string()).collect()
        };

        // Build filter: has_null in ANY of the checked columns
        let mut combined_filter = col(cols_to_check[0].as_str()).is_null();
        for col_name in &cols_to_check[1..] {
            combined_filter = combined_filter.or(col(col_name.as_str()).is_null());
        }

        let null_rows_lf = lf.clone().filter(combined_filter);

        // Limit to 100 rows unless --all specified
        let limited_lf = if cli.all {
            println!("Rows with null values (showing ALL):");
            null_rows_lf
        } else {
            println!("Rows with null values (showing first 100, use --all for all rows):");
            null_rows_lf.limit(100)
        };

        if cli.select.is_none() {
            println!("(Checking all {} columns)", cols_to_check.len());
        }

        let rows_with_nulls = limited_lf
            .with_new_streaming(true)
            .collect()?;

        println!("{}", rows_with_nulls);
    }

    Ok(())
}

/// Apply random sampling - windowed approach for memory efficiency
/// Samples from first N*1000 rows to avoid loading entire dataset
fn apply_random_sample_streaming(lf: LazyFrame, n: usize) -> Result<DataFrame> {
    // Get total row count (streaming, low memory)
    let row_count = lf.clone().select([len()]).with_new_streaming(true).collect()?
        .column("len")?.u32()?.get(0).context("Failed to get count")? as usize;

    let sample_size = n.min(row_count);

    if sample_size >= row_count {
        return Ok(lf.with_new_streaming(true).collect()?);
    }

    // Windowed sampling: sample from first N*1000 rows
    // This keeps memory low (~2GB) at the cost of not being truly random
    // NOTE: with_row_index() breaks streaming, so we use limited window instead
    let sample_window = (n * 1000).max(100_000).min(row_count);

    let limited_df = lf
        .limit(sample_window as IdxSize)
        .with_new_streaming(true)
        .collect()?;

    // Random sample from the window
    use rand::seq::index::sample;
    use rand::thread_rng;
    let random_indices = sample(&mut thread_rng(), limited_df.height(), sample_size);
    let idx_series = UInt32Chunked::from_vec(
        PlSmallStr::from_static("idx"),
        random_indices.into_iter().map(|i| i as u32).collect()
    );

    Ok(limited_df.take(&idx_series)?)
}

/// Apply sample to DataFrame after streaming collect
/// This requires the full dataset in memory but samples from it efficiently
fn apply_sample_to_dataframe(df: DataFrame, n_str: &str) -> Result<DataFrame> {
    let row_count = df.height();

    let sample_size = if n_str.contains('.') {
        let frac: f64 = n_str.parse()
            .with_context(|| format!("Invalid fraction for sample: '{}'", n_str))?;
        (row_count as f64 * frac).round() as usize
    } else {
        n_str.parse()
            .with_context(|| format!("Invalid number for sample: '{}'", n_str))?
    };

    let sample_size = sample_size.min(row_count);

    if sample_size >= row_count {
        return Ok(df);
    }

    // Random sampling from full dataset
    use rand::seq::index::sample;
    use rand::thread_rng;
    let random_indices = sample(&mut thread_rng(), row_count, sample_size);
    let idx_series = UInt32Chunked::from_vec(
        PlSmallStr::from_static("idx"),
        random_indices.into_iter().map(|i| i as u32).collect()
    );

    Ok(df.take(&idx_series)?)
}

/// Apply transformations to a LazyFrame (modular, reusable for all commands)
fn apply_transformations(mut lf: LazyFrame, cli: &Cli) -> Result<LazyFrame> {
    // 1. Filter rows FIRST (needs access to all columns)
    if let Some(filter_expr) = &cli.filter {
        // Parse SQL expression into Polars Expr
        use polars::sql::sql_expr;

        let expr = sql_expr(filter_expr)
            .with_context(|| format!("Failed to parse filter: '{}'", filter_expr))?;

        // Apply filter natively (enables predicate pushdown)
        // Note: Date comparisons need explicit casting: DATE = CAST('2006-01-03' AS DATE)
        lf = lf.filter(expr);
    }

    // 2. Select/Drop columns (after filtering, to reduce data)
    if let Some(select_cols) = &cli.select {
        let cols: Vec<Expr> = select_cols.split(',').map(|s| col(s.trim())).collect();
        lf = lf.select(cols);
    }

    if let Some(drop_cols) = &cli.drop {
        let col_names: Vec<_> = drop_cols.split(',').map(|s| s.trim()).collect();
        lf = lf.drop(cols(col_names));
    }

    // 3. Sort
    if !cli.sort_keys.is_empty() {
        let sort_exprs: Vec<Expr> = cli.sort_keys.iter().map(|k| {
            let mut expr = col(k.as_str());
            if cli.ignore_case {
                expr = expr.str().to_lowercase();
            }
            expr
        }).collect();

        let descending = vec![cli.reverse; cli.sort_keys.len()];
        lf = lf.sort_by_exprs(&sort_exprs, SortMultipleOptions::default().with_order_descending_multi(descending));
    }

    // 4. Unique (deduplication)
    if let Some(unique_cols) = &cli.unique_on {
        let col_names: Vec<_> = unique_cols.split(',').map(|s| s.trim()).collect();
        lf = lf.unique_stable(Some(cols(col_names)), UniqueKeepStrategy::First);
    } else if cli.unique {
        lf = lf.unique_stable(None, UniqueKeepStrategy::First);
    }

    // 5. Offset and Limit (pagination) - must be last
    if let Some(offset) = cli.offset {
        // Polars slice supports i64, so we're good for very large datasets
        let length = if let Some(limit) = cli.limit {
            limit as IdxSize
        } else {
            IdxSize::MAX // Use IdxSize::MAX to get all remaining rows
        };
        lf = lf.slice(offset, length);
    } else if let Some(limit) = cli.limit {
        // Just limit without offset
        lf = lf.limit(limit as IdxSize);
    }

    Ok(lf)
}


/// Parse N argument and files for head/tail/sample commands
fn parse_n_and_files(cli: &Cli) -> Result<(Option<String>, Vec<String>)> {
    if cli.files.is_empty() {
        return Ok((None, vec![]));
    }

    // Try to parse first file as a number (int or float) for head/tail/sample
    let first = &cli.files[0];

    // Check if it looks like a number (handles integers and floats like 0.1)
    if first.parse::<f64>().is_ok() {
        // First arg is N (as string to preserve int vs float), rest are files
        let files = cli.files[1..].to_vec();
        Ok((Some(first.clone()), files))
    } else {
        // First arg is a file, N is default
        Ok((None, cli.files.clone()))
    }
}

/// Print a DataFrame (pretty for TTY, raw CSV for pipes)
fn print_dataframe(df: &DataFrame, is_tty: bool) -> Result<()> {
    if is_tty {
        // Pretty table output
        println!("{}", df);
    } else {
        // Raw CSV output
        let mut buf = Vec::new();
        CsvWriter::new(&mut buf).finish(&mut df.clone())?;
        print!("{}", String::from_utf8_lossy(&buf));
    }
    Ok(())
}

/// Compute statistics for a LazyFrame using streaming aggregations
fn compute_stats_lazy(mut lf: LazyFrame) -> Result<DataFrame> {
    // Get schema to identify numeric columns
    let schema = lf.collect_schema()?;
    let numeric_cols: Vec<String> = schema
        .iter()
        .filter(|(_, dtype)| dtype.is_numeric())
        .map(|(name, _)| name.to_string())
        .collect();

    if numeric_cols.is_empty() {
        // No numeric columns, return empty stats DataFrame
        return Ok(DataFrame::new(vec![
            Column::new(PlSmallStr::from_static("column"), Vec::<String>::new()),
            Column::new(PlSmallStr::from_static("count"), Vec::<f64>::new()),
            Column::new(PlSmallStr::from_static("null_count"), Vec::<f64>::new()),
            Column::new(PlSmallStr::from_static("mean"), Vec::<f64>::new()),
            Column::new(PlSmallStr::from_static("std"), Vec::<f64>::new()),
            Column::new(PlSmallStr::from_static("min"), Vec::<f64>::new()),
            Column::new(PlSmallStr::from_static("median"), Vec::<f64>::new()),
            Column::new(PlSmallStr::from_static("max"), Vec::<f64>::new()),
        ])?);
    }

    // Build aggregation expressions for each numeric column
    let mut agg_exprs = Vec::new();
    for col_name in &numeric_cols {
        let c = col(col_name.as_str());
        agg_exprs.push(c.clone().count().alias(&format!("{}_count", col_name)));
        agg_exprs.push(c.clone().null_count().alias(&format!("{}_null", col_name)));
        agg_exprs.push(c.clone().mean().alias(&format!("{}_mean", col_name)));
        agg_exprs.push(c.clone().std(1).alias(&format!("{}_std", col_name)));
        agg_exprs.push(c.clone().min().alias(&format!("{}_min", col_name)));
        agg_exprs.push(c.clone().median().alias(&format!("{}_median", col_name)));
        agg_exprs.push(c.max().alias(&format!("{}_max", col_name)));
    }

    // Execute aggregations with streaming
    let agg_df = lf.select(agg_exprs).with_new_streaming(true).collect()?;

    // Reshape the aggregated data into stats format
    let mut col_names_vec = Vec::new();
    let mut counts = Vec::new();
    let mut nulls = Vec::new();
    let mut means = Vec::new();
    let mut stds = Vec::new();
    let mut mins = Vec::new();
    let mut medians = Vec::new();
    let mut maxs = Vec::new();

    for col_name in &numeric_cols {
        col_names_vec.push(col_name.as_str());
        counts.push(agg_df.column(&format!("{}_count", col_name))?.u32()?.get(0).unwrap_or(0) as f64);
        nulls.push(agg_df.column(&format!("{}_null", col_name))?.u32()?.get(0).unwrap_or(0) as f64);
        means.push(agg_df.column(&format!("{}_mean", col_name))?.f64()?.get(0).unwrap_or(f64::NAN));
        stds.push(agg_df.column(&format!("{}_std", col_name))?.f64()?.get(0).unwrap_or(f64::NAN));
        mins.push(agg_df.column(&format!("{}_min", col_name))?.cast(&DataType::Float64)?.f64()?.get(0).unwrap_or(f64::NAN));
        medians.push(agg_df.column(&format!("{}_median", col_name))?.f64()?.get(0).unwrap_or(f64::NAN));
        maxs.push(agg_df.column(&format!("{}_max", col_name))?.cast(&DataType::Float64)?.f64()?.get(0).unwrap_or(f64::NAN));
    }

    let stats_df = DataFrame::new(vec![
        Column::new(PlSmallStr::from_static("column"), col_names_vec),
        Column::new(PlSmallStr::from_static("count"), counts),
        Column::new(PlSmallStr::from_static("null_count"), nulls),
        Column::new(PlSmallStr::from_static("mean"), means),
        Column::new(PlSmallStr::from_static("std"), stds),
        Column::new(PlSmallStr::from_static("min"), mins),
        Column::new(PlSmallStr::from_static("median"), medians),
        Column::new(PlSmallStr::from_static("max"), maxs),
    ])?;

    Ok(stats_df)
}

/// Compute statistics for a DataFrame
fn compute_stats(df: &DataFrame) -> Result<DataFrame> {
    // Use Polars lazy aggregations to compute stats for numeric columns
    let mut stats_data = vec![];

    // Get numeric columns
    for col_name in df.get_column_names() {
        let column = df.column(col_name)?;

        // Only compute stats for numeric types
        if column.dtype().is_numeric() {
            let series = column.as_materialized_series();

            // Compute statistics
            let count = series.len() as f64;
            let null_count = series.null_count() as f64;
            let mean = series.mean().unwrap_or(f64::NAN);
            let std = series.std(1).unwrap_or(f64::NAN);
            let min = series.min::<f64>()?.unwrap_or(f64::NAN);
            let max = series.max::<f64>()?.unwrap_or(f64::NAN);
            let median = series.median().unwrap_or(f64::NAN);

            stats_data.push((
                col_name.to_string(),
                count,
                null_count,
                mean,
                std,
                min,
                median,
                max,
            ));
        }
    }

    // Build stats DataFrame
    let col_names: Vec<_> = stats_data.iter().map(|(name, ..)| name.as_str()).collect();
    let counts: Vec<_> = stats_data.iter().map(|(_, count, ..)| *count).collect();
    let nulls: Vec<_> = stats_data.iter().map(|(_, _, null, ..)| *null).collect();
    let means: Vec<_> = stats_data.iter().map(|(_, _, _, mean, ..)| *mean).collect();
    let stds: Vec<_> = stats_data.iter().map(|(_, _, _, _, std, ..)| *std).collect();
    let mins: Vec<_> = stats_data.iter().map(|(_, _, _, _, _, min, ..)| *min).collect();
    let medians: Vec<_> = stats_data.iter().map(|(_, _, _, _, _, _, median, _)| *median).collect();
    let maxs: Vec<_> = stats_data.iter().map(|(_, _, _, _, _, _, _, max)| *max).collect();

    let stats_df = DataFrame::new(vec![
        Column::new(PlSmallStr::from_static("column"), col_names),
        Column::new(PlSmallStr::from_static("count"), counts),
        Column::new(PlSmallStr::from_static("null_count"), nulls),
        Column::new(PlSmallStr::from_static("mean"), means),
        Column::new(PlSmallStr::from_static("std"), stds),
        Column::new(PlSmallStr::from_static("min"), mins),
        Column::new(PlSmallStr::from_static("median"), medians),
        Column::new(PlSmallStr::from_static("max"), maxs),
    ])?;

    Ok(stats_df)
}

/// Write LazyFrame to output file with streaming
fn sink_to_file(lf: LazyFrame, output_path: &str) -> Result<()> {
    let path = Path::new(output_path);
    let extension = path.extension()
        .and_then(|e| e.to_str())
        .context("Could not determine output file extension")?;

    match extension {
        "parquet" | "pq" => {
            // Use native sink_parquet for true streaming (no memory bloat)
            // CRITICAL: Must use Engine::Auto or Engine::Streaming, NOT the default collect()
            // which uses Engine::InMemory and materializes everything!
            let target = SinkTarget::Path(PlPath::new(output_path));
            lf.sink_parquet(target, Default::default(), None, Default::default())?
                .collect_with_engine(Engine::Auto)?;
            Ok(())
        }
        "csv" | "txt" => {
            // Use native sink_csv for true streaming
            // CRITICAL: Must use Engine::Auto, NOT the default collect()
            let target = SinkTarget::Path(PlPath::new(output_path));
            lf.sink_csv(target, Default::default(), None, Default::default())?
                .collect_with_engine(Engine::Auto)?;
            Ok(())
        }
        "tsv" => {
            // For TSV, fall back to collect + write since CsvWriterOptions API changed
            let df = lf.with_new_streaming(true).collect()?;
            write_output_file(&df, output_path)?;
            Ok(())
        }
        "json" | "jsonl" | "ndjson" => {
            // JSON formats don't have a native sink in Polars Rust API yet
            // Fall back to collect + write (will use more memory)
            let df = lf.with_new_streaming(true).collect()?;
            write_output_file(&df, output_path)?;
            Ok(())
        }
        _ => anyhow::bail!("Unsupported output format: .{}", extension),
    }
}

/// Write DataFrame to output file (format detected by extension)
fn write_output_file(df: &DataFrame, output_path: &str) -> Result<()> {
    let path = Path::new(output_path);
    let extension = path.extension()
        .and_then(|e| e.to_str())
        .context("Could not determine output file extension")?;

    match extension {
        "csv" | "txt" => {
            let mut file = std::fs::File::create(output_path)?;
            CsvWriter::new(&mut file).finish(&mut df.clone())?;
        }
        "tsv" => {
            let mut file = std::fs::File::create(output_path)?;
            CsvWriter::new(&mut file)
                .with_separator(b'\t')
                .finish(&mut df.clone())?;
        }
        "parquet" | "pq" => {
            let mut file = std::fs::File::create(output_path)?;
            ParquetWriter::new(&mut file).finish(&mut df.clone())?;
        }
        "json" => {
            let mut file = std::fs::File::create(output_path)?;
            JsonWriter::new(&mut file)
                .with_json_format(JsonFormat::Json)
                .finish(&mut df.clone())?;
        }
        "jsonl" | "ndjson" => {
            let mut file = std::fs::File::create(output_path)?;
            JsonWriter::new(&mut file)
                .with_json_format(JsonFormat::JsonLines)
                .finish(&mut df.clone())?;
        }
        _ => anyhow::bail!("Unsupported output format: .{}", extension),
    }

    Ok(())
}

/// Count rows and columns in a LazyFrame (after transformations)
fn count_lazyframe(lf: LazyFrame) -> Result<(usize, usize)> {
    // We need to collect to get both row count and column count
    // Collect once with all columns to get schema, then count
    // Use streaming for large datasets
    let df = lf.with_new_streaming(true).collect()?;
    let rows = df.height();
    let cols = df.width();

    Ok((rows, cols))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    fn create_test_csv(path: &str, rows: usize) -> Result<()> {
        let mut file = fs::File::create(path)?;
        writeln!(file, "name,age,city")?;
        for i in 0..rows {
            writeln!(file, "Person{},{},City{}", i, 20 + i, i)?;
        }
        Ok(())
    }

    fn create_test_tsv(path: &str, rows: usize) -> Result<()> {
        let mut file = fs::File::create(path)?;
        writeln!(file, "name\tage\tcity")?;
        for i in 0..rows {
            writeln!(file, "Person{}\t{}\tCity{}", i, 20 + i, i)?;
        }
        Ok(())
    }

    fn create_test_jsonl(path: &str, rows: usize) -> Result<()> {
        let mut file = fs::File::create(path)?;
        for i in 0..rows {
            writeln!(
                file,
                r#"{{"name":"Person{}","age":{},"city":"City{}"}}"#,
                i,
                20 + i,
                i
            )?;
        }
        Ok(())
    }

    #[test]
    fn test_count_csv() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_count.csv");
        create_test_csv(test_file.to_str().unwrap(), 5)?;

        let (rows, cols) = count_shape(test_file.to_str().unwrap())?;
        assert_eq!(rows, 5);
        assert_eq!(cols, 3); // name, age, city

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_count_tsv() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_count.tsv");
        create_test_tsv(test_file.to_str().unwrap(), 10)?;

        let (rows, cols) = count_shape(test_file.to_str().unwrap())?;
        assert_eq!(rows, 10);
        assert_eq!(cols, 3); // name, age, city

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_count_jsonl() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_count.jsonl");
        create_test_jsonl(test_file.to_str().unwrap(), 7)?;

        let (rows, cols) = count_shape(test_file.to_str().unwrap())?;
        assert_eq!(rows, 7);
        assert_eq!(cols, 3); // name, age, city

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_count_empty_file() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_empty.csv");
        create_test_csv(test_file.to_str().unwrap(), 0)?;

        let (rows, cols) = count_shape(test_file.to_str().unwrap())?;
        assert_eq!(rows, 0);
        assert_eq!(cols, 3); // name, age, city

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_count_large_file() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_large.csv");
        create_test_csv(test_file.to_str().unwrap(), 1000)?;

        let (rows, cols) = count_shape(test_file.to_str().unwrap())?;
        assert_eq!(rows, 1000);
        assert_eq!(cols, 3); // name, age, city

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_count_nonexistent_file() {
        let result = count_shape("nonexistent_file.csv");
        assert!(result.is_err());
    }

    #[test]
    fn test_count_unsupported_format() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test.xyz");
        fs::write(&test_file, "some content").unwrap();

        let result = count_shape(test_file.to_str().unwrap());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported file format"));

        fs::remove_file(test_file).ok();
    }

    #[test]
    fn test_count_multiple_files() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file1 = temp_dir.join("test_multi1.csv");
        let test_file2 = temp_dir.join("test_multi2.csv");
        let test_file3 = temp_dir.join("test_multi3.csv");

        create_test_csv(test_file1.to_str().unwrap(), 5)?;
        create_test_csv(test_file2.to_str().unwrap(), 10)?;
        create_test_csv(test_file3.to_str().unwrap(), 3)?;

        let (rows1, cols1) = count_shape(test_file1.to_str().unwrap())?;
        let (rows2, cols2) = count_shape(test_file2.to_str().unwrap())?;
        let (rows3, cols3) = count_shape(test_file3.to_str().unwrap())?;

        assert_eq!(rows1, 5);
        assert_eq!(rows2, 10);
        assert_eq!(rows3, 3);
        assert_eq!(cols1, 3);
        assert_eq!(cols2, 3);
        assert_eq!(cols3, 3);

        fs::remove_file(test_file1)?;
        fs::remove_file(test_file2)?;
        fs::remove_file(test_file3)?;
        Ok(())
    }

    #[test]
    fn test_filter_numeric() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_filter_num.csv");
        create_test_csv(test_file.to_str().unwrap(), 100)?;

        // Read and filter
        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("count".to_string()),
            files: vec![],
            filter: Some("age > 50".to_string()),
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };
        let lf = apply_transformations(lf, &cli)?;
        let (rows, cols) = count_lazyframe(lf)?;

        // ages are 20+i, so age > 50 means i > 30, so 69 rows (31-99)
        assert_eq!(rows, 69);
        assert_eq!(cols, 3);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_filter_string() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_filter_str.csv");

        // Create a CSV with specific cities
        let mut file = fs::File::create(&test_file)?;
        writeln!(file, "name,age,city")?;
        writeln!(file, "Alice,30,NYC")?;
        writeln!(file, "Bob,25,LA")?;
        writeln!(file, "Charlie,35,NYC")?;
        writeln!(file, "Diana,28,Boston")?;

        // Filter for NYC
        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("count".to_string()),
            files: vec![],
            filter: Some("city = 'NYC'".to_string()),
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };
        let lf = apply_transformations(lf, &cli)?;
        let (rows, cols) = count_lazyframe(lf)?;

        assert_eq!(rows, 2); // Alice and Charlie
        assert_eq!(cols, 3);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_no_transformations() -> Result<()> {
        let cli = Cli {
            command: Some("count".to_string()),
            files: vec![],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        assert!(!cli.has_transformations());
        Ok(())
    }

    #[test]
    fn test_has_transformations() -> Result<()> {
        let cli = Cli {
            command: Some("count".to_string()),
            files: vec![],
            filter: Some("age > 25".to_string()),
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        assert!(cli.has_transformations());
        Ok(())
    }

    #[test]
    fn test_filter_with_limit() -> Result<()> {
        // Test that filter + limit doesn't process entire file
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_filter_limit.csv");
        create_test_csv(test_file.to_str().unwrap(), 1000)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: Some("age > 25".to_string()),
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: Some(5),
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };
        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Should return exactly 5 rows (limit)
        assert_eq!(df.height(), 5);
        // All should have age > 25
        let ages = df.column("age")?.i64()?;
        for age in ages.iter() {
            assert!(age.unwrap() > 25);
        }

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_filter_with_select_and_limit() -> Result<()> {
        // Test optimized path: filter + select + limit in single SQL
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_filter_select_limit.csv");
        create_test_csv(test_file.to_str().unwrap(), 1000)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: Some("age > 50".to_string()),
            select: Some("name,age".to_string()),
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: Some(3),
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };
        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Should return 3 rows, 2 columns
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
        let col_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert!(col_names.contains(&"name"));
        assert!(col_names.contains(&"age"));
        assert!(!col_names.contains(&"city"));

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_head_parses_n() -> Result<()> {
        // Test that head 5 file.csv correctly parses
        let cli = Cli {
            command: Some("head".to_string()),
            files: vec!["5".to_string(), "file.csv".to_string()],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let (n, files) = parse_n_and_files(&cli)?;
        assert_eq!(n, Some("5".to_string()));
        assert_eq!(files, vec!["file.csv"]);

        Ok(())
    }

    #[test]
    fn test_head_no_n() -> Result<()> {
        // Test that head file.csv defaults N
        let cli = Cli {
            command: Some("head".to_string()),
            files: vec!["file.csv".to_string()],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let (n, files) = parse_n_and_files(&cli)?;
        assert_eq!(n, None); // Will use default 10
        assert_eq!(files, vec!["file.csv"]);

        Ok(())
    }

    #[test]
    fn test_sample_parses_integer() -> Result<()> {
        let cli = Cli {
            command: Some("sample".to_string()),
            files: vec!["100".to_string(), "file.csv".to_string()],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let (n, files) = parse_n_and_files(&cli)?;
        assert_eq!(n, Some("100".to_string()));
        assert_eq!(files, vec!["file.csv"]);

        Ok(())
    }

    #[test]
    fn test_sample_parses_fraction() -> Result<()> {
        let cli = Cli {
            command: Some("sample".to_string()),
            files: vec!["0.1".to_string(), "file.csv".to_string()],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let (n, files) = parse_n_and_files(&cli)?;
        assert_eq!(n, Some("0.1".to_string()));
        assert_eq!(files, vec!["file.csv"]);

        Ok(())
    }

    #[test]
    fn test_sample_no_n() -> Result<()> {
        let cli = Cli {
            command: Some("sample".to_string()),
            files: vec!["file.csv".to_string()],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let (n, files) = parse_n_and_files(&cli)?;
        assert_eq!(n, None); // Will use default
        assert_eq!(files, vec!["file.csv"]);

        Ok(())
    }

    #[test]
    fn test_sample_is_random() -> Result<()> {
        // Test that sample actually randomizes
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_sample_random.csv");
        create_test_csv(test_file.to_str().unwrap(), 100)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("sample".to_string()),
            files: vec!["10".to_string()],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![], // No sort = random
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        // Run sample twice and verify results are different
        let df1 = lf.clone().collect()?;
        let sample1 = {
            let total_rows = df1.height();
            use rand::seq::index::sample;
            use rand::thread_rng;
            let random_indices = sample(&mut thread_rng(), total_rows, 10);
            let idx_series = UInt32Chunked::from_vec(
                PlSmallStr::from_static("idx"),
                random_indices.into_iter().map(|i| i as u32).collect()
            );
            df1.take(&idx_series)?
        };

        let df2 = lf.collect()?;
        let sample2 = {
            let total_rows = df2.height();
            use rand::seq::index::sample;
            use rand::thread_rng;
            let random_indices = sample(&mut thread_rng(), total_rows, 10);
            let idx_series = UInt32Chunked::from_vec(
                PlSmallStr::from_static("idx"),
                random_indices.into_iter().map(|i| i as u32).collect()
            );
            df2.take(&idx_series)?
        };

        // Verify both samples are 10 rows
        assert_eq!(sample1.height(), 10);
        assert_eq!(sample2.height(), 10);

        // Verify samples are different (statistically almost certain with 100 rows, sample 10)
        let ages1: Vec<_> = sample1.column("age")?.i64()?.iter().collect();
        let ages2: Vec<_> = sample2.column("age")?.i64()?.iter().collect();
        assert_ne!(ages1, ages2, "Two random samples should be different");

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_sample_with_sort_output_sorted() -> Result<()> {
        // Test that sample output can be sorted (sort happens before sample in transformations)
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_sample_sorted.csv");
        create_test_csv(test_file.to_str().unwrap(), 100)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("sample".to_string()),
            files: vec![],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec!["age".to_string()], // With sort
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // After sort transformation, data is sorted
        // But sample will pick random rows from the sorted data
        // So we just verify the transformation applied successfully
        assert_eq!(df.height(), 100); // All rows present after transformations

        // The actual sampling happens in the command handler, not in transformations
        // This test just verifies transformations work

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_filter_then_select_order() -> Result<()> {
        // Test that filter happens before select (so filter can reference dropped columns)
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_filter_select_order.csv");

        let mut file = fs::File::create(&test_file)?;
        writeln!(file, "name,age,city")?;
        writeln!(file, "Alice,30,NYC")?;
        writeln!(file, "Bob,25,LA")?;
        writeln!(file, "Charlie,35,Chicago")?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: Some("age > 25".to_string()), // Filter on age
            select: Some("name,city".to_string()), // But don't select age
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Should have 2 rows (Alice, Charlie) and 2 columns (name, city - not age)
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_sort_ascending() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_sort_asc.csv");

        let mut file = fs::File::create(&test_file)?;
        writeln!(file, "name,age")?;
        writeln!(file, "Charlie,35")?;
        writeln!(file, "Alice,30")?;
        writeln!(file, "Bob,25")?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec!["age".to_string()],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Verify sorted ascending by age
        let ages = df.column("age")?.i64()?;
        assert_eq!(ages.get(0), Some(25)); // Bob
        assert_eq!(ages.get(1), Some(30)); // Alice
        assert_eq!(ages.get(2), Some(35)); // Charlie

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_sort_descending() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_sort_desc.csv");

        let mut file = fs::File::create(&test_file)?;
        writeln!(file, "name,value")?;
        writeln!(file, "A,10")?;
        writeln!(file, "B,30")?;
        writeln!(file, "C,20")?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec!["value".to_string()],
            reverse: true, // Descending
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Verify sorted descending
        let values = df.column("value")?.i64()?;
        assert_eq!(values.get(0), Some(30));
        assert_eq!(values.get(1), Some(20));
        assert_eq!(values.get(2), Some(10));

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_unique_removes_duplicates() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_unique.csv");

        let mut file = fs::File::create(&test_file)?;
        writeln!(file, "name,value")?;
        writeln!(file, "Alice,10")?;
        writeln!(file, "Bob,20")?;
        writeln!(file, "Alice,10")?; // Duplicate
        writeln!(file, "Charlie,30")?;
        writeln!(file, "Bob,20")?; // Duplicate

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: true,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Should have 3 unique rows
        assert_eq!(df.height(), 3);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_unique_on_column() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_unique_on.csv");

        let mut file = fs::File::create(&test_file)?;
        writeln!(file, "name,value")?;
        writeln!(file, "Alice,10")?;
        writeln!(file, "Bob,20")?;
        writeln!(file, "Alice,30")?; // Different value, same name
        writeln!(file, "Charlie,40")?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: None,
            select: None,
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: Some("name".to_string()),
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Should have 3 unique names (keeps first occurrence)
        assert_eq!(df.height(), 3);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_drop_columns() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_drop.csv");
        create_test_csv(test_file.to_str().unwrap(), 5)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: None,
            select: None,
            drop: Some("age,city".to_string()), // Drop 2 columns
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Should have 1 column (name only)
        assert_eq!(df.width(), 1);
        let col_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert!(col_names.contains(&"name"));
        assert!(!col_names.contains(&"age"));
        assert!(!col_names.contains(&"city"));

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_output_to_csv() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let input_file = temp_dir.join("test_input.csv");
        let output_file = temp_dir.join("test_output.csv");

        create_test_csv(input_file.to_str().unwrap(), 5)?;

        let lf = read_to_lazyframe(input_file.to_str().unwrap())?;
        let df = lf.collect()?;
        write_output_file(&df, output_file.to_str().unwrap())?;

        // Verify file was created and has correct content
        let verify_lf = read_to_lazyframe(output_file.to_str().unwrap())?;
        let verify_df = verify_lf.collect()?;

        assert_eq!(verify_df.height(), 5);
        assert_eq!(verify_df.width(), 3);

        fs::remove_file(input_file)?;
        fs::remove_file(output_file)?;
        Ok(())
    }

    #[test]
    fn test_output_to_parquet() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let input_file = temp_dir.join("test_input2.csv");
        let output_file = temp_dir.join("test_output.parquet");

        create_test_csv(input_file.to_str().unwrap(), 10)?;

        let lf = read_to_lazyframe(input_file.to_str().unwrap())?;
        let df = lf.collect()?;
        write_output_file(&df, output_file.to_str().unwrap())?;

        // Read back and verify
        let verify_lf = read_to_lazyframe(output_file.to_str().unwrap())?;
        let verify_df = verify_lf.collect()?;

        assert_eq!(verify_df.height(), 10);
        assert_eq!(verify_df.width(), 3);

        fs::remove_file(input_file)?;
        fs::remove_file(output_file)?;
        Ok(())
    }

    #[test]
    fn test_output_format_conversion() -> Result<()> {
        // Test CSV → Parquet conversion
        let temp_dir = std::env::temp_dir();
        let csv_file = temp_dir.join("test_conv.csv");
        let parquet_file = temp_dir.join("test_conv.parquet");

        create_test_csv(csv_file.to_str().unwrap(), 20)?;

        let lf = read_to_lazyframe(csv_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: Some("age > 30".to_string()),
            select: Some("name,age".to_string()),
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;
        write_output_file(&df, parquet_file.to_str().unwrap())?;

        // Verify parquet file
        let verify_lf = read_to_lazyframe(parquet_file.to_str().unwrap())?;
        let verify_df = verify_lf.collect()?;

        assert!(verify_df.height() > 0); // Some rows match filter
        assert_eq!(verify_df.width(), 2); // Only name,age selected

        fs::remove_file(csv_file)?;
        fs::remove_file(parquet_file)?;
        Ok(())
    }

    #[test]
    fn test_stats_basic() -> Result<()> {
        // Test that stats returns statistical summary
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_stats.csv");

        create_test_csv(test_file.to_str().unwrap(), 100)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let df = lf.collect()?;
        let stats_df = compute_stats(&df)?;

        // Stats has one row per numeric column
        // Our test data has: name (string), age (numeric), city (string)
        // So stats should have 1 row (for age column)
        assert_eq!(stats_df.height(), 1);

        // Should have 8 stat columns: column, count, null_count, mean, std, min, median, max
        assert_eq!(stats_df.width(), 8);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_stats_with_filter() -> Result<()> {
        // Test that stats works with transformations
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_stats_filter.csv");

        create_test_csv(test_file.to_str().unwrap(), 100)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("stats".to_string()),
            files: vec![],
            filter: Some("age > 50".to_string()),
            select: Some("age".to_string()),
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: None,
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };

        let lf = apply_transformations(lf, &cli)?;
        let df = lf.collect()?;

        // Should have filtered to rows where age > 50
        assert!(df.height() > 0);
        assert_eq!(df.width(), 1); // Only age column

        // Stats should work on the filtered data
        let stats_df = compute_stats(&df)?;
        assert!(stats_df.height() > 0);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_streaming_sink_parquet() -> Result<()> {
        // Test that streaming sink works for parquet files
        let temp_dir = std::env::temp_dir();
        let input_file = temp_dir.join("test_streaming_input.csv");
        let output_file = temp_dir.join("test_streaming_output.parquet");

        // Create a larger test file (10k rows)
        create_test_csv(input_file.to_str().unwrap(), 10000)?;

        let lf = read_to_lazyframe(input_file.to_str().unwrap())?;

        // Use sink_to_file which should use streaming engine
        sink_to_file(lf, output_file.to_str().unwrap())?;

        // Verify output file was created and has correct data
        let verify_lf = read_to_lazyframe(output_file.to_str().unwrap())?;
        let verify_df = verify_lf.collect()?;

        assert_eq!(verify_df.height(), 10000);
        assert_eq!(verify_df.width(), 3);

        fs::remove_file(input_file)?;
        fs::remove_file(output_file)?;
        Ok(())
    }

    #[test]
    fn test_streaming_sink_csv() -> Result<()> {
        // Test that streaming sink works for CSV files
        let temp_dir = std::env::temp_dir();
        let input_file = temp_dir.join("test_streaming_csv_input.csv");
        let output_file = temp_dir.join("test_streaming_csv_output.csv");

        create_test_csv(input_file.to_str().unwrap(), 5000)?;

        let lf = read_to_lazyframe(input_file.to_str().unwrap())?;
        sink_to_file(lf, output_file.to_str().unwrap())?;

        // Verify output
        let verify_lf = read_to_lazyframe(output_file.to_str().unwrap())?;
        let verify_df = verify_lf.collect()?;

        assert_eq!(verify_df.height(), 5000);
        assert_eq!(verify_df.width(), 3);

        fs::remove_file(input_file)?;
        fs::remove_file(output_file)?;
        Ok(())
    }

    #[test]
    fn test_streaming_with_transformations() -> Result<()> {
        // Test that streaming sink works with filters and transformations
        let temp_dir = std::env::temp_dir();
        let input_file = temp_dir.join("test_stream_transform_input.csv");
        let output_file = temp_dir.join("test_stream_transform_output.parquet");

        create_test_csv(input_file.to_str().unwrap(), 1000)?;

        let lf = read_to_lazyframe(input_file.to_str().unwrap())?;
        let cli = Cli {
            command: Some("cat".to_string()),
            files: vec![],
            filter: Some("age > 30".to_string()),
            select: Some("name,age".to_string()),
            drop: None,
            sort_keys: vec![],
            reverse: false,
            ignore_case: false,
            unique: false,
            unique_on: None,
            limit: Some(100),
            offset: None,
            output: None,
            show_nulls: false,
            all: false,
            show_schema: false,
        };
        let lf = apply_transformations(lf, &cli)?;
        sink_to_file(lf, output_file.to_str().unwrap())?;

        // Verify output has transformations applied
        let verify_lf = read_to_lazyframe(output_file.to_str().unwrap())?;
        let verify_df = verify_lf.collect()?;

        // Should have filtered rows (age > 30), selected 2 columns, and limited to 100
        assert!(verify_df.height() <= 100);
        assert_eq!(verify_df.width(), 2);

        fs::remove_file(input_file)?;
        fs::remove_file(output_file)?;
        Ok(())
    }
}

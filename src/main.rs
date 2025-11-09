use anyhow::{Context, Result};
use clap::Parser;
use polars::prelude::*;
use std::io::{IsTerminal, Read, Write};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser, Debug, Clone, Default)]
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
      tt sample 100 data.parquet                           # Random 100 rows (uniform)\n  \
      tt sample 0.01 data.parquet                          # Random 1% sample (uniform)\n  \
      tt sort age data.csv                                 # Sort by age (asc)\n  \
      tt sort name,age -r data.csv                         # Multi-key sort, descending\n  \
      tt filter \"age > 25\" data.csv                        # Filter rows\n  \
      tt stat data.csv                                     # Summary statistics\n  \
      tt lint data.csv --show-nulls --unique               # Data quality checks\n  \
      tt cat --limit 100 data.csv                          # First 100 rows\n  \
      tt head --filter \"age > 25\" data.csv                 # Filter then show\n  \
      tt cat --select \"name,age\" -k age data.csv           # Select columns and sort\n  \
      tt sample --filter \"city = 'NYC'\" -k age data.csv    # Filter, sample, sort output\n  \
      tt count --unique data.csv                           # Count unique rows\n  \
      tt stat --select \"age,value\" data.csv               # Summary statistics on specific columns\n\n\
      Performance tip: Use --select with filters on wide Parquet files\n  \
      tt cat --filter \"status = 'active'\" --select \"id,name\" data.parquet\n\n\
      Parquet: Uses zstd compression level 3 by default (good speed/size balance)\n\n  \
      Streaming: head/tail/count/lint and file-output writes (-o) stream row batches;\n  \
      sample and stat materialize the (post-transform) frame to aggregate.\n\n  \
      Project: https://github.com/wtn/tabular_tool"
)]
struct Cli {
    /// Command to execute: cat, head, tail, sample, sort, filter, stat, count, lint
    #[arg(value_name = "COMMAND", help = "Command: cat, head [N], tail [N], sample [N], sort COLS, filter EXPR, stat, count, lint")]
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

    /// Format of stdin input (required when a file arg is `-`).
    /// One of: csv, tsv, parquet, json, jsonl.
    #[arg(long, alias = "if", help = "Format of `-` (stdin): --input-format csv")]
    input_format: Option<String>,

    /// Treat the input file as having no header row (CSV/TSV only).
    /// Columns are auto-named `column_1`, `column_2`, ... and can be
    /// referenced in --filter / --select expressions.
    #[arg(long, alias = "no-headers", help = "Input has no header row (CSV/TSV)")]
    no_header: bool,
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

const KNOWN_COMMANDS: &[&str] = &[
    "cat", "head", "tail", "sample", "sort", "filter", "stat", "count", "lint",
];

/// Polars' pretty-print defaults to 10 rows / 8 cols. When a user asks for `sample 20`
/// or has a wide file, they want all of it -- so unset those caps unless the user
/// has explicitly set the env var themselves.
fn configure_display() {
    // SAFETY: called once at the top of main before any threads are spawned.
    unsafe {
        if std::env::var_os("POLARS_FMT_MAX_ROWS").is_none() {
            std::env::set_var("POLARS_FMT_MAX_ROWS", "-1");
        }
        if std::env::var_os("POLARS_FMT_MAX_COLS").is_none() {
            std::env::set_var("POLARS_FMT_MAX_COLS", "-1");
        }
    }
}

/// Restore default SIGPIPE handling so `tt ... | head` / `| less q` exit silently
/// instead of panicking on broken pipe. Rust ignores SIGPIPE by default, which turns
/// the first post-EPIPE write into a panic from the stdio layer.
#[cfg(unix)]
fn reset_sigpipe() {
    unsafe extern "C" {
        fn signal(signum: i32, handler: usize) -> usize;
    }
    // SIGPIPE = 13, SIG_DFL = 0 on every Unix we care about.
    unsafe { signal(13, 0); }
}

#[cfg(not(unix))]
fn reset_sigpipe() {}

fn main() -> Result<()> {
    reset_sigpipe();
    configure_display();
    let mut cli = Cli::parse();

    // Bare `tt` with nothing at all: print usage and exit clean, like most CLIs.
    if cli.command.is_none() && cli.files.is_empty() {
        use clap::CommandFactory;
        Cli::command().print_help()?;
        println!();
        return Ok(());
    }

    let command = parse_command(&mut cli.command, &mut cli.files)?;

    if cli.files.is_empty() {
        anyhow::bail!("{} requires at least one file", command.name());
    }

    // Replace any `-` entries with tempfiles backing stdin. The returned `TempFile`
    // handles are held alive for the rest of main() and clean up on drop.
    let _stdin_backing = resolve_stdin_inputs(&mut cli)?;

    match command {
        Command::Lint => run_lint(&cli),
        Command::Count => run_count(&cli),
        Command::Stat => run_stats(&cli),
        Command::Cat
        | Command::Head(_)
        | Command::Tail(_)
        | Command::Sample(_)
        | Command::Sort(_)
        | Command::Filter(_) => run_view(command, &cli),
    }
}

/// Swap any `-` file path for a tempfile buffering stdin. Returns the `TempFile`
/// handles so they stay alive (and get deleted) for the rest of main().
fn resolve_stdin_inputs(cli: &mut Cli) -> Result<Vec<TempFile>> {
    let stdin_indices: Vec<usize> = cli
        .files
        .iter()
        .enumerate()
        .filter_map(|(i, p)| (p == "-").then_some(i))
        .collect();

    if stdin_indices.is_empty() {
        return Ok(Vec::new());
    }
    if stdin_indices.len() > 1 {
        anyhow::bail!("`-` (stdin) can only appear once in the file list");
    }

    let format = cli.input_format.as_deref().context(
        "reading from stdin (`-`) requires --input-format <csv|tsv|parquet|json|jsonl>",
    )?;

    let mut stdin = std::io::stdin().lock();
    let temp = materialize_stdin_to_tempfile(format, &mut stdin)?;
    cli.files[stdin_indices[0]] = temp.path().to_string_lossy().into_owned();
    Ok(vec![temp])
}

fn run_lint(cli: &Cli) -> Result<()> {
    let show_separators = cli.files.len() > 1;
    for (idx, file_path) in cli.files.iter().enumerate() {
        if show_separators && idx > 0 {
            println!();
        }
        if show_separators {
            println!("==> {} <==", file_path);
        }
        let lf = read_to_lazyframe_opts(file_path, !cli.no_header)?;
        let lf = apply_transformations(lf, cli)?;
        lint_data(lf, cli)?;
    }
    Ok(())
}

fn run_count(cli: &Cli) -> Result<()> {
    for file_path in &cli.files {
        let (rows, cols) = if cli.has_transformations() {
            let lf = read_to_lazyframe_opts(file_path, !cli.no_header)?;
            let lf = apply_transformations(lf, cli)?;
            count_lazyframe(lf)?
        } else {
            // Fast path: metadata / streaming-len, no materialize.
            count_shape_opts(file_path, !cli.no_header)?
        };
        println!("{}\t{}\t{}", rows, cols, file_path);
    }
    Ok(())
}

fn run_stats(cli: &Cli) -> Result<()> {
    let show_separators = cli.files.len() > 1;
    let is_tty = std::io::stdout().is_terminal();
    for (idx, file_path) in cli.files.iter().enumerate() {
        if show_separators && idx > 0 {
            println!();
        }
        if show_separators {
            println!("==> {} <==", file_path);
        }
        let lf = read_to_lazyframe_opts(file_path, !cli.no_header)?;
        let lf = apply_transformations(lf, cli)?;
        let stats_df = compute_stats_lazy(lf)?;
        if let Some(output_file) = &cli.output {
            write_output_file(&stats_df, output_file)?;
        } else {
            print_dataframe(&stats_df, is_tty)?;
        }
    }
    Ok(())
}

/// `tt sort` / `tt filter` are sugar: hoist their positional args into the cli so the
/// rest of the pipeline (apply_transformations) treats them like global -k / --filter.
fn inject_view_command_args(command: &Command, mut cli: Cli) -> Result<Cli> {
    match command {
        Command::Sort(keys) => {
            cli.sort_keys.extend(keys.iter().cloned());
            if cli.sort_keys.is_empty() {
                anyhow::bail!(
                    "sort requires a column: tt sort COL[,COL...] FILE  (or pass -k COL)",
                );
            }
        }
        Command::Filter(expr) => {
            if let Some(existing) = &cli.filter {
                anyhow::bail!(
                    "filter expression set twice: positional {:?} conflicts with --filter {:?}",
                    expr, existing,
                );
            }
            cli.filter = Some(expr.clone());
        }
        _ => {}
    }
    Ok(cli)
}

/// cat / head / tail / sample share a lot: optionally set limit/offset from the command,
/// then scan → transform → sink-to-file OR materialize → sample → print.
fn run_view(command: Command, cli: &Cli) -> Result<()> {
    let cli_owned = inject_view_command_args(&command, cli.clone())?;
    let cli = &cli_owned;

    // Fast path: bare `tt file.csv` (cat, no transforms, no -o) is just cat(1).
    // Skip polars entirely -- no schema inference, no materialize, no re-serialize.
    // Gated on uncompressed files only: `.gz`/`.zst` must round-trip through polars
    // so we emit decompressed text, not raw gzip bytes.
    if matches!(command, Command::Cat)
        && !cli.has_transformations()
        && cli.output.is_none()
        && cli.files.iter().all(|p| passthrough_eligible(p))
    {
        let show_separators = cli.files.len() > 1;
        let mut out = std::io::stdout().lock();
        for (idx, file_path) in cli.files.iter().enumerate() {
            if show_separators && idx > 0 {
                writeln!(out)?;
            }
            if show_separators {
                writeln!(out, "==> {} <==", file_path)?;
            }
            copy_file_to(file_path, &mut out)?;
        }
        return Ok(());
    }

    // Bake head/tail's N into limit/offset unless user set them explicitly.
    let mut cli_with_limit = cli.clone();
    match command {
        Command::Head(n) if cli_with_limit.limit.is_none() => {
            cli_with_limit.limit = Some(n);
        }
        Command::Tail(n) if cli_with_limit.offset.is_none() => {
            cli_with_limit.offset = Some(-(n as i64));
        }
        _ => {}
    }

    let show_separators = cli.files.len() > 1;
    let is_tty = std::io::stdout().is_terminal();

    for (idx, file_path) in cli.files.iter().enumerate() {
        if show_separators && idx > 0 {
            println!();
        }
        if show_separators {
            println!("==> {} <==", file_path);
        }

        let lf = read_to_lazyframe_opts(file_path, !cli.no_header)?;
        let lf = apply_transformations(lf, &cli_with_limit)?;

        // Direct streaming sink for -o (not for sample -- sample materializes below).
        if let (Some(output_file), false) =
            (&cli.output, matches!(command, Command::Sample(_)))
        {
            sink_to_file(lf, output_file)?;
            continue;
        }

        let df = match command {
            Command::Sample(size) => {
                let n = match size {
                    SampleSize::Count(c) => c,
                    SampleSize::Frac(f) => {
                        let total = count_rows_lazy(lf.clone())?;
                        (total as f64 * f).round() as usize
                    }
                };
                let mut sampled = random_sample(lf, n)?;
                // Sort applies *after* sampling so the caller sees the sample in sorted order.
                if !cli.sort_keys.is_empty() {
                    let cols: Vec<_> = cli.sort_keys.iter().map(|s| s.as_str()).collect();
                    let descending = vec![cli.reverse; cli.sort_keys.len()];
                    sampled = sampled.sort(
                        cols,
                        SortMultipleOptions::default().with_order_descending_multi(descending),
                    )?;
                }
                sampled
            }
            _ => lf.with_new_streaming(true).collect()?,
        };

        if let Some(output_file) = &cli.output {
            write_output_file(&df, output_file)?;
        } else {
            print_dataframe(&df, is_tty)?;
        }
    }

    Ok(())
}

/// Detect logical format from a path's extension, peeking through `.gz` / `.zst`.
fn detect_format(file_path: &str) -> Result<&str> {
    let path = Path::new(file_path);
    let ext = path.extension().and_then(|e| e.to_str())
        .context("Could not determine file extension")?;
    if ext == "gz" || ext == "zst" {
        let stem = path.file_stem().and_then(|s| s.to_str())
            .context("Compressed file missing format extension")?;
        stem.rsplit_once('.').map(|(_, e)| e)
            .context("Compressed file missing format extension")
    } else {
        Ok(ext)
    }
}

/// Count rows in a LazyFrame without materializing column data.
fn count_rows_lazy(lf: LazyFrame) -> Result<usize> {
    let df = lf.select([len()]).with_new_streaming(true).collect()?;
    // `.idx()` (IdxSize) stays correct whether polars is built with the default u32
    // or the `bigidx` feature (u64).
    Ok(df.column("len")?.idx()?.get(0).context("row count missing")? as usize)
}

/// Row and column count via the lazy scan -- columns from schema only, rows via a streaming len().
#[cfg(test)]
fn count_shape(file_path: &str) -> Result<(usize, usize)> {
    count_shape_opts(file_path, true)
}

fn count_shape_opts(file_path: &str, has_header: bool) -> Result<(usize, usize)> {
    let mut lf = read_to_lazyframe_opts(file_path, has_header)?;
    let cols = lf.collect_schema()?.len();
    let rows = count_rows_lazy(lf)?;
    Ok((rows, cols))
}

/// Read a file into a LazyFrame (assumes CSV/TSV files have a header row).
#[cfg(test)]
fn read_to_lazyframe(file_path: &str) -> Result<LazyFrame> {
    read_to_lazyframe_opts(file_path, true)
}

/// Read a file into a LazyFrame. `has_header` is honored for CSV/TSV; ignored
/// for Parquet/JSON, which have their own schema mechanisms.
fn read_to_lazyframe_opts(file_path: &str, has_header: bool) -> Result<LazyFrame> {
    let format = detect_format(file_path)?;

    let lf = match format {
        "csv" | "txt" | "tsv" => {
            let separator = if format == "tsv" { b'\t' } else { b',' };
            LazyCsvReader::new(PlRefPath::new(file_path))
                .with_separator(separator)
                .with_has_header(has_header)
                // Polars' own default (100) misinferres real-world CSVs where a column
                // looks int for thousands of rows then hits a float. 10_000 is the
                // speed-vs-correctness balance; `--limit` etc. no longer force a full scan.
                .with_infer_schema_length(Some(10_000))
                .with_try_parse_dates(true)
                .finish()
        }
        "parquet" | "pq" => {
            LazyFrame::scan_parquet(PlRefPath::new(file_path), Default::default())
        }
        "json" | "jsonl" | "ndjson" => {
            // No lazy JSON reader in the Rust API; read eager then convert.
            std::fs::File::open(file_path)
                .map_err(Into::into)
                .and_then(|f| {
                    JsonReader::new(f)
                        .with_json_format(JsonFormat::JsonLines)
                        .infer_schema_len(NonZero::new(100_000))
                        .finish()
                })
                .map(|df| df.lazy())
        }
        _ => anyhow::bail!("Unsupported file format: .{}", format),
    };

    lf.with_context(|| format!("reading {}", file_path))
}

/// Null count per column in a single streaming pass -- returns (name, null_count) pairs
/// in schema order. Replaces the old one-collect-per-column loop that was O(cols) scans.
fn per_column_null_counts(lf: LazyFrame, schema: &Schema) -> Result<Vec<(String, usize)>> {
    let names: Vec<String> = schema.iter_names().map(|n| n.to_string()).collect();
    if names.is_empty() {
        return Ok(Vec::new());
    }

    let exprs: Vec<Expr> = names
        .iter()
        .map(|n| col(n.as_str()).null_count().alias(n.as_str()))
        .collect();
    let df = lf.select(exprs).with_new_streaming(true).collect()?;

    names
        .into_iter()
        .map(|n| {
            let count = df.column(n.as_str())?.idx()?.get(0).unwrap_or(0) as usize;
            Ok((n, count))
        })
        .collect()
}

/// Lint data for quality issues: duplicates, nulls, etc.
fn lint_data(mut lf: LazyFrame, cli: &Cli) -> Result<()> {
    let schema = lf.collect_schema()?;
    let total_rows = count_rows_lazy(lf.clone())?;
    let total_cols = schema.len();

    println!("Linting {} rows × {} columns", total_rows, total_cols);
    println!();

    println!("Null Value Check:");
    let null_counts = per_column_null_counts(lf.clone(), &schema)?;
    let mut has_nulls = false;
    for (col_name, null_count) in &null_counts {
        if *null_count > 0 {
            let pct = (*null_count as f64 / total_rows as f64) * 100.0;
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

        let unique_count = count_rows_lazy(unique_lf)?;

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
            schema.iter_names().map(|s| s.to_string()).collect()
        };

        if cols_to_check.is_empty() {
            println!("Rows with null values: skipped (no columns to check).");
            return Ok(());
        }

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

/// Uniformly sample `n` rows from a LazyFrame.
///
/// Materializes the frame via the streaming engine, then delegates to polars'
/// native `DataFrame::sample_n_literal` -- truly uniform, at the cost of holding
/// the full post-transform frame in memory. For files that won't fit in RAM,
/// narrow with `--filter` / `--select` / `--limit` first.
fn random_sample(lf: LazyFrame, n: usize) -> Result<DataFrame> {
    let df = lf.with_new_streaming(true).collect()?;
    if n >= df.height() {
        return Ok(df);
    }
    Ok(df.sample_n_literal(n, false, false, None)?)
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


/// How much to sample: a fixed row count or a fraction of the frame.
#[derive(Debug, Clone, Copy, PartialEq)]
enum SampleSize {
    Count(usize),
    Frac(f64),
}

/// The command dispatch, parsed out of `Cli` and carrying any positional argument
/// directly (no more stringly-typed `cli.command.as_deref() == Some("head")` checks).
#[derive(Debug, Clone, PartialEq)]
enum Command {
    Cat,
    Head(usize),
    Tail(usize),
    Sample(SampleSize),
    Sort(Vec<String>),
    Filter(String),
    Stat,
    Count,
    Lint,
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Command::Cat => "cat",
            Command::Head(_) => "head",
            Command::Tail(_) => "tail",
            Command::Sample(_) => "sample",
            Command::Sort(_) => "sort",
            Command::Filter(_) => "filter",
            Command::Stat => "stat",
            Command::Count => "count",
            Command::Lint => "lint",
        }
    }
}

/// A token in the command slot is treated as a path (rather than a typo'd subcommand)
/// if it has a path-ish shape or actually exists on disk. Bare words like `less` or
/// `more` fall through and get reported as unknown subcommands.
fn looks_like_path(s: &str) -> bool {
    s.contains('.') || s.contains('/') || s.starts_with('~') || Path::new(s).exists()
}

/// Turn the raw clap output into a typed `Command`, consuming any leading numeric N
/// out of `files` for head/tail/sample. If the first positional isn't a known command
/// keyword but looks like a path (contains `.`/`/` or exists on disk), it's rotated
/// back into `files` and the command defaults to Cat. A bareword that's neither a
/// known command nor a plausible path is rejected as an unknown subcommand.
fn parse_command(raw_command: &mut Option<String>, files: &mut Vec<String>) -> Result<Command> {
    if let Some(cmd) = raw_command.as_deref() {
        if !KNOWN_COMMANDS.contains(&cmd) {
            if looks_like_path(cmd) {
                files.insert(0, cmd.to_string());
                *raw_command = Some("cat".to_string());
            } else {
                anyhow::bail!("Unknown subcommand: '{}'. Try 'tt --help'", cmd);
            }
        }
    }

    let take_usize = |files: &mut Vec<String>, default: usize| -> Result<usize> {
        match files.first() {
            Some(s) if s.parse::<usize>().is_ok() => {
                let n = files.remove(0);
                n.parse().with_context(|| format!("Invalid number: '{}'", n))
            }
            _ => Ok(default),
        }
    };

    // `tt sort COL[,COL...] FILE` -- consume the first positional as sort keys if it
    // doesn't look like a path. If it does, return empty and rely on -k flags.
    let take_sort_keys = |files: &mut Vec<String>| -> Vec<String> {
        match files.first() {
            Some(s) if !looks_like_path(s) => {
                let raw = files.remove(0);
                raw.split(',').map(|s| s.trim().to_string()).collect()
            }
            _ => Vec::new(),
        }
    };

    // `tt filter EXPR FILE` -- the expression is always the first positional. We
    // don't try to detect "looks like a path" here because filter expressions
    // routinely contain dots (`name = 'foo.bar'`), which would defeat the heuristic.
    let take_filter_expr = |files: &mut Vec<String>| -> Result<String> {
        if files.is_empty() {
            anyhow::bail!(
                "filter requires an expression: tt filter \"age > 25\" FILE",
            );
        }
        Ok(files.remove(0))
    };

    let take_sample_size = |files: &mut Vec<String>| -> Result<SampleSize> {
        match files.first() {
            Some(s) if s.parse::<f64>().is_ok() => {
                let raw = files.remove(0);
                if raw.contains('.') {
                    let f: f64 = raw.parse()
                        .with_context(|| format!("Invalid fraction: '{}'", raw))?;
                    Ok(SampleSize::Frac(f))
                } else {
                    let n: usize = raw.parse()
                        .with_context(|| format!("Invalid number: '{}'", raw))?;
                    Ok(SampleSize::Count(n))
                }
            }
            _ => Ok(SampleSize::Count(10)),
        }
    };

    Ok(match raw_command.as_deref().unwrap_or("cat") {
        "cat" => Command::Cat,
        "head" => Command::Head(take_usize(files, 10)?),
        "tail" => Command::Tail(take_usize(files, 10)?),
        "sample" => Command::Sample(take_sample_size(files)?),
        "sort" => Command::Sort(take_sort_keys(files)),
        "filter" => Command::Filter(take_filter_expr(files)?),
        "stat" => Command::Stat,
        "count" => Command::Count,
        "lint" => Command::Lint,
        other => anyhow::bail!("Unknown command: {}. Try 'tt --help'", other),
    })
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

/// Stat columns -- listed in display order. Float-typed aggs read via `.f64()`;
/// index-typed aggs (count/null_count) via `.idx()` and cast to f64.
const STAT_NAMES: [&str; 7] = ["count", "null_count", "mean", "std", "min", "median", "max"];

/// Compute statistics for a LazyFrame using streaming aggregations.
fn compute_stats_lazy(mut lf: LazyFrame) -> Result<DataFrame> {
    let schema = lf.collect_schema()?;
    let numeric_cols: Vec<String> = schema
        .iter()
        .filter(|(_, dtype)| dtype.is_numeric())
        .map(|(name, _)| name.to_string())
        .collect();

    // Per-column vectors for the output frame, pre-sized.
    let n = numeric_cols.len();
    let mut col_name_out = Vec::with_capacity(n);
    let mut stats: [Vec<f64>; 7] = std::array::from_fn(|_| Vec::with_capacity(n));

    if n > 0 {
        // Build 7 aggs per numeric column, aliased positionally so we can read them by
        // index without any format!("{}_kind", name) name-mangling.
        let mut aggs = Vec::with_capacity(n * 7);
        for (i, name) in numeric_cols.iter().enumerate() {
            let c = col(name.as_str());
            let alias = |kind: &str| PlSmallStr::from_string(format!("c{}_{}", i, kind));
            aggs.push(c.clone().count().alias(alias("count")));
            aggs.push(c.clone().null_count().alias(alias("null_count")));
            aggs.push(c.clone().mean().alias(alias("mean")));
            aggs.push(c.clone().std(1).alias(alias("std")));
            // min/max return the input dtype (int stays int); cast to f64 so the
            // output frame has uniform types.
            aggs.push(c.clone().min().cast(DataType::Float64).alias(alias("min")));
            aggs.push(c.clone().median().alias(alias("median")));
            aggs.push(c.max().cast(DataType::Float64).alias(alias("max")));
        }

        let agg_df = lf.select(aggs).with_new_streaming(true).collect()?;
        let cols = agg_df.columns();

        for (i, name) in numeric_cols.iter().enumerate() {
            col_name_out.push(name.clone());
            let base = i * 7;
            // 0=count, 1=null_count → IDX_DTYPE; 2..=6 → f64.
            stats[0].push(cols[base].idx()?.get(0).unwrap_or(0) as f64);
            stats[1].push(cols[base + 1].idx()?.get(0).unwrap_or(0) as f64);
            for k in 2..7 {
                stats[k].push(cols[base + k].f64()?.get(0).unwrap_or(f64::NAN));
            }
        }
    }

    let mut out = Vec::with_capacity(1 + STAT_NAMES.len());
    out.push(Column::new(PlSmallStr::from_static("column"), col_name_out));
    let [counts, nulls, means, stds, mins, medians, maxs] = stats;
    let values = [counts, nulls, means, stds, mins, medians, maxs];
    for (name, v) in STAT_NAMES.iter().zip(values) {
        out.push(Column::new(PlSmallStr::from_string(name.to_string()), v));
    }
    Ok(DataFrame::new_infer_height(out)?)
}

/// Write LazyFrame to output file with streaming
fn sink_to_file(lf: LazyFrame, output_path: &str) -> Result<()> {
    let path = Path::new(output_path);
    let extension = path.extension()
        .and_then(|e| e.to_str())
        .context("Could not determine output file extension")?;

    let file_format = match extension {
        "parquet" | "pq" => FileWriteFormat::Parquet(Arc::new(ParquetWriteOptions::default())),
        "csv" | "txt" => FileWriteFormat::Csv(CsvWriterOptions::default()),
        // TSV and JSON fall back to eager collect + write:
        // the sink API's CsvWriterOptions has no direct separator override hooked up here,
        // and JSON/JSONL have no streaming sink yet.
        "tsv" | "json" | "jsonl" | "ndjson" => {
            let df = lf.with_new_streaming(true).collect()?;
            return write_output_file(&df, output_path);
        }
        _ => anyhow::bail!("Unsupported output format: .{}", extension),
    };

    let destination = SinkDestination::File {
        target: SinkTarget::Path(PlRefPath::new(output_path)),
    };
    lf.sink(destination, file_format, UnifiedSinkArgs::default())?
        .collect_with_engine(Engine::Streaming)?;
    Ok(())
}

/// Byte-copy a file to a writer -- the fast path for `tt file` with no transforms.
fn copy_file_to<W: Write>(file_path: &str, mut w: W) -> Result<()> {
    let mut f = std::fs::File::open(file_path)?;
    std::io::copy(&mut f, &mut w)?;
    Ok(())
}

/// RAII handle for a temp file -- deleted on drop. Used to back stdin so the rest of
/// the pipeline can keep treating inputs as "a path on disk".
struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Buffer stdin (or any reader) into a temp file with the given format extension so the
/// rest of the lazy-scan pipeline works unchanged.
fn materialize_stdin_to_tempfile<R: Read>(format: &str, stdin: &mut R) -> Result<TempFile> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut path = std::env::temp_dir();
    path.push(format!("tt_stdin_{}_{}.{}", std::process::id(), nanos, format));

    let mut file = std::fs::File::create(&path)
        .with_context(|| format!("creating tempfile {}", path.display()))?;
    std::io::copy(stdin, &mut file)
        .with_context(|| format!("writing stdin to tempfile {}", path.display()))?;
    file.sync_all().ok();
    Ok(TempFile { path })
}

/// Whether a file can be byte-copied through for bare `tt file` (no transforms, no -o).
/// Compressed files (`.gz`, `.zst`) must go through polars so they get decompressed.
/// Parquet is a binary columnar format -- byte-copying it dumps raw bytes to the terminal.
fn passthrough_eligible(file_path: &str) -> bool {
    !matches!(
        Path::new(file_path).extension().and_then(|e| e.to_str()),
        Some("gz") | Some("zst") | Some("parquet") | Some("pq"),
    )
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

/// Row and column count for a LazyFrame (after any transformations).
fn count_lazyframe(mut lf: LazyFrame) -> Result<(usize, usize)> {
    let cols = lf.collect_schema()?.len();
    let rows = count_rows_lazy(lf)?;
    Ok((rows, cols))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn test_pretty_print_shows_all_requested_rows_and_cols() -> Result<()> {
        // When a user runs `tt sample 20` on a 14-column file, polars' own defaults
        // (10 rows, 8 cols) truncate the output -- user asked for 20, sees 10. Lock in
        // that our display-config override actually takes effect.
        configure_display();

        let ids: Vec<i64> = (0..20).collect();
        let mut cols: Vec<Column> = (0..14)
            .map(|c| Column::new(format!("c{}", c).into(), ids.clone()))
            .collect();
        cols[0] = Column::new(PlSmallStr::from_static("id"), ids);
        let df = DataFrame::new_infer_height(cols)?;

        let formatted = format!("{}", df);

        // Count data rows -- table lines that contain a digit (skipping header / separator / dtype rows).
        let data_rows = formatted
            .lines()
            .filter(|l| l.contains('│') && l.chars().any(|c| c.is_ascii_digit()))
            .count();
        assert!(
            data_rows >= 20,
            "expected ≥ 20 data rows in pretty output, got {}\n{}",
            data_rows,
            formatted,
        );

        // All 14 column headers should appear -- if cols are truncated, a `…` row separator shows up.
        for c in 0..14 {
            let header = format!("c{}", c);
            assert!(
                formatted.contains(&header) || header == "c0",
                "expected column header `{}` in pretty output, got:\n{}",
                header,
                formatted,
            );
        }

        Ok(())
    }

    #[test]
    fn test_stdin_tempfile_roundtrip() -> Result<()> {
        // stdin is represented in CLI as "-". `materialize_stdin_to_tempfile` buffers
        // bytes to a tempfile with the user-supplied format extension so the existing
        // lazy-scan pipeline works unchanged.
        let bytes = b"name,age\nAlice,30\nBob,25\n";
        let temp = materialize_stdin_to_tempfile("csv", &mut &bytes[..])?;

        let lf = read_to_lazyframe(temp.path().to_str().unwrap())?;
        let df = lf.collect()?;
        assert_eq!(df.height(), 2);
        assert_eq!(df.column("name")?.str()?.get(0), Some("Alice"));
        Ok(())
    }

    #[test]
    fn test_per_column_null_counts_single_pass() -> Result<()> {
        // Wide-file regression: lint used to run one collect() per column, so a 100-col
        // file triggered 100 separate streaming scans. Helper collapses this to one pass.
        // We test behavior (counts are right) -- the single-pass structure is in the helper.
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_per_column_nulls.csv");
        let mut f = fs::File::create(&test_file)?;
        writeln!(f, "name,age,city")?;
        writeln!(f, "Alice,30,NYC")?;
        writeln!(f, ",25,LA")?;          // name is null
        writeln!(f, "Charlie,,Chicago")?; // age is null
        writeln!(f, ",,")?;               // all three null
        drop(f);

        let mut lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let schema = lf.collect_schema()?;
        let counts = per_column_null_counts(lf, &schema)?;

        let by_name: std::collections::HashMap<_, _> = counts.into_iter().collect();
        assert_eq!(by_name.get("name"), Some(&2));
        assert_eq!(by_name.get("age"), Some(&2));
        assert_eq!(by_name.get("city"), Some(&1));

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_infer_schema_length_handles_late_type_disambiguator() -> Result<()> {
        // Polars' own default of 100 rows is too aggressive for real-world CSVs:
        // if the first N rows look int-like and row N+1 is a float, the column gets
        // typed as Int and the float becomes null / errors.
        // Lock in a default that's high enough to handle reasonable drift -- specifically
        // a float disambiguator at row 2500 in a 3000-row file.
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_infer_schema_late.csv");
        let mut f = fs::File::create(&test_file)?;
        writeln!(f, "id,price")?;
        for i in 0..3000 {
            if i == 2500 {
                writeln!(f, "{},7.5", i)?; // the one float
            } else {
                writeln!(f, "{},1", i)?;
            }
        }
        drop(f);

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let df = lf.collect()?;
        let price_dtype = df.column("price")?.dtype().clone();
        assert!(
            price_dtype.is_float(),
            "expected price to be inferred as float, got {:?} -- default infer_schema_length is too low",
            price_dtype,
        );

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_passthrough_skips_compressed_files() -> Result<()> {
        // `tt file.csv.gz` must decompress and emit CSV text, not copy the gzip bytes.
        // Polars auto-decompresses with the `decompress` feature, but the cat fast path
        // was bypassing polars for all files; this locks in that .gz / .zst are NOT eligible
        // for byte passthrough.
        assert!(passthrough_eligible("data.csv"));
        assert!(passthrough_eligible("data.tsv"));
        assert!(!passthrough_eligible("data.parquet"));
        assert!(!passthrough_eligible("data.pq"));
        assert!(!passthrough_eligible("data.csv.gz"));
        assert!(!passthrough_eligible("data.csv.zst"));
        assert!(!passthrough_eligible("data.tsv.gz"));
        assert!(!passthrough_eligible("data.jsonl.zst"));
        Ok(())
    }

    #[test]
    fn test_no_header_csv() -> Result<()> {
        // Files with no header row (e.g. raw symbol/id dumps) must be readable without
        // the first data row being silently consumed as column names. With has_header=false,
        // polars synthesizes `column_1`, `column_2`, ... which can be used in filters/selects.
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_no_header.csv");
        fs::write(&test_file, "A,1\nAA,2\nAAA,3\nAAAA,4\nAAAC,5\n")?;

        let (rows_with, cols_with) = count_shape_opts(test_file.to_str().unwrap(), true)?;
        assert_eq!(rows_with, 4, "with header: first row consumed as header");
        assert_eq!(cols_with, 2);

        let (rows_no, cols_no) = count_shape_opts(test_file.to_str().unwrap(), false)?;
        assert_eq!(rows_no, 5, "no header: all rows are data");
        assert_eq!(cols_no, 2);

        let lf = read_to_lazyframe_opts(test_file.to_str().unwrap(), false)?;
        let df = lf.collect()?;
        let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert_eq!(names, vec!["column_1", "column_2"]);
        assert_eq!(df.column("column_1")?.str()?.get(0), Some("A"));
        assert_eq!(df.column("column_2")?.i64()?.get(4), Some(5));

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_cat_decompresses_gzipped_csv() -> Result<()> {
        // End-to-end: read a gzipped CSV through `read_to_lazyframe` and verify polars
        // sees the *decompressed* rows, not the gzip-framed bytes.
        use flate2::Compression;
        use flate2::write::GzEncoder;

        let temp_dir = std::env::temp_dir();
        let gz_file = temp_dir.join("test_cat_decompress.csv.gz");

        let plain = b"name,age\nAlice,30\nBob,25\nCharlie,35\n";
        let out = fs::File::create(&gz_file)?;
        let mut encoder = GzEncoder::new(out, Compression::default());
        encoder.write_all(plain)?;
        encoder.finish()?;

        let lf = read_to_lazyframe(gz_file.to_str().unwrap())?;
        let df = lf.collect()?;
        assert_eq!(df.height(), 3, "polars should see 3 decompressed rows");
        assert_eq!(df.width(), 2);
        assert_eq!(df.column("name")?.str()?.get(1), Some("Bob"));

        fs::remove_file(gz_file)?;
        Ok(())
    }

    #[test]
    fn test_passthrough_copies_bytes_verbatim() -> Result<()> {
        // `tt file.csv` with no transforms and no -o must be byte-identical to cat(1).
        // If we ever route it through polars (schema infer → materialize → re-serialize)
        // the output might round-trip-clean but will be slow on large files, and trailing
        // whitespace / quoting / blank-line quirks can shift. This test locks in byte equality.
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_passthrough.csv");
        // Include quoted strings, decimals with trailing zeros, and a blank trailing line
        // so any parse-and-reserialize path would show visible drift.
        let content = "symbol,strike,note\n\"SPY\",440.000,\"with, comma\"\n\"QQQ\",350.500,\n";
        fs::write(&test_file, content)?;

        let mut buf = Vec::new();
        copy_file_to(test_file.to_str().unwrap(), &mut buf)?;

        assert_eq!(String::from_utf8(buf)?, content);

        fs::remove_file(test_file)?;
        Ok(())
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
            filter: Some("age > 50".to_string()),
            ..Cli::default()
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

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            filter: Some("city = 'NYC'".to_string()),
            ..Cli::default()
        };
        let lf = apply_transformations(lf, &cli)?;
        let (rows, cols) = count_lazyframe(lf)?;

        assert_eq!(rows, 2); // Alice and Charlie
        assert_eq!(cols, 3);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_no_transformations() {
        assert!(!Cli::default().has_transformations());
    }

    #[test]
    fn test_has_transformations() {
        let cli = Cli {
            filter: Some("age > 25".to_string()),
            ..Cli::default()
        };
        assert!(cli.has_transformations());
    }

    #[test]
    fn test_filter_with_limit() -> Result<()> {
        // Test that filter + limit doesn't process entire file
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_filter_limit.csv");
        create_test_csv(test_file.to_str().unwrap(), 1000)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            filter: Some("age > 25".to_string()),
            limit: Some(5),
            ..Cli::default()
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
            filter: Some("age > 50".to_string()),
            select: Some("name,age".to_string()),
            limit: Some(3),
            ..Cli::default()
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
    fn test_parse_command_bare_file_defaults_to_cat() -> Result<()> {
        let mut cmd = None;
        let mut files = vec!["file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Cat));
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_file_shifted_into_files() -> Result<()> {
        // "file.csv" in the command slot gets treated as a file path.
        let mut cmd = Some("file.csv".to_string());
        let mut files = vec![];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Cat));
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_path_with_slash_shifted_into_files() -> Result<()> {
        let mut cmd = Some("tmp/cusips".to_string());
        let mut files = vec![];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Cat));
        assert_eq!(files, vec!["tmp/cusips"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_unknown_bareword_errors() {
        // `tt less file.csv` -- "less" looks like a typo'd subcommand, not a path.
        let mut cmd = Some("less".to_string());
        let mut files = vec!["tmp/cusips.csv.gz".to_string()];
        let err = parse_command(&mut cmd, &mut files).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("Unknown subcommand"), "got: {}", msg);
        assert!(msg.contains("less"), "got: {}", msg);
    }

    #[test]
    fn test_parse_command_head_with_n() -> Result<()> {
        let mut cmd = Some("head".to_string());
        let mut files = vec!["5".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Head(5)));
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_head_default_n() -> Result<()> {
        let mut cmd = Some("head".to_string());
        let mut files = vec!["file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Head(10)));
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_tail_with_n() -> Result<()> {
        let mut cmd = Some("tail".to_string());
        let mut files = vec!["7".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Tail(7)));
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_sample_integer() -> Result<()> {
        let mut cmd = Some("sample".to_string());
        let mut files = vec!["100".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Sample(SampleSize::Count(100))));
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_sample_fraction() -> Result<()> {
        let mut cmd = Some("sample".to_string());
        let mut files = vec!["0.1".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        match command {
            Command::Sample(SampleSize::Frac(f)) => assert!((f - 0.1).abs() < 1e-9),
            other => panic!("expected Sample(Frac), got {:?}", other),
        }
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_sample_default() -> Result<()> {
        let mut cmd = Some("sample".to_string());
        let mut files = vec!["file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        assert!(matches!(command, Command::Sample(SampleSize::Count(10))));
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_sort_single_key() -> Result<()> {
        let mut cmd = Some("sort".to_string());
        let mut files = vec!["age".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        match command {
            Command::Sort(keys) => assert_eq!(keys, vec!["age"]),
            other => panic!("expected Sort, got {:?}", other),
        }
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_sort_comma_separated_keys() -> Result<()> {
        // `tt sort name,age file.csv` -- comma list parsed into multiple keys,
        // matching --select syntax.
        let mut cmd = Some("sort".to_string());
        let mut files = vec!["name,age".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        match command {
            Command::Sort(keys) => assert_eq!(keys, vec!["name", "age"]),
            other => panic!("expected Sort, got {:?}", other),
        }
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_sort_no_positional_keys() -> Result<()> {
        // `tt sort file.csv` -- first arg looks like a path, so no positional keys
        // consumed. Caller is expected to error if -k is also empty (validated in
        // run_view), but parsing itself succeeds.
        let mut cmd = Some("sort".to_string());
        let mut files = vec!["file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        match command {
            Command::Sort(keys) => assert!(keys.is_empty()),
            other => panic!("expected Sort, got {:?}", other),
        }
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_filter_with_expression() -> Result<()> {
        let mut cmd = Some("filter".to_string());
        let mut files = vec!["age > 25".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        match command {
            Command::Filter(expr) => assert_eq!(expr, "age > 25"),
            other => panic!("expected Filter, got {:?}", other),
        }
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_filter_expression_with_dots() -> Result<()> {
        // Filter expressions routinely contain dots (`name = 'foo.bar'`); the
        // first positional must be taken as the expression unconditionally,
        // not run through looks_like_path.
        let mut cmd = Some("filter".to_string());
        let mut files = vec!["name = 'foo.bar'".to_string(), "file.csv".to_string()];
        let command = parse_command(&mut cmd, &mut files)?;
        match command {
            Command::Filter(expr) => assert_eq!(expr, "name = 'foo.bar'"),
            other => panic!("expected Filter, got {:?}", other),
        }
        assert_eq!(files, vec!["file.csv"]);
        Ok(())
    }

    #[test]
    fn test_parse_command_filter_missing_expression_errors() {
        let mut cmd = Some("filter".to_string());
        let mut files: Vec<String> = vec![];
        let err = parse_command(&mut cmd, &mut files).unwrap_err();
        assert!(format!("{}", err).contains("filter requires an expression"));
    }

    #[test]
    fn test_inject_sort_appends_keys() -> Result<()> {
        // `tt sort age,name -k city file.csv` -- positional keys land *after*
        // any -k flags, so primary sort is by -k.
        let cli = Cli {
            sort_keys: vec!["city".to_string()],
            ..Cli::default()
        };
        let cmd = Command::Sort(vec!["age".to_string(), "name".to_string()]);
        let out = inject_view_command_args(&cmd, cli)?;
        assert_eq!(out.sort_keys, vec!["city", "age", "name"]);
        Ok(())
    }

    #[test]
    fn test_inject_sort_errors_when_no_keys_anywhere() {
        let cli = Cli::default();
        let cmd = Command::Sort(vec![]);
        let err = inject_view_command_args(&cmd, cli).unwrap_err();
        assert!(format!("{}", err).contains("sort requires a column"));
    }

    #[test]
    fn test_inject_sort_accepts_keys_from_dash_k_alone() -> Result<()> {
        let cli = Cli {
            sort_keys: vec!["age".to_string()],
            ..Cli::default()
        };
        let cmd = Command::Sort(vec![]);
        let out = inject_view_command_args(&cmd, cli)?;
        assert_eq!(out.sort_keys, vec!["age"]);
        Ok(())
    }

    #[test]
    fn test_inject_filter_sets_expression() -> Result<()> {
        let cli = Cli::default();
        let cmd = Command::Filter("age > 25".to_string());
        let out = inject_view_command_args(&cmd, cli)?;
        assert_eq!(out.filter.as_deref(), Some("age > 25"));
        Ok(())
    }

    #[test]
    fn test_inject_filter_conflicts_with_global_filter() {
        let cli = Cli {
            filter: Some("age > 30".to_string()),
            ..Cli::default()
        };
        let cmd = Command::Filter("age > 25".to_string());
        let err = inject_view_command_args(&cmd, cli).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("filter expression set twice"), "got: {}", msg);
    }

    #[test]
    fn test_sample_is_random() -> Result<()> {
        // Test that sample actually randomizes
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_sample_random.csv");
        create_test_csv(test_file.to_str().unwrap(), 100)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;

        // Run sample twice and verify results are different
        let df1 = lf.clone().collect()?;
        let sample1 = {
            let total_rows = df1.height();
            use rand::seq::index::sample;
            use rand::rng;
            let random_indices = sample(&mut rng(), total_rows, 10);
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
            use rand::rng;
            let random_indices = sample(&mut rng(), total_rows, 10);
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
    fn test_sample_uniform_over_full_range() -> Result<()> {
        // Sampling must draw uniformly across the whole file, not just from a prefix window.
        // create_test_csv writes age = 20 + i, so row idx i corresponds to age 20+i.
        // With 120k rows and a sample of 100, any "sample from first 100k" impl will
        // never produce an age > 100_019. A truly uniform sample almost certainly will.
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_sample_uniform.csv");
        create_test_csv(test_file.to_str().unwrap(), 120_000)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let sampled = random_sample(lf, 100)?;
        assert_eq!(sampled.height(), 100);

        let max_age = sampled.column("age")?.i64()?.max().unwrap_or(i64::MIN);
        assert!(
            max_age > 100_019,
            "sample should span the full file, but max age was {} (≤ 100_019 means sampling is biased to the first 100k rows)",
            max_age,
        );

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
            filter: Some("age > 25".to_string()),      // filter references age
            select: Some("name,city".to_string()),     // but age is not selected
            ..Cli::default()
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
            sort_keys: vec!["age".to_string()],
            ..Cli::default()
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
            sort_keys: vec!["value".to_string()],
            reverse: true,
            ..Cli::default()
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
            unique: true,
            ..Cli::default()
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
            unique_on: Some("name".to_string()),
            ..Cli::default()
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
            drop: Some("age,city".to_string()),
            ..Cli::default()
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
            filter: Some("age > 30".to_string()),
            select: Some("name,age".to_string()),
            ..Cli::default()
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
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_stats.csv");

        create_test_csv(test_file.to_str().unwrap(), 100)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let stats_df = compute_stats_lazy(lf)?;

        // name (string) + age (numeric) + city (string) → one stats row for age.
        assert_eq!(stats_df.height(), 1);
        // column, count, null_count, mean, std, min, median, max.
        assert_eq!(stats_df.width(), 8);

        fs::remove_file(test_file)?;
        Ok(())
    }

    #[test]
    fn test_stats_with_filter() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_stats_filter.csv");

        create_test_csv(test_file.to_str().unwrap(), 100)?;

        let lf = read_to_lazyframe(test_file.to_str().unwrap())?;
        let cli = Cli {
            filter: Some("age > 50".to_string()),
            select: Some("age".to_string()),
            ..Cli::default()
        };

        let lf = apply_transformations(lf, &cli)?;
        let stats_df = compute_stats_lazy(lf)?;
        assert_eq!(stats_df.height(), 1);

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
            filter: Some("age > 30".to_string()),
            select: Some("name,age".to_string()),
            limit: Some(100),
            ..Cli::default()
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

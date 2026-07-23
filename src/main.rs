mod dedup;
mod deps;
mod encoding;
mod join;
mod ready;
mod ranking;
pub mod render;
mod tui;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use csv::{Reader, Writer};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use crate::deps::{find_functional_dependencies, print_report as print_fd_report, FdConfig};
use crate::join::{execute_join, find_join_candidates, print_join_report, print_plan_report, JoinConfig, JoinMode};
use crate::ranking::{
    compute_profiles, rank_columns, reorder_data, sort_rows_canonical, validate_cardinality_order,
    validate_column_order, validate_sorted, write_schema, RankingOptions, Schema, TypeHint,
};
// Re-export table types for pipeline operations (Phase 7)
pub use rsf::table::{Column, Expr, FieldValue, TypedTable};
/// RSF - Ranked Spreadsheet Format
///
/// Deterministic column ordering based on cardinality.
/// Columns are ranked from most unique (highest cardinality) to least unique.
#[derive(Parser)]
#[command(name = "rsf")]
#[command(about = "Ranked Spreadsheet Format - Stable scaffolding for tabular data", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Rank a CSV file by column cardinality
    Rank {
        /// Input CSV file (use - for stdin)
        #[arg(default_value = "-")]
        input: String,

        /// Output file (defaults to stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Generate schema.yaml file
        #[arg(short, long)]
        schema: bool,

        /// Count nulls as distinct values
        #[arg(long, default_value = "true")]
        nulls_distinct: bool,
    },

    /// Validate an RSF file
    Validate {
        /// RSF CSV file to validate
        input: PathBuf,

        /// Schema file (defaults to input.schema.yaml)
        #[arg(short, long)]
        schema: Option<PathBuf>,
    },

    /// Show cardinality statistics for a CSV
    Stats {
        /// Input CSV file
        input: PathBuf,
    },

    /// Detect functional dependencies in a CSV file
    Deps {
        /// Input CSV file (use - for stdin)
        #[arg(default_value = "-")]
        input: String,

        /// Treat empty strings as null values
        #[arg(long)]
        treat_empty_as_null: bool,
    },

    /// Detect and remove duplicate rows
    Dedup {
        /// Input CSV file (use - for stdin)
        #[arg(default_value = "-")]
        input: String,

        /// Output file (defaults to stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Number of top key columns to group by (default: 3)
        #[arg(long, default_value_t = 3)]
        keys: usize,
    },

    /// Join two RSF files on a common key column
    Join {
        /// First input CSV file
        #[arg(short, long)]
        left: String,

        /// Second input CSV file
        #[arg(short = 'r', long)]
        right: String,

        /// Output file (defaults to stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Join mode: inner, left, full_outer (default: inner)
        #[arg(long, default_value = "inner")]
        mode: String,

        /// Floating-point tolerance for near-match on numeric columns (default: 0.01)
        #[arg(long, default_value_t = 0.01)]
        tolerance: f64,
    },

    /// Convert production Excel export to RSF-ready format
    Ready {
        /// Input CSV file (UTF-16 or UTF-8)
        input: String,

        /// Output file (defaults to <input>_rsf_ready.csv)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Open a CSV file and output typed YAML for pipeline operations
    Open {
        /// Input CSV file (use - for stdin)
        #[arg(default_value = "-")]
        input: String,
    },

    /// Filter rows using typed expressions (e.g., `Status = "Released"`, `Amount > 100`)
    Where {
        /// Expression to filter by (use quotes for strings)
        expr: String,

        /// Input file (YAML from pipeline or CSV; use - for stdin)
        #[arg(default_value = "-")]
        input: String,

        /// Output format: yaml (default), csv
        #[arg(long, default_value = "yaml")]
        format: String,
    },

    /// Project specific columns with type preservation
    Select {
        /// Comma-separated list of column names to keep (e.g., "Name,City")
        #[arg(short, long)]
        columns: String,

        /// Input file (YAML from pipeline or CSV; use - for stdin)
        #[arg(default_value = "-")]
        input: String,

        /// Output format: yaml (default), csv
        #[arg(long, default_value = "yaml")]
        format: String,
    },

    /// Sort rows by a column (ascending)
    Sort {
        /// Column name to sort by
        column: String,

        /// Input file (YAML from pipeline or CSV; use - for stdin)
        #[arg(default_value = "-")]
        input: String,

        /// Output format: yaml (default), csv
        #[arg(long, default_value = "yaml")]
        format: String,
    },

    /// Open a CSV file in an interactive table viewer TUI
    View {
        /// Input CSV file (use - for stdin)
        #[arg(default_value = "-")]
        input: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Rank {
            input,
            output,
            schema,
            nulls_distinct,
        } => {
            let (headers, rows) = read_csv(&input)?;
            let options = ranking_options(nulls_distinct);
            let ranked_columns =
                rank_columns(&headers, &rows, options).map_err(|e| anyhow::anyhow!("{}", e))?;

            // Reorder data
            let (new_headers, new_rows) =
                reorder_data(&headers, &rows, &ranked_columns).map_err(|e| anyhow::anyhow!("{}", e))?;

            // Sort rows canonically
            let sorted_rows = sort_rows_canonical(&new_rows);

            // Write output
            write_csv(&new_headers, &sorted_rows, output.as_deref())?;

            // Generate schema if requested
            if schema {
                let schema_path = output
                    .as_ref()
                    .map(|p| PathBuf::from(format!("{}.schema.yaml", p.display())))
                    .unwrap_or_else(|| PathBuf::from("output.schema.yaml"));

                write_schema(&ranked_columns, &schema_path).map_err(|e| anyhow::anyhow!("{}", e))?;
                eprintln!("Schema written to: {}", schema_path.display());
            }

            // Print stats to stderr
            eprintln!("\n=== RSF Ranking Complete ===");
            eprintln!("Columns ranked by cardinality (highest → lowest):\n");
            for (rank, col) in ranked_columns.iter().enumerate() {
                eprintln!(
                    "  {}. {} (cardinality: {})",
                    rank + 1,
                    col.name,
                    col.cardinality
                );
            }
            eprintln!("\nRows sorted canonically by key columns.");
        }

        Commands::Validate { input, schema } => {
            let schema_path = schema.unwrap_or_else(|| {
                let mut p = input.clone();
                p.set_extension("schema.yaml");
                p
            });

            validate_rsf(&input, &schema_path)?;
            println!("✓ Valid RSF file");
        }

        Commands::Stats { input } => {
            let (headers, rows) = read_csv_file(&input)?;
            // For profiling, treat empty strings as nulls so we can report null%
            let options = RankingOptions {
                treat_empty_as_null: true,
                include_nulls: false,
            };
            let stats = rank_columns(&headers, &rows, options).map_err(|e| anyhow::anyhow!("{}", e))?;

            println!("\n=== Column Statistics ===\n");
            println!(
                "{:<20} {:>10} {:>8} {:>8}  {}",
                "Column", "Cardinality", "Null%", "Unique%", "Type"
            );
            println!("{}", "-".repeat(60));

            for stat in &stats {
                let null_pct = stat.null_pct.map(|p| format!("{:.1}", p)).unwrap_or_else(|| "-".to_string());
                let unique_pct = stat.unique_pct.map(|p| format!("{:.1}", p)).unwrap_or_else(|| "-".to_string());
                let type_hint = match &stat.type_hint {
                    Some(TypeHint::Unknown) => "text",
                    Some(th) => {
                        let s = format!("{:?}", th);
                        // Strip the TypeHint:: prefix and quotes for display
                        if s.contains("Id(") {
                            &s.replace("TypeHint::", "").replace("\"", "")
                        } else {
                            &s.replace("TypeHint::", "")
                        }
                    },
                    None => "text",
                };

                let const_marker = if stat.is_constant == Some(true) { "*" } else { "" };

                println!(
                    "{:<20} {:>10} {:>8} {:>8}  {}{}",
                    stat.name, stat.cardinality, null_pct, unique_pct, type_hint, const_marker
                );
            }

            println!("\n* = constant column (all non-null values are the same)");
        }

        Commands::Deps { input, treat_empty_as_null } => {
            let (headers, rows) = read_input(&input)?;

            // Compute column profiles for FD analysis.
            let options = RankingOptions {
                treat_empty_as_null: true,
                include_nulls: false,
            };
            let profiles = compute_profiles(&headers, &rows, options).map_err(|e| anyhow::anyhow!("{}", e))?;

            let config = FdConfig {
                treat_empty_as_null,
            };

            let deps = find_functional_dependencies(&headers, &rows, &profiles, &config);
            print_fd_report(&deps);
        }

        Commands::Dedup { input, output, keys } => {
            let (headers, rows) = read_csv(&input)?;

            let config = dedup::DedupConfig {
                key_columns: keys,
                trim_whitespace: true,
            };
            let result = dedup::find_duplicates(&headers, &rows, &config).map_err(|e| anyhow::anyhow!("{}", e))?;

            // Print report to stderr
            let key_indices = dedup::determine_key_columns_for_report(&headers, &rows, keys);
            dedup::print_report(&result, &headers, &key_indices);

            // Write cleaned data to stdout or file
            write_csv(&headers, &result.cleaned_data, output.as_deref())?;
        }

        Commands::Join { left, right, output, mode, tolerance } => {
            let (left_headers, left_rows) = read_csv(&left)?;
            let (right_headers, right_rows) = read_csv(&right)?;

            // Parse join mode.
            let join_mode = match mode.as_str() {
                "inner" => JoinMode::Inner,
                "left" => JoinMode::Left,
                "full_outer" | "full" => JoinMode::FullOuter,
                other => anyhow::bail!("Invalid join mode '{}'. Use: inner, left, full_outer", other),
            };

            // Compute column profiles for both files.
            let options = RankingOptions {
                treat_empty_as_null: true,
                include_nulls: false,
            };
            let left_profiles = compute_profiles(&left_headers, &left_rows, options).map_err(|e| anyhow::anyhow!("{}", e))?;
            let right_profiles = compute_profiles(&right_headers, &right_rows, options).map_err(|e| anyhow::anyhow!("{}", e))?;

            // Print join plan to stderr.
            let plan = find_join_candidates(&left_headers, &left_profiles, &right_headers, &right_profiles);
            print_plan_report(&plan, &left, &right);

            if plan.candidates.is_empty() {
                anyhow::bail!("No suitable join candidates found between these files.");
            }

            let config = JoinConfig {
                mode: join_mode,
                float_tolerance: tolerance,
            };

            // Execute the join.
            match execute_join(
                &left_headers, &left_rows, &left_profiles,
                &right_headers, &right_rows, &right_profiles,
                &config,
            ) {
                Ok(result) => {
                    print_join_report(&result, &left, &right);

                    write_csv(&result.output_headers, &result.output_rows_data, output.as_deref())?;
                }
                Err(e) => anyhow::bail!("{}", e),
            }
        }

        Commands::Ready { input, output } => {
            use crate::ready::{make_rsf_ready, ReadyConfig};

            let config = ReadyConfig::default();

            make_rsf_ready(&PathBuf::from(&input), output.as_deref(), config)?;
        }

        Commands::Open { input } => {
            // Read CSV data (file or stdin)
            let (headers, rows) = read_csv(&input)?;

            // Compute column profiles for type inference
            let options = RankingOptions {
                treat_empty_as_null: true,
                include_nulls: false,
            };
            let profiles = compute_profiles(&headers, &rows, options).map_err(|e| anyhow::anyhow!("{}", e))?;

            // Build typed table and output as YAML for pipeline consumption
            let table = TypedTable::from_untyped(&headers, &rows, &profiles);
            println!("{}", table.to_yaml());
        }

        Commands::Where { expr, input, format } => {
            let table = load_typed_table(&input)?;

            // Parse and evaluate the expression
            let column_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
            let parsed_expr = Expr::parse(&expr, &column_names).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;

            // Apply filter and output
            let result = table.where_clause(&parsed_expr);
            match format.as_str() {
                "csv" => print!("{}", result.to_csv()),
                _ => println!("{}", result.to_yaml()),
            }
        }

        Commands::Select { columns, input, format } => {
            let table = load_typed_table(&input)?;

            // Parse comma-separated column names and find indices (case-insensitive match)
            let requested_cols: Vec<&str> = columns.split(',').map(|s| s.trim()).collect();
            let col_lower: Vec<String> = requested_cols.iter().map(|s| s.to_lowercase()).collect();
            let indices: Vec<usize> = table.columns.iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    if col_lower.contains(&c.name.to_lowercase()) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();

            // Validate all requested columns were found
            let matched: Vec<String> = indices.iter().map(|i| table.column_name(*i).unwrap()).map(String::from).collect();
            for col in &requested_cols {
                if !matched.iter().any(|m| m.to_lowercase() == col.to_lowercase()) {
                    anyhow::bail!("Column '{}' not found. Available: {}", col, matched.join(", "));
                }
            }

            let result = table.select_columns(&indices);
            match format.as_str() {
                "csv" => print!("{}", result.to_csv()),
                _ => println!("{}", result.to_yaml()),
            }
        }

        Commands::Sort { column, input, format } => {
            let table = load_typed_table(&input)?;

            // Find column index by name (case-insensitive match)
            let col_idx = table.columns.iter()
                .position(|c| c.name.to_lowercase() == column.to_lowercase())
                .ok_or_else(|| {
                    let available: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
                    anyhow::anyhow!("Column '{}' not found. Available: {}", column, available.join(", "))
                })?;

            let result = table.sort_by_column(col_idx);
            match format.as_str() {
                "csv" => print!("{}", result.to_csv()),
                _ => println!("{}", result.to_yaml()),
            }
        }

        Commands::View { input } => {
            // Read CSV data (file or stdin)
            let (headers, rows) = read_csv(&input)?;

            // Compute column profiles for type inference
            let options = RankingOptions {
                treat_empty_as_null: true,
                include_nulls: false,
            };
            let profiles = compute_profiles(&headers, &rows, options).map_err(|e| anyhow::anyhow!("{}: {}", e, input))?;

            // Build typed table and launch TUI
            let table = TypedTable::from_untyped(&headers, &rows, &profiles);
            tui::run_tui(table)?;
        }
    }

    Ok(())
}

/// Read input from a file path or stdin (CSV).
fn read_csv(input: &str) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    if input == "-" {
        read_csv_reader(io::stdin())
    } else {
        read_csv_file(&PathBuf::from(input))
    }
}

/// Read input from a file path or stdin, trying YAML (pipeline) first then CSV.
fn read_input(input: &str) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    if input == "-" {
        // Read stdin once into memory — we may try two parsers on it
        let stdin_data = io::read_to_string(io::stdin())?;

        // Try YAML first (pipeline mode from `rsf open`)
        if let Ok(table) = TypedTable::from_yaml(&stdin_data) {
            let headers: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
            let rows: Vec<Vec<String>> = table.rows.iter()
                .map(|row| row.iter().map(|v| v.as_str()).collect())
                .collect();
            return Ok((headers, rows));
        }

        // Fall back to CSV parsing on the same data
        read_csv_reader(io::Cursor::new(stdin_data))
    } else {
        read_csv_file(&PathBuf::from(input))
    }
}

fn read_csv_file(path: &PathBuf) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let file = File::open(path).with_context(|| format!("Failed to open file: {:?}", path))?;
    read_csv_reader(BufReader::new(file))
}

/// Maximum number of columns allowed (prevents OOM on malformed data).
const MAX_COLUMNS: usize = 10_000;
/// Maximum number of rows to load into memory.
const MAX_ROWS: usize = 5_000_000;

fn read_csv_reader<R: io::Read>(reader: R) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut csv_reader = Reader::from_reader(reader);

    let headers: Vec<String> = csv_reader
        .headers()?
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Check column count limit early.
    if headers.len() > MAX_COLUMNS {
        anyhow::bail!(
            "Input has {} columns, exceeds maximum of {}",
            headers.len(),
            MAX_COLUMNS
        );
    }

    let rows: Result<Vec<Vec<String>>> = csv_reader
        .records()
        .enumerate()
        .map(|(i, result)| {
            if i >= MAX_ROWS {
                anyhow::bail!(
                    "Input has {} rows, exceeds maximum of {}. Use filtering to reduce input size.",
                    i + 1,
                    MAX_ROWS
                );
            }
            result
                .map(|record| record.iter().map(|s| s.to_string()).collect())
                .context("Failed to read CSV record")
        })
        .collect();

    Ok((headers, rows?))
}

/// Load YAML (pipeline) or CSV input into a TypedTable, trying YAML first then falling back to CSV parsing.
fn load_typed_table(input: &str) -> Result<TypedTable> {
    if input == "-" {
        let stdin_data = io::read_to_string(io::stdin())?;
        match TypedTable::from_yaml(&stdin_data) {
            Ok(t) => Ok(t),
            Err(_) => {
                let (headers, rows) = read_csv_reader(io::Cursor::new(stdin_data))?;
                let options = RankingOptions {
                    treat_empty_as_null: true,
                    include_nulls: false,
                };
                let profiles = compute_profiles(&headers, &rows, options)?;
                Ok(TypedTable::from_untyped(&headers, &rows, &profiles))
            }
        }
    } else {
        let file_data = std::fs::read_to_string(&input)?;
        match TypedTable::from_yaml(&file_data) {
            Ok(t) => Ok(t),
            Err(_) => {
                let (headers, rows) = read_csv_file(&PathBuf::from(&input))?;
                let options = RankingOptions {
                    treat_empty_as_null: true,
                    include_nulls: false,
                };
                let profiles = compute_profiles(&headers, &rows, options)?;
                Ok(TypedTable::from_untyped(&headers, &rows, &profiles))
            }
        }
    }
}

fn ranking_options(nulls_distinct: bool) -> RankingOptions {
    if nulls_distinct {
        RankingOptions {
            treat_empty_as_null: false,
            include_nulls: true,
        }
    } else {
        RankingOptions {
            treat_empty_as_null: true,
            include_nulls: true,
        }
    }
}

fn write_csv(headers: &[String], rows: &[Vec<String>], output: Option<&Path>) -> Result<()> {
    let writer: Box<dyn io::Write> = if let Some(path) = output {
        Box::new(File::create(path)?)
    } else {
        Box::new(io::stdout())
    };

    let mut csv_writer = Writer::from_writer(writer);

    csv_writer.write_record(headers)?;

    for row in rows {
        csv_writer.write_record(row)?;
    }

    csv_writer.flush()?;
    Ok(())
}

fn validate_rsf(csv_path: &PathBuf, schema_path: &PathBuf) -> Result<()> {
    // Read schema
    let schema_file = File::open(schema_path)
        .with_context(|| format!("Failed to open schema: {:?}", schema_path))?;
    let schema: Schema = serde_yaml::from_reader(schema_file)?;

    // Read CSV
    let (headers, rows) = read_csv_file(csv_path)?;

    validate_column_order(&headers, &schema.columns).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Validate ranks are sequential
    for (idx, col_meta) in schema.columns.iter().enumerate() {
        if col_meta.rank != idx + 1 {
            anyhow::bail!(
                "Column '{}' has invalid rank: expected {}, found {}",
                col_meta.name,
                idx + 1,
                col_meta.rank
            );
        }
    }

    let options = ranking_options(true);
    validate_cardinality_order(&headers, &rows, &schema.columns, options)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    validate_sorted(&rows).map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}

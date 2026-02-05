mod errors;
mod ranking;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use csv::{Reader, Writer};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use crate::errors::IntoAnyhow;
use crate::ranking::{
    rank_columns, reorder_data, sort_rows_canonical, validate_cardinality_order,
    validate_column_order, validate_sorted, write_schema, RankingOptions, Schema,
};

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
                rank_columns(&headers, &rows, options).map_err(IntoAnyhow::into_anyhow)?;

            // Reorder data
            let (new_headers, new_rows) =
                reorder_data(&headers, &rows, &ranked_columns).map_err(IntoAnyhow::into_anyhow)?;

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

                write_schema(&ranked_columns, &schema_path).map_err(IntoAnyhow::into_anyhow)?;
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
            let options = ranking_options(true);
            let stats = rank_columns(&headers, &rows, options).map_err(IntoAnyhow::into_anyhow)?;

            println!("\n=== Column Statistics ===\n");
            println!("{:<20} {:>12}", "Column", "Cardinality");
            println!("{}", "-".repeat(34));

            for stat in stats {
                println!("{:<20} {:>12}", stat.name, stat.cardinality);
            }
        }
    }

    Ok(())
}

fn read_csv(input: &str) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    if input == "-" {
        read_csv_reader(io::stdin())
    } else {
        read_csv_file(&PathBuf::from(input))
    }
}

fn read_csv_file(path: &PathBuf) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let file = File::open(path).with_context(|| format!("Failed to open file: {:?}", path))?;
    read_csv_reader(BufReader::new(file))
}

fn read_csv_reader<R: io::Read>(reader: R) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut csv_reader = Reader::from_reader(reader);

    let headers = csv_reader
        .headers()?
        .iter()
        .map(|s| s.to_string())
        .collect();

    let rows: Result<Vec<Vec<String>>> = csv_reader
        .records()
        .map(|result| {
            result
                .map(|record| record.iter().map(|s| s.to_string()).collect())
                .context("Failed to read CSV record")
        })
        .collect();

    Ok((headers, rows?))
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

    validate_column_order(&headers, &schema.columns).map_err(IntoAnyhow::into_anyhow)?;

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
        .map_err(IntoAnyhow::into_anyhow)?;

    validate_sorted(&rows).map_err(IntoAnyhow::into_anyhow)?;

    Ok(())
}

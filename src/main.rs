use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use csv::{Reader, Writer};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnMeta {
    name: String,
    rank: usize,
    cardinality: usize,
    #[serde(rename = "type")]
    col_type: ColumnType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ColumnType {
    Key,
    Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct Schema {
    version: String,
    columns: Vec<ColumnMeta>,
}

struct ColumnStats {
    name: String,
    cardinality: usize,
    distinct_values: HashSet<String>,
}

impl ColumnStats {
    fn new(name: String) -> Self {
        Self {
            name,
            cardinality: 0,
            distinct_values: HashSet::new(),
        }
    }

    fn add_value(&mut self, value: &str) {
        self.distinct_values.insert(value.to_string());
        self.cardinality = self.distinct_values.len();
    }
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
            let stats = compute_cardinality(&headers, &rows, nulls_distinct);
            let ranked_columns = rank_columns(&stats);

            // Reorder data
            let (new_headers, new_rows) = reorder_data(&headers, &rows, &ranked_columns);

            // Sort rows canonically
            let sorted_rows = sort_rows_canonical(&new_rows);

            // Write output
            write_csv(&new_headers, &sorted_rows, output.as_deref())?;

            // Generate schema if requested
            if schema {
                let schema_path = output
                    .as_ref()
                    .and_then(|p| p.to_str())
                    .map(|s| format!("{}.schema.yaml", s))
                    .unwrap_or_else(|| "output.schema.yaml".to_string());

                write_schema(&ranked_columns, &schema_path)?;
                eprintln!("Schema written to: {}", schema_path);
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
            let stats = compute_cardinality(&headers, &rows, true);

            println!("\n=== Column Statistics ===\n");
            println!("{:<20} {:>12}", "Column", "Cardinality");
            println!("{}", "-".repeat(34));

            let mut sorted_stats = stats;
            sorted_stats.sort_by(|a, b| b.cardinality.cmp(&a.cardinality));

            for stat in sorted_stats {
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

fn compute_cardinality(
    headers: &[String],
    rows: &[Vec<String>],
    nulls_distinct: bool,
) -> Vec<ColumnStats> {
    let mut stats: Vec<ColumnStats> = headers
        .iter()
        .map(|name| ColumnStats::new(name.clone()))
        .collect();

    for row in rows {
        for (i, value) in row.iter().enumerate() {
            let val = if value.trim().is_empty() && !nulls_distinct {
                "NULL"
            } else {
                value
            };

            if let Some(stat) = stats.get_mut(i) {
                stat.add_value(val);
            }
        }
    }

    stats
}

fn rank_columns(stats: &[ColumnStats]) -> Vec<ColumnMeta> {
    let mut columns: Vec<ColumnMeta> = stats
        .iter()
        .enumerate()
        .map(|(idx, stat)| ColumnMeta {
            name: stat.name.clone(),
            rank: idx,
            cardinality: stat.cardinality,
            col_type: ColumnType::Key, // We'll mark as Value later if needed
        })
        .collect();

    // Sort by cardinality (descending), then by original position (stable)
    columns.sort_by(|a, b| b.cardinality.cmp(&a.cardinality).then(a.rank.cmp(&b.rank)));

    // Update ranks
    for (new_rank, col) in columns.iter_mut().enumerate() {
        col.rank = new_rank + 1;
    }

    columns
}

fn reorder_data(
    headers: &[String],
    rows: &[Vec<String>],
    ranked_columns: &[ColumnMeta],
) -> (Vec<String>, Vec<Vec<String>>) {
    // Create mapping from old position to new position
    let mut old_to_new: HashMap<usize, usize> = HashMap::new();

    for (new_idx, col) in ranked_columns.iter().enumerate() {
        if let Some(old_idx) = headers.iter().position(|h| h == &col.name) {
            old_to_new.insert(old_idx, new_idx);
        }
    }

    // Reorder headers
    let new_headers: Vec<String> = ranked_columns.iter().map(|col| col.name.clone()).collect();

    // Reorder rows
    let new_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            let mut new_row = vec![String::new(); row.len()];
            for (old_idx, value) in row.iter().enumerate() {
                if let Some(&new_idx) = old_to_new.get(&old_idx) {
                    new_row[new_idx] = value.clone();
                }
            }
            new_row
        })
        .collect();

    (new_headers, new_rows)
}

fn sort_rows_canonical(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut sorted = rows.to_vec();

    // Sort lexicographically by all columns in order
    sorted.sort_by(|a, b| {
        for (val_a, val_b) in a.iter().zip(b.iter()) {
            match val_a.cmp(val_b) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }
        std::cmp::Ordering::Equal
    });

    sorted
}

fn write_csv(
    headers: &[String],
    rows: &[Vec<String>],
    output: Option<&std::path::Path>,
) -> Result<()> {
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

fn write_schema(columns: &[ColumnMeta], path: &str) -> Result<()> {
    let schema = Schema {
        version: "0.1".to_string(),
        columns: columns.to_vec(),
    };

    let file = File::create(path)?;
    serde_yaml::to_writer(file, &schema)?;

    Ok(())
}

fn validate_rsf(csv_path: &PathBuf, schema_path: &PathBuf) -> Result<()> {
    // Read schema
    let schema_file = File::open(schema_path)
        .with_context(|| format!("Failed to open schema: {:?}", schema_path))?;
    let schema: Schema = serde_yaml::from_reader(schema_file)?;

    // Read CSV
    let (headers, rows) = read_csv_file(csv_path)?;

    // Validate column order matches schema
    for (idx, col_meta) in schema.columns.iter().enumerate() {
        if idx >= headers.len() {
            anyhow::bail!("Schema has more columns than CSV");
        }

        if headers[idx] != col_meta.name {
            anyhow::bail!(
                "Column order mismatch at position {}: expected '{}', found '{}'",
                idx,
                col_meta.name,
                headers[idx]
            );
        }
    }

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

    // Validate cardinality ordering
    let stats = compute_cardinality(&headers, &rows, true);
    for window in schema.columns.windows(2) {
        let curr = &window[0];
        let next = &window[1];

        let curr_actual = stats.iter().find(|s| s.name == curr.name).unwrap();
        let next_actual = stats.iter().find(|s| s.name == next.name).unwrap();

        if curr_actual.cardinality < next_actual.cardinality {
            eprintln!(
                "Warning: Column '{}' (card: {}) ranks higher than '{}' (card: {})",
                curr.name, curr_actual.cardinality, next.name, next_actual.cardinality
            );
        }
    }

    // Validate rows are sorted
    let sorted = sort_rows_canonical(&rows);
    if sorted != rows {
        anyhow::bail!("Rows are not in canonical sorted order");
    }

    Ok(())
}

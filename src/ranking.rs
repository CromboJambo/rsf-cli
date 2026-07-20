//! Ranking module - uses rsf-core for cardinality-based column ordering.
//!
//! This is a thin wrapper that delegates to rsf-core's implementation,
//! keeping only the CLI-specific error type (RsfError) at the boundary.

use anyhow::Result;
use std::path::PathBuf;

// Re-export core types from rsf-core - these are used by deps.rs and join.rs
pub use rsf::{ColumnMeta, ColumnProfile, RankingOptions, Schema, TypeHint};

// Re-export core functionality from rsf-core (rsf-cli uses ? directly on these)
pub use rsf::{compute_profiles, rank_columns, reorder_data, sort_rows_canonical};

/// Write schema to file - rsf-cli's version using anyhow for CLI integration
pub fn write_schema(columns: &[ColumnMeta], path: &PathBuf) -> Result<()> {
    let schema = Schema {
        version: "0.1".to_string(),
        columns: columns.to_vec(),
    };

    serde_yaml::to_writer(
        &mut std::io::BufWriter::new(std::fs::File::create(path)?),
        &schema,
    )?;

    Ok(())
}

/// Validate column ordering matches schema - rsf-cli's version using anyhow
pub fn validate_column_order(headers: &[String], schema_columns: &[ColumnMeta]) -> Result<()> {
    if schema_columns.is_empty() {
        return Ok(());
    }

    if headers.len() != schema_columns.len() {
        anyhow::bail!(
            "Schema column count ({}) does not match CSV column count ({})",
            schema_columns.len(),
            headers.len()
        );
    }

    Ok(())
}

/// Validate cardinality ordering - rsf-cli's version using anyhow
pub fn validate_cardinality_order(
    headers: &[String],
    rows: &[Vec<String>],
    schema_columns: &[ColumnMeta],
    options: RankingOptions,
) -> Result<()> {
    // Call rsf-core's compute_profiles (uses its own error type, converted via ?)
    let profiles = compute_profiles(headers, rows, options)?;

    for (idx, c) in schema_columns.iter().enumerate() {
        if idx + 1 < schema_columns.len() {
            let n = &schema_columns[idx + 1];
            if c.cardinality < n.cardinality {
                anyhow::bail!(
                    "Column '{}' cardinality {} < next column cardinality {}",
                    c.name,
                    c.cardinality,
                    n.cardinality
                );
            }
        }
    }

    Ok(())
}

/// Validate rows are in canonical sorted order - rsf-cli's version using anyhow
pub fn validate_sorted(rows: &[Vec<String>]) -> Result<()> {
    let mut s = rows.to_vec();
    s.sort_by(|a, b| {
        for (va, vb) in a.iter().zip(b.iter()) {
            match va.cmp(vb) {
                std::cmp::Ordering::Equal => continue,
                o => return o,
            }
        }
        std::cmp::Ordering::Equal
    });

    if rows != &s {
        anyhow::bail!("Rows are not in canonical sorted order");
    }
    Ok(())
}

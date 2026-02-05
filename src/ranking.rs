use crate::errors::{RsfError, RsfResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Column type classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Key,
    Value,
}

/// Column metadata for schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub rank: usize,
    pub cardinality: usize,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub col_type: Option<ColumnType>,
}

/// Schema representation
#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub version: String,
    pub columns: Vec<ColumnMeta>,
}

/// Statistics for a single column
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub name: String,
    pub cardinality: usize,
    pub distinct_values: HashSet<String>,
}

impl ColumnStats {
    pub fn new(name: String) -> Self {
        Self {
            name,
            cardinality: 0,
            distinct_values: HashSet::new(),
        }
    }

    pub fn add_value(&mut self, value: &str) {
        self.distinct_values.insert(value.to_string());
        self.cardinality = self.distinct_values.len();
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn distinct_values(&self) -> &HashSet<String> {
        &self.distinct_values
    }
}

/// Options for ranking behavior
#[derive(Debug, Clone, Copy)]
pub struct RankingOptions {
    /// Treat empty strings as null
    pub treat_empty_as_null: bool,
    /// Include nulls as a distinct value
    pub include_nulls: bool,
}

impl Default for RankingOptions {
    fn default() -> Self {
        Self {
            treat_empty_as_null: true,
            include_nulls: false,
        }
    }
}

/// Rank columns by cardinality
pub fn rank_columns(
    headers: &[String],
    rows: &[Vec<String>],
    options: RankingOptions,
) -> RsfResult<Vec<ColumnMeta>> {
    if headers.is_empty() {
        return Ok(Vec::new());
    }

    if rows.is_empty() {
        return Ok(headers
            .iter()
            .enumerate()
            .map(|(idx, name)| ColumnMeta {
                name: name.clone(),
                rank: idx,
                cardinality: 0,
                col_type: None,
            })
            .collect());
    }

    // Compute cardinality statistics
    let stats = compute_cardinality(headers, rows, options)?;

    // Create initial column metadata
    let mut columns: Vec<ColumnMeta> = stats
        .into_iter()
        .enumerate()
        .map(|(idx, stat)| ColumnMeta {
            name: stat.name,
            rank: idx,
            cardinality: stat.cardinality,
            col_type: None,
        })
        .collect();

    // Sort by cardinality (descending), then by original position (stable)
    columns.sort_by(|a, b| b.cardinality.cmp(&a.cardinality).then(a.rank.cmp(&b.rank)));

    // Update ranks
    for (new_rank, col) in columns.iter_mut().enumerate() {
        col.rank = new_rank + 1;
    }

    Ok(columns)
}

/// Compute cardinality for each column
fn compute_cardinality(
    headers: &[String],
    rows: &[Vec<String>],
    options: RankingOptions,
) -> RsfResult<Vec<ColumnStats>> {
    if headers.is_empty() {
        return Ok(Vec::new());
    }

    // Initialize stats for each column
    let mut stats: Vec<ColumnStats> = headers
        .iter()
        .map(|name| ColumnStats::new(name.clone()))
        .collect();

    // Count distinct values per column
    for row in rows {
        // Handle rows with fewer columns than headers
        for (i, value) in row.iter().enumerate().take(headers.len()) {
            let val = normalize_value(value, options);
            if let Some(stat) = stats.get_mut(i) {
                stat.add_value(&val);
            }
        }
    }

    Ok(stats)
}

/// Normalize a value for cardinality counting
fn normalize_value(value: &str, options: RankingOptions) -> String {
    if options.treat_empty_as_null && value.trim().is_empty() {
        if options.include_nulls {
            "NULL".to_string()
        } else {
            "NULL".to_string()
        }
    } else {
        value.to_string()
    }
}

/// Reorder data according to ranked columns
pub fn reorder_data(
    headers: &[String],
    rows: &[Vec<String>],
    ranked_columns: &[ColumnMeta],
) -> RsfResult<(Vec<String>, Vec<Vec<String>>)> {
    if ranked_columns.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

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

    Ok((new_headers, new_rows))
}

/// Sort rows canonically by all columns in rank order
pub fn sort_rows_canonical(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    if rows.is_empty() {
        return Vec::new();
    }

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

/// Write schema to file
pub fn write_schema(columns: &[ColumnMeta], path: &PathBuf) -> RsfResult<()> {
    let schema = Schema {
        version: "0.1".to_string(),
        columns: columns.to_vec(),
    };

    let file = std::fs::File::create(path).map_err(|e| RsfError::io_error(path.clone(), e))?;

    serde_yaml::to_writer(file, &schema).map_err(|e| RsfError::schema_error(e.to_string()))?;

    Ok(())
}

/// Validate column ordering matches schema
pub fn validate_column_order(headers: &[String], schema_columns: &[ColumnMeta]) -> RsfResult<()> {
    if schema_columns.is_empty() {
        return Ok(());
    }

    if headers.len() != schema_columns.len() {
        return Err(RsfError::schema_error(format!(
            "Schema column count ({}) does not match CSV column count ({})",
            schema_columns.len(),
            headers.len()
        )));
    }

    // Validate column order matches schema
    for (idx, col_meta) in schema_columns.iter().enumerate() {
        if headers[idx] != col_meta.name {
            return Err(RsfError::column_order_error(
                idx,
                col_meta.name.clone(),
                headers[idx].clone(),
            ));
        }
    }

    Ok(())
}

/// Validate cardinality ordering
pub fn validate_cardinality_order(
    headers: &[String],
    rows: &[Vec<String>],
    schema_columns: &[ColumnMeta],
    options: RankingOptions,
) -> RsfResult<()> {
    if schema_columns.is_empty() {
        return Ok(());
    }

    // Compute actual cardinality
    let stats = compute_cardinality(headers, rows, options)?;
    let mut cardinalities = HashMap::with_capacity(stats.len());
    for stat in stats.iter() {
        cardinalities.insert(stat.name.clone(), stat.cardinality);
    }

    for col_meta in schema_columns.iter() {
        let actual = cardinalities.get(&col_meta.name).ok_or_else(|| {
            RsfError::schema_error(format!("Column '{}' not found in data", col_meta.name))
        })?;

        if *actual != col_meta.cardinality {
            return Err(RsfError::schema_error(format!(
                "Column '{}' cardinality mismatch: schema {}, actual {}",
                col_meta.name, col_meta.cardinality, actual
            )));
        }
    }

    // Validate that columns are ordered by descending cardinality
    for window in schema_columns.windows(2) {
        let curr = &window[0];
        let next = &window[1];

        let curr_actual = cardinalities.get(&curr.name).ok_or_else(|| {
            RsfError::schema_error(format!("Column '{}' not found in data", curr.name))
        })?;

        let next_actual = cardinalities.get(&next.name).ok_or_else(|| {
            RsfError::schema_error(format!("Column '{}' not found in data", next.name))
        })?;

        if curr_actual < next_actual {
            return Err(RsfError::cardinality_error(
                curr.name.clone(),
                *next_actual,
                *curr_actual,
            ));
        }
    }

    Ok(())
}

/// Validate rows are canonically sorted
pub fn validate_sorted(rows: &[Vec<String>]) -> RsfResult<()> {
    let sorted = sort_rows_canonical(rows);

    if sorted != rows {
        return Err(RsfError::sort_error());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rank_columns_basic() {
        let headers = vec!["A".to_string(), "B".to_string()];
        let rows = vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["2".to_string(), "x".to_string()],
            vec!["1".to_string(), "y".to_string()],
        ];

        let ranked = rank_columns(&headers, &rows, Default::default()).unwrap();

        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].name, "A");
        assert_eq!(ranked[0].cardinality, 2);
        assert_eq!(ranked[1].name, "B");
        assert_eq!(ranked[1].cardinality, 2);
    }

    #[test]
    fn test_rank_columns_with_tiebreaker() {
        let headers = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let rows = vec![
            vec!["1".to_string(), "x".to_string(), "alpha".to_string()],
            vec!["2".to_string(), "x".to_string(), "beta".to_string()],
            vec!["1".to_string(), "y".to_string(), "gamma".to_string()],
        ];

        let ranked = rank_columns(&headers, &rows, Default::default()).unwrap();

        assert_eq!(ranked.len(), 3);
        // A and B have same cardinality (2), so C (3) should be first
        assert_eq!(ranked[0].name, "C");
        assert_eq!(ranked[1].name, "A");
        assert_eq!(ranked[2].name, "B");
    }

    #[test]
    fn test_reorder_data() {
        let headers = vec!["A".to_string(), "B".to_string()];
        let rows = vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["2".to_string(), "y".to_string()],
        ];

        let ranked = vec![
            ColumnMeta {
                name: "B".to_string(),
                rank: 1,
                cardinality: 2,
                col_type: None,
            },
            ColumnMeta {
                name: "A".to_string(),
                rank: 2,
                cardinality: 2,
                col_type: None,
            },
        ];

        let (new_headers, new_rows) = reorder_data(&headers, &rows, &ranked).unwrap();

        assert_eq!(new_headers, vec!["B".to_string(), "A".to_string()]);
        assert_eq!(new_rows[0], vec!["x".to_string(), "1".to_string()]);
        assert_eq!(new_rows[1], vec!["y".to_string(), "2".to_string()]);
    }

    #[test]
    fn test_sort_rows_canonical() {
        let rows = vec![
            vec!["b".to_string(), "2".to_string()],
            vec!["a".to_string(), "1".to_string()],
            vec!["c".to_string(), "3".to_string()],
        ];

        let sorted = sort_rows_canonical(&rows);

        assert_eq!(sorted[0], vec!["a".to_string(), "1".to_string()]);
        assert_eq!(sorted[1], vec!["b".to_string(), "2".to_string()]);
        assert_eq!(sorted[2], vec!["c".to_string(), "3".to_string()]);
    }

    #[test]
    fn test_empty_input() {
        let ranked = rank_columns(&[], &[], Default::default()).unwrap();
        assert!(ranked.is_empty());

        let (new_headers, new_rows) = reorder_data(&[], &[], &[]).unwrap();
        assert!(new_headers.is_empty());
        assert!(new_rows.is_empty());

        let sorted = sort_rows_canonical(&[]);
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_trailing_columns() {
        let headers = vec!["A".to_string(), "B".to_string()];
        let rows = vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["2".to_string(), "y".to_string()],
        ];

        let ranked = rank_columns(&headers, &rows, Default::default()).unwrap();
        assert_eq!(ranked.len(), 2);

        let (new_headers, new_rows) = reorder_data(&headers, &rows, &ranked).unwrap();
        assert_eq!(new_headers.len(), 2);
        assert_eq!(new_rows.len(), 2);
    }
}

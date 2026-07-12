use crate::errors::{RsfError, RsfResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Column type classification (for schema)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Key,
    Value,
}

/// Detected data type hint for a column
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TypeHint {
    Unknown,
    Integer,
    Float,
    Date,
    Currency,
    Boolean,
    Id(String), // e.g. "uuid", "alphanumeric"
}

impl Default for TypeHint {
    fn default() -> Self {
        TypeHint::Unknown
    }
}

/// Column metadata for schema (persisted to YAML)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub rank: usize,
    pub cardinality: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_constant: Option<bool>,
    #[serde(
        default,
        rename = "type",
        skip_serializing_if = "Option::is_none"
    )]
    pub col_type: Option<ColumnType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_hint: Option<TypeHint>,
}

/// Schema representation
#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub version: String,
    pub columns: Vec<ColumnMeta>,
}

/// Statistics for a single column (internal)
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

/// Rich profile for a single column (Phase 1 addition)
#[derive(Debug, Clone)]
pub struct ColumnProfile {
    /// Total number of rows seen
    pub total_rows: usize,
    /// Number of null/empty values
    pub null_count: usize,
    /// Cardinality (number of distinct non-null values)
    pub cardinality: usize,
    /// Whether all non-null values are the same (constant column)
    pub is_constant: bool,
    /// Uniqueness ratio: distinct_values / total_non_null_rows
    pub uniqueness_ratio: f64,
    /// Detected type hint
    pub type_hint: TypeHint,
}

impl ColumnProfile {
    pub fn null_pct(&self) -> f64 {
        if self.total_rows == 0 {
            return 0.0;
        }
        (self.null_count as f64 / self.total_rows as f64) * 100.0
    }

    /// Percentage of rows that have a unique value among non-null rows
    pub fn unique_pct(&self) -> f64 {
        if self.total_rows == 0 {
            return 0.0;
        }
        let non_null = self.total_rows - self.null_count;
        if non_null == 0 {
            return 0.0;
        }
        (self.cardinality as f64 / non_null as f64) * 100.0
    }

    /// Percentage of non-null rows that are the constant value (if constant)
    pub fn constant_pct(&self) -> f64 {
        if self.total_rows == 0 {
            return 0.0;
        }
        let non_null = self.total_rows - self.null_count;
        if non_null == 0 {
            return 0.0;
        }
        // If constant, all non-null values are the same → cardinality == 1
        (non_null as f64 / self.total_rows as f64) * 100.0
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

/// Rank columns by cardinality and compute rich profiles
pub fn rank_columns(
    headers: &[String],
    rows: &[Vec<String>],
    options: RankingOptions,
) -> RsfResult<Vec<ColumnMeta>> {
    if headers.is_empty() {
        return Ok(Vec::new());
    }

    let num_rows = rows.len();

    // Compute profiles (includes cardinality + nulls + constants + type hints)
    let profiles = compute_profiles(headers, rows, options)?;

    // Create column metadata from profiles
    let mut columns: Vec<ColumnMeta> = profiles
        .iter()
        .enumerate()
        .map(|(idx, profile)| ColumnMeta {
            name: headers[idx].clone(),
            rank: idx,
            cardinality: profile.cardinality,
            null_pct: if num_rows > 0 && (profile.null_count as f64 / num_rows as f64) > 0.01 {
                Some((profile.null_count as f64 / num_rows as f64) * 100.0)
            } else {
                None
            },
            unique_pct: if profile.total_rows > 0 && (profile.cardinality as f64 / profile.total_rows as f64) < 0.99 {
                Some((profile.cardinality as f64 / profile.total_rows as f64) * 100.0)
            } else {
                None
            },
            is_constant: if profile.is_constant { Some(true) } else { None },
            col_type: None,
            type_hint: if profile.type_hint != TypeHint::Unknown {
                Some(profile.type_hint.clone())
            } else {
                None
            },
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

/// Compute rich profiles for each column
pub(crate) fn compute_profiles(
    headers: &[String],
    rows: &[Vec<String>],
    options: RankingOptions,
) -> RsfResult<Vec<ColumnProfile>> {
    if headers.is_empty() {
        return Ok(Vec::new());
    }

    let num_rows = rows.len();

    // Per-column tracking structures
    struct ColTracker<'a> {
        distinct_values: HashSet<&'a str>,
        null_count: usize,
        non_null_values: Vec<&'a str>,
    }

    let mut trackers: Vec<ColTracker> = headers
        .iter()
        .map(|_| ColTracker {
            distinct_values: HashSet::new(),
            null_count: 0,
            non_null_values: Vec::new(),
        })
        .collect();

    // Single pass over all data
    for row in rows {
        for (i, value) in row.iter().enumerate().take(headers.len()) {
            if let Some(tracker) = trackers.get_mut(i) {
                let normalized = normalize_value(value, options);
                if normalized == "NULL" {
                    tracker.null_count += 1;
                } else {
                    tracker.distinct_values.insert(value.as_str());
                    tracker.non_null_values.push(value.as_str());
                }
            }
        }
    }

    // Build profiles from trackers
    let profiles: Vec<ColumnProfile> = trackers
        .into_iter()
        .enumerate()
        .map(|(_i, tracker)| {
            let total_rows = num_rows;
            let null_count = tracker.null_count;
            let cardinality = tracker.distinct_values.len();
            let non_null = total_rows - null_count;

            // Check if constant: all non-null values are the same
            let is_constant = non_null > 0 && cardinality == 1;

            // Uniqueness ratio among non-null rows
            let uniqueness_ratio = if non_null > 0 {
                cardinality as f64 / non_null as f64
            } else {
                0.0
            };

            // Detect type hint from non-null values
            let type_hint = detect_type_hint(&tracker.non_null_values);

            ColumnProfile {
                total_rows,
                null_count,
                cardinality,
                is_constant,
                uniqueness_ratio,
                type_hint,
            }
        })
        .collect();

    Ok(profiles)
}

/// Normalize a value for cardinality counting
fn normalize_value(value: &str, options: RankingOptions) -> String {
    if options.treat_empty_as_null && value.trim().is_empty() {
        "NULL".to_string()
    } else {
        value.to_string()
    }
}

/// Detect the type of a column from its non-null values.
pub fn detect_type_hint(values: &[&str]) -> TypeHint {
    if values.is_empty() {
        return TypeHint::Unknown;
    }

    let total = values.len();

    // --- Boolean check ---
    let bool_values: HashSet<&str> = ["true", "false", "yes", "no", "1", "0"]
        .iter()
        .copied()
        .collect();
    let bool_matches = values.iter().filter(|v| bool_values.contains(&v.to_lowercase().as_str())).count();
    if bool_matches == total {
        return TypeHint::Boolean;
    }

    // --- Integer check ---
    let int_pattern = regex_int();
    let int_matches = values.iter().filter(|v| int_pattern.is_match(v.trim())).count();
    if int_matches == total {
        return TypeHint::Integer;
    }

    // --- Float check ---
    let float_pattern = regex_float();
    let float_matches = values.iter().filter(|v| float_pattern.is_match(v.trim())).count();
    if float_matches == total {
        return TypeHint::Float;
    }

    // --- Currency check (requires a currency symbol) ---
    let currency_pattern = regex_currency();
    let currency_matches = values.iter().filter(|v| currency_pattern.is_match(v.trim())).count();
    if currency_matches == total {
        return TypeHint::Currency;
    }

    // --- Date check (ISO, US format, common date patterns) ---
    let date_pattern = regex_date();
    let date_matches = values.iter().filter(|v| date_pattern.is_match(v.trim())).count();
    if date_matches == total {
        return TypeHint::Date;
    }

    // --- ID detection: UUID, hex, alphanumeric codes ---
    let uuid_pattern = regex_uuid();
    let uuid_matches = values.iter().filter(|v| uuid_pattern.is_match(v.trim())).count();
    if uuid_matches == total {
        return TypeHint::Id("uuid".to_string());
    }

    let hex_pattern = regex_hex();
    let hex_matches = values.iter().filter(|v| hex_pattern.is_match(v.trim())).count();
    if hex_matches == total && total > 0 {
        return TypeHint::Id("hex".to_string());
    }

    // Alphanumeric codes like TXN001, INV-2024-001
    let alnum_pattern = regex_alnum_id();
    let alnum_matches = values.iter().filter(|v| alnum_pattern.is_match(v.trim())).count();
    if alnum_matches == total {
        return TypeHint::Id("alphanumeric".to_string());
    }

    TypeHint::Unknown
}

// --- Regex helpers (no external crate needed) ---

fn regex_int() -> regex::Regex {
    regex::Regex::new(r"^[+-]?\d+$").unwrap()
}

fn regex_float() -> regex::Regex {
    regex::Regex::new(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$").unwrap()
}

fn regex_currency() -> regex::Regex {
    // Requires at least one currency symbol
    regex::Regex::new(r"^[$€£¥₹]\s*\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?\s*|^\s*\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?\s*[$€£¥₹]$").unwrap()
}

fn regex_date() -> regex::Regex {
    // ISO: 2024-01-15, US: 01/15/2024, Long: January 15, 2024
    regex::Regex::new(
        r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$|^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$|^[A-Z][a-z]+ \d{1,2},? \d{4}$",
    )
    .unwrap()
}

fn regex_uuid() -> regex::Regex {
    regex::Regex::new(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    )
    .unwrap()
}

fn regex_hex() -> regex::Regex {
    regex::Regex::new(r"^[0-9a-fA-F]+$").unwrap()
}

fn regex_alnum_id() -> regex::Regex {
    // Starts with letters, contains digits: TXN001, INV-2024-001, etc.
    regex::Regex::new(r"^[A-Za-z][A-Za-z0-9_-]*\d").unwrap()
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

/// Compute cardinality for each column (legacy, used by validation)
fn compute_cardinality(
    headers: &[String],
    rows: &[Vec<String>],
    options: RankingOptions,
) -> RsfResult<Vec<ColumnStats>> {
    if headers.is_empty() {
        return Ok(Vec::new());
    }

    let mut stats: Vec<ColumnStats> = headers
        .iter()
        .map(|name| ColumnStats::new(name.clone()))
        .collect();

    for row in rows {
        for (i, value) in row.iter().enumerate().take(headers.len()) {
            let val = normalize_value(value, options);
            if let Some(stat) = stats.get_mut(i) {
                stat.add_value(&val);
            }
        }
    }

    Ok(stats)
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
        // A has cardinality 2 (1,2), B has cardinality 2 (x,y) — tie broken by original position
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
        // C has cardinality 3 (alpha, beta, gamma), A and B have 2 each
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
                null_pct: None,
                unique_pct: None,
                is_constant: None,
                type_hint: None,
            },
            ColumnMeta {
                name: "A".to_string(),
                rank: 2,
                cardinality: 2,
                col_type: None,
                null_pct: None,
                unique_pct: None,
                is_constant: None,
                type_hint: None,
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

    // --- Phase 1 profile tests ---

    #[test]
    fn test_profile_null_detection() {
        let headers = vec!["A".to_string(), "B".to_string()];
        let rows = vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["2".to_string(), "".to_string()],
            vec!["3".to_string(), "x".to_string()],
        ];

        let options = RankingOptions {
            treat_empty_as_null: true,
            include_nulls: false,
        };
        let profiles = compute_profiles(&headers, &rows, options).unwrap();

        assert_eq!(profiles[0].null_count, 0); // A has no nulls
        assert_eq!(profiles[1].null_count, 1); // B has one empty value
    }

    #[test]
    fn test_profile_constant_detection() {
        let headers = vec!["A".to_string(), "B".to_string()];
        let rows = vec![
            vec!["1".to_string(), "constant".to_string()],
            vec!["2".to_string(), "constant".to_string()],
            vec!["3".to_string(), "constant".to_string()],
        ];

        let options = RankingOptions::default();
        let profiles = compute_profiles(&headers, &rows, options).unwrap();

        assert_eq!(profiles[0].cardinality, 3); // A: 1,2,3
        assert!(profiles[1].is_constant); // B: all "constant"
    }

    #[test]
    fn test_profile_uniqueness_ratio() {
        let headers = vec!["A".to_string()];
        let rows = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
            vec!["a".to_string()], // duplicate
        ];

        let options = RankingOptions::default();
        let profiles = compute_profiles(&headers, &rows, options).unwrap();

        assert_eq!(profiles[0].cardinality, 3); // a,b,c
        assert_eq!(profiles[0].total_rows, 4);
        // uniqueness_ratio = 3/4 = 0.75
        assert!((profiles[0].uniqueness_ratio - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_type_hint_integer() {
        let values: Vec<&str> = vec!["42", "7", "-3", "100"];
        assert_eq!(detect_type_hint(&values), TypeHint::Integer);
    }

    #[test]
    fn test_type_hint_float() {
        let values: Vec<&str> = vec!["3.14", "2.0", "-1.5", "0.001"];
        assert_eq!(detect_type_hint(&values), TypeHint::Float);
    }

    #[test]
    fn test_type_hint_boolean() {
        let values: Vec<&str> = vec!["true", "false", "yes", "no"];
        assert_eq!(detect_type_hint(&values), TypeHint::Boolean);
    }

    #[test]
    fn test_type_hint_currency() {
        let values: Vec<&str> = vec!["$45.99", "$12.50", "$89.00"];
        assert_eq!(detect_type_hint(&values), TypeHint::Currency);
    }

    #[test]
    fn test_type_hint_date() {
        let values: Vec<&str> = vec!["2024-01-15", "2024-02-20", "2024-03-10"];
        assert_eq!(detect_type_hint(&values), TypeHint::Date);
    }

    #[test]
    fn test_type_hint_uuid() {
        let values: Vec<&str> = vec![
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        ];
        assert_eq!(detect_type_hint(&values), TypeHint::Id("uuid".to_string()));
    }

    #[test]
    fn test_type_hint_alphanumeric_id() {
        let values: Vec<&str> = vec!["TXN001", "TXN002", "TXN003"];
        assert_eq!(detect_type_hint(&values), TypeHint::Id("alphanumeric".to_string()));
    }

    #[test]
    fn test_type_hint_unknown() {
        let values: Vec<&str> = vec!["hello world", "foo bar", "baz qux"];
        assert_eq!(detect_type_hint(&values), TypeHint::Unknown);
    }

    #[test]
    fn test_null_pct_calculation() {
        let profile = ColumnProfile {
            total_rows: 10,
            null_count: 3,
            cardinality: 5,
            is_constant: false,
            uniqueness_ratio: 0.714,
            type_hint: TypeHint::Unknown,
        };

        assert!((profile.null_pct() - 30.0).abs() < 0.001);
    }

    #[test]
    fn test_unique_pct_calculation() {
        let profile = ColumnProfile {
            total_rows: 10,
            null_count: 2, // 8 non-null rows
            cardinality: 4,
            is_constant: false,
            uniqueness_ratio: 0.5,
            type_hint: TypeHint::Unknown,
        };

        // unique_pct = 4/8 * 100 = 50%
        assert!((profile.unique_pct() - 50.0).abs() < 0.001);
    }
}

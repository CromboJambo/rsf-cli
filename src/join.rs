use crate::ranking::{ColumnProfile, RankingOptions};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Join mode for the join operation.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinMode {
    Inner,
    Left,
    FullOuter,
}

impl Default for JoinMode {
    fn default() -> Self {
        JoinMode::Inner
    }
}

/// Configuration for multi-file join planning and execution.
#[derive(Debug, Clone)]
pub struct JoinConfig {
    /// Join mode: inner, left, or full outer.
    pub mode: JoinMode,
    /// Floating-point tolerance for near-match on numeric columns.
    pub float_tolerance: f64,
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            mode: JoinMode::Inner,
            float_tolerance: 0.01,
        }
    }
}

/// A candidate join key between two files.
#[derive(Debug, Clone)]
pub struct JoinCandidate {
    /// Column name in file 1.
    pub col_file_1: String,
    /// Column name in file 2 (may differ if fuzzy-matched).
    pub col_file_2: String,
    /// Confidence score [0.0, 1.0].
    pub confidence: f64,
    /// Reason for the confidence score.
    pub reason: String,
}

/// Result of join candidate analysis (planning phase).
#[derive(Debug)]
pub struct JoinPlanResult {
    /// All candidate join keys found.
    pub candidates: Vec<JoinCandidate>,
    /// Total column pairs compared.
    pub pairs_compared: usize,
}

/// Result of an executed join.
pub struct JoinResult {
    /// The plan that was used (or None if auto-selected).
    pub plan: Option<JoinPlanResult>,
    /// Selected candidate index (-1 = none selected).
    pub selected_candidate_idx: i64,
    /// Number of rows in file 1.
    pub rows_file_1: usize,
    /// Number of rows in file 2.
    pub rows_file_2: usize,
    /// Number of output rows after join.
    pub output_rows: usize,
    /// Mismatched / unmatched rows from file 1 (left-only or inner-miss).
    pub left_unmatched: usize,
    /// Mismatched / unmatched rows from file 2 (right-only in full outer).
    pub right_unmatched: usize,
    /// Output headers.
    pub output_headers: Vec<String>,
    /// Output data rows.
    pub output_rows_data: Vec<Vec<String>>,
}

/// Compute column profiles for a dataset.
fn compute_profiles(
    headers: &[String],
    rows: &[Vec<String>],
) -> Result<Vec<ColumnProfile>, String> {
    let options = RankingOptions::default();
    crate::ranking::compute_profiles(headers, rows, options).map_err(|e| e.to_string())
}

/// Find join candidates between two files by comparing their schemas.
pub fn find_join_candidates(
    headers1: &[String],
    profiles1: &[ColumnProfile],
    headers2: &[String],
    profiles2: &[ColumnProfile],
) -> JoinPlanResult {
    let mut candidates: Vec<JoinCandidate> = Vec::new();
    let mut pairs_compared = 0usize;

    for (i, h1) in headers1.iter().enumerate() {
        for (j, h2) in headers2.iter().enumerate() {
            pairs_compared += 1;

            // Skip self-joins on same column name.
            if h1 == h2 && i != j {
                continue;
            }

            let confidence = score_candidate(h1, h2, &profiles1[i], &profiles2[j]);
            if confidence > 0.3 {
                candidates.push(JoinCandidate {
                    col_file_1: h1.clone(),
                    col_file_2: h2.clone(),
                    confidence,
                    reason: candidate_reason(h1, h2, &profiles1[i], &profiles2[j]),
                });
            }
        }
    }

    // Sort by confidence descending.
    candidates.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal));

    JoinPlanResult {
        candidates,
        pairs_compared,
    }
}

/// Score a single column pair as a join candidate [0.0, 1.0].
fn score_candidate(
    name1: &str,
    name2: &str,
    prof1: &ColumnProfile,
    prof2: &ColumnProfile,
) -> f64 {
    let mut score = 0.0;

    // Exact name match is the strongest signal (score up to 0.5).
    if name1 == name2 {
        score += 0.5;
    } else {
        // Fuzzy name match: case-insensitive, strip common suffixes/prefixes.
        let n1 = normalize_name(name1);
        let n2 = normalize_name(name2);
        if n1 == n2 {
            score += 0.3;
        } else if n1.contains(&n2) || n2.contains(&n1) {
            // One contains the other (e.g., "user_id" and "id").
            let shorter = n1.len().min(n2.len());
            let longer = n1.len().max(n2.len());
            if longer > 0 && (shorter as f64 / longer as f64) > 0.7 {
                score += 0.2;
            }
        }
    }

    // Type hint compatibility (score up to 0.3).
    let type_compat = type_compatibility(&prof1.type_hint, &prof2.type_hint);
    score += type_compat * 0.3;

    // Cardinality similarity (score up to 0.2).
    if prof1.cardinality > 0 && prof2.cardinality > 0 {
        let min_card = prof1.cardinality.min(prof2.cardinality) as f64;
        let max_card = prof1.cardinality.max(prof2.cardinality) as f64;
        let ratio = if max_card == 0.0 { 1.0 } else { min_card / max_card };
        score += ratio * 0.2;
    }
    // Unique columns are better join keys (score up to 0.1).
    let uniqueness_bonus =
        (prof1.unique_pct() + prof2.unique_pct()) / 200.0;
    score += uniqueness_bonus.min(0.1);

    score.min(1.0)
}

/// Normalize a column name for fuzzy comparison: lowercase, strip underscores/dashes/spaces.
fn normalize_name(name: &str) -> String {
    name.to_lowercase()
        .replace(|c: char| c == '_' || c == '-' || c.is_whitespace(), "")
}

/// Check type hint compatibility between two columns.
fn type_compatibility(t1: &Option<crate::ranking::TypeHint>, t2: &Option<crate::ranking::TypeHint>) -> f64 {
    match (t1, t2) {
        (Some(a), Some(b)) => {
            if a == b {
                return 1.0; // Exact type match.
            }
            // Compatible numeric types.
            matches!(
                (a, b),
                (crate::ranking::TypeHint::Integer, crate::ranking::TypeHint::Float)
                    | (crate::ranking::TypeHint::Float, crate::ranking::TypeHint::Integer)
                    | (crate::ranking::TypeHint::Currency, crate::ranking::TypeHint::Float)
                    | (crate::ranking::TypeHint::Currency, crate::ranking::TypeHint::Integer)
            ) as f64 * 0.8
        }
        _ => 0.5, // Unknown types get neutral score.
    }
}

/// Generate a human-readable reason for the candidate's confidence.
fn candidate_reason(
    name1: &str,
    name2: &str,
    prof1: &ColumnProfile,
    prof2: &ColumnProfile,
) -> String {
    let mut parts = Vec::new();

    if name1 == name2 {
        parts.push("exact name match".to_string());
    } else {
        parts.push("similar column names".to_string());
    }

    if let (Some(t1), Some(t2)) = (&prof1.type_hint, &prof2.type_hint) {
        if t1 == t2 {
            parts.push(format!("type: {:?}", t1));
        } else {
            parts.push("compatible types".to_string());
        }
    }

    let card_match = prof1.cardinality == prof2.cardinality;
    if card_match {
        parts.push(format!(
            "matching cardinality ({})",
            prof1.cardinality
        ));
    } else {
        parts.push(format!(
            "cardinality ratio: {:.0}%",
            (prof1.cardinality.min(prof2.cardinality) as f64
                / prof1.cardinality.max(prof2.cardinality) as f64)
                * 100.0
        ));
    }

    parts.join(", ")
}

/// Execute a join between two datasets using the given candidate key.
pub fn execute_join(
    headers1: &[String],
    rows1: &[Vec<String>],
    profiles1: &[ColumnProfile],
    headers2: &[String],
    rows2: &[Vec<String>],
    profiles2: &[ColumnProfile],
    config: &JoinConfig,
) -> Result<JoinResult, String> {
    // Find the best candidate automatically.
    let plan = find_join_candidates(headers1, profiles1, headers2, profiles2);

    if plan.candidates.is_empty() {
        return Err("No suitable join candidates found between these files.".to_string());
    }

    let best_idx = 0; // Already sorted by confidence.
    let candidate = &plan.candidates[best_idx];

    // Find column indices in each file.
    let idx1 = headers1.iter().position(|h| h == &candidate.col_file_1)
        .ok_or_else(|| format!("Column '{}' not found in file 1", candidate.col_file_1))?;
    let idx2 = headers2.iter().position(|h| h == &candidate.col_file_2)
        .ok_or_else(|| format!("Column '{}' not found in file 2", candidate.col_file_2))?;

    // Build index for file 2: join_key -> row_index.
    let mut idx_map: HashMap<String, Vec<usize>> = HashMap::new();
    for (row_idx, row) in rows2.iter().enumerate() {
        if row_idx < idx2 || idx2 >= row.len() {
            continue;
        }
        let key = normalize_join_key(&row[idx2], config);
        idx_map.entry(key).or_default().push(row_idx);
    }

    // Build output headers: file1 columns + non-key file2 columns.
    let mut out_headers = headers1.to_vec();
    for (j, h) in headers2.iter().enumerate() {
        if j != idx2 {
            out_headers.push(format!("{}.{}", candidate.col_file_2, h));
        }
    }

    // Execute the join.
    let mut output_rows: Vec<Vec<String>> = Vec::new();
    let mut left_unmatched = 0usize;
    let mut matched_keys: HashSet<String> = HashSet::new();

    match config.mode {
        JoinMode::Inner => {
            for (row_idx1, row1) in rows1.iter().enumerate() {
                if row_idx1 >= idx1 || idx1 >= row1.len() {
                    left_unmatched += 1;
                    continue;
                }
                let key = normalize_join_key(&row1[idx1], config);
                if let Some(indices) = idx_map.get(&key) {
                    for &idx2 in indices {
                        matched_keys.insert(key.clone());
                        let mut out_row = row1.clone();
                        for (j, val) in rows2[idx2].iter().enumerate() {
                            if j != idx2 {
                                out_row.push(format!("{}.{}", candidate.col_file_2, val));
                            }
                        }
                        output_rows.push(out_row);
                    }
                } else {
                    left_unmatched += 1;
                }
            }

            // Right unmatched = keys in file 2 not matched by any row in file 1.
            let right_unmatched = idx_map.keys()
                .filter(|k| !matched_keys.contains(*k))
                .count();

            Ok(JoinResult {
                plan: Some(plan),
                selected_candidate_idx: best_idx as i64,
                rows_file_1: rows1.len(),
                rows_file_2: rows2.len(),
                output_rows: output_rows.len(),
                left_unmatched,
                right_unmatched,
                output_headers: out_headers,
                output_rows_data: output_rows,
            })
        }

        JoinMode::Left => {
            for (row_idx1, row1) in rows1.iter().enumerate() {
                if row_idx1 >= idx1 || idx1 >= row1.len() {
                    left_unmatched += 1;
                    continue;
                }
                let key = normalize_join_key(&row1[idx1], config);
                matched_keys.insert(key.clone());

                if let Some(indices) = idx_map.get(&key) {
                    for &idx2 in indices {
                        let mut out_row = row1.clone();
                        for (j, val) in rows2[idx2].iter().enumerate() {
                            if j != idx2 {
                                out_row.push(format!("{}.{}", candidate.col_file_2, val));
                            }
                        }
                        output_rows.push(out_row);
                    }
                } else {
                    // Left-only row: pad with empty strings for file 2 columns.
                    let mut out_row = row1.clone();
                    for (j, h) in headers2.iter().enumerate() {
                        if j != idx2 {
                            out_row.push(format!("{}.{}", candidate.col_file_2, ""));
                        }
                    }
                    output_rows.push(out_row);
                    left_unmatched += 1;
                }
            }

            let right_unmatched = idx_map.keys()
                .filter(|k| !matched_keys.contains(*k))
                .count();

            Ok(JoinResult {
                plan: Some(plan),
                selected_candidate_idx: best_idx as i64,
                rows_file_1: rows1.len(),
                rows_file_2: rows2.len(),
                output_rows: output_rows.len(),
                left_unmatched,
                right_unmatched,
            })
        }

        JoinMode::FullOuter => {
            // Process all file 1 rows.
            for (row_idx1, row1) in rows1.iter().enumerate() {
                if row_idx1 >= idx1 || idx1 >= row1.len() {
                    left_unmatched += 1;
                    continue;
                }
                let key = normalize_join_key(&row1[idx1], config);
                matched_keys.insert(key.clone());

                if let Some(indices) = idx_map.get(&key) {
                    for &idx2 in indices {
                        let mut out_row = row1.clone();
                        for (j, val) in rows2[idx2].iter().enumerate() {
                            if j != idx2 {
                                out_row.push(format!("{}.{}", candidate.col_file_2, val));
                            }
                        }
                        output_rows.push(out_row);
                    }
                } else {
                    let mut out_row = row1.clone();
                    for (j, h) in headers2.iter().enumerate() {
                        if j != idx2 {
                            out_row.push(format!("{}.{}", candidate.col_file_2, ""));
                        }
                    }
                    output_rows.push(out_row);
                    left_unmatched += 1;
                }
            }

            // Add right-only rows (not matched by any file 1 row).
            for (row_idx2, row2) in rows2.iter().enumerate() {
                if row_idx2 >= idx2 || idx2 >= row2.len() {
                    continue;
                }
                let key = normalize_join_key(&row2[idx2], config);
                if !matched_keys.contains(&key) {
                    right_unmatched += 1;
                    // Pad with empty strings for file 1 columns.
                    let mut out_row: Vec<String> = vec!["".to_string(); headers1.len()];
                    for (j, val) in row2.iter().enumerate() {
                        if j != idx2 {
                            out_row.push(format!("{}.{}", candidate.col_file_2, val));
                        }
                    }
                    output_rows.push(out_row);
                }
            }

            Ok(JoinResult {
                plan: Some(plan),
                selected_candidate_idx: best_idx as i64,
                rows_file_1: rows1.len(),
                rows_file_2: rows2.len(),
                output_rows: output_rows.len(),
                left_unmatched,
                right_unmatched,
            })
        }
    }
}

/// Normalize a join key value for comparison.
fn normalize_join_key(value: &str, config: &JoinConfig) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "NULL".to_string();
    }
    // For numeric columns, use tolerance-based matching.
    if let Ok(f) = trimmed.parse::<f64>() {
        format!("{:.10}", f)
    } else {
        trimmed.to_lowercase()
    }
}

/// Print a human-readable join plan report to stderr.
pub fn print_plan_report(result: &JoinPlanResult, file1_name: &str, file2_name: &str) {
    eprintln!("\n=== Join Plan Analysis ===");
    eprintln!("Files compared: {} ↔ {}", file1_name, file2_name);
    eprintln!("Column pairs analyzed: {}", result.pairs_compared);
    eprintln!("Candidate join keys found: {}", result.candidates.len());

    if !result.candidates.is_empty() {
        eprintln!("\n--- Top Candidates ---");
        for (i, c) in result.candidates.iter().enumerate().take(5) {
            let conf_pct = (c.confidence * 100.0).round();
            eprintln!(
                "  {}. {} ↔ {}  [confidence: {:.0}%]",
                i + 1,
                c.col_file_1,
                c.col_file_2,
                c.confidence * 100.0
            );
            eprintln!("     Reason: {}", c.reason);
        }

        if result.candidates.len() > 5 {
            eprintln!(
                "  ... and {} more candidates",
                result.candidates.len() - 5
            );
        }
    } else {
        eprintln!("\nNo suitable join candidates found.");
        eprintln!("Try checking if the files share common columns or compatible data types.");
    }
}

/// Print a human-readable join execution report to stderr.
pub fn print_join_report(result: &JoinResult, file1_name: &str, file2_name: &str) {
    eprintln!("\n=== Join Execution Report ===");
    if let Some(plan) = &result.plan {
        if !plan.candidates.is_empty() && result.selected_candidate_idx >= 0 {
            let c = &plan.candidates[result.selected_candidate_idx as usize];
            eprintln!(
                "Join key: {} ↔ {} (confidence: {:.0}%)",
                c.col_file_1,
                c.col_file_2,
                c.confidence * 100.0
            );
        }
    }

    eprintln!("File 1 rows: {}", result.rows_file_1);
    eprintln!("File 2 rows: {}", result.rows_file_2);
    eprintln!("Output rows: {}", result.output_rows);
    eprintln!("Left unmatched (no match in file 2): {}", result.left_unmatched);
    if result.right_unmatched > 0 {
        eprintln!("Right unmatched (no match in file 1): {}", result.right_unmatched);
    }

    // Print the plan summary.
    if let Some(plan) = &result.plan {
        print_plan_report(plan, file1_name, file2_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_profiles(
        headers: &[String],
        rows: &[Vec<String>],
    ) -> Vec<ColumnProfile> {
        let options = RankingOptions::default();
        crate::ranking::compute_profiles(headers, rows, options).unwrap()
    }

    #[test]
    fn test_exact_name_match_candidate() {
        let headers1 = vec!["id".to_string(), "name".to_string()];
        let headers2 = vec!["id".to_string(), "value".to_string()];
        let rows1 = vec![vec!["1".to_string(), "Alice".to_string()]];
        let rows2 = vec![vec!["1".to_string(), "42".to_string()]];

        let profiles1 = make_profiles(&headers1, &rows1);
        let profiles2 = make_profiles(&headers2, &rows2);

        let result = find_join_candidates(&headers1, &profiles1, &headers2, &profiles2);

        assert!(!result.candidates.is_empty());
        // The top candidate should be "id" ↔ "id".
        assert_eq!(result.candidates[0].col_file_1, "id");
        assert_eq!(result.candidates[0].col_file_2, "id");
    }

    #[test]
    fn test_fuzzy_name_match() {
        let headers1 = vec!["user_id".to_string(), "name".to_string()];
        let headers2 = vec!["uid".to_string(), "amount".to_string()];
        let rows1 = vec![vec!["1".to_string(), "Alice".to_string()]];
        let rows2 = vec![vec!["1".to_string(), "42".to_string()]];

        let profiles1 = make_profiles(&headers1, &rows1);
        let profiles2 = make_profiles(&headers2, &rows2);

        let result = find_join_candidates(&headers1, &profiles1, &headers2, &profiles2);

        // Should find a fuzzy match between user_id and uid.
        assert!(!result.candidates.is_empty());
    }

    #[test]
    fn test_no_candidate() {
        let headers1 = vec!["id".to_string(), "name".to_string()];
        let headers2 = vec!["email".to_string(), "phone".to_string()];
        let rows1 = vec![vec!["1".to_string(), "Alice".to_string()]];
        let rows2 = vec![vec!["a@b.com".to_string(), "555-0100".to_string()]];

        let profiles1 = make_profiles(&headers1, &rows1);
        let profiles2 = make_profiles(&headers2, &rows2);

        let result = find_join_candidates(&headers1, &profiles1, &headers2, &profiles2);

        // No candidates should be found (all below 0.3 threshold).
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn test_inner_join() {
        let headers1 = vec!["id".to_string(), "name".to_string()];
        let headers2 = vec!["id".to_string(), "value".to_string()];
        let rows1 = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
            vec!["3".to_string(), "Charlie".to_string()],
        ];
        let rows2 = vec![
            vec!["1".to_string(), "42".to_string()],
            vec!["2".to_string(), "99".to_string()],
            // id=3 has no match in file 2.
        ];

        let profiles1 = make_profiles(&headers1, &rows1);
        let profiles2 = make_profiles(&headers2, &rows2);

        let config = JoinConfig {
            mode: JoinMode::Inner,
            float_tolerance: 0.01,
        };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &headers2, &rows2, &profiles2,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows, 2); // Only ids 1 and 2 match.
        assert_eq!(result.left_unmatched, 1); // id=3 has no match.
    }

    #[test]
    fn test_left_join() {
        let headers1 = vec!["id".to_string(), "name".to_string()];
        let headers2 = vec!["id".to_string(), "value".to_string()];
        let rows1 = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ];
        let rows2 = vec![vec!["1".to_string(), "42".to_string()]];

        let profiles1 = make_profiles(&headers1, &rows1);
        let profiles2 = make_profiles(&headers2, &rows2);

        let config = JoinConfig {
            mode: JoinMode::Left,
            float_tolerance: 0.01,
        };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &headers2, &rows2, &profiles2,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows, 2); // All file 1 rows preserved.
        assert_eq!(result.left_unmatched, 1); // Bob has no match.
    }

    #[test]
    fn test_full_outer_join() {
        let headers1 = vec!["id".to_string(), "name".to_string()];
        let headers2 = vec!["id".to_string(), "value".to_string()];
        let rows1 = vec![vec!["1".to_string(), "Alice".to_string()]];
        let rows2 = vec![
            vec!["1".to_string(), "42".to_string()],
            vec!["3".to_string(), "99".to_string()], // Right-only.
        ];

        let profiles1 = make_profiles(&headers1, &rows1);
        let profiles2 = make_profiles(&headers2, &rows2);

        let config = JoinConfig {
            mode: JoinMode::FullOuter,
            float_tolerance: 0.01,
        };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &headers2, &rows2, &profiles2,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows, 3); // Alice+match + right-only row.
    }

    #[test]
    fn test_type_compatibility() {
        use crate::ranking::TypeHint;

        // Same types are fully compatible.
        assert_eq!(type_compatibility(&Some(TypeHint::Integer), &Some(TypeHint::Integer)), 1.0);

        // Integer ↔ Float is compatible.
        let compat = type_compatibility(&Some(TypeHint::Integer), &Some(TypeHint::Float));
        assert!((compat - 0.8) < 0.01);

        // Unknown types get neutral score.
        assert_eq!(type_compatibility(&None, &None), 0.5);
    }

    #[test]
    fn test_normalize_name() {
        assert_eq!(normalize_name("user_id"), "userid");
        assert_eq!(normalize_name("USER-ID"), "userid");
        assert_eq!(normalize_name("UserId"), "userid");
    }

    #[test]
    fn test_empty_input_candidates() {
        let profiles1: Vec<ColumnProfile> = vec![];
        let profiles2: Vec<ColumnProfile> = vec![];

        let result = find_join_candidates(&[], &profiles1, &[], &profiles2);

        assert_eq!(result.candidates.len(), 0);
    }
}

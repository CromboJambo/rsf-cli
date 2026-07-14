use crate::ranking::{ColumnProfile, RankingOptions};
use std::collections::{HashMap, HashSet};

/// A functional dependency: every value of `determinant` maps to exactly one value of `dependent`.
#[derive(Debug, Clone)]
pub struct FunctionalDependency {
    /// Name of the determinant column (left side).
    pub determinant: String,
    /// Name of the dependent column (right side).
    pub dependent: String,
    /// Cardinality of the determinant.
    pub determinant_cardinality: usize,
    /// Cardinality of the dependent.
    pub dependent_cardinality: usize,
}

/// A candidate key — a single column that functionally determines all other columns.
#[derive(Debug, Clone)]
pub struct CandidateKey {
    /// Name of the candidate key column.
    pub name: String,
    /// Cardinality of this column.
    pub cardinality: usize,
}

/// Result of functional dependency analysis.
#[derive(Debug)]
pub struct FdResult {
    /// All detected functional dependencies.
    pub fds: Vec<FunctionalDependency>,
    /// Candidate keys (columns that determine all others).
    pub candidate_keys: Vec<CandidateKey>,
    /// Total column pairs analyzed.
    pub pairs_analyzed: usize,
}

/// Configuration for FD detection.
#[derive(Debug, Clone)]
pub struct FdConfig {
    /// Treat empty strings as null (matches ranking behavior).
    pub treat_empty_as_null: bool,
}

impl Default for FdConfig {
    fn default() -> Self {
        Self {
            treat_empty_as_null: true,
        }
    }
}

/// Detect all functional dependencies in the dataset.
pub fn find_functional_dependencies(
    headers: &[String],
    rows: &[Vec<String>],
    profiles: &[ColumnProfile],
    config: &FdConfig,
) -> FdResult {
    if rows.is_empty() || headers.len() < 2 {
        return FdResult {
            fds: Vec::new(),
            candidate_keys: Vec::new(),
            pairs_analyzed: 0,
        };
    }

    let num_rows = rows.len();
    let mut fds: Vec<FunctionalDependency> = Vec::new();
    let mut pairs_analyzed = 0usize;

    // For each ordered pair (A → B), check if A functionally determines B.
    for a_idx in 0..headers.len() {
        for b_idx in 0..headers.len() {
            if a_idx == b_idx {
                continue;
            }

            pairs_analyzed += 1;

            // Build mapping: determinant values → set of dependent values.
            let mut map: HashMap<String, HashSet<String>> = HashMap::new();

            for row in rows {
                let det_val = normalize_dep_value(&row[a_idx], config);
                let dep_val = normalize_dep_value(&row[b_idx], config);

                map.entry(det_val).or_default().insert(dep_val);
            }

            // Check if every determinant value maps to exactly one dependent value.
            let is_fd = map.values().all(|vals| vals.len() == 1);

            if is_fd {
                fds.push(FunctionalDependency {
                    determinant: headers[a_idx].clone(),
                    dependent: headers[b_idx].clone(),
                    determinant_cardinality: profiles.get(a_idx).map_or(0, |p| p.cardinality),
                    dependent_cardinality: profiles.get(b_idx).map_or(0, |p| p.cardinality),
                });
            }
        }
    }

    // Sort FDs by determinant cardinality descending (stronger determinants first),
    // then alphabetically for stable output.
    fds.sort_by(|a, b| {
        b.determinant_cardinality
            .cmp(&a.determinant_cardinality)
            .then_with(|| a.determinant.cmp(&b.determinant))
            .then_with(|| a.dependent.cmp(&b.dependent))
    });

    // Find candidate keys: columns that functionally determine ALL other columns.
    let candidate_keys = find_candidate_keys(&fds, headers.len());

    FdResult {
        fds,
        candidate_keys,
        pairs_analyzed,
    }
}

/// Normalize a value for FD comparison (consistent with ranking behavior).
fn normalize_dep_value(value: &str, config: &FdConfig) -> String {
    if config.treat_empty_as_null && value.trim().is_empty() {
        "NULL".to_string()
    } else {
        value.to_string()
    }
}

/// Find candidate keys — columns that functionally determine all other columns.
fn find_candidate_keys(fds: &[FunctionalDependency], num_columns: usize) -> Vec<CandidateKey> {
    if fds.is_empty() || num_columns < 2 {
        return Vec::new();
    }

    // Group FDs by determinant to see which columns determine others.
    let mut det_to_dependents: HashMap<&str, HashSet<&str>> = HashMap::new();
    for fd in fds {
        det_to_dependents
            .entry(fd.determinant.as_str())
            .or_default()
            .insert(&fd.dependent);
    }

    // A candidate key determines all other columns (num_columns - 1 dependents).
    let mut keys: Vec<CandidateKey> = Vec::new();
    for (det, dependents) in &det_to_dependents {
        if dependents.len() == num_columns - 1 {
            // Find the cardinality from the FDs.
            let cardinality = fds
                .iter()
                .find(|fd| fd.determinant == *det)
                .map_or(0, |fd| fd.determinant_cardinality);

            keys.push(CandidateKey {
                name: det.to_string(),
                cardinality,
            });
        }
    }

    // Sort by cardinality descending.
    keys.sort_by(|a, b| b.cardinality.cmp(&a.cardinality));
    keys
}

/// Print a human-readable report of functional dependencies to stderr.
pub fn print_report(result: &FdResult) {
    eprintln!("\n=== Functional Dependency Analysis ===");
    eprintln!("Column pairs analyzed: {}", result.pairs_analyzed);
    eprintln!("Functional dependencies found: {}", result.fds.len());

    if !result.candidate_keys.is_empty() {
        eprintln!(
            "Candidate keys: {}",
            result
                .candidate_keys
                .iter()
                .map(|k| format!("{} (card={})", k.name, k.cardinality))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    if !result.fds.is_empty() {
        eprintln!("\n--- Dependencies ---");

        // Group by determinant for cleaner output.
        let mut groups: HashMap<&str, Vec<&FunctionalDependency>> = HashMap::new();
        for fd in &result.fds {
            groups.entry(&fd.determinant).or_default().push(fd);
        }

        for (det, deps) in &groups {
            if deps.len() == 1 {
                let fd = &deps[0];
                eprintln!(
                    "  {} → {}  [{}→{}]",
                    det, fd.dependent, fd.determinant_cardinality, fd.dependent_cardinality
                );
            } else {
                // Multiple FDs from same determinant — group them.
                let dep_names: Vec<&str> = deps.iter().map(|fd| fd.dependent.as_str()).collect();
                let card = deps[0].determinant_cardinality;
                eprintln!(
                    "  {} → {{{}}}  [card={}]",
                    det,
                    dep_names.join(", "),
                    card
                );
            }
        }

        // Highlight candidate keys.
        if !result.candidate_keys.is_empty() {
            eprintln!("\n--- Candidate Keys ---");
            for key in &result.candidate_keys {
                eprintln!("  ★ {} (cardinality: {}) — determines all other columns", key.name, key.cardinality);
            }
        }
    } else {
        eprintln!("\nNo functional dependencies found.");
        eprintln!(
            "This means no column's values uniquely determine another column's values."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_profiles(headers: &[String], rows: &[Vec<String>]) -> Vec<ColumnProfile> {
        let options = RankingOptions::default();
        compute_profiles_for_test(headers, rows, options)
    }

    // Helper to compute profiles using the public ranking module.
    fn compute_profiles_for_test(
        headers: &[String],
        rows: &[Vec<String>],
        options: RankingOptions,
    ) -> Vec<ColumnProfile> {
        use crate::ranking::compute_profiles;
        compute_profiles(headers, rows, options).unwrap()
    }

    #[test]
    fn test_strict_fd() {
        // Vendor → Category: every vendor always has the same category.
        let headers = vec![
            "Vendor".to_string(),
            "Category".to_string(),
            "Amount".to_string(),
        ];
        let rows = vec![
            vec!["Safeway".to_string(), "Food".to_string(), "10.00".to_string()],
            vec!["Safeway".to_string(), "Food".to_string(), "20.00".to_string()],
            vec!["Uber".to_string(), "Transport".to_string(), "15.00".to_string()],
        ];

        let profiles = make_profiles(&headers, &rows);
        let result = find_functional_dependencies(&headers, &rows, &profiles, &FdConfig::default());

        // Vendor → Category should be detected (Safeway→Food, Uber→Transport).
        assert!(result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Category"));
    }

    #[test]
    fn test_no_fd() {
        // No column determines another.
        let headers = vec!["A".to_string(), "B".to_string()];
        let rows = vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["2".to_string(), "y".to_string()],
            vec!["1".to_string(), "z".to_string()], // 1 maps to both x and z → no FD.
        ];

        let profiles = make_profiles(&headers, &rows);
        let result = find_functional_dependencies(&headers, &rows, &profiles, &FdConfig::default());

        assert!(!result.fds.iter().any(|fd| fd.determinant == "A" && fd.dependent == "B"));
    }

    #[test]
    fn test_candidate_key() {
        // TransactionID has unique values → determines everything.
        // Other columns have repeated values so they don't determine all others.
        let headers = vec![
            "TransactionID".to_string(),
            "Vendor".to_string(),
            "Amount".to_string(),
        ];
        let rows = vec![
            vec!["TXN001".to_string(), "Safeway".to_string(), "10.00".to_string()],
            vec!["TXN002".to_string(), "Uber".to_string(), "15.00".to_string()],
            vec!["TXN003".to_string(), "Safeway".to_string(), "25.00".to_string()], // repeated Vendor
<<<<<<< HEAD
<<<<<<< HEAD
            vec!["TXN004".to_string(), "Uber".to_string(), "10.00".to_string()], // repeated Amount, Vendor
=======
>>>>>>> 15a23aa (Refine FD tests and simplify join boundary checks)
=======
            vec!["TXN004".to_string(), "Uber".to_string(), "10.00".to_string()], // repeated Amount, Vendor
>>>>>>> b7e54f3 (Improve FD test case to handle non-unique data)
        ];

        let profiles = make_profiles(&headers, &rows);
        let result = find_functional_dependencies(&headers, &rows, &profiles, &FdConfig::default());

        // Only TransactionID is a candidate key (determines all other columns).
        assert_eq!(result.candidate_keys.len(), 1);
        assert_eq!(result.candidate_keys[0].name, "TransactionID");
    }

    #[test]
    fn test_constant_column_determines_nothing() {
        // A constant column maps its single value to multiple values → not an FD.
        let headers = vec!["Status".to_string(), "Amount".to_string()];
        let rows = vec![
            vec!["active".to_string(), "10.00".to_string()],
            vec!["active".to_string(), "20.00".to_string()],
        ];

        let profiles = make_profiles(&headers, &rows);
        let result = find_functional_dependencies(&headers, &rows, &profiles, &FdConfig::default());

        // Status → Amount is NOT an FD because "active" maps to both 10 and 20.
        assert!(!result.fds.iter().any(|fd| fd.determinant == "Status"));
    }

    #[test]
    fn test_empty_input() {
        let profiles: Vec<ColumnProfile> = vec![];
        let result = find_functional_dependencies(&[], &[], &profiles, &FdConfig::default());

        assert_eq!(result.fds.len(), 0);
        assert_eq!(result.candidate_keys.len(), 0);
    }

    #[test]
    fn test_single_column() {
        let headers = vec!["A".to_string()];
        let rows = vec![vec!["1".to_string()]];
        let profiles = make_profiles(&headers, &rows);
        let result = find_functional_dependencies(&headers, &rows, &profiles, &FdConfig::default());

        assert_eq!(result.fds.len(), 0); // Need at least 2 columns for FDs.
    }

    #[test]
    fn test_grouped_output() {
        // Key → A, B, C should all be detected.
        // Add more rows so A, B, C have repeated values (not unique).
        let headers = vec![
            "Key".to_string(),
            "A".to_string(),
            "B".to_string(),
            "C".to_string(),
        ];
        let rows = vec![
            vec!["K1".to_string(), "x".to_string(), "p".to_string(), "m".to_string()],
            vec!["K2".to_string(), "y".to_string(), "q".to_string(), "n".to_string()],
            vec!["K3".to_string(), "x".to_string(), "p".to_string(), "m".to_string()], // repeated A, B, C values
        ];

        let profiles = make_profiles(&headers, &rows);
        let result = find_functional_dependencies(&headers, &rows, &profiles, &FdConfig::default());

        // Key → A, B, C should all be detected.
        assert!(result
            .fds
            .iter()
            .any(|fd| fd.determinant == "Key" && fd.dependent == "A"));
        assert!(result
            .fds
            .iter()
            .any(|fd| fd.determinant == "Key" && fd.dependent == "B"));
        assert!(result
            .fds
            .iter()
            .any(|fd| fd.determinant == "Key" && fd.dependent == "C"));

        // Key should be a candidate key (determines all 3 others).
        assert_eq!(result.candidate_keys.len(), 1);
        assert_eq!(result.candidate_keys[0].name, "Key");
    }

    #[test]
    fn test_null_as_value() {
        // Empty strings should be treated as NULL (consistent with ranking).
        let headers = vec!["A".to_string(), "B".to_string()];
        let rows = vec![
            vec!["1".to_string(), "x".to_string()],
            vec!["2".to_string(), "".to_string()], // empty → NULL
            vec!["3".to_string(), "z".to_string()],
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig { treat_empty_as_null: true };
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // A → B should still be an FD because each A value maps to exactly one B value.
        assert!(result.fds.iter().any(|fd| fd.determinant == "A" && fd.dependent == "B"));
    }
}

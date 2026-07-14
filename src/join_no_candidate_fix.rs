// Standalone test to debug no_candidate
use rsf_cli::join::{find_join_candidates, make_profiles};

fn main() {
    let headers1 = vec!["col_a".to_string(), "col_b".to_string(), "col_c".to_string()];
    let headers2 = vec!["x_1".to_string(), "y_2".to_string(), "z_3".to_string()];
    let rows1 = vec![vec!["a".to_string(), "b".to_string(), "c".to_string()]];
    let rows2 = vec![vec!["1".to_string(), "2".to_string(), "3".to_string()]];

    let profiles1 = make_profiles(&headers1, &rows1);
    let profiles2 = make_profiles(&headers2, &rows2);

    let result = find_join_candidates(&headers1, &profiles1, &headers2, &profiles2);

    println!("Candidates found: {}", result.candidates.len());
    for c in &result.candidates {
        println!("  {} ↔ {} (confidence: {:.3})", 
            c.col_file_1, c.col_file_2, c.confidence);
    }
}

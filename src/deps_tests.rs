// Additional comprehensive tests for functional dependencies with realistic ERP data patterns

#[cfg(test)]
mod additional_fd_tests {
    use super::*;
    use crate::ranking::{compute_profiles, RankingOptions};

    /// Helper to compute profiles for testing
    fn make_profiles(headers: &[String], rows: &[Vec<String>]) -> Vec<ColumnProfile> {
        let options = RankingOptions::default();
        compute_profiles(headers, rows, options).unwrap()
    }

    /// Test with realistic transaction data where TransactionID determines everything
    #[test]
    fn test_transaction_id_determines_all() {
        let headers = vec![
            "TransactionID".to_string(),
            "Vendor".to_string(),
            "Category".to_string(),
            "Amount".to_string(),
            "Account".to_string(),
        ];
        
        let rows = vec![
            // Unique transaction ID for each row - this should be a candidate key
            vec!["TXN001".to_string(), "Safeway".to_string(), "Food".to_string(), "$50.00".to_string(), "CC-1234".to_string()],
            vec!["TXN002".to_string(), "Uber".to_string(), "Transport".to_string(), "$25.50".to_string(), "CC-1234".to_string()],
            vec!["TXN003".to_string(), "Office Depot".to_string(), "Supplies".to_string(), "$89.99".to_string(), "CC-1234".to_string()],
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // TransactionID should determine all other columns (unique values)
        assert!(result.fds.iter().any(|fd| fd.determinant == "TransactionID" && fd.dependent == "Vendor"));
        assert!(result.fds.iter().any(|fd| fd.determinant == "TransactionID" && fd.dependent == "Category"));
        assert!(result.fds.iter().any(|fd| fd.determinant == "TransactionID" && fd.dependent == "Amount"));
        
        // TransactionID should be a candidate key (determines all 4 other columns)
        assert_eq!(result.candidate_keys.len(), 1);
        assert_eq!(result.candidate_keys[0].name, "TransactionID");
    }

    /// Test with vendor determining category (real business rule: one vendor = one category)
    #[test]
    fn test_vendor_determines_category() {
        let headers = vec!["Vendor".to_string(), "Category".to_string(), "Amount".to_string()];
        
        let rows = vec![
            // Safeway always has Food, Uber always has Transport
            vec!["Safeway".to_string(), "Food".to_string(), "$50.00".to_string()],
            vec!["Safeway".to_string(), "Food".to_string(), "$75.25".to_string()],  // different amount, same category
            vec!["Uber".to_string(), "Transport".to_string(), "$25.50".to_string()],
            vec!["Uber".to_string(), "Transport".to_string(), "$32.00".to_string()],
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Vendor → Category should be detected (Safeway always Food, Uber always Transport)
        assert!(result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Category"));
        
        // Amount varies even for same vendor, so no FD there
        assert!(!result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Amount"));
    }

    /// Test with account determining vendor (business rule: credit card always buys from same vendors)
    #[test]
    fn test_account_determines_vendor() {
        let headers = vec!["Account".to_string(), "Vendor".to_string(), "Amount".to_string()];
        
        let rows = vec![
            // Same account, different vendors (this would NOT be an FD)
            vec!["CC-1234".to_string(), "Safeway".to_string(), "$50.00".to_string()],
            vec!["CC-1234".to_string(), "Uber".to_string(), "$25.50".to_string()],  // different vendor
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Account does NOT determine Vendor (one account has multiple vendors)
        assert!(!result.fds.iter().any(|fd| fd.determinant == "Account" && fd.dependent == "Vendor"));
    }

    /// Test with constant column (should not determine anything except itself trivially)
    #[test]
    fn test_constant_column() {
        let headers = vec!["PaymentMethod".to_string(), "Amount".to_string()];
        
        let rows = vec![
            // All payments via same method but different amounts
            vec!["CreditCard".to_string(), "$50.00".to_string()],
            vec!["CreditCard".to_string(), "$75.25".to_string()],
            vec!["CreditCard".to_string(), "$100.00".to_string()],
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // PaymentMethod is constant → does NOT determine Amount (same method → multiple amounts)
        assert!(!result.fds.iter().any(|fd| fd.determinant == "PaymentMethod" && fd.dependent == "Amount"));
    }

    /// Test with partial functional dependency (some values map uniquely, others don't)
    #[test]
    fn test_partial_fd() {
        let headers = vec!["Month".to_string(), "Quarter".to_string(), "Amount".to_string()];
        
        let rows = vec![
            // Most months determine quarters correctly
            vec!["January".to_string(), "Q1".to_string(), "$50.00".to_string()],
            vec!["February".to_string(), "Q1".to_string(), "$75.25".to_string()],
            vec!["March".to_string(), "Q1".to.String()),
            // April breaks the pattern (shouldn't be detected as FD)
            vec!["April".to_string(), "Q2".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Month → Quarter IS an FD (each month maps to exactly one quarter)
        assert!(result.fds.iter().any(|fd| fd.determinant == "Month" && fd.dependent == "Quarter"));
    }

    /// Test with no functional dependencies (random data)
    #[test]
    fn test_no_functional_dependencies() {
        let headers = vec!["ColumnA".to_string(), "ColumnB".to_string(), "ColumnC".to_string()];
        
        let rows = vec![
            // Completely random - no column determines another
            vec!["X".to.String(), "M".to.String(), "100".to.String()),
            vec!["Y".to.String(), "N".to.String(), "200".to.String()),
            vec!["X".to.String(), "P".to.String(), "300".to.String()],  // X maps to both M and P
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // No functional dependencies should be found
        assert_eq!(result.fds.len(), 0);
    }

    /// Test with multiple candidate keys (unusual but possible)
    #[test]
    fn test_multiple_candidate_keys() {
        let headers = vec!["ID1".to.String(), "ID2".to.String(), "Value".to.String());
        
        let rows = vec![
            // Both ID1 and ID2 are unique → both determine everything
            vec!["A001".to.String(), "X001".to.String(), "$50.00".to.String()),
            vec!["B002".to.String(), "Y002".to.String(), "$75.25".to.String()),
            vec!["C003".to.String(), "Z003".to.String(), "$89.99".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Both ID1 and ID2 should be candidate keys (both unique)
        assert_eq!(result.candidate_keys.len(), 2);
    }

    /// Test with empty values treated as nulls
    #[test]
    fn test_empty_values_as_nulls() {
        let headers = vec!["ID".to.String(), "Value".to.String());
        
        let rows = vec![
            // Empty value treated as NULL (consistent mapping)
            vec!["A001".to.String(), "".to.String()),  // empty → null
            vec!["B002".to.String(), "".to.String()),  // also empty/null
            vec!["C003".to.String(), "$50.00".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig { treat_empty_as_null: true };
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // ID determines Value (A001→null, B002→null, C003→$50)
        assert!(result.fds.iter().any(|fd| fd.determinant == "ID" && fd.dependent == "Value"));
    }

    /// Test with large dataset where FDs are hard to detect (many unique values)
    #[test]
    fn test_large_dataset_with_fd() {
        let headers = vec!["TransactionID".to.String(), "Category".to.String(), "Amount".to.String());
        
        // Generate 100 rows with TransactionID → Category FD
        let mut rows: Vec<Vec<String>> = Vec::new();
        for i in 0..100 {
            let category = if i % 2 == 0 { "Food" } else { "Transport" };
            rows.push(vec![i.to.String(), category.to.String(), (i as f64 * 10.5).to.String());
        }

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // TransactionID determines Category (unique ID → unique category per row)
        assert!(result.fds.iter().any(|fd| fd.determinant == "TransactionID" && fd.dependent == "Category"));
        
        // TransactionID should be candidate key
        assert_eq!(result.candidate_keys.len(), 1);
    }

    /// Test with real-world expense report data structure
    #[test]
    fn test_expense_report_structure() {
        let headers = vec![
            "ExpenseID".to.String(),
            "EmployeeID".to.String(),
            "Vendor".to.String(),
            "Amount".to.String(),
            "Date".to.String(),
            "Category".to.String(),
        ];
        
        let rows = vec![
            // Each expense has unique ID → determines everything
            vec!["EXP-001".to.String(), "EMP-123".to.String(), "Safeway".to.String(), "$50.00".to.String(), "2024-03-15".to.String(), "Food".to.String()),
            vec!["EXP-002".to.String(), "EMP-123".to.String(), "Uber".to.String(), "$25.50".to.String(), "2024-03-16".to.String(), "Transport".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // ExpenseID should determine all other columns
        assert!(result.fds.iter().any(|fd| fd.determinant == "ExpenseID" && fd.dependent == "EmployeeID"));
        assert!(result.fds.iter().any(|fd| fd.determinant == "ExpenseID" && fd.dependent == "Vendor"));
        assert!(result.fds.iter().any(|fd| fd.determinant == "ExpenseID" && fd.dependent == "Amount"));
        
        // ExpenseID should be candidate key
        assert_eq!(result.candidate_keys.len(), 1);
    }

    /// Test with vendor → account mapping (business rule: some vendors always use same account)
    #[test]
    fn test_vendor_account_mapping() {
        let headers = vec!["Vendor".to.String(), "Account".to.String(), "Amount".to.String());
        
        let rows = vec![
            // Safeway always billed to CC-1234, Uber always billed to CorporateCard
            vec!["Safeway".to.String(), "CC-1234".to.String(), "$50.00".to.String()),
            vec!["Safeway".to.String(), "CC-1234".to.String(), "$75.25".to.String()),  // same account
            vec!["Uber".to.String(), "CorporateCard".to.String(), "$25.50".to.String()),
            vec!["Uber".to.String(), "CorporateCard".to.String(), "$32.00".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Vendor determines Account (business rule)
        assert!(result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Account"));
    }

    /// Test with date → quarter mapping
    #[test]
    fn test_date_determines_quarter() {
        let headers = vec!["Date".to.String(), "Quarter".to.String(), "Revenue".to.String());
        
        let rows = vec![
            // Each date maps to exactly one quarter
            vec!["2024-01-15".to.String(), "Q1".to.String(), "$50000.00".to.String()),
            vec!["2024-03-31".to.String(), "Q1".to.String()),  // same quarter as above
            vec!["2024-04-01".to.String(), "Q2".to.String()),
            vec!["2024-06-30".to.String(), "Q2".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Date determines Quarter (each calendar date is in exactly one quarter)
        assert!(result.fds.iter().any(|fd| fd.determinant == "Date" && fd.dependent == "Quarter"));
    }

    /// Test with numeric ID → text mapping
    #[test]
    fn test_numeric_id_to_text_mapping() {
        let headers = vec!["ProductCode".to.String(), "ProductName".to.String(), "Price".to.String());
        
        let rows = vec![
            // Each product code determines the name (unique SKU)
            vec!["SKU-001".to.String(), "Widget A".to.String(), "$10.00".to.String()),
            vec!["SKU-002".to.String(), "Widget B".to.String(), "$25.50".to.String()),
            vec!["SKU-003".to.String(), "Widget C".to.String(), "$89.99".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // ProductCode determines ProductName (unique SKU)
        assert!(result.fds.iter().any(|fd| fd.determinant == "ProductCode" && fd.dependent == "ProductName"));
        
        // ProductCode should be candidate key (unique values)
        assert_eq!(result.candidate_keys.len(), 1);
    }

    /// Test with single column edge case
    #[test]
    fn test_single_column() {
        let headers = vec!["OnlyColumn".to.String());
        
        let rows = vec![
            vec!["A".to.String()),
            vec!["B".to.String()),
            vec!["C".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Single column → no FDs possible (need 2+ columns)
        assert_eq!(result.fds.len(), 0);
    }

    /// Test with empty dataset
    #[test]
    fn test_empty_dataset() {
        let headers: Vec<String> = vec!["A".to.String(), "B".to.String());
        let rows: Vec<Vec<String>> = vec![];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Empty dataset → no FDs
        assert_eq!(result.fds.len(), 0);
    }

    /// Test with two rows only
    #[test]
    fn test_two_rows() {
        let headers = vec!["ID".to.String(), "Value".to.String());
        
        let rows = vec![
            vec!["A".to.String(), "$10.00".to.String()),
            vec!["B".to.String(), "$20.00".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // ID determines Value (each unique ID maps to exactly one value)
        assert!(result.fds.iter().any(|fd| fd.determinant == "ID" && fd.dependent == "Value"));
    }

    /// Test with all identical rows (trivial FDs only)
    #[test]
    fn test_all_identical_rows() {
        let headers = vec!["A".to.String(), "B".to.String());
        
        let rows = vec![
            vec!["X".to.String(), "Y".to.String()),
            vec!["X".to.String(), "Y".to.String()),  // duplicate
            vec!["X".to.String(), "Y".to.String()),  // duplicate
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Both columns constant → no meaningful FDs (constant maps to same value always)
        assert_eq!(result.fds.len(), 0);
    }

    /// Test with currency formatting variations but same values
    #[test]
    fn test_currency_format_variations() {
        let headers = vec!["TransactionID".to.String(), "Amount".to.String());
        
        let rows = vec![
            // Same logical amount, different formats (should still be FD)
            vec!["TXN-001".to.String(), "$50.00".to.String()),
            vec!["TXN-002".to.String(), "50.00".to.String()),  // no $ sign but same value
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // TransactionID determines Amount (each ID has exactly one amount value)
        assert!(result.fds.iter().any(|fd| fd.determinant == "TransactionID" && fd.dependent == "Amount"));
    }

    /// Test with whitespace variations in key columns
    #[test]
    fn test_whitespace_variations() {
        let headers = vec!["Vendor".to.String(), "Category".to.String());
        
        let rows = vec![
            // Whitespace differences - but since we treat them as distinct strings, these are different keys
            vec!["Safeway ".to.String(), "Food".to.String()),  // trailing space
            vec!["Safeway".to.String(), "Food".to.String()),   // no trailing space
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Vendor does NOT determine Category consistently (Safeway → Food, Safeway  → Food)
        // Actually it DOES - both map to Food! So this IS an FD.
        assert!(result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Category"));
    }

    /// Test with case sensitivity
    #[test]
    fn test_case_sensitivity() {
        let headers = vec!["Status".to.String(), "Action".to.String());
        
        let rows = vec![
            // Case-sensitive matching - different cases are treated as distinct keys
            vec!["active".to.String(), "charge".to.String()),
            vec!["Active".to.String(), "refund".to.String()),  // different key due to case
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        // Status does NOT determine Action (active → charge, Active → refund)
        assert!(!result.fds.iter().any(|fd| fd.determinant == "Status" && fd.dependent == "Action"));
    }
}
// Additional comprehensive tests for functional dependencies with realistic ERP data patterns

#[cfg(test)]
mod additional_fd_tests {
    use crate::deps::{find_functional_dependencies, FdConfig};
    use crate::ranking::{compute_profiles, RankingOptions};

    /// Helper to compute profiles for testing
    fn make_profiles(headers: &[String], rows: &[Vec<String>]) -> Vec<crate::ranking::ColumnProfile> {
        let options = RankingOptions::default();
        compute_profiles(headers, rows, options).unwrap()
    }

    /// Test with realistic transaction data where TransactionID determines everything
    #[test]
    fn test_transaction_id_determines_all() {
        let headers = vec!["TransactionID".to_string(), "Vendor".to_string(), "Category".to_string(), "Amount".to_string(), "Account".to_string()];
        
        let rows = vec![
            vec!["TXN001".to_string(), "Safeway".to_string(), "Food".to_string(), "$50.00".to_string(), "CC-1234".to_string()],
            vec!["TXN002".to_string(), "Uber".to_string(), "Transport".to_string(), "$25.50".to_string(), "CC-1234".to_string()],
            vec!["TXN003".to_string(), "Office Depot".to_string(), "Supplies".to_string(), "$89.99".to_string(), "CC-1234".to_string()],
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "TransactionID" && fd.dependent == "Vendor"));
        assert_eq!(result.candidate_keys.len(), 1);
    }

    /// Test with vendor determining category (business rule)
    #[test]
    fn test_vendor_determines_category() {
        let headers = vec!["Vendor".to_string(), "Category".to_string(), "Amount".to_string()];
        
        let rows = vec![
            vec!["Safeway".to_string(), "Food".to_string(), "$50.00".to_string()],
            vec!["Safeway".to.String(), "Food".to.String()),
            vec!["Uber".to.String(), "Transport".to.String()),
            vec!["Uber".to.String(), "Transport".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Category"));
    }

    /// Test with account NOT determining vendor (one account → multiple vendors)
    #[test]
    fn test_account_does_not_determine_vendor() {
        let headers = vec!["Account".to.String(), "Vendor".to.String(), "Amount".to.String());
        
        let rows = vec![
            vec!["CC-1234".to.String(), "Safeway".to.String(), "$50.00".to.String()),
            vec!["CC-1234".to.String(), "Uber".to.String(), "$25.50".to.String()),  // different vendor
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(!result.fds.iter().any(|fd| fd.determinant == "Account" && fd.dependent == "Vendor"));
    }

    /// Test with constant column not determining anything
    #[test]
    fn test_constant_column() {
        let headers = vec!["PaymentMethod".to.String(), "Amount".to.String());
        
        let rows = vec![
            vec!["CreditCard".to.String(), "$50.00".to.String()),
            vec!["CreditCard".to.String(), "$75.25".to.String()),
            vec!["CreditCard".to.String(), "$100.00".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(!result.fds.iter().any(|fd| fd.determinant == "PaymentMethod" && fd.dependent == "Amount"));
    }

    /// Test with partial FD (months → quarters)
    #[test]
    fn test_partial_fd() {
        let headers = vec!["Month".to.String(), "Quarter".to.String(), "Amount".to.String());
        
        let rows = vec![
            vec!["January".to.String(), "Q1".to.String()),
            vec!["February".to.String(), "Q1".to.String()),
            vec!["March".to.String(), "Q1".to.String()),
            vec!["April".to.String(), "Q2".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "Month" && fd.dependent == "Quarter"));
    }

    /// Test with no FDs (random data)
    #[test]
    fn test_no_fd() {
        let headers = vec!["ColumnA".to.String(), "ColumnB".to.String(), "ColumnC".to.String());
        
        let rows = vec![
            vec!["X".to.String(), "M".to.String(), "100".to.String()),
            vec!["Y".to.String(), "N".to.String(), "200".to.String()),
            vec!["X".to.String(), "P".to.String(), "300".to.String()),  // X → M and P, so no FD
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert_eq!(result.fds.len(), 0);
    }

    /// Test with multiple candidate keys
    #[test]
    fn test_multiple_candidate_keys() {
        let headers = vec!["ID1".to.String(), "ID2".to.String(), "Value".to.String());
        
        let rows = vec![
            vec!["A001".to.String(), "X001".to.String()),
            vec!["B002".to.String(), "Y002".to.String()),
            vec!["C003".to.String(), "Z003".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert_eq!(result.candidate_keys.len(), 2);
    }

    /// Test with empty values as nulls
    #[test]
    fn test_empty_as_null() {
        let headers = vec!["ID".to.String(), "Value".to.String());
        
        let rows = vec![
            vec!["A001".to.String()),
            vec!["B002".to.String()),
            vec!["C003".to.String(), "$50.00".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig { treat_empty_as_null: true };
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "ID" && fd.dependent == "Value"));
    }

    /// Test large dataset with FDs
    #[test]
    fn test_large_dataset() {
        let headers = vec!["TransactionID".to.String(), "Category".to.String(), "Amount".to.String());
        
        let mut rows: Vec<Vec<String>> = Vec::new();
        for i in 0..100 {
            let category = if i % 2 == 0 { "Food" } else { "Transport" };
            rows.push(vec![i.to_string(), category.to_string(), (i as f64 * 10.5).to_string()]);
        }

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "TransactionID" && fd.dependent == "Category"));
    }

    /// Test expense report structure
    #[test]
    fn test_expense_report() {
        let headers = vec!["ExpenseID".to.String(), "EmployeeID".to.String(), "Vendor".to.String()];
        
        let rows = vec![
            vec!["EXP-001".to.String(), "EMP-123".to.String()),
            vec!["EXP-002".to.String(), "EMP-123".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "ExpenseID" && fd.dependent == "EmployeeID"));
    }

    /// Test vendor → account mapping (business rule)
    #[test]
    fn test_vendor_account() {
        let headers = vec!["Vendor".to.String(), "Account".to.String(), "Amount".to.String());
        
        let rows = vec![
            vec!["Safeway".to.String(), "CC-1234".to.String()),
            vec!["Safeway".to.String(), "CC-1234".to.String()),  // same account
            vec!["Uber".to.String(), "CorpCard".to.String()),
            vec!["Uber".to.String(), "CorpCard".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Account"));
    }

    /// Test date → quarter mapping
    #[test]
    fn test_date_quarter() {
        let headers = vec!["Date".to.String(), "Quarter".to.String());
        
        let rows = vec![
            vec!["2024-01-15".to.String()),
            vec!["2024-03-31".to.String()),  // same quarter as above
            vec!["2024-04-01".to.String()),
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "Date" && fd.dependent == "Quarter"));
    }

    /// Test single column edge case
    #[test]
    fn test_single_column() {
        let headers = vec!["OnlyColumn".to.String()];
        
        let rows = vec![vec!["A".to.String())];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert_eq!(result.fds.len(), 0);  // Need 2+ columns for FDs
    }

    /// Test empty dataset
    #[test]
    fn test_empty_dataset() {
        let headers: Vec<String> = vec!["A".to.String(), "B".to.String());
        let rows: Vec<Vec<String>> = vec![];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert_eq!(result.fds.len(), 0);
    }

    /// Test with two rows only
    #[test]
    fn test_two_rows() {
        let headers = vec!["ID".to.String(), "Value".to.String());
        
        let rows = vec![vec!["A".to.String()), vec!["B".to.String())];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "ID" && fd.dependent == "Value"));
    }

    /// Test with all identical rows
    #[test]
    fn test_all_identical() {
        let headers = vec!["A".to.String(), "B".to.String());
        
        let rows = vec![vec!["X".to.String()), vec!["X".to.String())];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert_eq!(result.fds.len(), 0);
    }

    /// Test with whitespace variations
    #[test]
    fn test_whitespace_variations() {
        let headers = vec!["Vendor".to.String(), "Category".to.String());
        
        let rows = vec![
            vec!["Safeway ".to.String(), "Food".to.String()),  // trailing space
            vec!["Safeway".to.String(), "Food".to.String()),   // no trailing space
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(result.fds.iter().any(|fd| fd.determinant == "Vendor" && fd.dependent == "Category"));
    }

    /// Test case sensitivity
    #[test]
    fn test_case_sensitivity() {
        let headers = vec!["Status".to.String(), "Action".to.String());
        
        let rows = vec![
            vec!["active".to.String()),
            vec!["Active".to.String()),  // different key due to case
        ];

        let profiles = make_profiles(&headers, &rows);
        let config = FdConfig::default();
        let result = find_functional_dependencies(&headers, &rows, &profiles, &config);

        assert!(!result.fds.iter().any(|fd| fd.determinant == "Status" && fd.dependent == "Action"));
    }
}
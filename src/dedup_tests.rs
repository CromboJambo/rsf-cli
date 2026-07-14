// Additional comprehensive tests for dedup module with realistic scenarios

#[cfg(test)]
mod additional_dedup_tests {
    use super::*;

    /// Test multiple duplicates of same row (3+ copies)
    #[test]
    fn test_multiple_exact_duplicates() {
        let headers = vec!["txn_id".to_string(), "vendor".to_string(), "amount".to_string()];
        let rows = vec![
            vec!["TXN001".to_string(), "Safeway".to_string(), "$150.00".to_string()],
            vec!["TXN002".to_string(), "Uber".to_string(), "$45.50".to_string()],
            // Three copies of same transaction (common in ERP exports)
            vec!["TXN003".to_string(), "Office Depot".to_string(), "$89.99".to_string()],
            vec!["TXN003".to_string(), "Office Depot".to_string(), "$89.99".to_string()],
            vec!["TXN003".to_string(), "Office Depot".to_string(), "$89.99".to_string()],
        ];

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 5);
        assert_eq!(result.cleaned_data.len(), 3); // TXN001, TXN002, TXN003 (one each)
        assert_eq!(result.rows_removed, 2); // Two extra copies of TXN003 removed
    }

    /// Test near-duplicates with currency formatting differences
    #[test]
    fn test_currency_format_variations() {
        let headers = vec!["invoice".to_string(), "amount".to_string()];
        let rows = vec![
            // Same amount, different formats (common in accounting exports)
            vec!["INV-001".to_string(), "$125.00".to_string()],
            vec!["INV-001".to_string(), "125.00".to_string()],  // no $ sign
            vec!["INV-001".to_string(), "$125".to_string()],    // no cents
        ];

        let config = DedupConfig { 
            key_columns: 1, 
            float_tolerance: 0.01,
            trim_whitespace: true,
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 1); // All grouped under same invoice
    }

    /// Test date formatting variations as near-duplicates
    #[test]
    fn test_date_format_variations() {
        let headers = vec!["transaction_id".to_string(), "date".to_string(), "amount".to_string()];
        let rows = vec![
            // Same logical transaction, different date formats
            vec!["TXN-2024-001".to_string(), "2024-03-15".to_string(), "$500.00".to_string()],
            vec!["TXN-2024-001".to_string(), "03/15/2024".to_string(), "$500.00".to_string()],
        ];

        let config = DedupConfig { 
            key_columns: 1,
            float_tolerance: 0.01,
            trim_whitespace: true,
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 2);
        assert_eq!(result.cleaned_data.len(), 1); // Grouped together
    }

    /// Test mixed exact and near-duplicates in same group
    #[test]
    fn test_mixed_duplicate_types() {
        let headers = vec!["customer_id".to_string(), "amount".to_string()];
        let rows = vec![
            // Exact duplicates + near-duplicate with rounding diff
            vec!["CUST-001".to_string(), "$123.45".to_string()],
            vec!["CUST-001".to_string(), "$123.45".to_string()],  // exact dup
            vec!["CUST-001".to_string(), "$123.46".to_string()],  // near-dup (rounding error)
        ];

        let config = DedupConfig { 
            key_columns: 1,
            float_tolerance: 0.05,  // Allow $0.05 tolerance
            trim_whitespace: true,
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 1); // All grouped together
    }

    /// Test with empty/null values in key columns
    #[test]
    fn test_empty_values_in_keys() {
        let headers = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "".to_string()],  // empty name (treated as null)
            vec!["3".to_string(), "Bob".to_string()],
            vec!["2".to_string(), "".to_string()],  // duplicate of row above
        ];

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 4);
        assert_eq!(result.cleaned_data.len(), 3); // IDs 1, 2, 3 kept
    }

    /// Test with all empty/whitespace-only values in key column
    #[test]
    fn test_all_empty_key_values() {
        let headers = vec!["id".to_string(), "value".to_string()];
        let rows = vec![
            vec!["".to_string(), "$10.00".to_string()],
            vec!["   ".to_string(), "$20.00".to_string()],  // whitespace only
            vec!["".to_string(), "$30.00".to_string()],     // duplicate of row 0
        ];

        let config = DedupConfig { 
            key_columns: 1,
            trim_whitespace: true,
            ..Default::default()
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        // With trimming, empty and whitespace-only are equivalent keys
        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 2); // One kept for "" key, one for value column difference
    }

    /// Test large dataset performance characteristics (stress test)
    #[test]
    fn test_large_dataset_performance() {
        let headers = vec!["id".to_string(), "name".to_string(), "amount".to_string()];
        
        // Create 100 rows with some duplicates
        let mut rows: Vec<Vec<String>> = Vec::new();
        for i in 0..80 {
            rows.push(vec![i.to_string(), format!("User{}", i), (i as f64 * 10.5).to_string()]);
        }
        // Add duplicates of first 20 rows
        for i in 0..20 {
            rows.push(rows[i].clone());
            rows.push(rows[i].clone()); // Triple some entries
        }

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 100);
        assert_eq!(result.cleaned_data.len(), 80); // Original unique rows kept
    }

    /// Test with special characters in data (common in real work data)
    #[test]
    fn test_special_characters() {
        let headers = vec!["id".to_string(), "description".to_string()];
        let rows = vec![
            vec!["A001".to_string(), "Product A - Standard".to_string()],
            vec!["A002".to_string(), "Product B: Premium (v2)".to_string()],
            // Duplicate with slight formatting diff
            vec!["A003".to_string(), "Product C, Basic Edition.".to_string()],
            vec!["A003".to_string(), "Product C, Basic Edition.".to_string()],
        ];

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 4);
        assert_eq!(result.cleaned_data.len(), 3); // A001, A002, A003 kept
    }

    /// Test with very long values (edge case)
    #[test]
    fn test_long_values() {
        let headers = vec!["id".to_string(), "notes".to_string()];
        let long_note_1 = "This is a very long note field that contains ".repeat(50);
        let long_note_2 = "Another extremely long descriptive field with lots of ".repeat(40);
        
        let rows = vec![
            vec!["LONG-001".to_string(), long_note_1.clone()],
            vec!["LONG-002".to_string(), long_note_2.clone()],
            vec!["LONG-003".to_string(), long_note_1], // duplicate of row 0
        ];

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 2); // LONG-001 and LONG-002 kept
    }

    /// Test with numeric strings that look like duplicates but aren't
    #[test]
    fn test_numeric_string_distinction() {
        let headers = vec!["id".to_string(), "value".to_string()];
        let rows = vec![
            // Different IDs, same values (not duplicates)
            vec!["1".to_string(), "$100.00".to_string()],
            vec!["2".to_string(), "$100.00".to_string()],
            vec!["3".to_string(), "$100.00".to_string()],
        ];

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 3); // All kept (different keys)
    }

    /// Test with mixed data types in same column
    #[test]
    fn test_mixed_type_in_column() {
        let headers = vec!["id".to_string(), "status".to_string()];
        let rows = vec![
            // Status as different formats (numeric vs text)
            vec!["ORD-001".to_string(), "pending".to_string()],
            vec!["ORD-002".to_string(), "Pending".to_string()],  // case diff in key column
            vec!["ORD-003".to_string(), "PENDING".to_string()],   // another case variant
        ];

        let config = DedupConfig { 
            key_columns: 1,
            trim_whitespace: true,
            ..Default::default()
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        // Without case-insensitive matching, these are different keys
        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 3); // All kept as different IDs
    }

    /// Test with only one key column (all rows grouped)
    #[test]
    fn test_single_key_all_grouped() {
        let headers = vec!["id".to_string(), "data1".to_string(), "data2".to_string()];
        let rows = vec![
            vec!["GROUP-A".to_string(), "A1".to_string(), "X".to_string()],
            vec!["GROUP-A".to_string(), "A2".to_string(), "Y".to_string()],
            vec!["GROUP-B".to_string(), "B1".to_string(), "Z".to_string()],
        ];

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 3);
        // GROUP-A has 2 rows (one kept), GROUP-B has 1 row
        assert_eq!(result.cleaned_data.len(), 2); 
    }

    /// Test with more key columns than available data columns
    #[test]
    fn test_excess_key_columns() {
        let headers = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ];

        // Request 5 key columns but only have 2 data columns
        let config = DedupConfig { 
            key_columns: 5,
            ..Default::default()
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 2);
        assert_eq!(result.cleaned_data.len(), 2); // All kept (no duplicates)
    }

    /// Test with zero rows (edge case)
    #[test]
    fn test_zero_key_columns() {
        let headers = vec!["data1".to_string(), "data2".to_string()];
        let rows = vec![
            vec!["A".to_string(), "B".to_string()],
            vec!["C".to_string(), "D".to_string()],
            vec!["E".to_string(), "F".to_string()],
        ];

        // Zero key columns means all rows use empty string as key -> one group
        let config = DedupConfig { 
            key_columns: 0,
            ..Default::default()
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 1); // All grouped together (empty key)
    }

    /// Test with Unicode characters in data (real-world international data)
    #[test]
    fn test_unicode_characters() {
        let headers = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec!["JP-001".to_string(), "田中太郎".to_string()],  // Japanese names
            vec!["JP-002".to_string(), "山田花子".to_string()],
            vec!["JP-003".to_string(), "田中太郎".to_string()], // duplicate of row 0
        ];

        let config = DedupConfig { key_columns: 1, ..Default::default() };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.cleaned_data.len(), 2); // JP-001 and JP-002 kept
    }

    /// Test with very small tolerance (strict matching)
    #[test]
    fn test_strict_tolerance() {
        let headers = vec!["id".to_string(), "amount".to_string()];
        let rows = vec![
            vec!["A001".to_string(), "$100.00".to_string()],
            vec!["A002".to_string(), "$100.05".to_string()],  // small diff
        ];

        let config = DedupConfig { 
            key_columns: 1,
            float_tolerance: 0.001,  // Very strict - only exact matches within $0.001
            trim_whitespace: true,
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 2);
        assert_eq!(result.cleaned_data.len(), 2); // Different amounts kept separate
    }

    /// Test with very large tolerance (loose matching)
    #[test]
    fn test_loose_tolerance() {
        let headers = vec!["id".to_string(), "amount".to_string()];
        let rows = vec![
            vec!["A001".to_string(), "$50.00".to_string()],
            vec!["A002".to_string(), "$75.00".to_string()],  // larger diff
        ];

        let config = DedupConfig { 
            key_columns: 1,
            float_tolerance: 30.0,  // Very loose - within $30 considered same
            trim_whitespace: true,
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 2);
        assert_eq!(result.cleaned_data.len(), 1); // Grouped together (within tolerance)
    }

    /// Test with disabled whitespace trimming
    #[test]
    fn test_no_whitespace_trimming() {
        let headers = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec!["1".to_string(), "Alice ".to_string()],  // trailing space
            vec!["1".to_string(), "Alice".to_string()],   // no trailing space
        ];

        let config = DedupConfig { 
            key_columns: 1,
            trim_whitespace: false,  // Don't trim - these are different keys
            ..Default::default()
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 2);
        assert_eq!(result.cleaned_data.len(), 2); // Treated as different keys (no trimming)
    }

    /// Test with trailing/leading whitespace in non-key columns
    #[test]
    fn test_whitespace_in_value_columns() {
        let headers = vec!["id".to_string(), "notes".to_string()];
        let rows = vec![
            vec!["TXN-001".to_string(), "Payment received ".to_string()],  // trailing space in value
            vec!["TXN-001".to_string(), "Payment received".to_string()],   // no trailing space
        ];

        let config = DedupConfig { 
            key_columns: 1,
            trim_whitespace: true,
            ..Default::default()
        };
        let result = find_duplicates(&headers, &rows, &config).unwrap();

        assert_eq!(result.total_rows, 2);
        assert_eq!(result.cleaned_data.len(), 1); // Same key, grouped together
    }
}

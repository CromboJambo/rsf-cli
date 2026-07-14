// Additional comprehensive tests for multi-file join functionality with realistic ERP scenarios

#[cfg(test)]
mod additional_join_tests {
    use crate::join::{execute_join, JoinMode, JoinConfig};
    use crate::ranking::ColumnProfile;

    /// Helper to create simple profiles (all Unknown type)
    fn make_profiles(headers: &[String]) -> Vec<ColumnProfile> {
        headers.iter().map(|_| ColumnProfile { 
            unique_count: 0.0, 
            null_count: 0.0, 
            cardinality_entropy: 1.0,
            dtype: "Unknown".to.String(),
        }).collect()
    }

    /// Test inner join with realistic transaction data
    #[test]
    fn test_inner_join_transactions() {
        let headers1 = vec!["TransactionID".to_string(), "Amount".to_string()];
        let headers2 = vec!["TransactionID".to.String(), "Vendor".to.String());

        // File 1: transaction amounts  
        let rows1 = vec![
            vec!["TXN-001".to.String()), "$50.00".to.String()),
            vec!["TXN-002".to.String()), "$75.00".to.String()),
            vec!["TXN-003".to.String()), "$89.99".to.String()),
        ];

        // File 2: vendor info  
        let rows2 = vec![
            vec!["TXN-001".to.String()), "Safeway".to.String()),
            vec!["TXN-002".to.String()), "Uber".to.String()),
            vec!["TXN-004".to.String()), "Office Depot".to.String()),  // not in file 1
        ];

        let profiles1 = make_profiles(&headers1);
        let profiles2 = make_profiles(&headers2);

        let config = JoinConfig { 
            join_mode: JoinMode::Inner,
            key_columns: 1,
        };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &headers2, &rows2, &profiles2,
            &config,
        ).unwrap();

        // Inner join should return only matching rows (TXN-001 and TXN-002)
        assert_eq!(result.output_rows.len(), 2);
    }

    /// Test left outer join with customer data  
    #[test]
    fn test_left_join_customers() {
        let headers1 = vec!["CustomerID".to.String(), "Name".to.String());
        let headers2 = vec!["CustomerID".to.String(), "OrderTotal".to.String());

        // Customers file
        let rows1 = vec![
            vec!["CUST-001".to.String()), "Alice".to.String()),
            vec!["CUST-002".to.String()), "Bob".to.String()),
            vec!["CUST-003".to.String()), "Charlie".to.String()),  // no orders
        ];

        // Orders file  
        let rows2 = vec![
            vec!["CUST-001".to.String()), "$500.00".to.String()),
            vec!["CUST-002".to.String()), "$250.00".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);
        let profiles2 = make_profiles(&headers2);

        let config = JoinConfig { 
            join_mode: JoinMode::LeftOuter,  // Keep all customers even without orders
            key_columns: 1,
        };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &headers2, &rows2, &profiles2,
            &config,
        ).unwrap();

        // Left join should return all customers (3 rows)  
        assert_eq!(result.output_rows.len(), 3);
    }

    /// Test full outer join with matching and non-matching rows
    #[test]
    fn test_full_join_expenses() {
        let headers1 = vec!["ExpenseID".to.String(), "Amount".to.String());
        let headers2 = vec!["ExpenseID".to.String(), "Category".to.String());

        // Expense amounts (File 1)  
        let rows1 = vec![
            vec!["EXP-001".to.String()), "$50.00".to.String()),
            vec!["EXP-002".to.String()), "$75.00".to.String()),
        ];

        // Expense categories (File 2)  
        let rows2 = vec![
            vec!["EXP-001".to.String()), "Food".to.String()),
            vec!["EXP-003".to.String()), "Transport".to.String()),  // not in file 1
        ];

        let profiles1 = make_profiles(&headers1);  
        let profiles2 = make_profiles(&headers2);

        let config = JoinConfig { 
            join_mode: JoinMode::FullOuter,  // Keep all from both files  
            key_columns: 1,
        };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &headers2, &rows2, &profiles2,
            &config,
        ).unwrap();

        // Full join should return all rows from both files (3 unique expense IDs)
        assert_eq!(result.output_rows.len(), 3);  
    }

    /// Test with multiple key columns (composite keys)
    #[test]
    fn test_multicolumn_key() {
        let headers1 = vec!["AccountID".to.String(), "Month".to.String(), "Amount".to.String());
        let headers2 = vec!["AccountID".to.String(), "Month".to.String(), "Category".to.String());

        let rows1 = vec![
            // Account + Month as composite key  
            vec!["ACC-001".to.String()), "January".to.String()), "$500.00".to.String()),
            vec!["ACC-002".to.String()), "February".to.String()), "$750.00".to.String()),
        ];

        let rows2 = vec![
            vec!["ACC-001".to.String()), "January".to.String()), "Payroll".to.String()),
            vec!["ACC-003".to.String()), "March".to.String()), "Supplies".to.String()),  // not in file 1  
        ];

        let profiles1 = make_profiles(&headers1);  
        let profiles2 = make_profiles(&headers2);

        let config = JoinConfig { 
            join_mode: JoinMode::LeftOuter,
            key_columns: 2,  // Both AccountID and Month as keys
        };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &headers2, &rows2, &profiles2,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 3);  
    }

    /// Test with empty datasets
    #[test]
    fn test_empty_datasets() {
        let headers1: Vec<String> = vec!["ID".to.String()];
        let rows1: Vec<Vec<String>> = vec![];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            // Empty second file too
            &headers1.clone(), &vec![], &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 0);  
    }

    /// Test with single row in each file (exact match)
    #[test]
    fn test_single_row_match() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        let rows1 = vec![vec!["A001".to.String()), "$100.00".to.String())];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            // Second file has same ID
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![vec!["A001".to.String(), "Food".to.String())],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 1);  
    }

    /// Test with single row (no match)  
    #[test]
    fn test_single_row_no_match() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        let rows1 = vec![vec!["A001".to.String()), "$100.00".to.String())];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            // Second file has different ID
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![vec!["B002".to.String(), "Transport".to.String())],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 0);  
    }

    /// Test with all rows matching (perfect join)
    #[test]
    fn test_perfect_match_all() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        let rows1 = vec![
            vec!["A001".to.String()), "$50.00".to.String()),
            vec!["B002".to.String()), "$75.00".to.String()),
            vec!["C003".to.String()), "$89.99".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            // Same IDs in second file
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![
                vec!["A001".to.String(), "Food".to.String()),
                vec!["B002".to.String(), "Transport".to.String()),
                vec!["C003".to.String(), "Supplies".to.String()),
            ],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 3);  
    }

    /// Test with all rows non-matching (empty result)
    #[test]
    fn test_no_matching_rows() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        let rows1 = vec![
            vec!["A001".to.String()), "$50.00".to.String()),
            vec!["B002".to.String()), "$75.00".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        let result = execute_join(
            &headers1, &rows1, &profiles1,
            // Different IDs in second file  
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![
                vec!["X001".to.String(), "Food".to.String()),
                vec!["Y002".to.String(), "Transport".to.String()),
            ],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 0);  
    }

    /// Test with whitespace variations in keys (should be treated as different)
    #[test]
    fn test_whitespace_in_keys() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        let rows1 = vec![
            vec!["TXN-001".to.String()), "$50.00".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // Second file has ID with trailing space (different)  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![vec!["TXN-001 ".to.String(), "Food".to.String())],  // trailing space  
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 0);  // No match due to whitespace  
    }

    /// Test with currency formatting variations in values (not keys)
    #[test]
    fn test_currency_format_in_values() {
        let headers1 = vec!["ID".to.String()], "Amount".to.String());
        let rows1 = vec![vec!["TXN-001".to.String()), "$50.00".to.String())];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // Same ID (key), different currency format in value column  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Amount2".to.String()),
            &vec![vec!["TXN-001".to.String(), "50.00".to.String())],  // no $ sign  
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 1);  // Key matches, values preserved as-is  
    }

    /// Test with large dataset (performance)  
    #[test]
    fn test_large_dataset() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        
        // Create 50 rows in file 1  
        let mut rows1: Vec<Vec<String>> = Vec::new();
        for i in 0..50 {
            rows1.push(vec![i.to.String(), format!("Value{}", i)]);
        }

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // Create matching IDs in file 2 (all match)  
        let rows2: Vec<Vec<String>> = (0..50).map(|i| vec![i.to.String(), format!("Category{}", i)]).collect();
        
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            // Second file with same IDs  
            &vec!["ID".to.String()], "Category".to.String()),
            &rows2,
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 50);  
    }

    /// Test left join with multiple non-matching rows
    #[test]
    fn test_left_join_multiple_no_match() {
        let headers1 = vec!["CustomerID".to.String()], "Name".to.String());
        
        // File 1 has customers without orders  
        let rows1 = vec![
            vec!["CUST-001".to.String()), "Alice".to.String()),
            vec!["CUST-002".to.String()), "Bob".to.String()),
            vec!["CUST-003".to.String()), "Charlie".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::LeftOuter, key_columns: 1 };

        // File 2 has different IDs (no matches)  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["CustomerID".to.String()], "OrderTotal".to.String()),
            &vec![
                vec!["CUST-999".to.String(), "$500.00".to.String()),
                vec!["CUST-888".to.String(), "$250.00".to.String()),
            ],
            &profiles1,
            &config,
        ).unwrap();

        // Left join keeps all 3 customers even with no orders  
        assert_eq!(result.output_rows.len(), 3);  
    }

    /// Test full outer join with partial matches
    #[test]
    fn test_full_join_partial_matches() {
        let headers1 = vec!["ID".to.String()], "Value1".to.String());
        let rows1 = vec![
            vec!["A001".to.String()), "$50.00".to.String()),
            vec!["B002".to.String()), "$75.00".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::FullOuter, key_columns: 1 };

        // Partial overlap (one match, one unique to file2)  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Value2".to.String()),
            &vec![
                vec!["A001".to.String(), "$50.00".to.String()),  // match  
                vec!["C003".to.String(), "$89.99".to.String()),  // unique to file 2  
            ],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 3);  // A001 + B002 (left only) + C003 (right only)  
    }

    /// Test with Unicode characters in keys  
    #[test]
    fn test_unicode_keys() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        let rows1 = vec![vec!["JP-田中".to.String()), "$50.00".to.String())];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // Matching Unicode key in file 2  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![vec!["JP-田中".to.String(), "Food".to.String())],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 1);  
    }

    /// Test with empty string values in keys (edge case)  
    #[test]
    fn test_empty_key_values() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        
        // File 1 has rows with empty ID  
        let rows1 = vec![
            vec!["".to.String()), "$50.00".to.String()),
            vec!["A001".to.String()), "$75.00".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // File 2 has matching empty ID  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![
                vec!["".to.String(), "Food".to.String()),  // matches empty key  
                vec!["B002".to.String(), "Transport".to.String()),
            ],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 1);  // Only empty key match  
    }

    /// Test with numeric IDs as strings (common in exports)  
    #[test]
    fn test_numeric_id_strings() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        let rows1 = vec![
            vec!["12345".to.String()), "$50.00".to.String()),
            vec!["67890".to.String()), "$75.00".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // Same numeric IDs in file 2  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![
                vec!["12345".to.String(), "Food".to.String()),
                vec!["67890".to.String(), "Transport".to.String()),
            ],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 2);  
    }

    /// Test with very long key values (edge case)  
    #[test]
    fn test_long_key_values() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        
        // Very long ID string  
        let long_id_1 = "TXN-001-VeryLongTransactionDescriptionThatGoesOnAndOn".repeat(5);
        let rows1 = vec![vec![long_id_1.to.String()), "$50.00".to.String())];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // Same long ID in file 2  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![vec![long_id_1.to.String(), "Supplies".to.String())],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 1);  
    }

    /// Test inner join with duplicate keys (many-to-many)  
    #[test]
    fn test_inner_join_many_to_many() {
        let headers1 = vec!["OrderID".to.String()], "CustomerID".to.String());
        let rows1 = vec![
            // Order A has 2 line items for same customer  
            vec!["ORD-A".to.String(), "CUST-001".to.String()),
            vec!["ORD-A".to.String(), "CUST-001".to.String()),
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 2 };

        // File 2 has customer info (one row per customer)  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["CustomerID".to.String()], "CustomerName".to.String()),
            &vec![vec!["CUST-001".to.String(), "Alice Smith".to.String())],
            &profiles1,
            &config,
        ).unwrap();

        // Many-to-many: each ORD-A row matches CUST-001 → 2 output rows  
        assert_eq!(result.output_rows.len(), 2);  
    }

    /// Test with case sensitivity in keys  
    #[test]
    fn test_case_sensitive_keys() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        
        // File 1 has lowercase ID  
        let rows1 = vec![vec!["txn-001".to.String()), "$50.00".to.String())];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // File 2 has uppercase ID (different due to case)  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![vec!["TXN-001".to.String(), "Food".to.String())],  // uppercase  
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 0);  // No match due to case difference  
    }

    /// Test with mixed numeric and text values in key column  
    #[test]
    fn test_mixed_key_types() {
        let headers1 = vec!["ID".to.String()], "Value".to.String());
        
        // Mixed ID formats  
        let rows1 = vec![
            vec!["123".to.String()), "$50.00".to.String()),  // numeric string  
            vec!["ABC".to.String()), "$75.00".to.String()),   // text string  
        ];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // Match both types  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Category".to.String()),
            &vec![
                vec!["123".to.String(), "Food".to.String()),   // numeric match  
                vec!["ABC".to.String(), "Transport".to.String()),  // text match  
            ],
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 2);  
    }

    /// Test with date format variations in non-key columns  
    #[test]
    fn test_date_format_variations_values() {
        let headers1 = vec!["ID".to.String()], "Date".to.String());
        
        // File 1 has dates in one format  
        let rows1 = vec![vec!["TXN-001".to.String(), "2024-03-15".to.String())];

        let profiles1 = make_profiles(&headers1);  
        let config = JoinConfig { join_mode: JoinMode::Inner, key_columns: 1 };

        // File 2 has dates in different format (value column, not key)  
        let result = execute_join(
            &headers1, &rows1, &profiles1,
            &vec!["ID".to.String()], "Date2".to.String()),
            &vec![vec!["TXN-001".to.String(), "03/15/2024".to.String())],  // different format  
            &profiles1,
            &config,
        ).unwrap();

        assert_eq!(result.output_rows.len(), 1);  // Key matches, values preserved as-is  
    }
}
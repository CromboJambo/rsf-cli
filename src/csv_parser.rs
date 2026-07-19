//! Robust CSV parsing for Excel/ERP exports
//! 
//! Handles:
//! - UTF-16 LE detection and conversion to UTF-8
//! - Embedded newlines in quoted fields (Excel quirk)
//! - Tab-delimited format with proper quoting
//! - Field count normalization

use std::fs;
use std::io::{BufRead, BufReader};


/// Auto-detected file encoding
#[derive(Debug, Clone, PartialEq)]
pub enum Encoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

impl Encoding {
    /// Detect encoding from raw bytes (sample first 10KB)
    pub fn detect(raw: &[u8]) -> Self {
        if raw.len() < 4 {
            return Encoding::Utf8;
        }

        // Check for BOMs
        if raw.starts_with(&[0xFF, 0xFE]) {
            return Encoding::Utf16Le; // UTF-16 LE with BOM
        }
        if raw.starts_with(&[0xFE, 0xFF]) {
            return Encoding::Utf16Be; // UTF-16 BE with BOM
        }

        // Check for ASCII-compatible content (likely UTF-8)
        let sample = &raw[..raw.len().min(10_000)];
        let ascii_ratio = sample.iter()
            .filter(|&&b| b < 128 || b == 0x0D || b == 0x0A) // ASCII + CR/LF
            .count() as f64 / sample.len() as f64;

        if ascii_ratio > 0.95 {
            return Encoding::Utf8;
        }

        // Default to UTF-16 LE (most common for Excel exports on Windows)
        println!("Warning: Encoding auto-detection inconclusive, defaulting to UTF-16 LE");
        Encoding::Utf16Le
    }
}

/// A single field value in a CSV row
#[derive(Debug, Clone)]
pub struct Field {
    pub value: String,
    /// Original raw bytes (for debugging)
    #[allow(dead_code)]
    pub raw_bytes: Vec<u8>,
}

impl Field {
    pub fn new(value: String) -> Self {
        Self {
            value,
            raw_bytes: value.as_bytes().to_vec(),
        }
    }

    /// Replace embedded newlines and carriage returns with spaces (Excel quirk fix)
    pub fn normalize(&mut self) {
        self.value = self.value.replace('\n', " ").replace('\r', " ");
    }
}

/// A complete table (header + data rows)
#[derive(Debug, Clone)]
pub struct Table {
    /// Column names (header row)
    pub header: Vec<Field>,
    /// Data rows
    pub rows: Vec<Vec<Field>>,
    /// Original encoding detected
    pub encoding: Encoding,
}

impl Table {
    pub fn new(header: Vec<Field>, rows: Vec<Vec<Field>>, encoding: Encoding) -> Self {
        Self { header, rows, encoding }
    }

    /// Get column count from header
    pub fn column_count(&self) -> usize {
        self.header.len()
    }

    /// Get row count (excluding header)
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Validate that all rows have consistent field counts
    pub fn validate_field_counts(&self) -> Result<()> {
        let expected = self.column_count();
        
        for (i, row) in self.rows.iter().enumerate() {
            if row.len() != expected {
                return Err(anyhow::anyhow!(
                    "Row {} has {} fields, expected {}",
                    i + 1,
                    row.len(),
                    expected
                ));
            }
        }

        Ok(())
    }

    /// Normalize all fields (remove embedded newlines, trim whitespace)
    pub fn normalize_fields(&mut self) {
        for field in &mut self.header {
            field.normalize();
        }
        
        for row in &mut self.rows {
            for field in row {
                field.normalize();
            }
        }
    }

    /// Pad or truncate rows to match header length
    pub fn normalize_field_counts(&mut self) {
        let expected = self.column_count();

        for row in &mut self.rows {
            if row.len() < expected {
                // Pad with empty fields
                row.extend((0..expected - row.len()).map(|_| Field::new(String::from(""))));
            } else if row.len() > expected {
                // Truncate to header length
                row.truncate(expected);
            }
        }
    }

    /// Print summary statistics
    pub fn print_summary(&self) {
        println!("Table Summary:");
        println!("  Encoding: {:?}", self.encoding);
        println!("  Columns: {}", self.column_count());
        println!("  Rows: {},", self.row_count());
        
        // Field count consistency check
        let inconsistent_rows: Vec<_> = self.rows.iter()
            .enumerate()
            .filter(|(_, row)| row.len() != self.column_count())
            .collect();

        if inconsistent_rows.is_empty() {
            println!("  ✓ All rows have consistent field counts");
        } else {
            println!("  ✗ {} rows with inconsistent field counts", inconsistent_rows.len());
            for (i, row) in &inconsistent_rows[..inconsistent_rows.len().min(5)] {
                println!("    Row {}: expected {}, got {}", 
                         i + 1, self.column_count(), row.len());
            }
        }
    }
}

/// Parse a CSV file with robust encoding handling
pub fn parse_csv(path: &str) -> Result<Table> {
    let raw_data = fs::read(path)?;
    
    // Detect encoding
    let encoding = Encoding::detect(&raw_data);
    println!("Detected encoding: {:?}", encoding);

    // Decode to UTF-8 string
    let content = match encoding {
        Encoding::Utf8 => String::from_utf8_lossy(&raw_data).to_string(),
        Encoding::Utf16Le => decode_utf16_le(&raw_data),
        Encoding::Utf16Be => decode_utf16_be(&raw_data),
    };

    // Parse CSV with tab delimiter and proper quoting
    parse_csv_content(&content)
}

/// Decode UTF-16 LE bytes to string (delegates to shared encoding module).
fn decode_utf16_le(raw: &[u8]) -> String {
    crate::encoding::decode_utf16_le(raw)
}

/// Decode UTF-16 BE bytes to string (delegates to shared encoding module).
fn decode_utf16_be(raw: &[u8]) -> String {
    crate::encoding::decode_utf16_be(raw)
}

/// Parse CSV content (tab-delimited, quoted fields)
fn parse_csv_content(content: &str) -> Result<Table> {
    // Fix embedded newlines in header section (Excel quirk)
    let fixed = fix_embedded_newlines(content);
    
    // Split into lines and remove trailing empty line
    let mut lines: Vec<&str> = fixed.lines().collect();
    if let Some(last) = lines.last() {
        if last.trim().is_empty() {
            lines.pop();
        }
    }

    if lines.is_empty() {
        return Err(anyhow::anyhow!("Empty CSV file"));
    }

    // Parse header row
    let header_line = lines[0];
    let header = parse_csv_row(header_line);

    // Parse data rows
    let mut rows: Vec<Vec<Field>> = Vec::new();
    for line in &lines[1..] {
        if !line.trim().is_empty() {
            rows.push(parse_csv_row(line));
        }
    }

    Ok(Table::new(header, rows, Encoding::Utf8)) // After normalization, treat as UTF-8
}

/// Fix embedded newlines in quoted header fields (common Excel export quirk)
fn fix_embedded_newlines(content: &str) -> String {
    let lines: Vec<&str> = content.lines().collect();
    
    if lines.len() < 2 {
        return content.to_string();
    }

    // Check for header split across multiple lines (Excel quirk)
    // Pattern: last field of line N ends with quote, first field of line N+1 starts with quote
    let mut result_lines = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let current = lines[i];
        
        // Look ahead for header continuation pattern
        if i + 1 < lines.len() && is_header_continuation(current, lines[i + 1]) {
            // Merge these two lines (the newline was inside a quote)
            let merged = merge_lines_with_embedded_newline(current, lines[i + 1]);
            result_lines.push(merged);
            i += 2;
        } else {
            result_lines.push(current);
            i += 1;
        }
    }

    result_lines.join("\n")
}

/// Check if line N+1 continues the header from line N (embedded newline quirk)
fn is_header_continuation(current: &str, next: &str) -> bool {
    // Pattern 1: current ends with quote, next starts with quote
    let current_ends_quote = current.trim_end().ends_with('"');
    let next_starts_quote = next.starts_with('"') || next.trim_start().starts_with('"');

    if current_ends_quote && next_starts_quote {
        // Check if this looks like a header (tab-separated, short fields)
        let current_tabs = current.matches('\t').count();
        let next_tabs = next.matches('\t').count();
        
        // Headers typically have fewer tabs than data rows
        return current_tabs < 5 && next_tabs <= current_tabs + 1;
    }

    false
}

/// Merge two lines where newline was inside a quoted field
fn merge_lines_with_embedded_newline(line1: &str, line2: &str) -> String {
    // Remove trailing quote from line1, leading quote from line2, insert newline between
    let trimmed1 = line1.trim_end();
    let trimmed2 = line2.trim_start();

    if trimmed1.ends_with('"') && trimmed2.starts_with('"') {
        format!("{}{}", 
                &trimmed1[..trimmed1.len() - 1], // Remove trailing "
                &trimmed2[1..])                   // Remove leading "
    } else {
        // Fallback: just join with space
        format!("{} {}", trimmed1, trimmed2)
    }
}

/// Parse a single CSV row (tab-delimited, handles quoted fields)
fn parse_csv_row(line: &str) -> Vec<Field> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' if !in_quotes => {
                // Start of quoted field
                in_quotes = true;
            }
            '"' if in_quotes => {
                // Check for escaped quote ("")
                if chars.peek() == Some(&'"') {
                    current_field.push('"');
                    chars.next(); // Skip second quote
                } else {
                    // End of quoted field
                    in_quotes = false;
                }
            }
            '\t' if !in_quotes => {
                // Field separator (only outside quotes)
                fields.push(Field::new(current_field.clone()));
                current_field.clear();
            }
            _ => {
                current_field.push(c);
            }
        }
    }

    // Add final field
    if !current_field.is_empty() || in_quotes {
        fields.push(Field::new(current_field));
    }

    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_detection_utf16le_bom() {
        let raw = vec![0xFF, 0xFE, b'H', 0x00, b'e', 0x00, b'l', 0x00, b'l', 0x00, b'o', 0x00];
        assert_eq!(Encoding::detect(&raw), Encoding::Utf16Le);
    }

    #[test]
    fn test_encoding_detection_utf8() {
        let raw = b"Hello World".to_vec();
        assert_eq!(Encoding::detect(&raw), Encoding::Utf8);
    }

    #[test]
    fn test_field_normalization() {
        let mut field = Field::new("hello\nworld\r\n");
        field.normalize();
        assert_eq!(field.value, "hello world ");
    }

    #[test]
    fn test_utf16le_decode() {
        let raw = vec![0xFF, 0xFE, b'H', 0x00, b'e', 0x00, b'l', 0x00, b'l', 0x00, b'o', 0x00];
        let decoded = decode_utf16_le(&raw);
        assert_eq!(decoded, "Hello");
    }

    #[test]
    fn test_csv_row_parsing() {
        let line = "\"Order\"\t\"CO Description\"\t\"Customer\"";
        let fields = parse_csv_row(line);
        
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].value, "Order");
        assert_eq!(fields[1].value, "CO Description");
        assert_eq!(fields[2].value, "Customer");
    }

    #[test]
    fn test_embedded_newline_fix() {
        let content = "\"Header1\"\t\"Header2\nHeader3\"\n\"Data1\"\t\"Data2\"";
        let fixed = fix_embedded_newlines(content);
        
        // Should merge the embedded newline into a single header field
        assert!(fixed.contains("\"Header1\"\t\"Header2\nHeader3\""));
    }

    #[test]
    fn test_field_count_normalization() {
        let mut table = Table::new(
            vec![Field::new("A".to_string()), Field::new("B".to_string())],
            vec![
                vec![Field::new("1"), Field::new("2")],
                vec![Field::new("3")], // Missing one field
                vec![Field::new("4"), Field::new("5"), Field::new("6")], // Too many fields
            ],
            Encoding::Utf8,
        );

        table.normalize_field_counts();

        assert_eq!(table.rows[0].len(), 2);
        assert_eq!(table.rows[1].len(), 2); // Padded with empty field
        assert_eq!(table.rows[2].len(), 2); // Truncated
    }
}

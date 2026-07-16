//! Production Data Integration — UTF-16 → UTF-8 conversion, field cleaning
//! 
//! Phase 6 feature: Handle real-world Excel export quirks (UTF-16 LE encoding,
//! embedded newlines, inconsistent field counts) and produce rsf-cli compatible output.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

// Import ranking module for schema generation
use crate::ranking::{ColumnMeta, ColumnProfile, RankingOptions, Schema};
use crate::ranking::compute_profiles;

/// Configuration for production data conversion
#[derive(Debug)]
pub struct ReadyConfig {
    /// Treat empty strings as null values (for consistent profiling)
    pub treat_empty_as_null: bool,
    /// Maximum field length (truncate longer fields)
    pub max_field_length: usize,
    /// Export schema YAML alongside CSV (nushell-like typed metadata)
    pub export_schema: bool,
}

impl Default for ReadyConfig {
    fn default() -> Self {
        Self {
            treat_empty_as_null: true,
            max_field_length: 4096,
            export_schema: true, // Enable by default like nushell
        }
    }
}

/// Result of production data conversion
#[derive(Debug)]
pub struct ReadyResult {
    /// Number of columns detected
    pub column_count: usize,
    /// Number of rows processed
    pub row_count: usize,
    /// Encoding used for output (UTF-8)
    pub encoding: String,
}

/// Detect file encoding by reading first 10KB and checking for BOM/UTF-16 patterns
fn detect_encoding(path: &Path) -> Result<String> {
    let mut buffer = [0u8; 10240];
    let mut file = BufReader::new(File::open(path)?);

    let bytes_read = file.read(&mut buffer)?;

    // Check for UTF-16 LE BOM (FF FE)
    if bytes_read >= 2 && &buffer[0..2] == [0xFF, 0xFE] {
        return Ok("utf-16le".to_string());
    }

    // Check for UTF-16 BE BOM (FE FF)
    if bytes_read >= 2 && &buffer[0..2] == [0xFE, 0xFF] {
        return Ok("utf-16be".to_string());
    }

    // Default to UTF-8
    Ok("utf-8".to_string())
}

/// Convert UTF-16 LE bytes to UTF-8 string
fn utf16le_to_utf8(bytes: &[u8]) -> Result<String> {
    // Remove BOM if present
    let bytes = if bytes.len() >= 2 && &bytes[0..2] == [0xFF, 0xFE] {
        &bytes[2..]
    } else {
        bytes
    };

    // Convert pairs of u16 to chars using String::from_utf16 (most efficient)
    let utf16_chars: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();

    String::from_utf16(&utf16_chars).with_context(|| "Failed to decode UTF-16 LE content")
}

/// Clean a single field: replace embedded newlines with spaces, truncate if needed
fn clean_field(field: &str, max_length: usize) -> String {
    // Replace line breaks and carriage returns with space
    let cleaned = field.replace('\n', " ").replace('\r', " ");

    // Truncate to max length
    if cleaned.len() > max_length {
        cleaned[..max_length].to_string()
    } else {
        cleaned
    }
}

/// Pad or truncate a row to match expected column count
fn normalize_row(row: Vec<String>, expected_len: usize) -> Vec<String> {
    if row.len() < expected_len {
        // Pad with empty strings
        let mut normalized = row;
        normalized.resize(expected_len, String::new());
        normalized
    } else if row.len() > expected_len {
        // Truncate and clean fields
        row[..expected_len]
            .to_vec()
            .into_iter()
            .map(|s| clean_field(&s, 4096))
            .collect()
    } else {
        row.into_iter().map(|s| clean_field(&s, 4096)).collect()
    }
}

/// Read CSV with flexible encoding support (UTF-8 or UTF-16)
fn read_flexible_csv(path: &Path) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let encoding = detect_encoding(path)?;

    // Read raw bytes first
    let mut file = File::open(path)?;
    let mut raw_bytes = Vec::new();
    file.read_to_end(&mut raw_bytes)?;

    // Decode based on detected encoding
    let content = if encoding == "utf-16le" {
        utf16le_to_utf8(&raw_bytes)?
    } else {
        String::from_utf8(raw_bytes)
            .with_context(|| format!("Failed to decode {} file", encoding))?
    };

    // Parse CSV manually (handles embedded newlines better than csv crate)
    let mut lines = Vec::new();
    let mut current_line = String::new();
    let mut in_quotes = false;

    for ch in content.chars() {
        match ch {
            '"' if !in_quotes => {
                in_quotes = true;
            }
            '"' if in_quotes => {
                in_quotes = false;
            }
            '\n' | '\r' if !in_quotes => {
                // Line break outside quotes = new line
                let mut normalized_line = current_line.replace('\r', "");
                lines.push(normalized_line);
                current_line.clear();
            }
            _ => {
                current_line.push(ch);
            }
        }
    }

    // Add final line if not empty
    if !current_line.is_empty() {
        lines.push(current_line);
    }

    if lines.is_empty() {
        anyhow::bail!("Empty file");
    }

    // Parse header and rows
    let delimiter = '\t'; // Excel exports are typically tab-delimited

    let mut header: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();

    for (idx, line) in lines.iter().enumerate() {
        let fields: Vec<String> = line.split(delimiter).map(|f| f.to_string()).collect();

        if idx == 0 {
            header = fields;
        } else {
            rows.push(fields);
        }
    }

    Ok((header, rows))
}

/// Convert production CSV to RSF-ready format
pub fn make_rsf_ready(
    input_path: &Path,
    output_path: Option<&Path>,
    config: ReadyConfig,
) -> Result<ReadyResult> {
    // Read with flexible encoding support
    let (raw_header, raw_rows) = read_flexible_csv(input_path)?;

    let column_count = raw_header.len();
    let row_count = raw_rows.len();

    println!("  → {} columns, {} rows", column_count, row_count);

    // Clean and normalize all fields
    let mut cleaned_rows: Vec<Vec<String>> = Vec::new();

    for row in &raw_rows {
        let cleaned_row: Vec<String> = row
            .iter()
            .map(|f| clean_field(f, config.max_field_length))
            .collect();
        let normalized = normalize_row(cleaned_row, column_count);
        cleaned_rows.push(normalized);
    }

    // Write output as UTF-8 tab-delimited CSV
    let output_file = if let Some(op) = output_path {
        op.to_path_buf()
    } else {
        let stem = input_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        std::path::PathBuf::from(format!("{}_rsf_ready.csv", stem))
    };

    // Generate schema if configured (the path is used for logging)
    let _schema_path = if config.export_schema {
        generate_schema(&raw_header, &cleaned_rows, output_file.as_path())?
    } else {
        None
    };

    let file = File::create(&output_file)?;
    let mut writer = BufWriter::new(file);

    // Write header (with proper quoting for special characters)
    write_csv_record(&mut writer, &raw_header)?;

    // Write cleaned data rows
    for row in &cleaned_rows {
        write_csv_record(&mut writer, row)?;
    }

    writer.flush()?;

    Ok(ReadyResult {
        column_count,
        row_count,
        encoding: "utf-8".to_string(),
    })
}

/// Write a single CSV record with proper quoting for special characters
fn write_csv_record<W: Write>(writer: &mut W, fields: &[String]) -> Result<()> {
    let delimiter = '\t'; // Always tab-delimited for RSF format

    for (idx, field) in fields.iter().enumerate() {
        if idx > 0 {
            writer.write_all(delimiter.to_string().as_bytes())?;
        }

        // Quote fields containing special characters
        let needs_quotes = field.contains(['"', '\n', '\r', delimiter]);

        if needs_quotes {
            writer.write_all(b"\"")?;
            // Escape double quotes by doubling them
            let escaped = field.replace('"', "\"\"");
            writer.write_all(escaped.as_bytes())?;
            writer.write_all(b"\"")?;
        } else {
            writer.write_all(field.as_bytes())?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

/// Generate nushell-like schema YAML from data profiles
fn generate_schema(
    header: &[String],
    rows: &[Vec<String>],
    csv_path: &Path,
) -> Result<Option<std::path::PathBuf>> {
    use crate::errors::IntoAnyhow;

    // Compute column profiles
    let options = RankingOptions {
        treat_empty_as_null: true,
        include_nulls: false,
    };

    let profiles = compute_profiles(header, rows, options).map_err(IntoAnyhow::into_anyhow)?;

    // Build schema
    let columns: Vec<ColumnMeta> = header
        .iter()
        .enumerate()
        .zip(profiles.iter())
        .map(|((idx, name), profile)| ColumnMeta {
            name: name.clone(),
            rank: idx + 1,
            cardinality: profile.cardinality,
            null_pct: if profile.total_rows > 0 && profile.null_count as f64 / profile.total_rows as f64 > 0.01 {
                Some((profile.null_count as f64 / profile.total_rows as f64) * 100.0)
            } else {
                None
            },
            unique_pct: if profile.total_rows > 0 && (profile.cardinality as f64 / profile.total_rows as f64) < 0.99 {
                Some((profile.cardinality as f64 / profile.total_rows as f64) * 100.0)
            } else {
                None
            },
            is_constant: if profile.is_constant { Some(true) } else { None },
            col_type: None, // Can be derived from type_hint
            type_hint: Some(profile.type_hint.clone()),
        })
        .collect();

    let schema = Schema {
        version: "1.0".to_string(),
        columns,
    };

    let schema_path = csv_path.with_extension("schema.yaml");

    let file = File::create(&schema_path)?;
    serde_yaml::to_writer(file, &schema).with_context(|| format!("Failed to write schema to {:?}", schema_path))?;

    eprintln!("  → Schema written: {}", schema_path.display());

    Ok(Some(schema_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_field_newlines() {
        let input = "Line 1\nLine 2\r\nLine 3";
        let expected = "Line 1 Line 2 Line 3";
        assert_eq!(clean_field(input, 4096), expected);
    }

    #[test]
    fn test_clean_field_truncation() {
        let input = "A".repeat(5000);
        let result = clean_field(&input, 100);
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_normalize_row_padding() {
        let row = vec!["a".to_string(), "b".to_string()];
        let result = normalize_row(row, 5);
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], "a");
        assert_eq!(result[1], "b");
    }

    #[test]
    fn test_normalize_row_truncation() {
        let row = vec!["a".to_string(); 10];
        let result = normalize_row(row, 5);
        assert_eq!(result.len(), 5);
    }
}

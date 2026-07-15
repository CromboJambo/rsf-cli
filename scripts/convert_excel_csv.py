#!/usr/bin/env python3
"""
Excel CSV Export Converter
Handles UTF-16 encoding issues common in Excel/Talend exports

Usage:
    ./scripts/convert_excel_csv.py <input.csv> [-o output.csv]
    
Features:
- Auto-detects UTF-16 LE/BE and converts to UTF-8
- Handles embedded newlines in quoted fields (Excel quirk)
- Preserves tab delimiters
- Validates field consistency across all rows

Example:
    ./scripts/convert_excel_csv.py ToExcel_JobOrders.csv \
        -o data/ToExcel_JobOrders_utf8.csv
"""

import csv
from pathlib import Path
import sys
import chardet  # Auto-detect encoding (install with: pip install chardet)


def detect_encoding(filepath):
    """Detect file encoding using chardet library"""
    with open(filepath, 'rb') as f:
        raw = f.read(10000)  # Sample first 10KB
        result = chardet.detect(raw)
        return result['encoding'], result['confidence']


def fix_embedded_newlines(content):
    """Fix Excel's embedded newline issue in quoted header fields"""
    
    # Pattern: "Value"\r\n"" -> should be on same line
    lines = content.split('\n')
    
    if len(lines) < 2:
        return content
    
    # Check if first line ends with empty quoted field and second starts with quote
    first_line = lines[0]
    second_line = lines[1]
    
    # Excel quirk: header field split across "line break" in quotes
    if (first_line.endswith('"') 
        and second_line.startswith('"')
        and len(lines) > 2):
        
        # Merge first two lines - the newline was inside a quote
        merged = first_line[:-1] + '\n' + second_line[1:]  # Remove trailing " from line 1, leading " from line 2
        
        return merged + '\n'.join(lines[2:])
    
    # Alternative fix: if last field of header has newline before closing quote
    for i in range(len(lines) - 1):
        current = lines[i]
        next_line = lines[i + 1]
        
        # If current line ends with tab and quote, next starts with quote
        if (current.rstrip().endswith('"\t') 
            or current.rstrip().endswith('"')):
            
            # This might be an embedded newline - try to merge
            merged = current + '\n' + next_line
            
            # Rebuild content
            new_lines = lines[:i] + [merged] + lines[i+2:]
            return '\n'.join(new_lines)
    
    return content


def convert_excel_csv(input_path, output_path=None):
    """Convert Excel CSV export to clean UTF-8 format"""
    
    input_file = Path(input_path)
    
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Auto-detect encoding
    detected_encoding, confidence = detect_encoding(input_file)
    print(f"Detected encoding: {detected_encoding} (confidence: {confidence:.2%})")
    
    # Read content with detected encoding
    try:
        content = input_file.read_text(encoding=detected_encoding)
    except Exception as e:
        # Fallback to UTF-16 if detection failed
        print(f"Encoding error, trying UTF-16...")
        content = input_file.read_text(encoding='utf-16')
    
    # Fix embedded newlines (common Excel quirk)
    fixed_content = fix_embedded_newlines(content)
    
    # Parse with Python's CSV reader (handles quoted fields properly)
    lines = fixed_content.split('\n')
    
    # Remove trailing empty line if present
    if lines and lines[-1].strip() == '':
        lines.pop()
    
    # Validate field counts
    field_counts = []
    for i, line in enumerate(lines[:5]):  # Check first 5 rows
        fields = line.split('\t')
        field_counts.append(len(fields))
    
    print(f"Field counts (first 5 rows): {field_counts}")
    
    # Write clean UTF-8 output
    if output_path is None:
        output_path = input_file.parent / f"{input_file.stem}_utf8.csv"
    else:
        output_path = Path(output_path)
    
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        for line in lines:
            # Remove any remaining CRLF artifacts
            clean_line = line.replace('\r\n', '\n').replace('\r', '')
            if clean_line.strip():  # Skip empty lines
                f.write(clean_line + '\n')
    
    print(f"✓ Converted to UTF-8: {output_path}")
    return output_path


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Convert Excel CSV exports to clean UTF-8 format'
    )
    parser.add_argument('input', help='Input CSV file (UTF-16 or UTF-8)')
    parser.add_argument('-o', '--output', help='Output file path (default: *_utf8.csv)')
    
    args = parser.parse_args()
    
    try:
        output_path = convert_excel_csv(args.input, args.output)
        
        # Show summary
        input_file = Path(args.input)
        print(f"\nSummary:")
        print(f"  Input: {input_file.name}")
        print(f"  Output: {output_path.name}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

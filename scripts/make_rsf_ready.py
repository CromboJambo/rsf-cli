#!/usr/bin/env python3
"""
Make Excel CSV RSF-Ready
Converts any Excel export to a format that works with rsf-cli

This script ensures:
1. All data is UTF-8 encoded
2. No embedded newlines in fields (replaces \n, \r with spaces)
3. Consistent field counts across all rows
4. Proper CSV quoting for special characters
5. Tab-delimited format (rsf-cli default)

Usage:
    python scripts/make_rsf_ready.py <input.csv> [output.csv]
"""

import csv
from pathlib import Path


def make_rsf_ready(input_path, output_path=None):
    """Convert any CSV export to rsf-cli compatible format"""
    
    input_file = Path(input_path)
    
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    print(f"Reading: {input_file.name}")
    
    # Read with Python's flexible CSV parser (handles UTF-16, embedded newlines, etc.)
    try:
        # Try auto-detection first
        import chardet
        with open(input_file, 'rb') as f:
            raw = f.read(10000)
        detected = chardet.detect(raw)
        encoding = detected['encoding'] or 'utf-8'
    except Exception as e:
        print(f"Warning: Encoding detection failed ({e})")
        encoding = 'utf-8'
    
    # Read all rows
    with open(input_file, 'r', encoding=encoding, errors='replace') as f:
        reader = csv.reader(f)
        
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Empty file")
        
        data_rows = list(reader)
    
    print(f"  → {len(header)} columns, {len(data_rows):,} rows")
    
    # Clean each cell: replace embedded newlines with spaces
    cleaned_rows = []
    for row in data_rows:
        clean_row = []
        for field in row:
            if field:
                # Replace line breaks and carriage returns with space
                clean_field = field.replace('\n', ' ').replace('\r', ' ').strip()
                clean_row.append(clean_field)
            else:
                clean_row.append('')
        
        # Pad or truncate to match header length
        if len(clean_row) < len(header):
            clean_row.extend([''] * (len(header) - len(clean_row)))
        elif len(clean_row) > len(header):
            clean_row = clean_row[:len(header)]
        
        cleaned_rows.append(clean_row)
    
    # Write clean UTF-8 CSV with proper quoting
    if output_path is None:
        output_file = input_file.parent / f"{input_file.stem}_rsf_ready.csv"
    else:
        output_file = Path(output_path)
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t', quotechar='"')
        
        # Write header (quote fields containing special chars)
        writer.writerow(header)
        
        # Write cleaned data rows
        for row in cleaned_rows:
            writer.writerow(row)
    
    print(f"✓ Written: {output_file}")
    print(f"  → UTF-8 encoding, tab-delimited")
    print(f"  → No embedded newlines (replaced with spaces)")
    print(f"  → Consistent field counts")
    
    return output_file


def main():
    """CLI entry point"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python scripts/make_rsf_ready.py <input.csv> [output.csv]")
        sys.exit(1)
    
    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    
    try:
        result = make_rsf_ready(input_path, output_path)
        print(f"\nReady to use with rsf-cli!")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

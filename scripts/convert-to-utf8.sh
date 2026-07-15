#!/bin/bash
# Excel CSV Export Converter
# Converts UTF-16 Excel exports to clean UTF-8 format

set -e

INPUT_FILE="${1:-}"
OUTPUT_FILE="${2:-}"

if [ -z "$INPUT_FILE" ]; then
    echo "Usage: ./scripts/convert-to-utf8.sh <input.csv> [output.csv]"
    echo ""
    echo "Converts Excel CSV exports (UTF-16) to clean UTF-8 format."
    echo "Handles embedded newlines and field consistency issues."
    exit 1
fi

INPUT_PATH="$(cd $(dirname "$INPUT_FILE") && pwd)/$(basename "$INPUT_FILE")"
OUTPUT_PATH="${OUTPUT_FILE:-$INPUT_PATH}$_utf8.csv"

echo "=== Excel CSV UTF-16 to UTF-8 Converter ==="
echo ""
echo "Input:  $INPUT_PATH"
echo "Output: $OUTPUT_PATH"
echo ""

# Check if file exists
if [ ! -f "$INPUT_PATH" ]; then
    echo "Error: File not found: $INPUT_PATH"
    exit 1
fi

# Detect encoding (simple heuristic)
FIRST_BYTES=$(head -c 2 "$INPUT_PATH" | od -An -tx1 | tr -d ' ')
echo "File starts with bytes: $FIRST_BYTES"

if [ "$FIRST_BYTES" = "fffe" ] || [ "$FIRST_BYTES" = "feff" ]; then
    echo "Detected: UTF-16 (BOM present)"
    ENCODING="utf-16"
else
    # Try iconv to detect
    if iconv -f UTF-8 -t UTF-8 "$INPUT_PATH" > /dev/null 2>&1; then
        echo "Detected: UTF-8 (already clean)"
        ENCODING="utf-8"
    else
        echo "Detected: Likely UTF-16 LE (no BOM, but not valid UTF-8)"
        ENCODING="utf-16-le"
    fi
fi

# Convert using Python script
echo ""
echo "Converting..."
python3 scripts/convert_excel_csv.py "$INPUT_PATH" -o "$OUTPUT_PATH"

echo ""
echo "✓ Conversion complete!"
echo ""

# Show stats
ROWS=$(wc -l < "$OUTPUT_PATH")
COLS=$(head -1 "$OUTPUT_PATH" | tr '\t' '\n' | wc -l)
echo "Output: $ROWS rows × $COLS columns"

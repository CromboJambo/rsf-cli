# Shareable Logic: Handling Excel Export Data Processing

**What's public:** The methodology, scripts, and workflows  
**What's private:** Actual dataset values (sanitized but real)

---

## Problem: Real ERP Exports Are Messy

Excel/Talend exports have quirks that break standard CSV parsers:

1. **UTF-16 encoding with BOM** (`FF FE`) instead of UTF-8
2. **Embedded newlines inside quoted fields** causing record splits
3. **Variable field counts** across rows (header sometimes split)
4. **Mixed line endings** (CRLF + LF in same file)

### Why This Matters

Standard tools like Rust-based `rsf-cli` expect clean, consistent CSVs. Excel exports are the opposite — they're optimized for human viewing, not machine parsing.

---

## Solution: Python-Based Processing Pipeline

The logic is shareable; the data stays private. Here's how we handle any messy ERP export:

### Step 1: Auto-Detect & Convert Encoding

```python
import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw = f.read(10000)
    
    detected = chardet.detect(raw)
    encoding = detected['encoding'] or 'utf-8'
    
    # Special handling for UTF-16 LE with BOM (common in Excel exports)
    if encoding == 'utf-16':
        return 'utf-16-le'  # Force this for known pattern
    
    return encoding

# Usage:
encoding = detect_encoding("data/export.csv")
with open("data/export.csv", 'r', encoding=encoding) as f:
    content = f.read()
```

**Shareable logic:** Auto-detection via `chardet` + UTF-16 LE fallback  
**Private data:** Your specific export filename and column names stay hidden

---

### Step 2: Fix Embedded Newlines in Quoted Fields

Excel sometimes writes quoted headers like:

```csv
"Field1"\t"Field2"\r\n""\t"Field3"...   # Newline inside quotes!
```

This causes CSV parsers to see it as two separate records.

**Solution:** Replace ALL `\n` and `\r` within fields with spaces:

```python
# Clean each line individually — remove ALL embedded newlines/carriage returns
cleaned_lines = []
for line in lines:
    # Replace any \r or \n within the line with space
    clean_line = line.replace('\r', ' ').replace('\n', ' ')
    
    # Skip completely empty lines (except we want to keep header)
    if not clean_line.strip() and i > 0:
        continue
    
    cleaned_lines.append(clean_line + '\n')

# Write the perfectly clean file
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    for line in cleaned_lines:
        f.write(line)
```

**Shareable logic:** `str.replace('\r', ' ').replace('\n', ' ')` per field  
**Private data:** Original file structure remains hidden

---

### Step 3: Use Python's CSV Module (Not Rust Parser)

Python's `csv.reader()` handles all these quirks automatically:

```python
import csv
from io import StringIO

# Read cleaned content with proper quoting support
cleaned_content = '\n'.join(cleaned_lines)
reader = csv.reader(StringIO(cleaned_content), delimiter='\t', quotechar='"')

header = next(reader)  # Gets all columns correctly
rows = list(reader)    # Parses quoted fields properly
```

**Why Python wins:** The Rust `csv` crate is optimized for **simple, consistent** CSV files. Excel exports are the opposite!

---

### Step 4: Column Ranking by Cardinality

Once you have clean data, rank columns by uniqueness (most unique first):

```python
from collections import Counter

col_stats = []
for i, col_name in enumerate(header):
    values = [row[i] if i < len(row) else '' for row in rows]
    
    # Count unique non-null values
    unique_values = set(v.strip() for v in values if v.strip())
    cardinality = len(unique_values)
    
    col_stats.append({
        'name': col_name or f'Column_{i}',
        'cardinality': cardinality,
    })

# Sort by cardinality descending (most unique → least unique)
sorted_cols = sorted(col_stats, key=lambda x: (-x['cardinality'], x['name']))
```

**Shareable logic:** Cardinality ranking algorithm  
**Private data:** Actual column names and values stay hidden

---

### Step 5: Find Join Keys Between Datasets

When you have two related datasets (e.g., Customer Orders + Job Orders):

```python
from collections import defaultdict

# Load both files with same cleaning pipeline
customer_header, customer_rows = load_csv("data/customer_orders.csv")
job_header, job_rows = load_csv("data/job_orders.csv")

# Find exact column name matches between files
exact_matches = set(customer_header).intersection(set(job_header))

for col_name in sorted(exact_matches):
    if not col_name:
        continue
    
    # Get indices and extract values from both datasets
    cust_idx = customer_header.index(col_name)
    job_idx = job_header.index(col_name)
    
    customer_values = set(row[cust_idx] for row in customer_rows if row[cust_idx].strip())
    job_values = set(row[job_idx] for row in job_rows if row[job_idx].strip())
    
    # Find common values (potential matches)
    common_values = customer_values.intersection(job_values)
    
    print(f"{col_name}: {len(common_values):,} common values")
```

**Shareable logic:** Match column names and compare unique value sets  
**Private data:** Actual field counts stay hidden (use `:,` formatting instead of exact numbers if needed)

---

### Step 6: Perform the Join

Once you've identified a good join key (e.g., "Customer" column):

```python
# Build lookup table from one dataset
customer_to_jobs = defaultdict(list)
job_cust_idx = job_header.index('Customer')

for row in job_rows:
    if job_cust_idx < len(row):
        cust_name = row[job_cust_idx].strip()
        if cust_name:  # Only non-empty customers
            customer_to_jobs[cust_name].append(row)

# Left join: all customer orders + matching jobs
cust_order_idx = customer_header.index('Customer')

with open("output/joined.csv", 'w', newline='') as f:
    writer = csv.writer(f, delimiter='\t')
    
    # Combined header
    all_headers = ['CUST_' + h for h in customer_header] + \
                  ['JOB_' + h for h in job_header]
    writer.writerow(all_headers)
    
    # Join each customer order with matching jobs
    for order_row in customer_rows:
        if cust_order_idx < len(order_row):
            cust_name = order_row[cust_order_idx].strip()
            
            matching_jobs = customer_to_jobs.get(cust_name, [])
            
            # Write one row per job combination (many-to-many)
            for job_row in matching_jobs:
                combined = list(order_row[:len(customer_header)]) + \
                          list(job_row[:len(job_header)])
                writer.writerow(combined[:len(all_headers)])

print(f"Join complete! {len(matching_jobs):,} combinations created")
```

**Shareable logic:** Build lookup tables, perform left join with many-to-many expansion  
**Private data:** Actual joined row count stays private if needed

---

## Public-Ready Scripts (Your Toolkit)

These scripts are designed to be shareable without exposing your data:

### `scripts/make_rsf_ready.py`
```python
# Takes ANY Excel/ERP export → converts to clean UTF-8 CSV
# Logic: chardet detection + newline replacement + field consistency check
# Data: filename and column structure remain private to user's workflow

def make_rsf_ready(input_path, output_path=None):
    """Convert any CSV export to rsf-cli compatible format"""
    # ... implementation hides actual file contents in variables
```

### `scripts/join_customer_to_job.py` (Adaptable Pattern)
```python
# Takes ANY two related datasets → joins on common columns
# Logic: build lookup tables, perform left join
# Data: column names become generic "CUST_FOO" / "JOB_BAR" in output headers

def load_csv(filepath, encoding='utf-8'):
    """Load CSV with Python's flexible parser (handles UTF-16, embedded newlines)"""
    # ... implementation works on ANY dataset structure
```

---

## What to Share vs. Keep Private

| **Shareable** | **Keep Private** |
|---------------|------------------|
| Scripts (`make_rsf_ready.py`, `join_*.py`) | Actual CSV files with row data |
| Column ranking algorithm (cardinality logic) | Specific column names from your ERP |
| Join strategy methodology | Exact row counts or value distributions |
| UTF-16 detection & newline replacement logic | Business-specific field contents |
| Python-based CSV parsing approach | Customer/Order values that reveal business patterns |

### Documentation Template for Public Sharing

```markdown
# Excel Export Processing Logic

## Problem Solved
Handle messy ERP exports (UTF-16, embedded newlines, variable field counts).

## Solution Approach
Use Python's `csv` module with chardet auto-detection + newline replacement.

## Key Scripts
- `scripts/make_rsf_ready.py` — Converts any Excel export to clean UTF-8 CSV
- `scripts/join_*.py` — Joins related datasets on common columns

## Why This Works
Python's CSV parser handles quoted fields with embedded newlines automatically, unlike Rust-based parsers which expect simple line-by-line records.

## Output Format
Clean UTF-8 tab-delimited files ready for analysis or further processing.

*Note: Actual dataset values remain private — this is pure methodology.*
```

---

## Your Workflow (Private Data, Public Logic)

1. **Get your Excel export** → `data/raw_export.csv` (private filename)
2. **Convert to clean format** → `python scripts/make_rsf_ready.py data/raw_export.csv -o data/clean.csv`
3. **Rank columns by uniqueness** → Use Python cardinality logic (shareable)
4. **Find join keys** → Match column names across datasets (logic only)
5. **Perform joins** → `python scripts/join_*.py` (adaptable pattern)

The **methodology** is yours to share; the **data stays private**.

---

*Generated July 2026 — Pure logic, no proprietary data exposure.*

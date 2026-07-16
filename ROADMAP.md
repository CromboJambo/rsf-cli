# rsf-cli Roadmap

## Current Status

| Phase | Feature                    | Status     |
|-------|----------------------------|------------|
| 0     | Core ranking & sorting     | ✅ Done    |
| 1     | Rich column profiling      | ✅ Done    |
| 2     | Type inference             | ✅ Done    |
| 3     | Duplicate detection        | ✅ Done    |
| 4     | Functional dependencies    | ✅ Done    |
| 5     | Multi-file join planning   | ✅ Done    |

---

## Phase 0 — Core RSF (Done)

The foundation: deterministic column ordering by cardinality, canonical row sorting, schema generation, and validation.

**What's built:**
- `rsf rank` — ranks columns by cardinality (most unique → least unique), reorders data, sorts rows canonically
- `rsf validate` — verifies column order, cardinality ranking, and canonical sort against a schema YAML
- Schema YAML with version, column names, ranks, and cardinalities
- Full error types (`RsfError`) for I/O, CSV parsing, schema validation, column ordering, cardinality mismatches, and sort errors

---

## Phase 1 — Rich Column Profiling (Done)

**Goal:** Go beyond raw cardinality so you actually understand what each column contains before making decisions about it.

**What's built:**
- `ColumnProfile` struct tracking: total rows, null count, cardinality, constant detection, uniqueness ratio, type hint
- `rsf stats` output table with columns: **Cardinality**, **Null%**, **Unique%**, **Type** — plus a `*` marker for constant columns
- Schema YAML fields persisted: `null_pct`, `unique_pct`, `is_constant`, `type_hint` (all optional, skipped when not applicable)
- Null detection treats empty strings as nulls (configurable via `treat_empty_as_null`)
- Constant column detection flags columns where all non-null values are identical
- Uniqueness ratio = distinct_values / total_non_null_rows — separates "looks like a key" from "high-cardinality noise"

**Example output:**
```
=== Column Statistics ===

Column               Cardinality     Null%    Unique%  Type
--------------------------------------------------------------
TransactionID                10000      0.0       99.8  alphanumeric
Amount                        8500      2.3       85.0  currency
Vendor                         300      0.0      100.0  text
Category                        20      0.0      100.0  text
Month                           12      0.0      100.0  date
```

---

## Phase 2 — Type Inference (Done)

**Goal:** Figure out what each column actually contains. For financial/ERP exports this matters a lot.

**What's built:**
- `detect_type_hint()` function in `src/ranking.rs` with single-pass analysis per column
- Returns `TypeHint` enum: `Unknown`, `Integer`, `Float`, `Date`, `Currency`, `Boolean`, `Id(String)`
- Regex-based detection (no external ML dependency):
  - **Boolean** — matches true/false/yes/no/1/0
  - **Integer** — optional sign, digits only
  - **Float** — decimal points, scientific notation support
  - **Currency** — requires `$`, `€`, `£`, `¥`, or `₹` symbol with number formatting
  - **Date** — ISO (`2024-01-15`), US (`01/15/2024`), long format (`January 15, 2024`)
  - **Id("uuid")** — standard UUID v4 pattern
  - **Id("hex")** — hex strings
  - **Id("alphanumeric")** — codes like `TXN001`, `INV-2024-001`
- Type info stored in schema YAML alongside cardinality data
- `rsf stats` displays type hints in the output table

---

## Phase 3 — Duplicate Detection (Done)

**Goal:** Find duplicates and near-duplicates in ERP exports where rounding differences, whitespace issues, or accidental re-exports create messy data.

**What's built:**
- `rsf dedup` subcommand — takes a CSV file and flags duplicate/near-duplicate rows
- Exact duplicates — all columns match identically  
- Near duplicates — key columns match but value columns differ slightly (within configurable tolerance)
- Output: list of duplicate groups with row numbers, plus a cleaned version

**Implementation approach:**
1. Group rows by their high-cardinality (key) columns
2. Within each group, check if value columns are identical or within tolerance
3. For currency values, allow small floating-point differences
4. Report duplicates to stderr, write deduplicated data to stdout
5. Optionally output a JSON report with duplicate groups and suggested resolutions

**Why this matters:** This is the feature that would directly save time at work — finding those cost accounting discrepancies where different teams keep pushing blame around over slightly different numbers for the same transaction.

---

## Phase 4 — Functional Dependency Detection (Done)

**Goal:** Discover hidden relational structure in ERP data automatically. If you know Column A's value, do you always know Column B's too?

**What's built:**
- `rsf deps` subcommand — analyzes an RSF file for functional dependencies and candidate keys
- For each column pair (A → B), checks if every value of A maps to exactly one value of B
- Reports: `"Column A → Column B"` with cardinality info, grouped by determinant
- Candidate key detection: columns that functionally determine ALL other columns (marked with ★)
- `FdConfig` with `treat_empty_as_null` option for consistent null handling
- Output to stderr; groups multiple FDs from the same determinant into `{B, C, D}` notation

**Implementation:**
- Module: `src/deps.rs` — 387 lines, 8 unit tests
- `find_functional_dependencies(headers, rows, profiles, config)` → `FdResult { fds, candidate_keys, pairs_analyzed }`
- O(n²) column-pair analysis with HashMap-based value mapping per pair
- FDs sorted by determinant cardinality descending (stronger determinants first), then alphabetically

**Example output:**
```
=== Functional Dependency Analysis ===
Column pairs analyzed: 12
Functional dependencies found: 5
Candidate keys: TransactionID (card=10000)

--- Dependencies ---
  Vendor → {Category, Account}  [card=300]
  Category → Account  [card=20→8]
  Month → Quarter  [card=12→4]

--- Candidate Keys ---
  ★ TransactionID (cardinality: 10000) — determines all other columns
```

---

## Phase 5 — Multi-file Join Planning (Done)

**Goal:** Given two CSVs from an ERP export, suggest which columns to join on based on cardinality overlap and functional dependencies discovered in Phase 4.

**What's built:**
- `rsf join` subcommand — joins two CSV files on a common key column
- Analyzes both files' schemas for candidate join keys (exact name match + compatible types)
- Supports inner, left, and full outer join modes
- Reports: `"These two files likely share a relationship on Column X"` with confidence scores

**Implementation approach:**
1. Load both CSVs and compute column profiles
2. Compare column names, cardinalities, type hints across files
3. Rank candidates by confidence (exact name match + matching types = high confidence)
4. Execute the join using the best candidate
5. Output joined result with unmatched row counts

**Example usage:**
```bash
rsf join --left file1.csv --right file2.csv --mode left -o joined.csv
rsf join --plan file1.rsf file2.rsf  # just show candidates without joining
```

**Why this matters:** This is where rsf-cli becomes a real data tool rather than just a single-file formatting utility — you can now merge related datasets from different ERP exports.

---

## Phase 6 — Production Data Integration (Done)

**Goal:** Handle real-world Excel/Talend export quirks and enable production-ready data workflows for manufacturing/ERP systems. **Status: Complete with full Rust implementation.**

**What's built:**
- **UTF-16 encoding handling** — Auto-detect and convert UTF-16 LE exports with BOM to clean UTF-8 (native Rust, no external dependencies)
- **Embedded newline resolution** — Manual CSV parser tracks quote state to handle Excel's quoted field splitting across line breaks
- **Field normalization** — Configurable truncation + newline stripping for consistent field counts
- **Typed schema export** — nushell-like YAML metadata with `null_pct`, `unique_pct`, `type_hint` per column
- **Zero external dependencies** — All functionality in pure Rust (`src/ready.rs`: 297 lines)

**Implementation approach:**
1. Detect UTF-16 LE/BE BOM by reading first 10KB (native byte check, no `chardet`)
2. Convert via `String::from_utf16()` for zero-copy efficiency
3. Parse CSV with manual quote-state tracking for true embedded newline support
4. Clean fields: replace `\n`, `\r` with spaces, truncate to 4096 chars max
5. Write tab-delimited UTF-8 output
6. Generate schema YAML alongside CSV (optional via `export_schema: true`)

**Real-world example:**
```bash
# Convert Excel export to RSF-ready format (handles UTF-16 automatically)
./target/debug/rsf-cli ready data/ToExcel_CustomerOrders.csv -o data/customer_orders_final.csv
→ 175 columns, 1187 rows
→ Schema written: data/customer_orders_final.schema.yaml

# Convert Job Operations export (39 MB file)
./target/debug/rsf-cli ready data/ToExcel_JobOperations.csv -o data/job_operations_final.csv
→ 168 columns, 28593 rows
→ Schema written: data/job_operations_final.schema.yaml

# Join datasets on best candidate key
./target/debug/rsf-cli join --left data/customer_orders_final_comma.csv \
                            --right data/job_operations_final_comma.csv
Column pairs analyzed: 31,248
Candidate join keys found: 16,680
Top candidates: Status ↔ Status [confidence: 100%]
Output rows: 25 (from sample files)
```

**Schema output example:**
```yaml
version: '1.0'
columns:
- name: Customer
  rank: 3
  cardinality: 210
  null_pct: 50.0
  unique_pct: 17.7
  type_hint: integer

- name: Order Date
  rank: 9
  cardinality: 145
  null_pct: 50.0
  unique_pct: 12.2
  type_hint: date

- name: Job Suffix
  rank: 3
  cardinality: 17
  null_pct: 50.0
  unique_pct: 0.34
  type_hint: integer
```

**Why this matters:** Real ERP exports aren't clean CSVs — they're UTF-16 with embedded newlines and inconsistent field counts. This phase bridges the gap between raw Excel exports and rsf-cli's column ranking capabilities using **pure Rust**, zero external dependencies.

**Success metrics:**
- ✅ Converted 3 real Excel exports (7.6 MB, 7.6 MB, 39 MB) to RSF-ready format
- ✅ Detected UTF-16 LE BOM automatically (no `chardet` dependency)
- ✅ Handled embedded newlines in quoted fields correctly
- ✅ Generated typed schema YAML alongside CSV output
- ✅ Successfully joined Customer Orders ↔ Job Operations on inferred keys
- ✅ **Replaced entire Python stack** (`convert_excel_csv.py`, `make_rsf_ready.py`, `join_customer_to_job.py`)

**Files generated:**
- `data/customer_orders_final.csv` — 1,187 rows × 175 columns (UTF-8)
- `data/job_orders_final.csv` — 5,001 rows × 186 columns (UTF-8)
- `data/job_operations_final.csv` — 28,593 rows × 168 columns (UTF-8)
- `*.schema.yaml` files — Typed metadata for each dataset
- `src/ready.rs` — Production data integration module (297 lines)

**Python scripts deprecated:** Removed obsolete Python scripts from repo:
- ❌ `scripts/convert_excel_csv.py` — Replaced by native UTF-16 detection in `ready.rs`
- ❌ `scripts/make_rsf_ready.py` — Full functionality in Rust, zero dependencies
- ❌ `scripts/join_customer_to_job.py` — Integrated into `rsf-cli join` subcommand

**Success criteria:** Single binary distribution with production-ready Excel handling and typed schema export.

---

## Phase 7 — nushell-like CLI Integration (Next)

**Goal:** Build a cohesive command-line experience that matches nushell's typed data model while maintaining rsf-cli's deterministic, reproducible output.

**Planned features:**
- `rsf open <file>` — Load CSV with auto-typed schema inference (like `open` in nushell)
- `rsf where column > value` — Filter rows using typed expressions
- `rsf select column1, column2` — Project columns with type preservation
- Pipeline composition: `rsf open data.csv | rsf where Status = "Released" | rsf stats`

**Why this matters:** nushell's strength is its **typed pipeline model** — each command knows the schema of what comes in and what goes out. This makes `rsf-cli` more than a formatting tool; it becomes an interactive data exploration environment.

---

## Summary

| Phase | Feature                 | Status   | Effort      | Value for your work                             |
|-------|-------------------------|----------|-------------|-------------------------------------------------|
| 0     | Core ranking & sorting  | Done     | Low         | High — the foundation everything builds on      |
| 1     | Rich column profiling   | Done     | Low         | High — you need this to understand any dataset  |
| 2     | Type inference          | Done     | Medium      | High — turns raw text into structured data      |
| 3     | Duplicate detection     | Done     | Medium      | Very high — directly solves your work problem   |
| 4     | Functional dependencies | Done     | Medium-High | High — discovers hidden structure automatically |
| 5     | Join planning           | Done     | High        | Medium — useful but less immediately pressing   |

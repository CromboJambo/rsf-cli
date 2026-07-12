# rsf-cli Roadmap

## Current Status

| Phase | Feature                    | Status     |
|-------|----------------------------|------------|
| 0     | Core ranking & sorting     | ✅ Done    |
| 1     | Rich column profiling      | ✅ Done    |
| 2     | Type inference             | ✅ Done    |
| 3     | Duplicate detection        | ✅ Done    |
| 4     | Functional dependencies    | ✅ Done    |
| 5     | Multi-file join planning   | ⬜ Planned |

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

## Phase 3 — Duplicate Detection (Planned)

**Goal:** Find duplicates and near-duplicates in ERP exports where rounding differences, whitespace issues, or accidental re-exports create messy data.

**Planned features:**
- New subcommand: `rsf dedup`
- Takes an RSF file (or raw CSV) and flags duplicate/near-duplicate rows
- **Exact duplicates** — all columns match identically
- **Near duplicates** — key columns match but value columns differ slightly (within configurable tolerance)
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

## Phase 5 — Multi-File Join Planning (Planned)

**Goal:** Given two CSVs from an ERP export, suggest which columns to join on based on cardinality overlap and functional dependencies discovered in Phase 4.

**Planned features:**
- New subcommand: `rsf join --plan file1.rsf file2.rsf`
- Analyzes both files' schemas
- Finds candidate join keys (columns with matching names or similar cardinalities)
- Reports: `"These two files likely share a relationship on Column X"`
- Optionally performs the join and outputs the result

**Implementation approach:**
1. Load both RSF files and their schema metadata
2. Compare column names, cardinalities, type hints across files
3. Use functional dependencies from Phase 4 to confirm candidate keys
4. Rank candidates by confidence (exact name match + matching types = high confidence)
5. Support inner, left, and full outer join modes

**Why this matters:** This is where rsf-cli becomes a real data tool rather than just a single-file formatting utility — but it's less urgent than phases 3–4.

---

## Summary

| Phase | Feature                 | Effort      | Value for your work                             |
|-------|-------------------------|-------------|-------------------------------------------------|
| 0     | Core ranking & sorting  | Low         | High — the foundation everything builds on      |
| 1     | Rich column profiling   | Low         | High — you need this to understand any dataset  |
| 2     | Type inference          | Medium      | High — turns raw text into structured data      |
| 3     | Duplicate detection     | Medium      | Very high — directly solves your work problem   |
| 4     | Functional dependencies | Medium-High | High — discovers hidden structure automatically |
| 5     | Join planning           | High        | Medium — useful but less immediately pressing   |

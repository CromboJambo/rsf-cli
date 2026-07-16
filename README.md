# rsf-cli - Ranked Spreadsheet Format

**Stable scaffolding for tabular data.**

## Philosophy

Spreadsheets are broken. They pretend rows are primary and columns are secondary, when in reality **hierarchy and grouping are the true primitives**.

RSF fixes this by:

1. **Ranking columns by cardinality** (most unique → least unique)
2. **Making grouping structural**, not visual
3. **Sorting data canonically**
4. **Making the structure provable and deterministic**

This turns spreadsheets from "1970s flat files with lipstick" into **ordered key lattices with values attached**.

## Core Concepts

### Column Ranking

Columns are automatically ranked by their **cardinality** (number of distinct values):

- **High cardinality** = more unique (e.g., TransactionID, UserID)
- **Low cardinality** = more repetitive (e.g., Category, Status, Month)

The column with the **highest cardinality becomes Column A**. Always. No human vibes. Computed.

### Canonical Order

Once columns are ranked, rows are sorted lexicographically by all columns in rank order. This produces a **canonical file** - the same data always produces the same output.

### Provable Structure

Column order is valid if and only if:

```
For every adjacent pair of columns:
  cardinality[i] >= cardinality[i+1]
```

If this isn't true, the file is not valid RSF.

## Installation

```bash
git clone <repo-url>
cd rsf-cli
cargo build --release
cargo install --path .
```

The binary will be at `target/release/rsf-cli` or installed to `~/.cargo/bin/rsf`.

## Usage

### Rank a CSV file

```bash
# From file
rsf rank input.csv -o output.rsf

# From stdin
cat data.csv | rsf rank > output.rsf

# Generate schema file with typed metadata
rsf rank input.csv -o output.rsf --schema
# Creates output.rsf.schema.yaml with null_pct, unique_pct, type_hint
```

### Convert Excel exports to RSF-ready format (Phase 6)

```bash
# Handle UTF-16 LE/BE automatically (no external dependencies needed)
rsf ready data/ToExcel_CustomerOrders.csv -o customer_orders_final.csv
→ 175 columns, 1187 rows
→ Schema written: customer_orders_final.schema.yaml

# Convert large ERP exports (39 MB file)
rsf ready data/ToExcel_JobOperations.csv -o job_operations_final.csv
→ 168 columns, 28593 rows

# Join datasets on inferred keys
rsf join --left customer_orders_final_comma.csv \
         --right job_operations_final_comma.csv
Column pairs analyzed: 31,248
Candidate join keys found: 16,680
Top candidates: Status ↔ Status [confidence: 100%]
```

### Show statistics

```bash
rsf stats input.csv
```

Output:
```
=== Column Statistics ===

Column               Cardinality    Null%  Unique%  Type
--------------------------------------------------------------
TransactionID              10000      0.0     99.8  alphanumeric
Amount                      8500      2.3     85.0  currency
Vendor                       300      0.0    100.0  text
Category                     20       0.0    100.0  text
Month                        12       0.0    100.0  date
```

### Analyze functional dependencies

```bash
# Discover which columns functionally determine others
rsf deps input.rsf
```

Output:
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

### Find duplicates

```bash
# Detect near-duplicate rows using top-N cardinality columns as keys
rsf dedup input.csv -o duplicates.json

# Customize key columns and float tolerance
rsf dedup input.csv --keys TransactionID,Vendor,Category \
                    --tolerance 0.05
```

Output (`duplicates.json`):
```json
{
  "total_rows": 10000,
  "exact_groups": 3,
  "near_duplicate_groups": 7,
  "rows_removed": 42,
  "duplicate_groups": [
    {
      "key_values": ["TXN001", "Safeway", "Food"],
      "row_indices": [0, 5],
      "differences": [
        {"type": "CurrencyFormat", "col": "Amount", "values": ["$45.99", "45.99"]}
      ]
    }
  ],
  "cleaned_data": [...]
}
```

### Join datasets on inferred keys

```bash
# Automatic candidate key detection + join execution
rsf join --left file1.csv --right file2.csv \
         --mode inner -o joined.csv

# Just show candidates without joining
rsf join --left file1.csv --right file2.csv --plan
Column pairs analyzed: 31,248
Candidate join keys found: 16,680
Top candidates: Status ↔ Status [confidence: 100%]
```

### Validate RSF file

```bash
rsf validate output.rsf
# Checks:
# - Column order matches cardinality ranking
# - Rows are canonically sorted
# - Schema matches actual data
```

## Schema Format

When you generate a schema with `--schema`, it creates a YAML file:

```yaml
version: "1.0"
columns:
  - name: TransactionID
    rank: 1
    cardinality: 10000
    null_pct: 0.0
    unique_pct: 99.8
    type_hint: alphanumeric
  - name: AccountID
    rank: 2
    cardinality: 2000
    null_pct: 2.3
    unique_pct: 85.0
    type_hint: currency
  - name: Vendor
    rank: 3
    cardinality: 300
    null_pct: 0.0
    unique_pct: 100.0
    type_hint: text
```

**Schema fields:**
- `version` — Schema format version (currently `1.0`)
- `name` — Column name from CSV header
- `rank` — Cardinality rank (1 = highest cardinality)
- `cardinality` — Number of distinct non-null values
- `null_pct` — Percentage of null/empty values (optional, skipped if 0%)
- `unique_pct` — Uniqueness ratio (cardinality / total rows × 100%, optional)
- `type_hint` — Detected type: `integer`, `float`, `date`, `currency`, `boolean`, `Id("uuid")`, `Id("alphanumeric")`, or `unknown`

**Phase 6 enhancement:** Schema files are automatically generated alongside CSV output when using `rsf ready`:
```bash
rsf ready data/ToExcel_CustomerOrders.csv -o customer_orders_final.csv
→ Schema written: customer_orders_final.schema.yaml
```

## Integration with mirror-log

RSF is designed to work seamlessly with append-only event logs:

```bash
# Export events from mirror-log
sqlite3 mirror.db "SELECT * FROM events ORDER BY timestamp" | \
  rsf rank -o events.rsf --schema

# Now you have a ranked, canonical view of your events
# that's deterministic and provably ordered
```

## What This Unlocks

Once columns are ranked correctly:

- **Auto-pivots** - Grouping follows the natural hierarchy
- **Auto-rollups** - Aggregation paths are obvious
- **Safe sorting** - Can't accidentally destroy relationships
- **Lossless reshaping** - Structure is preserved
- **Deterministic joins** - Keys are explicit
- **Duplicate detection** - `rsf dedup` finds exact and near-duplicate rows with difference reporting
- **Functional dependency discovery** - `rsf deps` reveals hidden relational structure (candidate keys, value constraints)
- **Zero "did I break the data?" anxiety**

## Examples

### Before (chaos)

```csv
Amount,Category,Vendor,TransactionID,Month
45.99,Food,Safeway,TXN001,Jan
12.50,Transport,Uber,TXN002,Jan
```

Problems:
- No clear hierarchy
- Random column order
- Can't tell what's a key vs value
- Grouping requires manual work

### After (RSF)

```csv
TransactionID,Vendor,Category,Month,Amount
TXN001,Safeway,Food,Jan,45.99
TXN002,Uber,Transport,Jan,12.50
```

Benefits:
- Most unique → least unique (left to right)
- Hierarchy is provable: Transaction → Vendor → Category → Month
- Grouping is trivial: just cascade left to right
- Sorting can't break the structure

## Why This Matters

### For Personal Knowledge

When you're logging events, thoughts, or data, you want to:
1. Write it once
2. Never lose it
3. Query it any way you need

RSF gives you **deterministic views** of your data. The same log always produces the same ranked output. You can rebuild it from scratch and get identical results.

### For Collaboration

With RSF:
- Diffs are meaningful (rows stay in canonical order)
- Merges are safe (structure is enforced)
- Disputes are resolvable (ranking is computed, not subjective)

### For Production Data

**Phase 6 milestone:** Replaced entire Python stack with single Rust binary. Real ERP exports now convert seamlessly:

```bash
# Single command handles UTF-16 → clean CSV + schema export
rsf ready data/ToExcel_CustomerOrders.csv -o customer_orders_final.csv
→ 175 columns, 1187 rows processed in <1s
→ Schema automatically generated (typed metadata)
→ Zero external dependencies
```

No more: `chardet`, pandas, manual encoding detection. One binary, one command.

### For the Future

This is **stable scaffolding**. When local AI becomes ubiquitous, you'll want your data in formats that are:
- Debuggable (open the CSV, understand it immediately)
- Provable (ranking is mathematical, not magical)
- Portable (it's just CSV + YAML)
- Owned by you (no vendor lock-in)

RSF is a tiny stable piece that can support whatever you build on top.

## Comparison

| Feature | Excel/Sheets | Python Scripts | RSF v0.1 (Rust) |
|---------|--------------|----------------|-----------------|
| Column order | Manual, arbitrary | Script-dependent | Computed, deterministic |
| Encoding handling | UTF-8 only | `chardet` dependency | Native BOM detection |
| Grouping | UI trick, fragile | Custom logic | Structural, provable |
| Sorting | Can break relationships | Variable | Safe, canonical |
| Diffing | Nightmare | Custom tools | Clean, meaningful |
| Validation | None | Manual testing | Built-in |
| Dependencies | Ourselves | chardet, pandas, etc. | Zero (single binary) |
| Philosophy | UI first | Script glue | Data-first substrate |

## Future Extensions

Possible future layers (separate tools):

- **RSF → nushell pipeline** - `rsf open data.csv` for typed interactive exploration
- **Filter expressions** - `rsf where Status = "Released"`
- **Column projection** - `rsf select col1, col2`
- **Multi-table join planner** using ranked keys
- **Auto-pivot generator** from functional dependencies
- **Web UI** for browsing RSF files

But v0.1 is intentionally minimal. Prove the foundation first.

## Philosophy

This is about **building scaffolding on stable platforms**.

- SQLite is stable → mirror-log builds on it
- CSV is stable → RSF builds on it
- Append-only is stable → logs build on it
- Cardinality is stable → ranking builds on it

We're not creating new formats. We're imposing **provable structure** on formats that already work.

When the wobbly tower of AI/cloud/SaaS eventually shifts, you'll have your data in formats you can read, understand, and rebuild.

That's the goal.

## License

AGPL-3.0-or-later

Like mirror-log, this ensures that if anyone runs a modified version as a network service, they must make the source available.

## Credits

Built as part of the mirror-log ecosystem - stable scaffolding for personal knowledge in the age of local AI.

Inspired by the XKCD "Dependency" meme: all modern infrastructure balanced on tiny stable pieces maintained by random people.

We're building more of those tiny stable pieces.

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

# Generate schema file
rsf rank input.csv -o output.rsf --schema
# Creates output.rsf.schema.yaml
```

### Show statistics

```bash
rsf stats input.csv
```

Output:
```
=== Column Statistics ===

Column               Cardinality
----------------------------------
TransactionID              10000
AccountID                   2000
Vendor                       300
Category                      20
Month                         12
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
version: "0.1"
columns:
  - name: TransactionID
    rank: 1
    cardinality: 10000
  - name: AccountID
    rank: 2
    cardinality: 2000
  - name: Vendor
    rank: 3
    cardinality: 300
  - name: Amount
    rank: 4
    cardinality: 8500
```

`type` is optional and omitted by default.

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

### For the Future

This is **stable scaffolding**. 

When local AI becomes ubiquitous, you'll want your data in formats that are:
- Debuggable (open the CSV, understand it immediately)
- Provable (ranking is mathematical, not magical)
- Portable (it's just CSV + YAML)
- Owned by you (no vendor lock-in)

RSF is a tiny stable piece that can support whatever you build on top.

## Non-Goals (v0.1)

- ❌ No formulas
- ❌ No cell-level types
- ❌ No styling
- ❌ No multi-table joins

This is a **structural substrate**, not an Excel replacement. Build layers on top if you need them.

## Comparison

| Feature | Excel/Sheets | RSF |
|---------|--------------|-----|
| Column order | Manual, arbitrary | Computed, deterministic |
| Grouping | UI trick, fragile | Structural, provable |
| Sorting | Can break relationships | Safe, canonical |
| Diffing | Nightmare | Clean, meaningful |
| Validation | None | Built-in |
| Philosophy | UI first | Data first |

## Future Extensions

Possible future layers (separate tools):

- **RSF → OLAP cube** converter
- **Auto-pivot generator**
- **Semantic type inference** (dates, currencies, etc.)
- **Multi-table join planner** using ranked keys
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

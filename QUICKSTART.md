# RSF Quick Start

## Try it with the example file

```bash
# 1. Show column statistics
rsf stats example.csv

# Output:
# === Column Statistics ===
# 
# Column               Cardinality
# ----------------------------------
# TransactionID                 10
# Vendor                         6
# Amount                         8
# AccountID                      2
# Category                       4
# Month                          3

# 2. Rank the file
rsf rank example.csv -o example.rsf --schema

# Output (to stderr):
# === RSF Ranking Complete ===
# Columns ranked by cardinality (highest → lowest):
# 
#   1. TransactionID (cardinality: 10)
#   2. Vendor (cardinality: 6)
#   3. Amount (cardinality: 8)
#   4. AccountID (cardinality: 2)
#   5. Category (cardinality: 4)
#   6. Month (cardinality: 3)
# 
# Rows sorted canonically by key columns.
# Schema written to: example.rsf.schema.yaml

# 3. Look at the ranked file
cat example.rsf

# Notice:
# - TransactionID is now column A (highest cardinality)
# - Columns go from most unique to least unique
# - Rows are sorted canonically

# 4. Look at the schema
cat example.rsf.schema.yaml

# 5. Validate it
rsf validate example.rsf

# Output:
# ✓ Valid RSF file
```

## Integration with mirror-log

```bash
# Export your events
sqlite3 mirror.db "SELECT id, timestamp, source, content FROM events" > events.csv

# Rank them
rsf rank events.csv -o events.rsf --schema

# Now you have:
# - events.rsf - canonical ranked view
# - events.rsf.schema.yaml - provable structure
```

## What just happened?

1. **Computed cardinality** - Counted distinct values per column
2. **Ranked columns** - Ordered from highest to lowest cardinality
3. **Reordered data** - Moved columns to match rank
4. **Sorted canonically** - Ordered rows lexicographically
5. **Generated schema** - Made the structure explicit and verifiable

The result is a **provably ordered** view of your data.

## Why this matters

Before RSF:
- "Which column should be first?" (subjective guess)
- "How should I group this?" (manual work)
- "Did sorting break my data?" (anxiety)

After RSF:
- Column order is computed, not guessed
- Grouping follows natural hierarchy
- Sorting is safe and canonical
- Structure is provable

## Next steps

1. Try ranking your own CSV files
2. Use with mirror-log to rank your event logs
3. Build tools on top that assume RSF structure
4. Share the pattern with others building community gardens

This is stable scaffolding. Build whatever you need on top.

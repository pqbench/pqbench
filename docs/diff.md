# Comparing column byte sizes

`diff` compares two Parquet paths or saved bytemass streams (plain NDJSON or
lz4). One input may be `-` for stdin. Differences are right minus left.

```console
$ pqb diff before.parquet after.parquet | pqb fmt
$ pqb diff before.ndjson after.ndjson --depth 2
```

`--depth N` sums dotted **physical** path prefixes before calculating changes.
It includes Parquet list/map wrapper names. Literal dots in field names are
indistinguishable from path separators; use full paths when those names occur.
A depth must be positive.

Each `diff-column` record reports left/right compressed bytes, signed delta,
percentage change, value counts, and bytes per value. Missing columns have null
bytes, distinct from a present zero-byte column. Percentage change is null
when the left column is absent or has zero bytes. A column's bytes and values
are summed over the files that measured it. Equal totals do not prove equal
data or schema; compare the same logical data before interpreting a reduction
as a saving.

The command rejects incomplete streams, duplicate columns, non-`Bytemass`
records, and arithmetic overflow. Output is NDJSON; `pqb fmt` renders resources, and a delta (which has no
name) is JSON-only.

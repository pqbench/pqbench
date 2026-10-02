# Comparing column byte sizes

`diff` compares two Parquet paths or saved bytemass streams (plain NDJSON or
lz4). One input may be `-` for stdin. Differences are right minus left.

```console
$ pqbench diff before.parquet after.parquet --format table
$ pqbench diff before.ndjson after.ndjson --depth 2
```

`--depth N` sums dotted **physical** path prefixes before calculating changes.
It includes Parquet list/map wrapper names. Literal dots in field names are
indistinguishable from path separators in legacy bytemass output; use full
paths when those names occur. A depth must be positive.

Each `diff-column` record reports left/right compressed bytes, signed delta,
percentage change, row counts, and bytes per row. Missing columns have null
bytes, distinct from a present zero-byte column. Percentage change is null
when the left column is absent or has zero bytes. Row counts are counted once
per file, not per chunk. Equal counts do not prove equal data or schema;
compare the same logical data before interpreting a reduction as a saving.

The command rejects incomplete streams, duplicate chunks, inconsistent row
counts and arithmetic overflow. Each stream must describe at most one table;
select the intended table before comparing multi-table reports. `--format`
chooses table or JSON output, and `-o FILE` also saves the lz4 NDJSON stream.

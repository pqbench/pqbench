# Diff

`diff` compares two Parquet paths or saved bytemass streams (plain NDJSON or
lz4). One input may be `-` for stdin. Differences are right minus left. Compare
the smoke sample with the Reddit fixture:

```console run json
$ pqb diff examples/quickstart.parquet crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | head -2
{"column":"communityName","left_bytes":null,"right_bytes":2468,"delta_bytes":2468,"change_percent":null,"left_values":0,"right_values":3000,"left_bytes_per_value":null,"right_bytes_per_value":0.8226666666666667}
{"column":"dataType","left_bytes":null,"right_bytes":360,"delta_bytes":360,"change_percent":null,"left_values":0,"right_values":3000,"left_bytes_per_value":null,"right_bytes_per_value":0.12}
```

`--depth N` sums dotted **physical** path prefixes before calculating changes
(`pqb diff before.parquet after.parquet --depth 2`). It includes Parquet
list/map wrapper names. Literal dots in field names are indistinguishable from
path separators; use full paths when those names occur. A depth must be
positive.

Each `ColumnDelta` record reports left/right compressed bytes, signed delta,
percentage change, value counts, and bytes per value. Missing columns have null
bytes, distinct from a present zero-byte column. Percentage change is null
when the left column is absent or has zero bytes. A column's bytes and values
are summed over the files that measured it. Equal totals do not prove equal
data or schema; compare the same logical data before interpreting a reduction
as a saving.

The command rejects incomplete streams, duplicate columns, non-`Column`
records, and arithmetic overflow. Output is NDJSON; `pqb fmt` renders resources,
and a delta (which has no `name`) is JSON-only. See
[Documents](documents.md) for the `ColumnDelta` shape.

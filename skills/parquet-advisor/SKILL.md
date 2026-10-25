---
name: parquet-advisor
description: >-
  Turns pqbench facts into write-path, table DDL, and compression-level
  recipes (with pros and cons). Use after bytemass or profile when suggesting
  Parquet layout, sort keys, Iceberg/Delta DDL, ingest settings, codecs, or
  bytes-per-value improvements.
---

# Parquet advisor

Facts come from the CLI. This skill proposes recipes from them. Load the full
text anytime with:

```console run json
$ pqbench skill
{"kind":"pqbench.skill","version":1,"name":"parquet-advisor","description":"Turns pqbench facts into write-path, table DDL, and compression-level recipes","documents":["parquet-advisor","recipes"]}
```

```console run
$ pqbench skill parquet-advisor | head -1
---
$ pqbench skill parquet-advisor recipes | head -1
# Recipes: write path, DDL, compression levels
```

## Tools (what exists)

| Layer | Command | Returns | Cost |
| --- | --- | --- | --- |
| L0 footer | `pqbench table-file FILE | pqbench bytemass` | per column: compressed/uncompressed bytes, codec, encodings, dictionary, value count, null fraction, first/last value, bytes per value, compression ratio | cheap (footer only) |
| Table files | `pqbench list partitions DIR \| pqbench list files` | the files a window's commits added (path, blob, size) | cheap (log only) |
| L1 columns | `pqbench profile FILE --rows first:8192` | physical kind, nulls, NDV, entropy, top values, min/max, string lengths, runs, monotonicity | decode |

Not in the CLI (do not invent measurements): ALP encoding, a built-in
recommender, page-min/max overlap, JSON-explode rewrites, pairwise/locality
measures, and pattern detection (`uuid`, `json`, …).

Keep these separate: **fact** (measured) → **hypothesis** → **recipe** (only
once the change is measured in the target writer, or clearly labeled
unverified).

## Workflow

1. **Where are the bytes?** `pqbench table-file FILE | pqbench bytemass`. Rank columns by
   `compressed_bytes`. Do not tune a 0.2% column before the 70% column.
2. **Column facts.** `pqbench profile data.parquet --rows first:8192`.
   Pass the same local file, or a file `list files` named from a table.
3. **Hypotheses** from the facts (see the table below).
4. **Recipes** from the hypotheses: write/ingest settings, table DDL,
   compression level. See [recipes.md](recipes.md) (`pqbench skill
   parquet-advisor recipes`).

Bound the search: `--rows first:8192`, at most four sort columns.

## Fact → hypothesis → recipe

Prioritize `optimization_value ~= byte_mass * expected_improvement`.

| Finding | Hypothesis | Change to try |
| --- | --- | --- |
| `physical_kind` string, but `top_values` all look numeric | wrong physical type | cast the column to int64/double at ingest |
| `ndv <= 32` or low `ndv_ratio` with non-trivial mass | dictionary + low-cardinality sort key | enable the dictionary; sort by the column |
| `dictionary: false` on a low-NDV column in bytemass | dictionary was off at write | enable the dictionary |
| `monotonic` increasing, `physical_kind` integer | delta encoding / keep time order | delta encoding; keep time order |
| high `adjacent_equal_fraction` / long `run_length_*` | already clustered; don't shuffle | codec/dict only, not a new sort |
| `ndv_ratio ~ 1` unique id, low mass | skip; not a sort key | — |
| high mass + high `entropy`, no structure | inherently random; codec/type only | `zstd@3` vs `snappy` |
| huge `length_max` / `length_p90` | data-model: extract fields at ingest | representation change, not a codec fix |
| two numeric filter columns | Z-order / Hilbert | Z-order / Hilbert sort keys |
| `compression_ratio` near 1 in bytemass | codec or representation is ineffective | cast, dict, or `zstd@3` |
| payload dominates byte mass | do not sort by a tiny enum first | sort keys that prefix the heavy column's locality |

## Recipe output (required shape)

Write recipes — not a codec name alone.

```markdown
## Recipe: <short name>
- Aim: storage | skipping | both
- Evidence: <command + metric + value> (source: footer | sample N)
- Write / ingest: <writer props or job change>
- Table DDL: <Iceberg/Delta/Spark/DuckDB statement or "no DDL change">
- Compression: <codec@level> — why this level (pros / cons)
- Risk: <reader compatibility, CPU, worse skip, …>
- Unverified: <what was not measured>
```

Label a recipe **unverified** until the target writer confirms the change on
production-shaped data.

## Compression levels (short)

Default: `zstd@3`. Then compare `zstd@1`, `zstd@7`, `snappy` if write CPU
matters. Full pros/cons: `pqbench skill parquet-advisor recipes`.

| Choice | Use when | Cost |
| --- | --- | --- |
| `zstd@1` | ingest is CPU-bound, ratio still needed | worse ratio than 3 |
| `zstd@3` | default lake write | small extra CPU vs 1 |
| `zstd@7`–`@9` | cold/archive, write budget allows | slower writes, more RAM |
| `zstd@15+` | rarely | diminishing BPV, high CPU |
| `snappy` / `lz4` | latency-sensitive ingest | weaker ratio |
| `gzip` | old readers | slow write/read |
| `uncompressed` | values already compressed (zstd JSON, images) | no gain, more IO |

Do not raise the zstd level to "fix" a UUID-as-string or unsorted
payload. Fix representation and order first; then pick a level.

## What not to do

- Recommend a layout from a single number.
- Pairwise-scan every column (`O(columns²)`); profile has no pairs.
- Treat sample facts as file-wide without saying `source: sample`.
- Promise production bytes-per-value from an 8k-row sample. The sample ranks
  ideas; the production write confirms them.

---
name: parquet-advisor
description: >-
  Turns pqbench facts into write-path, table DDL, and compression-level
  recipes (with pros and cons). Use after bytemass, profile, or experiment
  when suggesting Parquet layout, sort keys, Iceberg/Delta DDL, ingest
  settings, codecs, or bytes-per-row improvements.
---

# Parquet advisor

Facts come from the CLI. This skill proposes recipes. It does not replace
a measured experiment. Load the full text anytime with:

```sh
pqbench skill parquet-advisor
pqbench skill parquet-advisor recipes
```

## Tools (what exists)

| Layer | Command | Returns | Cost |
| --- | --- | --- | --- |
| L0 footer | `pqbench bytemass FILE` | compressed/uncompressed bytes, codec, encodings, dictionary, footer min/max/null/ndv, BPR | cheap (footer only) |
| L1 pages | `pqbench bytemass FILE --indexes` | `page_count`, page compressed bytes | one extra range |
| Sample | `pqbench dump FILE` | Parquet sample (default zstd) | decode selected row groups |
| L2 columns | `pqbench dump … \| pqbench profile` | NDV, entropy, quantiles, skew, deltas, run lengths, alphabet, patterns | cheap, `--rows first:8192` |
| Locality | `profile --pairs A,B --measures NAME` | NDV(A,B), H(·\|·), MI, FD, null Jaccard, Pearson/Spearman, Cramér's V | medium, opt-in |
| L3 rewrite | `pqbench experiment --rewrite SPEC --aim storage\|skipping\|all` | BPR and skip-span vs a control rewrite | write + measure |

`profile` and `experiment` begin records list `capabilities`. Do not
memorize flags — read that list.

Not in the CLI (do not invent measurements): ALP encoding, a built-in
recommender, page-min/max overlap derived from L1, JSON-explode rewrites.

Keep these separate: **fact** (measured) → **hypothesis** → **experiment**
→ **recipe** (only after a result, or clearly labeled unverified).

## Workflow

1. **Where are the bytes?** `bytemass` (add `--indexes` only if page
   locality matters). Rank columns by `compressed_bytes`. Do not tune a
   0.2% column before the 70% column.
2. **Sample once.** `dump --row-groups first:1 -o sample.parquet` (or
   pipe). Later profile/experiment calls reuse that file.
3. **Cheap column facts.** `pqbench profile sample.parquet`.
4. **Locality only for promising pairs.** Heavy mass + enum-like / FD
   candidates: `--pairs country,city --measures functional_dependency`.
5. **Hypotheses, then rewrite.** One `--rewrite` per idea. `--aim
   storage` for BPR; `--aim skipping` for min/max locality; `--aim all`
   when the table is both scanned and filtered.
6. **Recipes** from winners: write/ingest settings, table DDL,
   compression level. See [recipes.md](recipes.md) (`pqbench skill
   parquet-advisor recipes`).

Bound the search: `--rows first:8192`, at most a few rewrites, at most
four sort columns.

## Fact → hypothesis → experiment

Prioritize `optimization_value ~= byte_mass * expected_improvement`.

| Finding | Hypothesis | Verify |
| --- | --- | --- |
| `patterns` contains `uuid` / `integer_string` / `timestamp_string` / `decimal_string` | wrong physical type | `--rewrite cast:COL:int64` (or `double` / `string`) |
| `enum_like` or `ndv <= 32` and non-trivial mass | dictionary + low-cardinality sort key | `--rewrite dictionary:on` and `--rewrite sort:COL` |
| `dictionary: false` on a low-NDV column in bytemass | dictionary was off at write | `--rewrite dictionary:on` |
| `adjacent_delta_*` small, `monotonic` increasing | delta encoding / keep time order | `--rewrite encoding:delta` and `--rewrite sort:COL` |
| high `adjacent_equal_fraction` / long `run_length_*` | already clustered; don't shuffle | codec/dict only, not a new sort |
| `ndv_ratio ~ 1` unique id, low mass | skip pairwise; not a sort key | — |
| high mass + high entropy, no pattern | inherently random; codec/type only | `--rewrite codec:zstd@3` vs `snappy` |
| `json` pattern or huge `length_p99` | data-model: extract fields at ingest | `drop` after extract; not a codec fix |
| FD `functional_dependency_right` high (A → B) | `SORT(A,B)` | `--rewrite sort:A,B --aim all` |
| two numerics, high Spearman, filters on both | Z-order / Hilbert | `--rewrite zorder:A,B` and `hilbert:A,B --aim skipping` |
| `skip_span_ratio` high on a filter column | files are not clustered for skip | `--rewrite sort:COL --aim skipping` |
| `compression_ratio` near 1 on bytemass | codec or representation is ineffective | cast, dict, or `codec:zstd@3` |
| payload dominates byte mass | do not sort by a tiny enum first | sort keys that prefix the heavy column's locality |

## Recipe output (required shape)

After experiments, write recipes — not a codec name alone.

```markdown
## Recipe: <short name>
- Aim: storage | skipping | both
- Evidence: <command + metric + value> (source: footer | sample N | experiment)
- Write / ingest: <writer props or job change>
- Table DDL: <Iceberg/Delta/Spark/DuckDB statement or "no DDL change">
- Compression: <codec@level> — why this level (pros / cons)
- Risk: <reader compatibility, CPU, worse skip, …>
- Unverified: <what was not measured>
```

Prefer a measured `file_bytes_ratio` or `skip_span_ratio` over a guess.
If you did not run `experiment`, label the recipe **unverified**.

## Compression levels (short)

Default verify: `codec:zstd@3`. Then compare `zstd@1`, `zstd@7`,
`snappy` if write CPU matters. Full pros/cons: `pqbench skill
parquet-advisor recipes`.

| Choice | Use when | Cost |
| --- | --- | --- |
| `zstd@1` | ingest is CPU-bound, ratio still needed | worse ratio than 3 |
| `zstd@3` | default lake write | small extra CPU vs 1 |
| `zstd@7`–`@9` | cold/archive, write budget allows | slower writes, more RAM |
| `zstd@15+` | rarely | diminishing BPR, high CPU |
| `snappy` / `lz4` | latency-sensitive ingest | weaker ratio |
| `gzip` | old readers | slow write/read |
| `uncompressed` | values already compressed (zstd JSON, images) | no gain, more IO |

Do not raise the zstd level to "fix" a UUID-as-string or unsorted
payload. Fix representation and order first; then pick a level.

## What not to do

- Recommend a layout from Pearson alone.
- Pairwise-scan every column (`O(columns²)`).
- Treat sample facts as file-wide without saying `source: sample`.
- Promise production BPR from an 8k-row rewrite. The experiment ranks
  ideas; production write confirms them.

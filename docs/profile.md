# pqbench profile

Per-column facts from a decoded row sample. Unlike
[bytemass](cli.md) — which reads the Parquet footer only and never decodes
data — `profile` reads row values and decodes them.

## Usage

```console run
$ pqbench profile examples/quickstart.parquet --format table
id                           column  kind     values  nulls  ndv
---------------------------  ------  -------  ------  -----  ---
examples/quickstart.parquet  id      integer       8      0    8
examples/quickstart.parquet  year    integer       8      0    8
files: 1
rows: 8
columns: 2
$ pqbench profile examples/quickstart.parquet --columns 'id' --top 5 -o /tmp/profile.ndjson.zst
```

The stream is the explicit machine output, useful for the next command:

```console run json
$ pqbench profile examples/quickstart.parquet --rows all --json | head -1
{"kind":"pqbench.profile","version":1,"event":"begin"}
```

Inputs are local Parquet files. Each file is opened, up to `--rows` leading
rows are decoded, and a profile is computed per selected column.

| Flag | Meaning |
| --- | --- |
| `--columns GLOB` | Repeatable. A `glob::Pattern` matched against the whole column name. Default: every column. A glob that matches nothing is an error. |
| `--rows METHOD` | `all` or `first:N` with `N >= 1`. Default: `first:8192`. |
| `--top N` | How many top values to keep per column. Default: 8. |
| `-o` / `--output FILE` | Also write the lz4 NDJSON stream to FILE, independent of what stdout shows. |
| `--format` | `auto` (table on a terminal, NDJSON on a pipe), `table`, or `json`. |
| `--json` | Stream NDJSON on stdout (same as `--format json`). |

A pipe streams one `pqbench.profile-column` line per column between a
`pqbench.profile` begin and end. `row_count` and `column_count` are on the
end record.

## Fields

Each `pqbench.profile-column` line carries the input path as `id` and:

| Field | Meaning |
| --- | --- |
| `column` | Column name. |
| `physical_kind` | Inferred from the decoded values: `empty`, `boolean`, `integer`, `number`, or `string`. |
| `num_values` | Sampled cells for this column (including nulls). |
| `null_count` / `null_fraction` | Null cells and their share of `num_values`. |
| `ndv` / `ndv_ratio` | Distinct non-null values and their share of `num_values`. |
| `entropy` | Shannon entropy in bits over non-null values. |
| `top_values` | Up to `--top` `{value, count}` entries, by count then value. |
| `min_value` / `max_value` | Bounds over non-null values: numeric for `integer`/`number` columns, lexical otherwise. |
| `length_mean` / `length_p50` / `length_p90` / `length_max` | String byte-length stats (nearest-rank percentiles). |
| `adjacent_equal_fraction` | Share of adjacent equal cells (nulls count as a value). |
| `run_length_mean` / `run_length_max` | Mean and max run length. |
| `monotonic` | `CONSTANT`, `INCREASING`, `DECREASING`, or `UNORDERED` over non-null values (numeric order for `integer`/`number`). |

## Document

```json
{"kind":"pqbench.profile","version":1,"event":"begin"}
{"kind":"pqbench.profile-column","id":"data.parquet","column":"text", "...": "..."}
{"kind":"pqbench.profile","event":"end","file_count":1,"row_count":3000,"column_count":4}
```

# Profile

Per-column facts from a decoded row sample. Unlike
[bytemass](cli.md#pqbench-bytemass) — which reads the Parquet footer only and
never decodes data — `profile` reads row values and decodes them.

## Usage

```console run
$ pqb profile examples/quickstart.parquet | pqb fmt
id                           column  kind     values  nulls  ndv
---------------------------  ------  -------  ------  -----  ---
examples/quickstart.parquet  id      integer       8      0    8
examples/quickstart.parquet  year    integer       8      0    8
```

The stream is the explicit machine output, useful for the next command:

```console run json
$ pqb profile examples/quickstart.parquet --rows all | head -1 | jq -c '{column, physical_kind, ndv}'
{"column":"id","physical_kind":"integer","ndv":8}
```

Inputs are local Parquet files. Each file is opened, up to `--rows` leading
rows are decoded, and a profile is computed per selected column.

| Flag | Meaning |
| --- | --- |
| `--columns GLOB` | Repeatable. A `glob::Pattern` matched against the whole column name. Default: every column. A glob that matches nothing is an error. |
| `--rows METHOD` | `all` or `first:N` with `N >= 1`. Default: `first:8192`. |
| `--top N` | How many top values to keep per column. Default: 8. |

A pipe streams one `ColumnProfile` line per column. The resource name is
`blobs/{service}/{locator}/columns/{column}/profile`; see
[Documents](documents.md) for the resource model.

## Fields

Each `ColumnProfile` line carries the CLI's `id` (the input path) and:

| Field | Meaning |
| --- | --- |
| `name` | Resource name: `blobs/{service}/{locator}/columns/{column}/profile`. |
| `id` | The input path, added by the CLI wrapper. |
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
{"name":"blobs/file/examples%2Fquickstart.parquet/columns/id/profile","id":"examples/quickstart.parquet","column":"id","physical_kind":"integer","num_values":8,"null_count":0,"null_fraction":0.0,"ndv":8,"ndv_ratio":1.0,"entropy":3.0,"top_values":[{"value":"0","count":1}],"min_value":"0","max_value":"7","length_mean":1.0,"length_p50":1,"length_p90":1,"length_max":1,"adjacent_equal_fraction":0.0,"run_length_mean":1.0,"run_length_max":1,"monotonic":"INCREASING"}
```

# pqbench experiment

Rewrite a decoded row sample and measure the resulting Parquet.
[profile](profile.md) reports facts; `experiment` applies a requested
rewrite and writes the sample back, so you can verify a layout empirically
instead of predicting its compression.

## Usage

```sh run
pqbench experiment examples/quickstart.parquet
pqbench experiment examples/quickstart.parquet --rewrite sort:id --aim all
pqbench experiment examples/quickstart.parquet --rewrite 'sort:id' --rewrite codec:snappy
pqbench experiment examples/quickstart.parquet --rewrite 'sort:id;dictionary:off' -o /tmp/trials.ndjson.zst
```

| Flag | Meaning |
| --- | --- |
| `--rows METHOD` | `all` or `first:N` with `N >= 1`. Default: `first:8192`. |
| `--rewrite SPEC` | Repeatable. Each value is one trial; semicolons compose rewrites. |
| `--trial SPEC` | Alias for `--rewrite`. |
| `--aim AIM` | `storage` (default), `skipping`, or `all`. |
| `-o` / `--output FILE` | Write the zstd NDJSON stream. Required on a terminal. |
| `--json` | Stream NDJSON on stdout (same as a pipe). |

A pipe streams one `pqbench.experiment-trial` per trial and one
`pqbench.experiment-column` per trial+column, between a `pqbench.experiment`
begin and end.

## `--aim`

| Aim | What is measured |
| --- | --- |
| `storage` | File bytes, bytes per row, and per-column compressed bytes. |
| `skipping` | Per-column row-group min/max locality (`skip_span_ratio`, `skip_point_equal_fraction`). |
| `all` | Both. |

`skipping` and `all` split the sample into row groups of 2048 rows unless a
trial sets `row-group-size`. A single row group cannot show skip locality.

## `--rewrite SPEC`

Each `--rewrite` (or `--trial`) is one trial. Semicolons compose rewrites in
that trial.

| Spec | Effect |
| --- | --- |
| `sort:A` / `sort:A,B` | Row order: numeric for integer/number columns, bytewise for others. |
| `zorder:A,B` | Morton order over value ranks (any number of columns). |
| `hilbert:A,B` | 2-D Hilbert order; exactly two columns. |
| `codec:zstd@3` | `uncompressed`, `snappy`, `gzip`, `lz4`, `zstd`; level after `@`. |
| `dictionary:on` / `off` | Dictionary encoding. |
| `dictionary:BYTES` | Dictionary on with a page size limit. |
| `encoding:delta` | Also `plain`, `rle`, `delta_length`, `delta_byte_array`, `byte_stream_split`. Type-aware: applied only to compatible columns. |
| `page-size:BYTES` | Data page size limit. |
| `row-group-size:ROWS` | Rows per row group. |
| `cast:COL:int64` | Also `double`, `string`; changes the column's type before write. |

A missing column, an unknown key, or a bad value is an error. The
`pqbench.experiment` begin record lists the supported aims and rewrites under
`capabilities`.

## Control and ratios

The **control** trial is always written first: same writer defaults (zstd,
dictionary on), no rewrite. For `storage` it is a single row group; for
`skipping`/`all` it is split like the trials. A trial's `ratio` is
`bytes / control.bytes`; it is absent on the control row. Ratios compare to
that control, not to the source file's original encodings.

## What this command does not do

It does not do `drop`, `--indexes`, or input row-group selection, and it
does not recommend a layout. Footer masses of an existing file stay on
[bytemass](cli.md).

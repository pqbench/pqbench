# pqbench

[![CI](https://github.com/pqbench/pqbench/actions/workflows/ci.yml/badge.svg)](https://github.com/pqbench/pqbench/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-pqbench.github.io-blue.svg)](https://pqbench.github.io/pqbench/)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](https://github.com/pqbench/pqbench/blob/main/LICENSE-MIT)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/pqbench/pqbench/blob/main/LICENSE-APACHE)

*One binary for Parquet optimization.*

<!-- --8<-- [start:landing] -->
Measure how well each codec compresses a Parquet file and how many on-disk
bytes each column costs. Byte masses come from the footer and the table
metadata, not the data pages, so they are fast on files of any size.

Commands compose on pipes — each writes a typed `pqbench.v1` document the next
reads — so the whole job is one pipeline. The [walk](#the-walk-end-to-end) goes
from a catalog's tables to the bytes each column costs.

## Quick start

Install the released `pqbench` binary (Linux and macOS, x86_64 and arm64):

```sh
curl -LsSf https://github.com/pqbench/pqbench/releases/latest/download/pqbench-cli-installer.sh | sh
```

The installer verifies the download against the release checksums and puts
`pqbench` in `~/.cargo/bin`, with `pqb` as a symlink to it. To pin a version,
build from source, or read about the release process, see
[Distribution](https://pqbench.github.io/pqbench/reference/distribution/).

## The walk, end to end

pqbench's whole job is one walk: descend a catalog, narrow to a table's files,
and measure the bytes each column costs. Every stage streams JSON to the next,
so the walk is one bash pipeline.

```console no-run
$ set -euo pipefail
$ export PQB_ENDPOINT=https://<workspace>.cloud.databricks.com PQB_TOKEN=dapi-…
$ pqb ls catalogs | pqb rl \
    | pqb ls schemas | pqb rl \
    | pqb ls tables | pqb rl \
    | pqb credentials check \
    | pqb credentials get \
    | pqb get table \
    | pqb ls files \
    | pqb bytemass \
    | pqb summarize columns \
    | pqb viz -o /tmp/report
```

Read it left to right:

- `list catalogs`, `list schemas`, and `list tables` descend the catalog. A
  live endpoint fans out one request per ref, so `rl` paces each level at
  15 refs/s by default — the schema and table listings and the table stages
  (`credentials check`, `credentials get`, `get table`) alike; the
  retry-at-half-the-rate recipe is in the
  [CLI reference](https://pqbench.github.io/pqbench/reference/cli/).
- `credentials check` drops the tables you can't read outside compute — a view,
  a system catalog, Databricks default storage — and prints the reason;
  `credentials get` vends a short-lived lease onto each remaining ref.
- `get table` enriches each ref with its format and snapshot; the Delta log is
  read with `without_files()`, so the read is O(1) in files. `list files` then
  resolves each snapshot and writes its active files, one `TableFile` per line,
  carrying the table's lease.
- `bytemass` reads each file's footer and prints the on-disk bytes per column,
  the table in [bytemass](#bytemass). In a pipe it streams
  `Blob` and `Bytemass` NDJSON instead; `pqb summarize columns` estimates each
  table column's mass and `pqb viz` draws that as a treemap.

The same walk runs offline against the local Delta and Iceberg stand,
`make lakehouse`. [Getting started](https://pqbench.github.io/pqbench/getting-started/) takes one Parquet
file to a treemap, and the [CLI reference](https://pqbench.github.io/pqbench/reference/cli/) documents every command.

## Showcases

[Showcases](https://pqbench.github.io/pqbench/guides/showcases/) runs the commands as
jobs on real public data — triage which columns own the file, choose a codec,
verify a layout change, and read SQuAD and a TPC-DS table from the wild. On a
2.1 MB Reddit comments sample, three of seven columns are 99.7% of the bytes:

```console run
$ pqb table-file crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqb bytemass | pqb fmt
column            type        codec         encoding   bytes  dict bytes  bytes/row  values
----------------  ----------  ------------  --------  ------  ----------  ---------  ------
text              BYTE_ARRAY  UNCOMPRESSED  dict      743566      738658     247.86    3000
label             BYTE_ARRAY  UNCOMPRESSED  dict        2469         527       0.82    3000
dataType          BYTE_ARRAY  UNCOMPRESSED  dict         360          33       0.12    3000
communityName     BYTE_ARRAY  UNCOMPRESSED  dict        2468         527       0.82    3000
datetime          BYTE_ARRAY  UNCOMPRESSED  dict          87          28       0.03    3000
username_encoded  BYTE_ARRAY  UNCOMPRESSED  dict      446197      441331     148.73    3000
url_encoded       BYTE_ARRAY  UNCOMPRESSED  dict      907117      901963     302.37    3000
```

`profile` explains it and `compression` picks the codec. The
[Showcases](https://pqbench.github.io/pqbench/guides/showcases/) page walks them end to
end.

## Commands

The core measurements. The walk's [`get` and `list`](#the-walk-end-to-end) come
first; the [CLI reference](https://pqbench.github.io/pqbench/reference/cli/) covers
every command, and [Documents](https://pqbench.github.io/pqbench/reference/documents/)
maps the records they exchange.

### lz

lzbench-style compression benchmark over raw file bytes. The default sweeps
every wired codec (zstd, lz4, gzip, snappy); `-c codec@level` (repeatable)
picks one config:

```console run
$ pqb lz data/samples/squad.parquet --samples 1 --warmup-iterations 0 | pqb fmt
codec   level  compress MB/s  decompress MB/s  ratio
------  -----  -------------  ---------------  -----
gzip        1            ±44             ±226   0.85
zstd        1           ±915            ±1391   0.85
lz4         1          ±2176            ±5180   0.98
snappy      1          ±1608            ±1981   0.99
```

Speeds vary run to run; the stream is NDJSON.

### compression

The same sweep over a parquet file's encoded pages. The input must be
NONE-compressed; `--per-column` adds a report per column.

```console run
$ pqb compression crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet --samples 1 --warmup-iterations 0 | pqb fmt
codec   level  compress MB/s  decompress MB/s  ratio
------  -----  -------------  ---------------  -----
zstd        1           ±458            ±1722   0.56
gzip        1            ±68             ±270   0.58
lz4         1           ±897            ±8476   0.81
snappy      1           ±912            ±2101   0.81
```

The stream is NDJSON.

### bytemass

Per-column byte masses — the on-disk bytes each column takes, with its
value count, its chunks summed over the file's row groups.

```console run
$ pqb table-file data/samples/delta_byte_array.parquet | pqb bytemass | pqb fmt
column                 type        codec         encoding          bytes  dict bytes  bytes/row  values
---------------------  ----------  ------------  ----------------  -----  ----------  ---------  ------
c_customer_id          BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   8248           0       8.25    1000
c_salutation           BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   3362           0       3.36    1000
c_first_name           BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   6595           0       6.59    1000
c_last_name            BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   6955           0       6.96    1000
c_preferred_cust_flag  BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   1220           0       1.22    1000
c_birth_country        BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   9599           0       9.60    1000
c_login                BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY     42           0       0.04    1000
c_email_address        BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY  27763           0      27.76    1000
c_last_review_date     BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   3511           0       3.51    1000
```

- Reads only the footer, so it works on any file, compressed or not.
- Reads `TableFile` records on stdin: `table-file PATH` names one path or URI,
  and `list files` names a table's files.
- `s3://` needs the `aws` feature; a pipe streams one `Blob` per file and one
  `Bytemass` per column.

Filtering which files to measure is a shell job on the `list files` stream —
see [Visual demos](https://pqbench.github.io/pqbench/guides/demo/).

### list files

List a table's active files — the files its snapshot holds now.

```console run delta
$ cat docker/e2e-lakehouse/table.ndjson | pqb ls files | pqb fmt
path                                                                 size_bytes
-------------------------------------------------------------------  ----------
part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet         796
```

- Reads each table's Delta / Iceberg log and checkpoint, never the data files.
- `--last 1m|1h|1d|1w` keeps the files the log dates in the window ending now.
- `--partition 2026-10-28` keeps one UTC day.
- One line per file, written as the walk produces it: the run holds one log
  batch plus one file per table in flight (`--fan-out`).
- Delta needs `--features delta`, Iceberg `iceberg`; add `-s3` to either for
  `s3://`.
- Reads Table refs on standard input (a `get table` stream).

The files are the input to `bytemass`:

```console run delta
$ cat docker/e2e-lakehouse/table.ndjson | pqb ls files | pqb bytemass | pqb fmt
column  type        codec   encoding  bytes  dict bytes  bytes/row  values
------  ----------  ------  --------  -----  ----------  ---------  ------
id      INT64       SNAPPY  dict         66          37      22.00       3
label   BYTE_ARRAY  SNAPPY  dict         72          43      24.00       3
```

The lease rides on every file, so a short-lived lease travels to `bytemass`.
A producer names a file with `table-file`; `AWS_*` in the environment (or a
vended lease on the file record) supplies storage access:

```console no-run
$ pqb table-file s3://bucket/table/part-0.parquet | AWS_PROFILE=analytics pqb bytemass | pqb fmt
```

### summarize columns

Fold the `list columns` stream into one `ColumnMass` row per table column: weighted
bytes, values, file count, storage classes, and the monthly S3 cost.

```console no-run
$ pqb table-file data/samples/delta_byte_array.parquet | pqb list columns | pqb summarize columns
```

### viz

Draw a `summarize columns` document as a static HTML treemap — no server, no
build step.

```console run delta
$ pqb table-file data/samples/delta_byte_array.parquet | pqb list columns | pqb summarize columns | pqb viz -o /tmp/report
$ cat docker/e2e-lakehouse/table.ndjson | pqb ls files | pqb list columns | pqb summarize columns | pqb viz -o /tmp/report
```

The page lands at `/tmp/report.html`. Open it in a browser.

## Documentation

Start with [Getting started](https://pqbench.github.io/pqbench/getting-started/). The
[documentation index](https://pqbench.github.io/pqbench/) maps every page by kind.
`pqb --help` is the command guide; `pqb <command> --help` is local to
that command.

- [Getting started](https://pqbench.github.io/pqbench/getting-started/) — install, first measurement, the metadata walk
- [Showcases](https://pqbench.github.io/pqbench/guides/showcases/) — real jobs on real data: triage the bytes, pick a codec, verify a layout change
- [Distribution](https://pqbench.github.io/pqbench/reference/distribution/) — install the binary, targets, and how releases are cut
- [CLI reference](https://pqbench.github.io/pqbench/reference/cli/) — every command, generated from the clap tree
- [Documents](https://pqbench.github.io/pqbench/reference/documents/) — the records the commands exchange
- [Authentication](https://pqbench.github.io/pqbench/reference/authentication/) — object storage, catalog config, and governed tables
- [Visual demos](https://pqbench.github.io/pqbench/guides/demo/) — Parquet, Delta, Iceberg, catalog walk, and Unix file selection
- [Delta tables](https://pqbench.github.io/pqbench/guides/delta/) — log load, `list files | bytemass`, limitations
- [Iceberg tables](https://pqbench.github.io/pqbench/guides/iceberg/) — metadata load, `list files | bytemass`, limitations
- [Visualization](https://pqbench.github.io/pqbench/reference/cli/#pqbench-viz) — `pqb viz`, a static HTML treemap
- [Python](https://pqbench.github.io/pqbench/reference/python/) — install the binary wheel; run the CLI, read the files
- [Unity Catalog and Iceberg REST E2E](https://github.com/pqbench/pqbench/tree/main/docker/e2e-lakehouse) — catalogs
  naming tables for `pqb ls files`, over rustfs S3

## Python

[`python/`](https://pqbench.github.io/pqbench/reference/python/) ships the same binary as
a Python wheel: `pip install pqbench` puts `pqbench` on your `PATH`. There is
no Python extension and no importable module — the CLI is the interface, and
the files it writes are the data. For streaming pipelines, see the
[Python orchestration guide](https://pqbench.github.io/pqbench/guides/python/).

```console no-run
$ pip install pqbench
```

```console run delta
$ pqbench list files < docker/e2e-lakehouse/table.ndjson | zstd -3 > /tmp/files.jsonl.zst
```

```python
import polars as pl

files = pl.scan_ndjson("/tmp/files.jsonl.zst")
```

## Contributing

PRs welcome. The gate is `make check` (`fmt-check` + `check-docs` +
`clippy -D warnings` + `isolation` + `lfs-check` + `test`).
The Python wheel is `make check-python`. The runnable commands on this
page are generated into tests by `docscheck` (`make sync-docs`); see
[CONTRIBUTING.md](https://pqbench.github.io/pqbench/contributing/) for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](https://github.com/pqbench/pqbench/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/pqbench/pqbench/blob/main/LICENSE-APACHE).

<!-- --8<-- [end:landing] -->

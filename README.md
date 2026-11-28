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
$ pqb setup --check
$ pqb daemon start
$ pqb ls catalogs \
    | pqb ls schemas \
    | pqb ls tables \
    | pqb get table \
    | pqb ls files \
    | pqb bytemass \
    | pqb summarize columns \
    | pqb format --html > /tmp/report.html
```

Read it left to right:

- `list catalogs`, `list schemas`, `list tables`, and `get table` descend the
  catalog. A live endpoint rate-limits its REST calls, so the daemon that the
  next bullet starts owns those requests and paces Unity at 15/s, the catalog's
  per-endpoint ceiling.
- `setup --check` is the preflight: it reports the resolved configuration, the
  credential, and the endpoint's answer to the walk's first level as three
  `ok`/`fail` lines and exits non-zero on a failure, so a misconfigured run
  fails before it starts.
- With no static token, a Databricks service principal authenticates with
  `PQB_CLIENT_ID` and `PQB_CLIENT_SECRET`: the daemon mints the OAuth M2M
  bearer, holds it, and mints the next one five minutes before it expires.
  Neither the secret nor the bearer is printed. See
  [Authentication](https://pqbench.github.io/pqbench/reference/authentication/).
- `daemon start` runs one daemon for the walk. It makes every catalog request
  and caches the storage lease each table is read under, serving both to every
  stage, so the catalog vends each table once rather than once per command.
  A stage that needs the catalog and finds no daemon fails with a hint to
  start one; the leases are held in memory and never appear in a document.
- `get table` enriches each ref with its format and snapshot; the Delta log is
  read with `without_files()`, so the read is O(1) in files. `list files` then
  resolves each snapshot and writes its active files, one `TableFile` per line.
- `bytemass` reads each file's footer and prints the on-disk bytes per column,
  the table in [bytemass](#bytemass). In a pipe it streams
  `Blob` and `Column` NDJSON instead; `pqb summarize columns` estimates each
  table column's mass and `pqb format --html` draws that as a treemap.

A ref the catalog will not vend a lease for — a view, a system table, a table on
Databricks default storage — is dropped with the cause on standard error, and
that stage exits non-zero; `set -e` then stops the pipeline. Walk a catalog that
is externally readable, or filter the refs to one first (`scripts/dbx-walk.sh`
shows the `jq` filter). The lease itself is never written to a document.

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
  `Column` per column.

Filtering which files to measure is a shell job on the `list files` stream —
see [Visual demos](https://pqbench.github.io/pqbench/guides/demo/).

### list suggestions

The storage a file's values do not need. One suggestion per column the file's
footer and a representative row group make plausible, each with the change that
recovers the bytes:

```console run
$ pqb table-file crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqb list suggestions | pqb fmt
COLUMN            RULE        VERIFIED  SAVINGS  REMEDY                                                                               MESSAGE
----------------  ----------  --------  -------  -----------------------------------------------------------------------------------  ------------------------------
url_encoded       codec-zstd  -                  compression={'url_encoded': 'zstd'}, compression_level={'url_encoded': 3}            uncompressed at 302.37 B/value
text              codec-zstd  -                  compression={'text': 'zstd'}, compression_level={'text': 3}                          uncompressed at 247.86 B/value
username_encoded  codec-zstd  -                  compression={'username_encoded': 'zstd'}, compression_level={'username_encoded': 3}  uncompressed at 148.73 B/value
```

Twelve rules read one column's chunk; the first that detects a column wins, so
a column's bytes are never priced twice:

| Rule | What it finds | Remedy |
| --- | --- | --- |
| `uuid-text` | canonical UUID text, 36 bytes for 16 of data | write it as `BINARY(16)` |
| `base64-text` | Base64 text of binary payloads, four characters per three bytes | decode it to a binary column |
| `int96-timestamp` | the deprecated INT96 timestamp | write an `INT64` microsecond timestamp |
| `text-timestamp` | every value is ISO-8601 timestamp text | write a microsecond `INT64` timestamp |
| `codec-zstd` | a chunk stored uncompressed, snappy, or gzip | write the column with zstd |
| `page-size` | many tiny data pages from a low page-size limit | write it with the standard 1 MiB page size |
| `delta-integer` | dense or monotone integers without `DELTA_BINARY_PACKED` | write the column Delta binary packed (page v2) |
| `dictionary-unique` | a dictionary page on nearly unique values | write the column without a dictionary |
| `dictionary-missing` | repeated values written without a dictionary | write the column with one |
| `delta-text-encoding` | nearly-unique text without a dictionary | write Delta byte/length arrays (page v2) |
| `dictionary-fallback` | a dictionary that fell back to `PLAIN` data pages | raise the dictionary page size limit, or turn the dictionary off |
| `stats-truncated` | footer `min_value`/`max_value` past 64 bytes | truncate statistics at 64 bytes |

`dictionary-unique`, `dictionary-missing`, and `dictionary-fallback` cover every
physical type a dictionary can pay on — byte arrays, fixed-length byte arrays,
`INT32`, `INT64`, `FLOAT`, and `DOUBLE` — and `dictionary-fallback` prices three
arms at the file's own codec, keeping the smaller of a raised limit and no
dictionary. `page-size` is measured as a controlled pair: the source's own
effective page size against the standard 1 MiB, at the file's codec and
encoding. `base64-text` reads only values that are canonical Base64 of binary
bytes: a UUID, a hex digest, and a word all use Base64 characters without being
one, and a decoded payload that is mostly printable is a text column, not a
binary one. `stats-truncated` needs no re-encode: a footer's bounds are the
writer's own bytes, so the bytes past the truncation length are exact.

`uuid-text`, `base64-text`, `text-timestamp`, `delta-integer`,
`delta-text-encoding`, the dictionary rules, and `page-size` decode and
re-encode a leaf's values, so they measure scalar leaves only. `codec-zstd` and
`stats-truncated` read page headers, page bytes, and footer facts, so they
measure any leaf, nested `list<struct>` columns included.

Two more decide from `list partitions` totals, so they read no file and hold
for partitions a host never copies:

| Rule | What it finds | Remedy |
| --- | --- | --- |
| `tombstones` | Delta's removed files the log still holds | `VACUUM` (Delta), expire snapshots (Iceberg) |
| `small-files` | many small files in one partition | `OPTIMIZE` (Delta), `rewrite_data_files` (Iceberg), a coalesce before write |

A suggestion's `remedy` is the setting in the detected writer's own terms —
parquet-mr takes per-column keys, pyarrow and parquet-rs write options, Photon
forwards them unverified — and `remedies` carries the same change for every
engine pqbench knows: `spark`, `pyarrow`, `parquet-rs`, `delta`, `iceberg`.
So a table compacted by any of them has its setting without a second lookup.
`get suggestion` re-writes the sample under the remedy's control and marks the
suggestion `verified` with the bytes it saves; `list suggestions` only names
the candidates.

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

### format --html

Draw a `summarize columns` document as a static HTML treemap — no server, no
build step.

```console run delta
$ pqb table-file data/samples/delta_byte_array.parquet | pqb list columns | pqb summarize columns | pqb fmt
table                           column                 bytes  values  bytes/value  ratio  files  class  $/month
------------------------------  ---------------------  -----  ------  -----------  -----  -----  -----  -------
local.delta_byte_array.parquet  c_birth_country         9599    1000         9.60   1.00      1
local.delta_byte_array.parquet  c_customer_id           8248    1000         8.25   1.00      1
local.delta_byte_array.parquet  c_email_address        27763    1000        27.76   1.00      1
local.delta_byte_array.parquet  c_first_name            6595    1000         6.59   1.00      1
local.delta_byte_array.parquet  c_last_name             6955    1000         6.96   1.00      1
local.delta_byte_array.parquet  c_last_review_date      3511    1000         3.51   1.00      1
local.delta_byte_array.parquet  c_login                   42    1000         0.04   1.00      1
local.delta_byte_array.parquet  c_preferred_cust_flag   1220    1000         1.22   1.00      1
local.delta_byte_array.parquet  c_salutation            3362    1000         3.36   1.00      1
$ pqb table-file data/samples/delta_byte_array.parquet | pqb list columns | pqb summarize columns | pqb format --html > /tmp/report.html
$ cat docker/e2e-lakehouse/table.ndjson | pqb ls files | pqb list columns | pqb summarize columns | pqb format --html > /tmp/report.html
```

The page lands at `/tmp/report.html`. Open it in a browser.

## The driver

`pqbench-driver` keeps a map of partition versions — a partition at one etag —
and drives each one through price, download, bytemass, lint, and verify with the
plain `pqbench` commands, one per CPU, newest data first. Its walk re-lists the
catalog every `--walk-period-seconds`, so a partition whose etag moved is
measured again and a version the walk no longer finds leaves the map. State is
checkpointed under `--store DIR`, results land in `DIR/results/`, and
`pqbench list partition-versions` / `pqbench get partition-version` read it back
over the driver's own socket:

```console no-run
$ pqbench-driver start --store /var/lib/pqbench --walk-period-seconds 86400
$ export PQB_DRIVER_SOCK=/var/lib/pqbench/driver.sock
$ pqbench list partition-versions catalogs/main/schemas/sales
$ pqbench get partition-version catalogs/main/schemas/sales/tables/orders/partitions/day%3D1
$ pqbench-driver status
$ pqbench-driver stop --store /var/lib/pqbench
```

One operator setting — `step-timeout` — changes rarely enough to live only in
`~/.pqbenchrc`; see
[Configuration](https://pqbench.github.io/pqbench/reference/configuration/#driver-settings).
The catalog daemon is independent: the driver's commands talk to it when one is
running, and local tables need none.

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
- [Format and HTML](https://pqbench.github.io/pqbench/reference/format/) — `pqb format`, tables and the static HTML treemap
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

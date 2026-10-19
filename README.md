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

Commands compose on pipes — each writes a versioned JSON document the next
reads — so the whole job is one pipeline. The [walk](#the-walk-end-to-end) goes
from a catalog's tables to the bytes each column costs.

## Quick start

Install the released `pqbench` binary (Linux and macOS, x86_64 and arm64):

```sh
curl -LsSf https://github.com/pqbench/pqbench/releases/latest/download/pqbench-cli-installer.sh | sh
```

The installer verifies the download against the release checksums and puts
`pqbench` in `~/.cargo/bin`. To pin a version, build from source, or read about
the release process, see [Distribution](https://pqbench.github.io/pqbench/distribution/).

## The walk, end to end

pqbench's whole job is one walk: descend a catalog, narrow to a table's files,
and measure the bytes each column costs. Every stage streams JSON to the next,
so the walk is one bash pipeline.

```console no-run
$ set -euo pipefail
$ export PQB_ENDPOINT=https://<workspace>.cloud.databricks.com PQB_TOKEN=dapi-…
$ pqbench list catalogs | pqbench list schemas | pqbench list tables \
    | pqbench credentials check | pqbench credentials get | pqbench get table \
    | pqbench list partitions | pqbench list files | pqbench bytemass --format table
```

Read it left to right:

- `list catalogs`, `list schemas`, and `list tables` descend the catalog. A
  rate-limited endpoint wants `pqbench ratelimit --rate 15` between two levels;
  the retry-at-half-the-rate recipe is in the [CLI reference](https://pqbench.github.io/pqbench/cli/).
- `credentials check` drops the tables you can't read outside compute — a view,
  a system catalog, Databricks default storage — and prints the reason;
  `credentials get` vends a short-lived lease onto each remaining ref.
- `get table` enriches each ref with its format and snapshot; the Delta log is
  read with `without_files()`, so the read is O(1) in files. `list partitions`
  groups a table's commits into commit-time windows, and `list files` names the
  files those commits added.
- `bytemass` reads each file's footer and prints the on-disk bytes per column,
  the table in [bytemass](#bytemass). In a pipe it streams
  `pqbench.bytemass-row` NDJSON instead; `pqbench viz` draws that stream as a
  treemap.

The same walk runs offline against the local Delta and Iceberg stand,
`make lakehouse`. [Getting started](https://pqbench.github.io/pqbench/getting-started/) takes one Parquet
file to a treemap, and the [CLI reference](https://pqbench.github.io/pqbench/cli/) documents every command.

## Showcases

[Showcases](https://pqbench.github.io/pqbench/showcases/) runs the commands as
jobs on real public data — triage which columns own the file, choose a codec,
verify a layout change, and read SQuAD and a TPC-DS table from the wild. On a
2.1 MB Reddit comments sample, three of seven columns are 99.7% of the bytes:

```console run
$ pqbench bytemass crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet --format table
column            type        codec         encodings                  bytes  values
----------------  ----------  ------------  ------------------------  ------  ------
text              BYTE_ARRAY  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  743566    3000
label             BYTE_ARRAY  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    2469    3000
dataType          BYTE_ARRAY  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     360    3000
communityName     BYTE_ARRAY  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    2468    3000
datetime          BYTE_ARRAY  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY      87    3000
username_encoded  BYTE_ARRAY  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  446197    3000
url_encoded       BYTE_ARRAY  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  907117    3000
files: 1
rows: 3000
columns: 7
```

`profile` explains it, `compression` picks the codec, and `experiment` proves a
layout change before you apply it. The [Showcases](https://pqbench.github.io/pqbench/showcases/)
page walks all four end to end.

## Commands

The core measurements. The walk's [`get` and `list`](#the-walk-end-to-end) come
first; the [CLI reference](https://pqbench.github.io/pqbench/cli/) covers `profile`, `experiment`, `diff`,
`skill`, `credentials`, `ratelimit`, and `setup`.

### lz

lzbench-style compression benchmark over raw file bytes. The default sweeps
every wired codec (zstd, lz4, gzip, snappy); `-c codec@level` (repeatable)
picks one config:

```console run
$ pqbench lz data/samples/squad.parquet --samples 1 --warmup-iterations 0 --format table
codec   level  compress MB/s  decompress MB/s  ratio
------  -----  -------------  ---------------  -----
gzip        1            ±44             ±226   0.85
zstd        1           ±915            ±1391   0.85
lz4         1          ±2176            ±5180   0.98
snappy      1          ±1608            ±1981   0.99
file: data/samples/squad.parquet
rows: 4
```

Speeds vary run to run; `--format json` (or `--json`) emits NDJSON.

### compression

The same sweep over a parquet file's encoded pages. The input must be
NONE-compressed; `--per-column` adds a report per column.

```console run
$ pqbench compression crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet --samples 1 --warmup-iterations 0 --format table
codec   level  compress MB/s  decompress MB/s  ratio
------  -----  -------------  ---------------  -----
zstd        1           ±458            ±1722   0.56
gzip        1            ±68             ±270   0.58
lz4         1           ±897            ±8476   0.81
snappy      1           ±912            ±2101   0.81
file: crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet
rows: 4
columns: 0
```

`--format json` (or `--json`) emits NDJSON.

### bytemass

Per-column byte masses — the on-disk bytes each column chunk takes, with its
value count.

```console run
$ pqbench bytemass data/samples/delta_byte_array.parquet --format table
column                 type        codec         encodings         bytes  values
---------------------  ----------  ------------  ----------------  -----  ------
c_customer_id          BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   8248    1000
c_salutation           BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   3362    1000
c_first_name           BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   6595    1000
c_last_name            BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   6955    1000
c_preferred_cust_flag  BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   1220    1000
c_birth_country        BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   9599    1000
c_login                BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY     42    1000
c_email_address        BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY  27763    1000
c_last_review_date     BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   3511    1000
files: 1
rows: 1000
columns: 9
```

- Reads only the footer, so it works on any file, compressed or not.
- Accepts paths, quoted globs, `s3://` URIs, and a `TableFile` /
  `remote-source` document; `--indexes` adds page counts.
- `s3://` needs the `aws` feature; a pipe streams one NDJSON row per column
  chunk (`--format json`, or `-o FILE`).

Filtering which files to measure is a shell job on the `list files` stream —
see [Visual demos](https://pqbench.github.io/pqbench/demo/).

### list partitions

List a table's natural partitions — its commits grouped into epoch-aligned
commit-time windows.

```console run delta
$ pqbench list partitions docker/e2e-lakehouse/table --format table
name                           first_time      last_time  commits
--------------------------  -------------  -------------  -------
catalogs/local/schemas/local/tables/table/partitions/1789862400000  1789862400000  1789948800000        1
partitions: 1
```

- Reads each table's Delta / Iceberg log, never the data files.
- `--every 1h|1d|1w` sets the window width (default `1d`).
- `--last 1h|1d|1w` bounds the read to the newest commits; only whole windows
  are emitted.
- Delta needs `--features delta`, Iceberg `iceberg`; add `-s3` to either for
  `s3://`.
- A pipe streams `Partition` NDJSON; feed it to `list files`.

### list files

List the files a partition's commits added — the input to `bytemass`.

```console run delta
$ pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass --format table
column  type        codec   encodings                 bytes  values
------  ----------  ------  ------------------------  -----  ------
id      INT64       SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   BYTE_ARRAY  SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
files: 1
rows: 3
columns: 2
```

- `list files` re-reads the window's commits and emits the files they
  **added** — a file a later commit removes is still named.
- The lease rides on every file, so a short-lived lease travels to `bytemass`.

A producer can hand `bytemass` a `remote-source` document — one table
URI plus `AWS_*` credentials:

```console no-run
$ cat <<'EOF' | pqbench bytemass --format table
{"version":1,
 "inputs":["s3://bucket/table/part-0.parquet"],
 "env":{"AWS_ACCESS_KEY_ID":"…","AWS_SECRET_ACCESS_KEY":"…","AWS_SESSION_TOKEN":"…","AWS_REGION":"us-east-1"}}
EOF
```

### viz

Collect a bytemass stream into a static HTML treemap — no server, no build
step.

```console run delta
$ pqbench bytemass data/samples/delta_byte_array.parquet | pqbench viz -o /tmp/report
$ pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass | pqbench viz -o /tmp/report
```

The page lands at `/tmp/report.html`. Open it in a browser.

## Documentation

Start with [Getting started](https://pqbench.github.io/pqbench/getting-started/). The
[documentation index](https://pqbench.github.io/pqbench/) maps every page by kind.
`pqbench --help` is the command guide; `pqbench <command> --help` is local to
that command.

- [Getting started](https://pqbench.github.io/pqbench/getting-started/) — install, first measurement, the metadata walk
- [Showcases](https://pqbench.github.io/pqbench/showcases/) — real jobs on real data: triage the bytes, pick a codec, verify a layout change
- [Distribution](https://pqbench.github.io/pqbench/distribution/) — install the binary, targets, and how releases are cut
- [CLI reference](https://pqbench.github.io/pqbench/cli/) — the command table, documents, flags, and auth
- [Visual demos](https://pqbench.github.io/pqbench/demo/) — Parquet, Delta, Iceberg, catalog walk, and Unix file selection
- [Delta tables](https://pqbench.github.io/pqbench/delta/) — log load, `list partitions | list files | bytemass`, limitations
- [Iceberg tables](https://pqbench.github.io/pqbench/iceberg/) — metadata load, `list partitions | list files | bytemass`, limitations
- [Visualization](https://pqbench.github.io/pqbench/viz/) — `pqbench viz`, a static HTML treemap
- [Python bindings](https://pqbench.github.io/pqbench/python/) — install the wheel: the commands, and the walk
- [Unity Catalog and Iceberg REST E2E](https://github.com/pqbench/pqbench/tree/main/docker/e2e-lakehouse) — catalogs
  naming tables for `pqbench list partitions`, over rustfs S3

## Python

[`python/`](https://pqbench.github.io/pqbench/python/) is a PyO3 wheel. The
commands are functions under `pqbench.native`, one per library path; `pqbench`
carries the client handles they take and `walk`, the whole-metastore walk:

```python
import pqbench
import pqbench.native

workspace = pqbench.Workspace(pqbench.Config(endpoint, token))

async for file in pqbench.walk(workspace):
    print(file["uri"])
```

## Contributing

PRs welcome. The gate is `make check` (`fmt-check` + `check-docs` +
`check-version` + `clippy -D warnings` + `isolation` + `lfs-check` + `test`).
The PyO3 wheel is `make check-python`. The runnable commands on this
page are generated into tests by `docscheck` (`make sync-docs`); see
[CONTRIBUTING.md](https://pqbench.github.io/pqbench/contributing/) for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](https://github.com/pqbench/pqbench/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/pqbench/pqbench/blob/main/LICENSE-APACHE).

<!-- --8<-- [end:landing] -->

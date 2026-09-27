# pqbench

[![CI](https://github.com/pqbench/pqbench/actions/workflows/ci.yml/badge.svg)](https://github.com/pqbench/pqbench/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

*lzbench for parquet.*

Measure how well each codec compresses a parquet file and how many on-disk bytes
each column costs.

One binary for parquet optimizations.

Commands compose on pipes: each writes a versioned JSON document the next reads.
The usual pipe is `lake` → `table` → `bytemass` → `viz`; `dump` copies the files.

## Quick start

Docker is the fastest way to try it — no Rust toolchain, no build:

```sh
docker pull pqbench/pqbench:latest
docker run --rm -v "$PWD:/data:ro" pqbench/pqbench:latest bytemass /data/your.parquet
```

The published image is a portable baseline build; see
[docs/docker.md](docs/docker.md) for how its numbers compare to a native build.

From a checkout, install the binary on your `PATH`:

```sh
cargo install --path crates/pqbench-cli
```

## Commands

### lz

lzbench-style compression benchmark over raw file bytes:

```console run
$ pqbench lz examples/quickstart.parquet -c zstd@3 --samples 1 --warmup-iterations 0 --format table
codec  level  compress MB/s  decompress MB/s  ratio
-----  -----  -------------  ---------------  -----
zstd       3          ±12.5            ±31.8   0.63
file: examples/quickstart.parquet
rows: 1
```

Omit `-c` to sweep every wired codec. Speeds vary run to run, so a `±`-prefixed
number is a placeholder the docs check tolerates. `--format json` (or `--json`)
emits the same report as composable NDJSON.

### compression

The same sweep over the encoded pages of a **NONE-compressed** parquet file. A
compressed file is rejected; `--per-column` adds one table per column.

```console run
$ pqbench compression examples/quickstart.parquet --samples 1 --warmup-iterations 0 --format table
codec   level  compress MB/s  decompress MB/s  ratio
------  -----  -------------  ---------------  -----
lz4         1          ±71.0            ±89.0   0.78
snappy      1          ±20.0            ±40.0   0.85
zstd        1          ±12.0            ±28.0   1.01
gzip        1           ±0.6             ±4.0   1.26
file: examples/quickstart.parquet
rows: 4
columns: 0
```

Rows are ordered by compression ratio, a property of the data, so the order is
reproducible; only the speeds vary. `--format json` (or `--json`) emits NDJSON.

### bytemass

Per-column byte masses — how many on-disk bytes each column takes per row.
Reads only the footer, so it works on any file regardless of compression.

```console run
$ pqbench bytemass examples/quickstart.parquet --format table
column  type   codec         encodings                 bytes  values
------  -----  ------------  ------------------------  -----  ------
id      INT64  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    102       8
year    INT32  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     68       8
files: 1
rows: 8
columns: 2
```

- Multiple paths, quoted glob masks, and storage URIs are aggregated.
- Remote reads fetch the object metadata, the Parquet trailer, and the
  serialized footer — never the data pages.
- `--indexes` also loads ColumnIndex/OffsetIndex page counts.

An `s3://` URI is the same command with the `aws` feature:
`pqbench bytemass s3://bucket/table/part-0.parquet`. A URI whose backend is not
compiled in fails at runtime with the missing feature named. The library entry
point (`bytemass::bytemass`) is always available and never feature-gated.
Selecting which files to measure is a shell job: filter the `table` stream with
`jq`, `sort`, and `head` before `bytemass` (see [docs/demo.md](docs/demo.md)).
A terminal prints the columns as a table; a pipe streams one NDJSON row per
column (`--format json` forces the stream and `-o` also writes it).

### table

Detect the table format and load one snapshot's metadata.

```console run delta
$ pqbench table docker/e2e-lakehouse/table --format table
path                                                                 size_bytes  num_records
-------------------------------------------------------------------  ----------  -----------
part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet         796            3
tables: 1
files: 1 (796 bytes)
$ pqbench table docker/e2e-lakehouse/table | pqbench bytemass --format table
column  type        codec   encodings                 bytes  values
------  ----------  ------  ------------------------  -----  ------
id      INT64       SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   BYTE_ARRAY  SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
files: 1
rows: 3
columns: 2
$ pqbench table docker/e2e-lakehouse/table -o /tmp/table.ndjson.zst
```

- Detection runs first: `_delta_log` is Delta (wins UniForm);
  `metadata/version-hint.text` or `.metadata.json` is Iceberg.
- `--version N` picks a Delta commit or Iceberg snapshot id (default: latest).
- Delta needs `--features delta` (`delta-s3` for `s3://`); Iceberg needs
  `iceberg` (`iceberg-s3` for `s3://`).
- Every line carries a table `id` so `bytemass` can attribute rows. One `table`
  process loads one table at a time — scan a catalog by running one process per
  table and let the shell fan out (`xargs -P`).
- A terminal prints the active files as a table; a pipe streams NDJSON
  (`--format json` also forces it).

A producer can hand `table` a `pqbench.remote-source` document — one table URI
plus optional `AWS_*` credentials — and the table document carries those
credentials to `bytemass`:

```console run delta
$ pqbench lake docs/demos/lake.json | pqbench table | pqbench bytemass --format table
column  type        codec   encodings                 bytes  values
------  ----------  ------  ------------------------  -----  ------
id      INT64       SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   BYTE_ARRAY  SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
files: 1
rows: 3
columns: 2
```

### lake

List tables as `pqbench.table-ref` lines, one per table for the shell to fan
out (`xargs -P`).

```console run delta
$ pqbench lake docker/e2e-lakehouse --include table --exclude 'iceberg/*' --format table
name   uri
-----  ----------------------------------------
table  file://<root>/docker/e2e-lakehouse/table
tables: 1
$ pqbench lake docker/e2e-lakehouse --include table | pqbench table | pqbench bytemass --format table
column  type        codec   encodings                 bytes  values
------  ----------  ------  ------------------------  -----  ------
id      INT64       SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   BYTE_ARRAY  SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
files: 1
rows: 3
columns: 2
```

- A directory, `file://` URI, or `s3://` prefix is walked until a table marker.
  Children of a table are not searched; `--max-depth` (default 8) bounds a tree
  with no marker.
- `--include` / `--exclude` match an FQN (`main`, `main.default`,
  `main.default.events`) as a glob or prefix, and prune the walk when the
  leading name is a literal.
- `s3://` listing needs `--features aws`. `file://` and a bare path name the
  same tables.

A `pqbench.lake-source` document, from a file or stdin, lists a catalog.
`GET /v1/config` chooses the protocol: a 200 with a `defaults` object is
Iceberg REST; a 200 without `defaults`, or HTTP 404, is Unity. A down catalog
is an error, not Unity. The same Unity routes serve
[Unity Catalog OSS](https://docs.unitycatalog.io/) and
[Databricks](https://docs.databricks.com/api/workspace/tables/list): catalogs,
then schemas, then tables, following `next_page_token`. Iceberg REST lists
namespaces and tables, then `loadTable` for each metadata location. `token` is
the Databricks bearer token. `env` holds `AWS_*` storage credentials and is
copied onto each table-ref.

The committed fixture tree also holds an Iceberg table under
`docker/e2e-lakehouse/iceberg/`; measuring it needs the stand
(`make lakehouse`). A catalog is listed from a `pqbench.lake-source` document
(`pqbench lake unity.json | pqbench table | pqbench bytemass`), and `s3://`
needs `--features aws`.

```json
{"kind": "pqbench.lake-source", "version": 1,
 "endpoint": "https://example.cloud.databricks.com",
 "token": "...",
 "catalog": "main",
 "env": {"AWS_REGION": "us-east-1"}}
```

### viz

Collect a bytemass stream into a static HTML page. The page embeds the
measured rows and loads the d3 modules it uses from a CDN, drawing one
treemap per table id. Open the HTML in a browser; no server is needed.

```console run delta
$ pqbench bytemass examples/quickstart.parquet | pqbench viz -o /tmp/report
$ pqbench table docker/e2e-lakehouse/table | pqbench bytemass | pqbench viz -o /tmp/report
```

The page lands at `/tmp/report.html`. Open it in a browser; no server is needed.

### dump

Copy the Parquet files a table names to a local directory — an `aws s3 cp`-style
fetch for a Delta or Iceberg table, local or remote:

```console run delta
$ pqbench table docker/e2e-lakehouse/table | pqbench dump /tmp/sample
dump: 1 file(s), 796 bytes -> /tmp/sample
```

`pqbench dump /tmp/sample s3://bucket/table` fetches from S3 (needs the `aws`
feature), and `pqbench lake docker/e2e-lakehouse --include table | pqbench
table | pqbench dump /tmp/mirror` mirrors a whole lake.

Each file lands at its table-relative path, so partition directories are
preserved. Which files to keep is a shell decision on the `table` stream
(`jq`, `sort`, `head`); a lake nests each table under its name. A path that
would escape the output directory is refused, and `s3://` needs the `aws`
feature.

## Documentation

Start with [Getting started](docs/getting-started.md). The
[documentation index](docs/README.md) maps every page by kind.
`pqbench --help` is the command guide; `pqbench <command> --help` is local to
that command.

- [Getting started](docs/getting-started.md) — install, first measurement, the lake pipeline
- [CLI reference](docs/cli.md) — the command table, documents, flags, and auth
- [Visual demos](docs/demo.md) — Parquet, Delta, Iceberg, lake walk, catalogs
- [Delta tables](docs/delta.md) — log load, `table | bytemass`, limitations
- [Iceberg tables](docs/iceberg.md) — metadata load, `table | bytemass`, limitations
- [Visualization](docs/viz.md) — `pqbench viz`, a static HTML treemap
- [Python bindings](python/README.md) — install the wheel and call every command
- [Docker](docs/docker.md) — build, run, and publish a container image
- [Unity Catalog and Iceberg REST E2E](docker/e2e-lakehouse/README.md) — catalogs
  naming tables for `pqbench table`, over rustfs S3

## Python

[`python/`](python/README.md) is a PyO3 wheel. Each CLI command is a function
that calls the library in-process:

```python
import pqbench

rows = pqbench.bytemass("data.parquet")
pqbench.viz(rows, output="report")
```

## Contributing

PRs welcome. The gate is `make check` (`fmt-check` + `clippy -D warnings` +
`test`). The PyO3 wheel is `make check-python`. The runnable commands on this
page are generated into tests by `docscheck` (`make sync-docs`); see
[CONTRIBUTING.md](CONTRIBUTING.md) for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

# pqbench

*lzbench for parquet.*

Measure how well each codec compresses a parquet file and how many on-disk bytes
each column costs.

## Quick start

Docker is the fastest way to try it — no Rust toolchain, no build:

```sh
docker pull pqbench/pqbench:latest
docker run --rm -v "$PWD:/data:ro" pqbench/pqbench:latest bytemass /data/your.parquet
```

The published image is a portable baseline build; see
[docs/docker.md](docs/docker.md) for how its numbers compare to a native build.

## Commands

### lz

lzbench-style compression benchmark over raw file bytes:

```sh
pqbench lz file.bin -c zstd@3 --samples 10
```

`--json` emits the same report as composable JSON.

### compression

The same codec sweep over the encoded pages of a **NONE-compressed** parquet
file:

```sh
pqbench compression data.parquet --per-column
```

`--json` emits the same report as composable JSON (the per-column rows are
included when `--per-column` is set).

### bytemass

Per-column byte masses — how many on-disk bytes each column takes per row.
Reads only the footer metadata, so it works on any file regardless of
compression. Multiple paths, quoted glob masks, and storage URIs are
aggregated:

```sh
pqbench bytemass data.parquet
pqbench bytemass 'data/part-*.parquet'
pqbench bytemass s3://bucket/table/part-0.parquet   # requires --features aws
```

Remote reads fetch the object metadata, the Parquet trailer, and the
serialized footer — never the data pages. `s3://` support is the `aws`
feature; a URI whose backend is not compiled in fails at runtime with the
missing feature named. The library entry point (`bytemass::bytemass`) is
always available and never feature-gated. A pipe streams one NDJSON row per
column as each file is measured; a terminal prints a short summary and
requires `-o` (zstd NDJSON). `--include` / `--exclude` are Unix globs on the
file path (`year=2024/**`); `--sample all`, `every:N`, or `first:N` runs after
that. Lake `--include` / `--exclude` already select table names.

### viz

Collect a bytemass stream into SQLite and a static HTML page. The page loads
sql.js and d3 from a CDN, queries the embedded database, and draws one
treemap per table id. Open the HTML in a browser; query the SQLite file with
any SQLite client.

```sh
pqbench bytemass data.parquet | pqbench viz -o report
pqbench table ./delta-table | pqbench bytemass | pqbench viz -o report
xdg-open report.html
```

### dump

Write a sample from the same files `bytemass` would measure. Output is
Parquet (`--output` or a redirect); newly written columns default to zstd.
`--sample` picks files (`all`, `every:N`, `first:N`); `--row-groups
first:N` reads only the leading row groups of each file so a remote object
need not be fetched whole.

```sh
pqbench dump data.parquet --output sample.parquet
pqbench table ./delta-table | pqbench dump --row-groups first:1 --output sample.parquet
pqbench table ./delta-table | pqbench dump --include 'year=2024/**' --sample first:1 -o sample.parquet
```

### table

Detect the table format and load its metadata. For Delta this is the
transaction log and the active files. For Iceberg it is the metadata JSON and
Avro manifests. A pipe writes NDJSON. Every line carries a table `id`, so
`lake` can stream table-refs, `table` can load several at once, and
`bytemass` can measure a file as soon as that line arrives — even if tables
finish out of order. A terminal prints a short summary and requires `-o` (zstd
NDJSON):

```sh
pqbench table ./path/to/table -o table.ndjson.zst
pqbench table ./path/to/table | pqbench bytemass
pqbench table ./path/to/table --concurrency 8 | pqbench bytemass --concurrency 8
pqbench bytemass table.ndjson.zst
```

Format detection runs first (`_delta_log` is Delta; `metadata/version-hint.text`
or `.metadata.json` is Iceberg). Delta needs `--features delta` (`delta-s3` for
`s3://`); Iceberg needs `iceberg` (`iceberg-s3` for `s3://`).

A producer can hand `table` a `pqbench.remote-source` document — one table URI
plus optional `AWS_*` credentials — and the table document carries those
credentials to `bytemass`:

```sh
producer | pqbench table | pqbench bytemass
```

### lake

List tables as `pqbench.table-ref` lines. A directory, `file://` URI, or
`s3://` prefix is walked until a table marker that `pqbench table` also
accepts: `_delta_log` is Delta; Iceberg is `metadata/version-hint.text` or
`metadata/*.metadata.json` (one path component). A remote walk lists each
prefix once and reads format off the listing (`_delta_log/` or Iceberg
metadata objects) instead of probing every child with HEADs. Children of a
table are not searched. UniForm stays Delta. `file://` and a bare path name
the same tables. `--max-depth` (default 8) bounds a tree with no marker.
`--concurrency` lists sibling prefixes, schemas, or namespaces in parallel.
`s3://` listing needs `--features aws`. A `pqbench.lake-source` document,
from a file or stdin, lists a catalog. `GET /v1/config` chooses the protocol:
a 200 with a `defaults` object is Iceberg REST; a 200 without `defaults`, or
HTTP 404, is Unity. A down catalog is an error, not Unity. The same Unity
routes serve [Unity Catalog OSS](https://docs.unitycatalog.io/) and
[Databricks](https://docs.databricks.com/api/workspace/tables/list).
`--include` / `--exclude` match an FQN as a glob or a prefix, and prune the
walk when the leading name is a literal. `token` is the Databricks bearer
token. `env` holds `AWS_*` storage credentials and is copied onto each
table-ref. `info` stays unset until `pqbench table` loads.

```sh
pqbench lake ./warehouse --include 'sales/*' --exclude 'sales/tmp*'
pqbench lake s3://bucket/warehouse --max-depth 2 --concurrency 8 | pqbench table | pqbench bytemass
pqbench lake unity.json --include main --concurrency 8 | pqbench table | pqbench bytemass
```

```json
{"kind": "pqbench.lake-source", "version": 1,
 "endpoint": "https://example.cloud.databricks.com",
 "token": "...",
 "catalog": "main",
 "env": {"AWS_REGION": "us-east-1"}}
```

## Documentation

- [Visual demos](docs/demo.md) — Parquet, Delta, Iceberg, lake walk, catalogs
- [Unity Catalog and Iceberg REST E2E](docker/e2e-lakehouse/README.md) — catalogs
  naming tables for `pqbench table`, over rustfs S3
- [Delta tables](docs/delta.md) — log load, `table | bytemass`, limitations
- [Iceberg tables](docs/iceberg.md) — metadata load, `table | bytemass`, limitations
- [Visualization](docs/viz.md) — `pqbench viz`, SQLite + static HTML
- [Python bindings](python/README.md) — install the wheel and call every command
- [Docker](docs/docker.md) — build, run, and publish a container image

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
`test`). The PyO3 wheel is `make check-python`. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

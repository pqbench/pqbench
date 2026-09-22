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

`--json` emits a flat per-column JSON table (file/row counts plus one record
per column); `--d3` emits a self-contained HTML treemap:

```sh
pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
```

Remote reads fetch the object metadata, the Parquet trailer, and the
serialized footer — never the data pages. `s3://` support is the `aws`
feature; a URI whose backend is not compiled in fails at runtime with the
missing feature named. The library entry point (`bytemass::bytemass`) is
always available and never feature-gated.

### table

Detect the table format and load its metadata. For Delta this is the
transaction log and the active files. A terminal pretty-prints the log; a pipe
writes a `pqbench.table` document that `bytemass` measures:

```sh
pqbench table ./path/to/table
pqbench table ./path/to/table | pqbench bytemass
pqbench table ./path/to/table | pqbench bytemass --d3 > treemap.html
```

Format detection runs first (`_delta_log` is Delta; `metadata/version-hint.text`
is Iceberg). Iceberg is recognized and rejected until a loader exists. Delta
needs `--features delta` (`delta-s3` for `s3://`).

A producer can hand `table` a `pqbench.remote-source` document — one table URI
plus optional `AWS_*` credentials — and the table document carries those
credentials to `bytemass`:

```sh
producer | pqbench table | pqbench bytemass
```

### lake

List Delta tables and write a `pqbench.lake` document that `pqbench table` can
load. A directory that contains `_delta_log` is one table. A
`pqbench.lake-source` document, from a file or stdin, lists a Unity Catalog.
The same routes serve [Unity Catalog OSS](https://docs.unitycatalog.io/) and
[Databricks](https://docs.databricks.com/api/workspace/tables/list): catalogs,
then schemas, then tables, following `next_page_token`. `token` is the
Databricks bearer token. `env` holds `AWS_*` storage credentials and is copied
onto each table for the next command.

```sh
pqbench lake ./warehouse | pqbench table | pqbench bytemass
pqbench lake unity.json | pqbench table | pqbench bytemass
```

```json
{"kind": "pqbench.lake-source", "version": 1,
 "endpoint": "https://example.cloud.databricks.com",
 "token": "...",
 "env": {"AWS_REGION": "us-east-1"}}
```

## Documentation

- [Unity Catalog E2E example](docker/e2e-lakehouse/README.md) — a catalog vending
  expiring credentials into `pqbench table`, over rustfs S3
- [Delta tables](docs/delta.md) — log load, `table | bytemass`, limitations
- [Docker](docs/docker.md) — build, run, and publish a container image

## Contributing

PRs welcome. The gate is `make check` (`fmt-check` + `clippy -D warnings` +
`test`) and every change must pass it. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

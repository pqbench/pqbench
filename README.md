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

```console run
$ pqbench lz examples/quickstart.parquet -c zstd@3 --samples 1 --warmup-iterations 0 | jq -c 'del(.compress_estimate,.decompress_estimate)'
{"kind":"pqbench.lz","version":1,"event":"begin","file":"examples/quickstart.parquet"}
{"kind":"pqbench.lz-row","codec":"zstd","level":3,"compressed_bytes":343,"uncompressed_bytes":541,"ratio":0.634011090573013}
{"kind":"pqbench.lz","event":"end","row_count":1}
```

`--json` emits the same report as composable JSON.

### compression

The same codec sweep over the encoded pages of a **NONE-compressed** parquet
file:

```console run
$ pqbench compression examples/quickstart.parquet --per-column --samples 1 --warmup-iterations 0 \
>   | jq -s -c '.[0], ([.[] | select(.kind=="pqbench.compression-row")] | sort_by(.compressed_bytes)[] | del(.compress_estimate,.decompress_estimate))' \
>   | head -3
{"kind":"pqbench.compression","version":1,"event":"begin","file":"examples/quickstart.parquet"}
{"kind":"pqbench.compression-row","codec":"lz4","level":1,"compressed_bytes":83,"uncompressed_bytes":106,"ratio":0.7830188679245284}
{"kind":"pqbench.compression-row","codec":"snappy","level":1,"compressed_bytes":90,"uncompressed_bytes":106,"ratio":0.8490566037735849}
```

A sweep ranks codecs by measured speed, which varies run to run, so the
transcript sorts the rows by compressed size to stay reproducible.

`--json` emits the same report as composable JSON (the per-column rows are
included when `--per-column` is set).

### bytemass

Per-column byte masses — how many on-disk bytes each column takes per row.
Reads only the footer metadata, so it works on any file regardless of
compression. Multiple paths, quoted glob masks, and storage URIs are
aggregated:

```console run
$ pqbench bytemass examples/quickstart.parquet --json | head -2
{"kind":"pqbench.bytemass","version":1,"event":"begin"}
{"kind":"pqbench.bytemass-file","id":"examples/quickstart.parquet","path":"examples/quickstart.parquet","file":"examples/quickstart.parquet","size":541}
$ pqbench bytemass crates/pqbench/tests/fixtures/small_*.parquet | head -2
{"kind":"pqbench.bytemass","version":1,"event":"begin"}
{"kind":"pqbench.bytemass-file","id":"crates/pqbench/tests/fixtures/small_reddit_none.parquet","path":"crates/pqbench/tests/fixtures/small_reddit_none.parquet","file":"crates/pqbench/tests/fixtures/small_reddit_none.parquet","size":2107406}
```

An `s3://` URI is the same command with the `aws` feature:
`pqbench bytemass s3://bucket/table/part-0.parquet`.

Remote reads fetch the object metadata, the Parquet trailer, and the
serialized footer — never the data pages. `s3://` support is the `aws`
feature; a URI whose backend is not compiled in fails at runtime with the
missing feature named. The library entry point (`bytemass::bytemass`) is
always available and never feature-gated. A pipe streams one NDJSON row per
column as each file is measured; a terminal prints a short summary and
requires `-o` (zstd NDJSON). Selecting which files to measure is a shell job:
filter the `table` stream with `jq`, `sort`, and `head` before `bytemass` (see
[docs/demo.md](docs/demo.md)).

### table

Detect the table format and load its metadata. For Delta this is the
transaction log and the active files. For Iceberg it is the metadata JSON and
Avro manifests. A pipe writes NDJSON. Every line carries a table `id` so
`bytemass` can attribute rows. One `table` process loads one table at a time
— a table is the work unit, so scan a catalog by running one process per
table and let the shell fan out (`xargs -P`). A terminal prints a short
summary and requires `-o` (zstd
NDJSON):

```console run delta
$ pqbench table docker/e2e-lakehouse/table -o /tmp/table.ndjson.zst
$ pqbench table docker/e2e-lakehouse/table | pqbench bytemass | head -2
{"kind":"pqbench.bytemass","version":1,"event":"begin"}
{"kind":"pqbench.bytemass-file","id":"docker/e2e-lakehouse/table","path":"part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet","file":"<root>/docker/e2e-lakehouse/table/part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet","size":796,"stats":{"num_records":3,"bytes_per_row":265.3333333333333,"min_values":{"id":1,"label":"lake"},"max_values":{"id":3,"label":"remote"},"null_count":{"id":0,"label":0}}}
$ pqbench bytemass /tmp/table.ndjson.zst | head -2
{"kind":"pqbench.bytemass","version":1,"event":"begin"}
{"kind":"pqbench.bytemass-file","id":"docker/e2e-lakehouse/table","path":"part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet","file":"<root>/docker/e2e-lakehouse/table/part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet","size":796,"stats":{"num_records":3,"bytes_per_row":265.3333333333333,"min_values":{"id":1,"label":"lake"},"max_values":{"id":3,"label":"remote"},"null_count":{"id":0,"label":0}}}
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

List tables as `pqbench.table-ref` lines, one line per table for the shell to
fan out (`xargs -P`). A directory, `file://` URI, or `s3://` prefix is walked
until a table marker that `pqbench table` also accepts: `_delta_log` is Delta;
Iceberg is `metadata/version-hint.text` or `metadata/*.metadata.json` (one path
component). A remote walk lists each prefix once and reads format off the
listing (`_delta_log/` or Iceberg metadata objects) instead of probing every
child with HEADs. Children of a table are not searched. UniForm stays Delta.
`file://` and a bare path name the same tables. `--max-depth` (default 8)
bounds a tree with no marker. `s3://` listing needs `--features aws`. A
`pqbench.lake-source` document, from a file or stdin, lists a catalog.
`GET /v1/config` chooses the protocol: a 200 with a `defaults` object is
Iceberg REST; a 200 without `defaults`, or HTTP 404, is Unity. A down catalog
is an error, not Unity. The same Unity routes serve
[Unity Catalog OSS](https://docs.unitycatalog.io/) and
[Databricks](https://docs.databricks.com/api/workspace/tables/list): catalogs,
then schemas, then tables, following `next_page_token`. Iceberg REST lists
namespaces and tables, then `loadTable` for each metadata location.
`--include` / `--exclude` match an FQN (`main`, `main.default`,
`main.default.events`) as a glob or a prefix, and prune the walk when the
leading name is a literal. `token` is the Databricks bearer token. `env`
holds `AWS_*` storage credentials and is copied onto each table-ref.

```console run delta
$ pqbench lake docker/e2e-lakehouse --include table --exclude 'iceberg/*' | head -2
{"kind":"pqbench.lake","version":1,"event":"begin"}
{"kind":"pqbench.table-ref","version":1,"id":"table","uri":"file://<root>/docker/e2e-lakehouse/table"}
$ pqbench lake docker/e2e-lakehouse --include table | pqbench table | pqbench bytemass | head -2
{"kind":"pqbench.bytemass","version":1,"event":"begin"}
{"kind":"pqbench.bytemass-file","id":"table","path":"part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet","file":"<root>/docker/e2e-lakehouse/table/part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet","size":796,"stats":{"num_records":3,"bytes_per_row":265.3333333333333,"min_values":{"id":1,"label":"lake"},"max_values":{"id":3,"label":"remote"},"null_count":{"id":0,"label":0}}}
```

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

- [Visual demos](docs/demo.md) — Parquet, Delta, Iceberg, lake walk, catalogs
- [Unity Catalog and Iceberg REST E2E](docker/e2e-lakehouse/README.md) — catalogs
  naming tables for `pqbench table`, over rustfs S3
- [Delta tables](docs/delta.md) — log load, `table | bytemass`, limitations
- [Iceberg tables](docs/iceberg.md) — metadata load, `table | bytemass`, limitations
- [CLI guide](docs/cli.md) — `pqbench --help` copy: auth, documents, flags
- [Visualization](docs/viz.md) — `pqbench viz`, a static HTML treemap
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
`test`). The PyO3 wheel is `make check-python`. The runnable commands on this
page are generated into tests by `docscheck` (`make sync-docs`); see
[CONTRIBUTING.md](CONTRIBUTING.md) for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

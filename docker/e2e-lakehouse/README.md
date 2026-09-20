# Local lakehouse → pqbench

Three catalog examples share one S3-compatible object store, [rustfs](https://docs.rustfs.com/en/installation/container/docker/).
The services run without Spark, Postgres or a UI; Python/DuckDB runs only when
seeding or resolving files. Unity Catalog's official image is still a large
download (about 2.4 GB).

| Catalog | Example | Data |
| --- | --- | --- |
| Unity Catalog OSS | `pqbench.demo.events`, external Parquet | `s3://lakehouse/unity/events/` |
| Iceberg REST | `demo.events` | `s3://lakehouse/iceberg/` |
| DuckLake + SQLite | `lake.events` | `s3://lakehouse/ducklake/` |

Each has three rows and two columns (`id`, `label`). These are real Parquet
objects accessed over HTTP, including pqbench's S3 range reads of footers.

## Run everything

Requires Docker Compose v2, a Rust toolchain for this branch, Bash, and Python 3
for smoke-test assertions. From the repository root:

```bash
bash docker/e2e-lakehouse/smoke.sh
```

This builds pqbench with `aws`, builds the example client, starts the services,
seeds each table, and checks all three JSON pipelines. Services remain running.
Seeding is repeatable: it overwrites only these demo tables/data. DuckLake's
inline storage is disabled so even this tiny example writes Parquet to S3.

## Ready-to-use pipes

Run these in Bash from the repository root:

```bash
set -o pipefail
compose() { docker compose -f docker/e2e-lakehouse/compose.yaml "$@"; }

# If you haven't run the smoke script:
cargo build -p pqbench-cli --features aws
compose build examples
compose up -d --wait
compose run --rm -T examples seed

# Unity Catalog → remote source document → byte-mass JSON
compose run --rm -T examples unity |
  target/debug/pqbench bytemass --source - --json

# Iceberg's current snapshot → physical file byte masses
compose run --rm -T examples iceberg |
  target/debug/pqbench bytemass --source -

# DuckLake's current file list → HTML treemap
compose run --rm -T examples ducklake |
  target/debug/pqbench bytemass --source - --d3 > ducklake-treemap.html

# Inspect the protocol without running pqbench:
compose run --rm -T examples iceberg | python3 -m json.tool
```

`-T` prevents terminal formatting from contaminating stdout. Producer errors
go to stderr. The JSON uses `kind: pqbench.remote-source`, `version: 1`,
`inputs`, and `object_store_options`; pqbench itself has no catalog dependency.
The stock published pqbench image is not assumed to contain this branch or its
AWS feature, hence the explicit local build.

## Endpoints and state

Host endpoints bind to loopback: S3 `http://localhost:9000`, Unity Catalog
`http://localhost:8080`, Iceberg REST `http://localhost:8181`. Override with
`RUSTFS_PORT`, `UNITY_CATALOG_PORT`, and `ICEBERG_REST_PORT`.
The producer automatically uses `RUSTFS_PORT` in the JSON for host pqbench.
Container clients use `http://rustfs:9000`; when piping into a container on
this Compose network, pass `-e SOURCE_S3_ENDPOINT=http://rustfs:9000` to the
producer's `compose run`. Credentials are local dummy values `test` / `test`,
region `us-east-1`, HTTP and path-style S3.

The object store is pinned to `rustfs/rustfs:1.0.0`, so a rerun next month
measures the same bytes; bump it deliberately. Compose polls the image's
`GET /health` with its bundled `curl` and starts the catalogs only once that
answers, which keeps `compose up -d --wait` reliable.

Any S3 client reaches the objects — the AWS CLI needs only the endpoint:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:9000 s3 ls s3://lakehouse/ --recursive
curl http://localhost:8181/v1/config
curl http://localhost:8080/api/2.1/unity-catalog/tables/pqbench.demo.events
compose down           # stop; retain named volumes
# Explicit reset of this stand's data only:
compose down -v
```

The rustfs objects, Unity Catalog's H2 metadata, and DuckLake's SQLite metadata
use named volumes. The Iceberg fixture catalog is disposable; rerun `seed`
after recreating it. SQLite is local to Docker; this is not a remote SQL server.

## Scope

Unity Catalog resolves an external Parquet table location; the example lists
Parquet files under that location and supplies local test credentials directly.
It does **not** test credential vending, IAM enforcement, or Databricks managed
tables. Iceberg uses `scan().plan_files()` and DuckLake uses
`ducklake_list_files`, rather than recursively measuring obsolete objects.
Those examples reject delete files; pqbench measures physical Parquet storage,
not logical rows after deletions. rustfs speaks the S3 API but is not AWS: it
reproduces neither WAN latency nor IAM authorization. These checks are opt-in
and separate from the fast Rust test suite.

References: [rustfs in Docker](https://docs.rustfs.com/en/installation/container/docker/),
[Unity Catalog Compose](https://docs.unitycatalog.io/docker_compose/),
[Iceberg REST fixture](https://iceberg.apache.org/spark-quickstart/),
[DuckLake file listing](https://ducklake.select/docs/stable/duckdb/metadata/list_files).

# Local lakehouse → pqbench

Three catalog examples share one S3-compatible object store, [rustfs](https://docs.rustfs.com/en/installation/container/docker/).
The services run without Spark, Postgres or a UI; Python/DuckDB runs only when
seeding or resolving files. Unity Catalog's official image is still a large
download (about 2.4 GB).

| Catalog | Example | Data |
| --- | --- | --- |
| Unity Catalog OSS (vends credentials) | `pqbench.demo.events`, external Parquet | `s3://lakehouse/unity/events/` |
| Iceberg REST | `demo.events` | `s3://lakehouse/iceberg/` |
| DuckLake + SQLite | `lake.events` | `s3://lakehouse/ducklake/` |

Each has three rows and two columns (`id`, `label`). These are real Parquet
objects accessed over HTTP, including pqbench's S3 range reads of footers.

## Run everything

Requires Docker Compose v2, a Rust toolchain for this branch, Bash, and `jq`.
From the repository root:

```bash
bash docker/e2e-lakehouse/smoke.sh
```

This builds pqbench with `aws`, builds the example client, starts the services,
seeds each table, and checks all three JSON pipelines. Services remain running.
Seeding is repeatable: it overwrites only these demo tables/data. DuckLake's
inline storage is disabled so even this tiny example writes Parquet to S3.

If a host port is already taken, override it (`UNITY_CATALOG_PORT=18080 bash
docker/e2e-lakehouse/smoke.sh`); see [Endpoints and state](#endpoints-and-state).

## Ready-to-use pipes

Run these in Bash from the repository root:

```bash
set -o pipefail
compose() { docker compose -f docker/e2e-lakehouse/compose.yaml "$@"; }

# If you haven't run the smoke script (storage first, so that the credential
# Unity vends exists before Unity starts):
cargo build -p pqbench-cli --features aws
compose build examples
compose up -d --wait rustfs
eval "$(compose run --rm -T examples credentials)"
export VENDED_ACCESS_KEY_ID VENDED_SECRET_ACCESS_KEY VENDED_SESSION_TOKEN
compose up -d --wait
compose run --rm -T examples seed

# For the pipes that don't carry credentials, pqbench reads this stand's S3 the
# same way it reads any other:
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
export AWS_ENDPOINT=http://localhost:9000
export AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false

# Unity Catalog → vended credentials in the document → byte-mass JSON
# (no AWS_* needed here: the credentials travel with the objects)
compose run --rm -T examples unity |
  target/debug/pqbench bytemass --source - --json

# Iceberg's current snapshot → physical file byte masses
compose run --rm -T examples iceberg |
  target/debug/pqbench bytemass --source -

# DuckLake's current file list → HTML treemap
compose run --rm -T examples ducklake |
  target/debug/pqbench bytemass --source - --d3 > ducklake-treemap.html

# Inspect the protocol without running pqbench:
compose run --rm -T examples iceberg | jq .
```

`-T` prevents terminal formatting from contaminating stdout. Producer errors
go to stderr. The JSON is `{"kind": "pqbench.remote-source", "version": 1,
"inputs": [...]}`, so the producer answers *which objects* and pqbench itself has
no catalog dependency. Storage configuration normally comes from the `AWS_*`
environment; a producer whose catalog vends expiring credentials can instead put
them in an optional `"env"` object, which pqbench applies to its own environment
before reading. Only `AWS_*` names are accepted there, and anything else is a
loud error. The stock published pqbench image is not assumed to contain this
branch or its AWS feature, hence the explicit local build.

## Credential vending in one line

Unity vends read credentials for the table, and the objects are measured with
those credentials and nothing else — no keys in your shell, no long-lived key
anywhere in the pipe:

```bash
UC=http://localhost:8080/api/2.1/unity-catalog
curl -s -X POST $UC/temporary-table-credentials -H 'Content-Type: application/json' -d "$(curl -s $UC/tables/pqbench.demo.events | jq -c '{table_id, operation: "READ"}')" | jq -c '{kind: "pqbench.remote-source", version: 1, inputs: ["s3://lakehouse/unity/events/part-0.parquet"], env: (.aws_temp_credentials | {AWS_ACCESS_KEY_ID: .access_key_id, AWS_SECRET_ACCESS_KEY: .secret_access_key, AWS_SESSION_TOKEN: .session_token, AWS_REGION: "us-east-1", AWS_ENDPOINT: "http://localhost:9000", AWS_ALLOW_HTTP: "true", AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"})}' | target/debug/pqbench bytemass --source -
```

`compose run --rm -T examples unity` is the same flow with the file list resolved
from the catalog instead of hard-coded, which is what the smoke script checks.

One caveat is worth knowing before you copy this shape onto real infrastructure.
Unity Catalog OSS mints vended credentials by calling AWS STS `AssumeRole` and
has no way to send that call to an S3-compatible endpoint
([unitycatalog#43](https://github.com/unitycatalog/unitycatalog/issues/43)), so
against rustfs it would talk to real AWS and fail. The stand therefore mints a
12-hour session credential from rustfs's own STS and configures Unity with it
(`s3.accessKey.0`/`s3.secretKey.0`/`s3.sessionToken.0`), which is Unity's preset
credential mode: it vends that credential verbatim. The credential is genuinely
temporary and issued by the object store, but it is not scoped per table, and the
request's `operation` does not narrow it. Against real AWS, Unity would assume a
role and return a policy-scoped session instead.

## Endpoints and state

Host endpoints bind to loopback: S3 `http://localhost:9000`, Unity Catalog
`http://localhost:8080`, Iceberg REST `http://localhost:8181`. Override with
`RUSTFS_PORT`, `UNITY_CATALOG_PORT`, and `ICEBERG_REST_PORT`. Containers on the
Compose network reach the same objects at `http://rustfs:9000`, so a pqbench
running there wants that as its `AWS_ENDPOINT` instead of `localhost`.
Credentials are local dummy values `test` / `test`, region `us-east-1`, HTTP and
path-style S3 — hence `AWS_ALLOW_HTTP` and
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`.

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

Unity Catalog resolves an external Parquet table location and vends a credential
for it; the example lists the Parquet files under that location. Vending here is
Unity's preset-credential mode (see [above](#credential-vending-in-one-line)), so
it does **not** test per-table scoping, IAM enforcement, or Databricks managed
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

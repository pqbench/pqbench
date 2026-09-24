# Unity Catalog → pqbench

A catalog that vends expiring credentials, an S3-compatible object store, and a
Delta table measured through the pipe between them. Two services, no Spark, no
Postgres, no UI: [Unity Catalog OSS](https://docs.unitycatalog.io/docker_compose/)
and [rustfs](https://docs.rustfs.com/en/installation/container/docker/). Unity's
official image is a large download (about 2.4 GB). Iceberg REST and DuckLake
examples will land here separately.

The table is `pqbench.demo.events` — external Delta at `s3://lakehouse/unity/events`,
three rows, two columns (`id`, `label`). It is committed as data in
[`table/`](table), so a rerun next month measures the same bytes.

## Run it

Needs Docker Compose v2, a Rust toolchain for this branch (the stand builds
`pqbench` with `--features delta-s3`), Bash, `curl`, and `jq`. From the
repository root:

```bash
make lakehouse
```

That reaches a stand you can query, in three steps you can also run alone:

| Target | What it does |
| --- | --- |
| `make lakehouse-up` | starts rustfs, mints the session credential Unity will vend, waits for the catalog API to answer |
| `make lakehouse-seed-s3` | uploads [`table/`](table) to `s3://lakehouse/unity/events` |
| `make lakehouse-seed-unity` | registers `pqbench.demo.events` as an external Delta table |
| `make lakehouse` | all three, then the table and lake checks below |

Every step is idempotent, and the minted credential is cached in
`local/lakehouse/vended.env` for its 12-hour life, so a rerun neither mints a new
session nor restarts Unity. Services stay up afterwards. If a host port is taken,
override it: `UNITY_CATALOG_PORT=18080 make lakehouse`.

## Curl checks

```bash
UC=http://localhost:8080/api/2.1/unity-catalog
S3=http://localhost:9000
BIN=${CARGO_TARGET_DIR:-target}/debug/pqbench

# What the catalog holds:
curl -s $UC/tables/pqbench.demo.events |
  jq '{table_id, data_source_format, storage_location}'
```

```json
{
  "table_id": "2fe1f2b8-465c-41a4-9850-729d1af3b6e9",
  "data_source_format": "DELTA",
  "storage_location": "s3://lakehouse/unity/events"
}
```

Now the point of the stand: Unity vends read credentials for that table, and the
bytes are measured with those and nothing else — no key in your shell, no
long-lived key in the pipe.

```bash
TABLE=$(curl -s $UC/tables/pqbench.demo.events)

curl -s -X POST $UC/temporary-table-credentials -H 'Content-Type: application/json' \
    -d "$(jq -c '{table_id, operation: "READ"}' <<< "$TABLE")" |
  jq -c --arg table "$(jq -r .storage_location <<< "$TABLE")" --arg s3 "$S3" \
    '{kind: "pqbench.remote-source", version: 1, inputs: [$table],
      env: (.aws_temp_credentials | {AWS_ACCESS_KEY_ID: .access_key_id,
        AWS_SECRET_ACCESS_KEY: .secret_access_key,
        AWS_SESSION_TOKEN: .session_token, AWS_REGION: "us-east-1",
        AWS_ENDPOINT: $s3, AWS_ENDPOINT_URL: $s3, AWS_ALLOW_HTTP: "true",
        AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"})}' |
  "$BIN" table |
  "$BIN" bytemass
```

```text
bytemass: part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet
column                              bytes/row
label                                   24.00
id                                      22.00
--------------------------------------------
total                                   46.00
```

`make lakehouse` runs exactly this pipe as its last step, then lists the same
table with `pqbench lake` (a `pqbench.lake-source` document for the endpoint)
and pipes it through `table | bytemass` again, so the catalog-listing path is
seen to work too. Pass `--d3` to `bytemass` to get a treemap, or stop after
`pqbench table` and pipe to `jq .` to read the log document itself.

The document is `{"kind": "pqbench.remote-source", "version": 1, "inputs": [...],
"env": {...}}`. The producer answers *which table*, `pqbench table` detects
the format and loads the log, and `pqbench bytemass` measures the files named
in that table document. pqbench keeps no catalog dependency. Storage
configuration normally comes from the `AWS_*` environment; a producer whose
catalog vends expiring credentials puts them in `env` instead, which travels
on the table document to `bytemass`. Only `AWS_*` names are accepted there,
and anything else is a loud error. Both endpoint names appear because
pqbench's object store reads `AWS_ENDPOINT` while delta-rs reads
`AWS_ENDPOINT_URL`; against real AWS neither is needed. `inputs` is one table
URI.

One caveat before copying this shape onto real infrastructure. Unity Catalog OSS
mints vended credentials by calling AWS STS `AssumeRole` and cannot send that
call to an S3-compatible endpoint
([unitycatalog#43](https://github.com/unitycatalog/unitycatalog/issues/43)), so
against rustfs it would talk to real AWS and fail. The stand therefore mints a
12-hour session credential from rustfs's own STS and configures Unity with it
(`s3.accessKey.0`/`s3.secretKey.0`/`s3.sessionToken.0`), Unity's preset
credential mode: it vends that credential verbatim. The credential is genuinely
temporary and issued by the object store, but it is not scoped per table, and the
request's `operation` does not narrow it. Against real AWS, Unity would assume a
role and return a policy-scoped session.

## The same pipe against Databricks

Databricks Unity Catalog vends genuine STS sessions and its CLI has a command for
that, so the shape survives the move off the stand. The vending response's `url`
is the table's storage path, which is all `pqbench table` needs — it reads the
log itself, so nothing has to enumerate files:

```bash
TABLE=main.demo.events

databricks temporary-table-credentials generate-temporary-table-credentials \
    --table-id "$(databricks tables get "$TABLE" -o json | jq -r .table_id)" \
    --operation READ -o json |
  jq -c '{kind: "pqbench.remote-source", version: 1, inputs: [.url],
    env: (.aws_temp_credentials | {AWS_ACCESS_KEY_ID: .access_key_id,
      AWS_SECRET_ACCESS_KEY: .secret_access_key,
      AWS_SESSION_TOKEN: .session_token, AWS_REGION: "us-east-1"})}' |
  pqbench table | pqbench bytemass
```

`--table-id` wants the table's UUID, hence the inner `tables get`. `AWS_REGION`
is the bucket's region. Vending is off until a metastore admin sets
`external_access_enabled` and a catalog owner grants `EXTERNAL USE SCHEMA` on the
schema; `databricks tables get "$TABLE" --include-manifest-capabilities -o json`
says whether a table is eligible at all
([credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)).
The session expires — `expiration_time` is epoch milliseconds — so vend inside
the pipe rather than caching it in your shell. Azure and GCP return
`azure_user_delegation_sas` or `gcp_oauth_token` instead, which an `AWS_*`-only
`env` cannot carry.

## Endpoints and state

Host endpoints bind to loopback: S3 `http://localhost:9000`, Unity Catalog
`http://localhost:8080`. Override with `RUSTFS_PORT` and `UNITY_CATALOG_PORT`.
Containers on the Compose network reach the same objects at `http://rustfs:9000`,
so a pqbench running there wants that endpoint instead of `localhost`.
Credentials are the local dummy values `test` / `test`, region `us-east-1`, HTTP
and path-style S3 — hence `AWS_ALLOW_HTTP` and
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`.

The images are pinned (`rustfs/rustfs:1.0.0`, `unitycatalog/unitycatalog:v0.6.0`,
and `amazon/aws-cli:2.36.49` as a one-off client); bump them deliberately.
Compose polls rustfs's `GET /health` with its bundled `curl` and starts Unity
only once that answers.

Any S3 client reaches the objects — the AWS CLI needs only the endpoint:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:9000 s3 ls s3://lakehouse/ --recursive
compose() { docker compose -f docker/e2e-lakehouse/compose.yaml "$@"; }
compose down     # stop; retain named volumes
compose down -v  # explicit reset of this stand's data only
```

The rustfs objects and Unity's H2 metadata use named volumes.

## Scope

Unity resolves an external Delta table's location and vends a credential for it;
pqbench reads the log and measures the active files' footers. Vending here is
Unity's preset-credential mode (see [above](#curl-checks)), so it does **not**
test per-table scoping, IAM enforcement, or Databricks managed tables.
Measurement is physical Parquet storage, not logical rows after deletions, and
objects the log no longer references are not measured. rustfs speaks the S3 API
but is not AWS: it reproduces neither WAN latency nor IAM authorization. This
stand is opt-in and separate from the fast Rust test suite.

References: [Unity Catalog Compose](https://docs.unitycatalog.io/docker_compose/),
[Unity Catalog credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending),
[rustfs in Docker](https://docs.rustfs.com/en/installation/container/docker/),
[delta-rs](https://delta-io.github.io/delta-rs/).

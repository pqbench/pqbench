# Authentication

Local paths need no credentials. Remote objects and catalogs do. Credentials on
a table override the process environment and are **not** written back into it.
The [Databricks authentication guide](../guides/auth.md) creates the service
principal; this page is the reference for the variables and the credential
flow.

## Object storage (`s3://`)

Needs `--features aws` (or `delta-s3` / `iceberg-s3`). The storage adapter
builds an `AmazonS3Builder` from the default AWS provider chain, then applies
the table's `credentials`.

| Name | Role |
| --- | --- |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Long-lived or vended keys |
| `AWS_SESSION_TOKEN` | STS / Unity temp credentials |
| `AWS_REGION` | Bucket region |
| `AWS_PROFILE` | Process env only (shared config) |
| `AWS_ENDPOINT` / `AWS_ENDPOINT_URL` | S3-compatible host (MinIO, rustfs) |
| `AWS_ALLOW_HTTP` | `true` for `http://` |
| `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | `false` for path-style |
| `AWS_SKIP_SIGNATURE` | `true` for a public bucket |

Process env (instance role, SSO, shared credentials):

```console no-run
AWS_PROFILE=analytics pqb table-file s3://bucket/table/part-0.parquet | pqb bytemass
```

## Catalog config (Unity / Databricks / Iceberg REST)

The walk's config resolves from three layers, least to most specific:
`~/.pqbenchrc` (an INI of named profiles, like `~/.databrickscfg`), then the
environment, then the command line. A profile names an endpoint, a bearer, and
a dialect, so one workspace can carry `[unity]` and `[iceberg]` profiles; pick
one with `--profile` (`PQB_PROFILE`), then override `--endpoint` / `--token` /
`--table-format` for one run. Everything else is a per-invocation flag.

| Variable | Role |
| --- | --- |
| `PQB_PROFILE` | the `~/.pqbenchrc` profile to run under |
| `PQB_ENDPOINT` | Databricks workspace URL, Unity OSS, or Iceberg REST base |
| `PQB_TOKEN` | Bearer PAT (`dapi-…`) or OAuth token; catalog API only |
| `PQB_TABLE_FORMAT` | `unity` (the default) or `iceberg`; for `iceberg` the endpoint names the catalog base (`{root}/v1` or `{root}/v1/{prefix}`) |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | a lower-priority fallback for the endpoint and bearer |

The token is a Bearer on the **catalog** API only. Only `AWS_*` is copied onto
listed tables. A PAT does not open `s3://`.

`get metastore` reads the same endpoint and reports its metastore record
(name, uid, cloud_provider, region) as `Metastore`. `list catalogs` lists the
catalogs at the endpoint, one `Catalog` line each (name, catalog_type). `get
catalog` reports one catalog's record (name, catalog_type, comment, owner) as a
single `Catalog` line. `list schemas` lists the schemas in the catalog, one
`Schema` line each (name, comment, uri): Unity REST serves `/schemas`; with
`PQB_TABLE_FORMAT=iceberg` (or `"table_format": "iceberg"` in the document) the
command speaks Iceberg REST instead — the endpoint then names the catalog base
(`{root}/v1` or `{root}/v1/{prefix}`) and the command reads `/namespaces` under
it. Unity is the default; no config probe runs.

The metadata levels pipe: `PQB_ENDPOINT` / `PQB_TOKEN` / `PQB_TABLE_FORMAT`
carry the walk's context, and each level reads the parent's refs on standard
input as they arrive — one `Catalog` line per catalog, then one `Schema` line
per schema. Each ref's request starts as its record is read; `--fan-out` (64 on
the listing levels and `list files`, one per core on `get table`) caps the
requests in flight — it is a limit, not a batch: up to that many run at once on
one thread, and rows are written as requests finish, not in ref order. Reading
is demand-driven: a slow endpoint or a slow downstream pipe stops the reads, so
the level above backpressures instead of buffering. A `pqb rl` stage paces the
refs between two levels at a target rate — records pass through unchanged, one
bucket per resource type, nothing dropped. A ref whose read fails — a timeout, a
dropped connection, an endpoint answer such as a 429, or a storage fault — is
dropped by the level: it names the ref and the cause on standard error,
finishes the stream, and exits non-zero with the count, so one bad ref does not
truncate a walk. The drop is only the read: a ref that does not name a
resource, a record whose fields are wrong, or an output failure still fails the
level. `rl` keeps the endpoint from answering 429 in the first place; a level
that fails outright — the root `list catalogs`, a malformed stream — still
fails the pipeline, and the shell can retry it at a lower rate:

```console no-run
$ set -o pipefail
$ rate=15
$ until pqb ls catalogs | pqb rl --rate "$rate" | pqb ls schemas; do
>     rate=$((rate / 2))
> done
```

`list catalogs | list schemas` lists every schema at the endpoint; `list
catalogs | get catalog` enriches each catalog instead. `get` emits the same
kind of resource as the `list` it reads from, so you can insert it into a walk
or skip it; `tee` writes each level into the job tree.

```console no-run
$ PQB_ENDPOINT=https://example.cloud.databricks.com PQB_TOKEN=dapi-… \
>     pqb ls catalogs | pqb ls schemas
name                                   comment  uri
-------------------------------------  -------  ----------------------
catalogs/dbx_samples/schemas/bakehouse  …        s3://lakehouse/bakehouse
catalogs/dbx_samples/schemas/nyctaxi    …        s3://lakehouse/nyctaxi
catalogs/samples/schemas/accuweather    …        s3://lakehouse/weather
```

`get schema` reads one schema (name, display_name, comment, uri, properties)
from Unity `/schemas/{full_name}` or Iceberg REST `loadNamespace`; it takes
`Schema` records on standard input. `list tables` lists the tables in a schema,
one `Table` line each — the document `get table` enriches. Unity's `/tables`
pages carry the full name and storage location, so the ref is complete; Iceberg
REST lists identifiers only, so the ref carries its name and no storage URI —
`get table` derives the `loadTable` URL and reads the metadata inline. A view
carries no location, so its ref keeps its `uri` unset; `credentials check` is
the stage that drops it. Refs are addresses; the walk context (endpoint, token,
storage options) comes from the environment (`PQB_*`, `AWS_*`). The walk is a
plain pipeline: every command streams refs and keeps `--fan-out` in flight,
`credentials get` writes the vended keys onto the refs, and `get table` passes
them on so `bytemass` reads the files under the same lease:

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ echo '"dbx_samples.nyctaxi"' |
>     pqb ls tables |
>     pqb credentials check |
>     pqb credentials get |
>     pqb get table
```

`pqb setup` prints that environment for the shell to evaluate:

```console no-run
$ eval "$(pqb setup)"
```

It resolves the endpoint and token the way the Databricks SDKs do — the flag,
then `PQB_*`, then `DATABRICKS_*` — and adds `AWS_REGION` and
`AWS_EC2_METADATA_DISABLED=true` (the AWS SDK otherwise probes EC2 metadata for
the region, which hangs where that endpoint is blackholed). A notebook kernel's
dbutils context is not visible to a subprocess, so a notebook sets
`DATABRICKS_*` from it first.

## Databricks-governed tables

A governed table has two access modes, and pqbench is the second one:

- **Engine-mediated.** Notebooks, SQL, and BI drivers ask Databricks compute to
  read the table; the platform reaches storage with its own identity and no
  cloud credentials reach the caller. Use `spark.sql` / the SQL warehouse for
  this; pqbench does not.
- **Credential-mediated.** An external reader calls the catalog's vending route
  and reads storage itself under a short-lived, downscoped lease — the
  `credentials check` → `credentials get` → `get table` walk.

Whether the second mode exists is the storage's property, not the caller's:

| Table storage | External read under a vended lease |
| --- | --- |
| External location (customer S3), external table | yes |
| External location, managed table | yes |
| Databricks default storage, managed table | no — the catalog reports `TABLE_DB_STORAGE`; the vending route refuses, and the objects deny externally issued sessions |
| Managed volume | files via FUSE in compute or the Files API; not a table read |

`credentials check` reports the answer (a system table, a view, or Databricks
default storage) with a reason; `credentials get` vends the lease.

Two environment notes:

- The vended response carries the keys, a session token, and the storage URL,
  but no region: set `AWS_REGION` (or the client's equivalent) for the read.
- Inside serverless compute, Unity refuses to mint storage credentials to
  notebook code (`UC_SERVERLESS_UNTRUSTED_DOMAIN_STORAGE_TOKEN_MINTING`); the
  refs pass through and the compute's own engines are the readers. Classic
  compute reaches storage through its instance profile.

## References

- Temporary table credentials: <https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials>
- AWS default credential chain: <https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html>
- Iceberg REST catalog: <https://iceberg.apache.org/docs/latest/rest-catalog-spec/>

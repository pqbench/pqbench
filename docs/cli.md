# pqbench CLI

Command guide for humans and agents. `pqb --help` is the long page
(auth, documents, format skills). `pqb <command> --help` is local to
that command and links back. This file is the durable copy.

## What to run

| Goal | Command |
| --- | --- |
| Name a file for measurement | `pqb table-file FILE` |
| On-disk bytes per column | `pqb bytemass` (records on stdin) |
| List a table's active files | `pqb ls files` (Table refs on stdin) |
| Read the endpoint's metastore record | `pqb get metastore` (`PQB_ENDPOINT`) |
| List the catalogs at a catalog endpoint | `pqb ls catalogs` (`PQB_ENDPOINT`) |
| Read one catalog's record | `pqb get catalog` (Catalog refs on stdin) |
| List the schemas in a catalog | `pqb ls schemas` (Catalog refs on stdin) |
| Read one schema's record | `pqb get schema` (Schema refs on stdin) |
| List the tables in a schema | `pqb ls tables` (Schema refs on stdin) |
| Read one table's record | `pqb get table` (Table refs on stdin) |
| Render a stream as tables | `pqb fmt` (NDJSON on stdin) |
| Check + vend read credentials (refs) | `pqb credentials check` / `pqb credentials get` (refs on stdin) |
| Pace a ref stream to N records/s | `pqb rl [--rate N]` (15/s default) |
| Visualize a bytemass stream | `pqb bytemass … \| pqb viz -o report` |
| Codec speed on raw bytes | `pqb lz FILE -c zstd@3` |
| Codec speed on Parquet pages | `pqb compression FILE` (NONE-compressed only) |
| Column facts from row values | `pqb profile FILE` |
| Agent skill (write / DDL / codec recipes) | `pqb skill parquet-advisor` |

The walk reads with two verbs: `get` reads one resource of the walk, `list`
lists one of its collections. Each level reads its parent's refs on standard
input, so the walk is a pipe:

```console no-run
$ pqb ls catalogs | pqb rl \
    | pqb ls schemas | pqb rl \
    | pqb ls tables
$ echo '"dbx_samples.nyctaxi.trips"' | pqb get table
```

A level reads standard input only when it is a stream — a pipeline's pipe, or a
`<file` redirect. A terminal, and a notebook kernel's socket, are not, so a
level run with no input fails instead of blocking on a read that never returns.

The usual walk:

```console run delta
$ cat docker/e2e-lakehouse/table.ndjson \
>   | pqb ls files \
>   | pqb bytemass \
>   | pqb viz -o /tmp/report
```

Standard output is NDJSON. `pqb fmt` renders it as aligned tables: one
section per record type, with only the curated columns shown. Credentials travel on the refs (`AWS_*`) —
`credentials get` writes the vended keys onto the refs it passes on, and
pqbench never writes them into the process environment itself.

## Documents

| resource | Produced by | Consumed by |
| --- | --- | --- |
| `Metastore` | `get metastore` | humans / scripts  |
| `Catalog` | `list catalogs`, `get catalog` | `get catalog`, `list schemas`, humans / scripts  |
| `Schema` | `list schemas`, `get schema` | `get schema`, `list tables` |
| `Table` | `list tables`, `get table`, `credentials get` | `get table`, `list files`, `credentials get` |
| `TableFile` | `list files`, `sample files`, `table-file` | `sample files`, `bytemass`, humans / scripts  |
| `Blob` / `Bytemass` | `bytemass` | `viz`, `diff`, humans / scripts  |
| `pqbench.profile-column` | `profile` | humans / scripts  |
| `pqbench.skill` | `skill` (list) | an agent |

A resource's `name` is its identity: the collection segment (`catalogs`,
`schemas`, `tables`, `files`) names the type, so no record carries a `kind` or a
version of its own — the API version is the package's.

## Flags that repeat

| Flag | Meaning |
| --- | --- |

## Auth — how to reach data

Local paths need no credentials. Remote objects and catalogs do. Credentials
on a table override the process environment and are **not** written back
into it.

### Object storage (`s3://`)

Needs `--features aws` (or `delta-s3` / `iceberg-s3`). `AmazonS3Builder`
reads the default AWS provider chain, then applies the table's `credentials`.

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

### Catalog config (Unity / Databricks / Iceberg REST)

See [configuration.md](configuration.md) for the profile file and precedence.

The walk's config resolves from three layers, least to most specific:
`~/.pqbenchrc` (an INI of named profiles, like `~/.databrickscfg`), then the
environment, then the command line. A profile names an endpoint, a bearer, and
a dialect, so one workspace can carry `[unity]` and `[iceberg]` profiles; pick
one with `--profile` (`PQB_PROFILE`), then override `--endpoint` / `--token` /
`--table-format` for one run. Everything else is a per-invocation flag.
`pqb setup` prints the resolved config for the shell to eval:

| Variable | Role |
| --- | --- |
| `PQB_ENDPOINT` | Databricks workspace URL, Unity OSS, or Iceberg REST base |
| `PQB_TOKEN` | Bearer PAT (`dapi-…`) or OAuth token; list API only |
| `PQB_TABLE_FORMAT` | `unity` (the default) or `iceberg`; for `iceberg` the endpoint names the catalog base (`{root}/v1` or `{root}/v1/{prefix}`) |

The token is a Bearer on the **list** API only. Only `AWS_*` is copied onto
listed tables. A PAT does not open `s3://`.

`get metastore` reads the same endpoint and reports its metastore
record (name, id, cloud, region) as `Metastore`; `list catalogs` lists
the catalogs at the endpoint, one `Catalog` line each (name,
catalog_type). `get catalog` reports one catalog's record (name, catalog_type,
comment, owner) as a single `Catalog` line. `list schemas` lists the
schemas in the catalog, one `Schema` line each (catalog, name): Unity
REST serves `/schemas`; with `PQB_TABLE_FORMAT=iceberg` (or `"table_format":
"iceberg"` in the document) the command speaks Iceberg REST instead — the
endpoint then names the catalog base (`{root}/v1` or `{root}/v1/{prefix}`) and
the command reads `/namespaces` under it. Unity is the default; no config
probe runs.

The metadata levels pipe: `PQB_ENDPOINT` / `PQB_TOKEN` / `PQB_TABLE_FORMAT`
carry the walk's context, and each level reads the parent's refs on standard
input as they arrive — one `Catalog` line per catalog, then one
`Schema` line per schema. Each ref's request starts as its record is
read; `--fan-out` (64 on the listing levels and `list files`, one per core on
`get table`)
caps the requests in flight — it is a limit,
not a batch: up to that many run at once on one thread, and rows are written as
requests finish, not in ref order. Reading is demand-driven: a slow endpoint or
a slow downstream pipe stops the reads, so the level above backpressures
instead of buffering. A `pqb rl` stage
paces the refs between two levels at a target rate — records pass through
unchanged, one bucket per resource type, nothing dropped. A ref whose read
fails — a timeout, a dropped connection, an endpoint answer such as a 429, or
a storage fault — is dropped by the level: it names the ref and the cause on
standard error, finishes the stream, and exits non-zero with the count, so one
bad ref does not truncate a walk. The drop is only the read: a ref that does not
name a resource, a record whose fields are wrong, or an output failure still
fails the level. `rl` keeps the endpoint from answering
429 in the first place; a level that fails outright — the root `list catalogs`,
a malformed stream — still fails the pipeline, and the shell can retry it at a
lower rate:

```console
set -o pipefail
rate=15
until pqb ls catalogs | pqb rl --rate "$rate" | pqb ls schemas; do
    rate=$((rate / 2))
done
```

`list catalogs | list schemas` lists every schema at the endpoint; `list catalogs |
get catalog` enriches each catalog instead. `get` emits the same kind of resource
as the `list` it reads from, so you can insert it into a walk or skip it; `tee` writes each level into the job tree.

```console no-run
$ PQB_ENDPOINT=https://example.cloud.databricks.com PQB_TOKEN=dapi-… \
    pqb ls catalogs | pqb ls schemas
name                                   comment  uri
-------------------------------------  -------  ----------------------
catalogs/dbx_samples/schemas/bakehouse  …        s3://lakehouse/bakehouse
catalogs/dbx_samples/schemas/nyctaxi    …        s3://lakehouse/nyctaxi
catalogs/samples/schemas/accuweather    …        s3://lakehouse/weather
```

`get schema` reads one schema (catalog, name, comment, location, properties)
from Unity `/schemas/{full_name}` or Iceberg REST `loadNamespace`; it takes
`Schema` records on standard input. `list tables` lists the tables in a schema,
one `Table` line each — the document `get table` enriches. Unity's `/tables` pages carry the full name
and storage location, so the ref is complete; Iceberg REST lists identifiers
only, so the ref carries its name and no storage URI — `get table` derives the
`loadTable` URL and reads the metadata inline.
A view carries no location, so its ref keeps its `uri` unset;
`credentials check` is the stage that drops it. Refs are addresses; the walk
context (endpoint, token, storage options) comes from the environment
(`PQB_*`, `AWS_*`). The walk is a plain pipeline: every command streams refs
and keeps `--fan-out` in flight, `credentials get` writes the vended keys onto
the refs, and `get table` passes them on so `bytemass` reads the files under
the same lease:

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ echo '"dbx_samples.nyctaxi"' |
    pqb ls tables |
    pqb credentials check |
    pqb credentials get |
    pqb get table
```

`pqb setup` prints that environment for the shell to evaluate:

```console no-run
$ eval "$(pqb setup)"
```

It resolves the endpoint and token the way the Databricks SDKs do — the flag,
then `PQB_*`, then `DATABRICKS_*` — and adds `AWS_REGION` and
`AWS_EC2_METADATA_DISABLED=true` (the AWS SDK otherwise probes EC2 metadata
for the region, which hangs where that endpoint is blackholed). A notebook
kernel's dbutils context is not visible to a subprocess, so a notebook sets
`DATABRICKS_*` from it first.

`get table` enriches that ref — name, format, snapshot, columns, partition
columns, format properties — and keeps the same resource name, so
`list tables | get table` chains. Unity `/tables/{full_name}` names the
storage location whose Delta log is read with `without_files()`, and the
catalog's declared columns and properties are merged over the log's; Iceberg
REST `loadTable` carries the metadata inline, so the Iceberg path runs no
storage read at all. The Delta read is O(1) in files, so a walk can descend to
every table before deciding which files to measure. `list files` lists the
table's active files.

The table read knows nothing about credentials: it reads with the lease it is
given — the table's own `credentials` and the process environment the storage
client also reads — and carries that lease on the record, so the
next stage reads the data files under the same lease. Everything
credential-shaped is the `credentials` stage's concern. The Delta path needs a
readable storage location; the environment's `AWS_*` and a vended lease both
supply one. The Iceberg REST path needs no
storage read.

A vended lease is a storage fact, not a caller choice. Databricks serves
managed tables to external systems through its catalog APIs; resolving the
Delta log by path is not that interface, and Databricks-managed default
storage explicitly denies externally issued sessions on its objects (verified
for data files, Iceberg manifests, and the Delta log) — the catalog reports
those tables as Databricks default storage (`TABLE_DB_STORAGE`), so
`credentials check` drops the ref with the reason and no storage read runs.
Customer-storage tables read under the lease; compatibility mode publishes a
read-only copy for path-based clients. Inside Databricks compute the lease is
not the mechanism: serverless notebooks are refused storage-credential minting
outright (`UC_SERVERLESS_UNTRUSTED_DOMAIN_STORAGE_TOKEN_MINTING`) and refs
pass through, while classic compute reaches storage through its own instance
profile.

`credentials check` and `credentials get` read the same refs. The check is the
walk's single filter: it drops a `system` catalog ref, a view (the catalog
reports no location), and a table on Databricks default storage, with the
reason on standard error; eligible refs pass through unchanged. A drop is the
filter working, not a failure — a whole-catalog walk always has tables it
cannot read outside compute — so an ineligible ref still leaves the stage
green; a ref whose check fails is dropped and counted, and the stage exits
non-zero. `credentials get` is the stage that materializes the
credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`) on the refs for the table read and other tools: Unity's
`temporary-table-credentials`, or under `PQB_TABLE_FORMAT=iceberg` the
catalog's `loadCredentials` route (`GET …/tables/{table}/credentials`) and the
`storage-credentials` it returns (the metadata-inline Iceberg read needs no
keys, but the next storage operations do); a catalog that does not serve the
route falls back to the delegated `loadTable`. A table the Iceberg catalog
cannot serve passes through with its own credentials.

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ echo '"dbx_samples.nyctaxi"' |
    pqb ls tables |
    pqb credentials check | pqb credentials get
```

`list files` reads each ref's Delta / Iceberg log — never the data files — and
writes the snapshot's active files, one `TableFile` per line: the files the
table holds now, so one a later commit removed is absent. It resolves the
snapshot (Delta: the checkpoint and the commits after it), streams the add
actions one row at a time, and names each file as `{table}/files/{path}` with
the table's lease, so `bytemass` reads it with no catalog call. Nothing is
collected: a table of any size costs one decoded batch plus one file, and
`--fan-out` (64 by default) is how many tables are walked at once.

`--last 1d` (or `1w`) keeps the files whose logged modification time falls in
the window ending now; `--partition 2026-10-28` keeps one UTC day. The filter
runs as each file arrives, so a daily run reads the day's writes without holding
what the table already held. Neither flag filters the *commit*: it is the file's
own time the log records, not the version that added it. Iceberg records no
per-file time, so a window over an Iceberg table is an error rather than an
empty listing.

```console no-run
$ cat table.ndjson | pqb ls files --last 1d | pqb bytemass
```

```console no-run
$ echo '{"name":"pqbench.demo.events"}' | PQB_ENDPOINT=… pqb get table
name                           format  snapshot_version  columns  uri
-----------------------------  ------  ----------------  -------  ----------------------
catalogs/pqbench/schemas/demo/tables/events  delta  0       2  s3://lakehouse/unity/events
```

List a table's active files from a table record; a terminal prints them:

```console run delta
$ cat docker/e2e-lakehouse/table.ndjson | pqb ls files | pqb fmt
path                                                                 size_bytes
-------------------------------------------------------------------  ----------
part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet         796
```

List the catalogs at the endpoint:

```console no-run
$ pqb ls catalogs
name                  catalog_type
--------------------  -----------------
catalogs/dbx_samples  CATALOG_TYPE_MANAGED
catalogs/samples      CATALOG_TYPE_SYSTEM
```

Read one catalog's record:

```console no-run
$ echo '"dbx_samples"' | pqb get catalog
name                  catalog_type
--------------------  --------------------
catalogs/dbx_samples  CATALOG_TYPE_MANAGED
```

List the schemas in a catalog:

```console no-run
$ echo '"dbx_samples"' | pqb ls schemas
name                                   comment  uri
-------------------------------------  -------  ----------------------
catalogs/dbx_samples/schemas/bakehouse  …        s3://lakehouse/bakehouse
catalogs/dbx_samples/schemas/nyctaxi    …        s3://lakehouse/nyctaxi
```

### Databricks-governed tables

A governed table has two access modes, and pqbench is the second one:

- **Engine-mediated.** Notebooks, SQL, and BI drivers ask Databricks compute
  to read the table; the platform reaches storage with its own identity and no
  cloud credentials reach the caller. Use `spark.sql` / the SQL warehouse for
  this; pqbench does not.
- **Credential-mediated.** An external reader calls the catalog's vending
  route and reads storage itself under a short-lived, downscoped lease — the
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

- Temporary table credentials: <https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials>
- AWS default credential chain: <https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html>

## Features

| Feature | What it unlocks |
| --- | --- |
| `delta` / `delta-s3` | Load a Delta log (`s3://` needs `delta-s3`) |
| `iceberg` / `iceberg-s3` | Load Iceberg metadata (`s3://` needs `iceberg-s3`) |
| `aws` | `s3://` listing and footer reads |

A URI whose backend is not compiled in fails and names the missing feature.

## Format skills

Read these when the input is a Parquet file or a table format:

- Parquet file format: <https://parquet.apache.org/docs/file-format/>
- Parquet thrift spec: <https://github.com/apache/parquet-format>
- Delta transaction log: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- Iceberg table spec: <https://iceberg.apache.org/spec/>
- Iceberg REST catalog: <https://iceberg.apache.org/docs/latest/rest-catalog-spec/>
- Unity / Databricks tables list: <https://docs.databricks.com/api/workspace/tables/list>
- AWS default credentials: <https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html>

Repo docs: [delta.md](delta.md), [iceberg.md](iceberg.md), [viz.md](viz.md),
[profile.md](profile.md), [skill.md](skill.md), [demo.md](demo.md).

## File footer facts

`bytemass` reads only the footer. It emits one `Blob` per file — its
`size_bytes`, `row_group_count`, and `column_count` — and one `Bytemass` per
column, its chunks summed over the file's row groups: on-disk and uncompressed
bytes, the codec, the encodings, whether a dictionary page is present, the
value and null counts, and the first and last values from the statistics. No
data page is decoded, so it works on any Parquet file, compressed or not.

## Sampling files

`sample files` keeps a rate of the stream, by size, so a table's footers can be
measured without reading every file:

```console no-run
$ cat table.ndjson | pqb list files | pqb sample files --rate 1/100 | pqb bytemass
```

A file is kept with probability `rate × size / 256 MiB`, capped at one, drawn
from a hash of its name: the same record always makes the same decision. A
256 MiB file is kept at `--rate` (the default keeps about one in a hundred), a
1 GiB file four times as often, and `--rate 1` keeps every file. The decision
needs only the record's name and size, which the log already carries, so the
stage streams and never holds the table.

Each kept `TableFile` records the probability it was kept with as
`sample_rate`; `bytemass` copies it onto the `Blob` and every `Bytemass`. Scale
only the additive quantities by its inverse — `size_bytes`, `compressed_bytes`,
`uncompressed_bytes`, `value_count`, `null_count`, `row_group_count` — and
recompute the ratios (`bytes_per_value`, `compression_ratio`, `null_fraction`)
from the weighted sums; `distinct_count` is not additive. A sum over the kept
records alone is the sample, not the table — the coverage line on standard
error says how many files and bytes it holds. A record that already carries a
`sample_rate` composes with the smaller of the two probabilities, since both
threshold the same hash. `viz` does not apply the weight yet, so a sampled
stream's page is a sample, not the table. A record with no positive size is an
error, not a silent drop.

Filtering by path or partition is still a shell job on the `list files` stream
(`jq`, `sort`, `head`, `xargs`) — see [demo.md](demo.md). `bytemass` reads its
files in flight (`--fan-out`, 128 by default), so the per-file footer round
trips overlap instead of adding up.

`bytemass` measures the table-file records it is given; it does not expand
patterns or list a prefix. A pattern belongs to the producer: `list files`
names a table's files from its log, and `table-file` names one path or URI.

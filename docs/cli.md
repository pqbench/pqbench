# pqbench CLI

Command guide for humans and agents. `pqbench --help` is the long page
(auth, documents, format skills). `pqbench <command> --help` is local to
that command and links back. This file is the durable copy.

## What to run

| Goal | Command |
| --- | --- |
| On-disk bytes per column | `pqbench bytemass FILE` |
| List a table's natural partitions | `pqbench list partitions DIR` (Table refs on stdin) |
| List a partition's files | `pqbench list files` (partitions on stdin) |
| Read the endpoint's metastore record | `pqbench get metastore` (`PQB_ENDPOINT`) |
| List the catalogs at a catalog endpoint | `pqbench list catalogs` (`PQB_ENDPOINT`) |
| Read one catalog's record | `pqbench get catalog [CATALOG]` (refs on stdin) |
| List the schemas in a catalog | `pqbench list schemas [CATALOG]` (refs on stdin) |
| Read one schema's record | `pqbench get schema CATALOG.SCHEMA` (refs on stdin) |
| List the tables in a schema | `pqbench list tables CATALOG.SCHEMA` (refs on stdin) |
| Read one table's record | `pqbench get table CATALOG.SCHEMA.TABLE` (Table refs on stdin) |
| Check + vend read credentials (refs) | `pqbench credentials check` / `pqbench credentials get` (refs on stdin) |
| Pace a ref stream to N records/s | `pqbench ratelimit [--rate N]` |
| Visualize a bytemass stream | `pqbench bytemass … \| pqbench viz -o report` |
| Codec speed on raw bytes | `pqbench lz FILE -c zstd@3` |
| Codec speed on Parquet pages | `pqbench compression FILE` (NONE-compressed only) |
| Column facts from row values | `pqbench profile FILE` |
| Agent skill (write / DDL / codec recipes) | `pqbench skill parquet-advisor` |

The walk reads with two verbs: `get` reads one resource of the walk, `list`
lists one of its collections. Each level reads its parent's refs on standard
input, or takes the parent as its argument, so the walk is a pipe:

```console no-run
$ pqbench list catalogs | pqbench list schemas | pqbench list tables
$ pqbench get table dbx_samples.nyctaxi.trips
```

A level reads standard input only when it is a stream — a pipeline's pipe, or a
`<file` redirect. A terminal, and a notebook kernel's socket, are not: the level
falls back to its argument (or its own error) instead of blocking on a read that
never returns.

The usual walk:

```console run delta
$ pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass | pqbench viz -o /tmp/report
```

A terminal prints an aligned table; a pipe streams NDJSON. The table keeps
the stream clean: only data rows are shown, bounded to 1000 rows, with the
count of hidden rows reported. Credentials travel on the refs (`AWS_*`) —
`credentials get` writes the vended keys onto the refs it passes on, and
pqbench never writes them into the process environment itself.

## Documents

| resource | Produced by | Consumed by |
| --- | --- | --- |
| `Metastore` | `get metastore` | humans / scripts (`--json`) |
| `Catalog` | `list catalogs`, `get catalog` | `get catalog`, `list schemas`, humans / scripts (`--json`) |
| `Schema` | `list schemas`, `get schema` | `get schema`, `list tables` |
| `Table` | `list tables`, `get table`, `credentials get` | `get table`, `list partitions`, `credentials get` |
| `Partition` | `list partitions` | `list files`, humans / scripts (`--json`) |
| `TableFile` | `list files` | `bytemass`, humans / scripts (`--json`) |
| `remote-source` | a producer | `bytemass` |
| `Blob` / `Bytemass` | `bytemass` | `viz`, `diff`, humans / scripts (`--json`) |
| `pqbench.profile-column` | `profile` | humans / scripts (`--json`) |
| `pqbench.skill` | `skill` (list) | an agent |

A resource's `name` is its identity: the collection segment (`catalogs`,
`schemas`, `tables`, `partitions`, `files`) names the type, so no record carries
a `kind` or a version of its own — the API version is the package's.

## Flags that repeat

| Flag | Meaning |
| --- | --- |
| `-o` / `--output` | `FILE` for the lz4 NDJSON stream; `PREFIX` on `viz`. Independent of what stdout shows. |
| `--format` | Every streaming command: `auto` (table on a terminal, NDJSON on a pipe), `table`, or `json`. |
| `--json` | `bytemass`, `lz`, `compression`, `profile`: stream NDJSON on stdout (same as `--format json`). |

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

Only `AWS_*` names are accepted on a `remote-source` document.

Process env (instance role, SSO, shared credentials):

```console no-run
AWS_PROFILE=analytics pqbench bytemass s3://bucket/table/part-0.parquet
```

Vended STS on a producer document piped into `bytemass`:

```console no-run
cat <<'EOF' | pqbench bytemass
{"version":1,
 "inputs":["s3://bucket/table/part-0.parquet"],
 "env":{"AWS_ACCESS_KEY_ID":"…","AWS_SECRET_ACCESS_KEY":"…",
        "AWS_SESSION_TOKEN":"…","AWS_REGION":"us-east-1"}}
EOF
```

### Catalog config (Unity / Databricks / Iceberg REST)

See [configuration.md](configuration.md) for the profile file and precedence.

The walk's config resolves from three layers, least to most specific:
`~/.pqbenchrc` (an INI of named profiles, like `~/.databrickscfg`), then the
environment, then the command line. A profile names an endpoint, a bearer, and
a dialect, so one workspace can carry `[unity]` and `[iceberg]` profiles; pick
one with `--profile` (`PQB_PROFILE`), then override `--endpoint` / `--token` /
`--table-format` for one run. Everything else is a per-invocation flag.
`pqbench setup` prints the resolved config for the shell to eval:

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
read; `--fan-out` (64 on the listing levels, one per core on `get table`)
caps the requests in flight — it is a limit,
not a batch: up to that many run at once on one thread, and rows are written as
requests finish, not in ref order. Reading is demand-driven: a slow endpoint or
a slow downstream pipe stops the reads, so the level above backpressures
instead of buffering. A `pqbench ratelimit` stage
paces the refs between two levels at a target rate — records pass through
unchanged, one bucket per resource type, nothing dropped. A 429 fails the level with the
endpoint's status and body; pace the walk and retry it at a lower rate in the
shell:

```console
set -o pipefail
rate=15
until pqbench list catalogs | pqbench ratelimit --rate "$rate" | pqbench list schemas; do
    rate=$((rate / 2))
done
```

`list catalogs | list schemas` lists every schema at the endpoint; `list catalogs |
get catalog` enriches each catalog instead. `get` emits the same kind of resource
as the `list` it reads from, so you can insert it into a walk or skip it; `tee`
(or `-o`) writes each level into the job tree.

```console no-run
$ PQB_ENDPOINT=https://example.cloud.databricks.com PQB_TOKEN=dapi-… \
    pqbench list catalogs | pqbench list schemas
name                                   comment  uri
-------------------------------------  -------  ----------------------
catalogs/dbx_samples/schemas/bakehouse  …        s3://lakehouse/bakehouse
catalogs/dbx_samples/schemas/nyctaxi    …        s3://lakehouse/nyctaxi
catalogs/samples/schemas/accuweather    …        s3://lakehouse/weather
schemas: 3
```

`get schema` reads one schema (catalog, name, comment, location, properties)
from Unity `/schemas/{full_name}` or Iceberg REST `loadNamespace`. `list tables`
lists the tables in it, one `Table` line each — the
document `get table` enriches. Unity's `/tables` pages carry the full name
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
$ pqbench list tables dbx_samples.nyctaxi --format json |
    pqbench credentials check |
    pqbench credentials get |
    pqbench get table --format json
```

`pqbench setup` prints that environment for the shell to evaluate:

```console no-run
$ eval "$(pqbench setup)"
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
every table before deciding which files to measure. `list partitions` lists a table's
partitions and `list files` its files.

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
cannot read outside compute — so the stage still succeeds; only a real error (a
missing table, an endpoint fault) fails it. `credentials get` is the stage that materializes the
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
$ pqbench list tables dbx_samples.nyctaxi --format json |
    pqbench credentials check | pqbench credentials get
```

`list partitions` reads each ref's Delta / Iceberg log — never the data files — and
groups the table's commits into **natural partitions** by commit time (Delta
`commitInfo.timestamp`; Iceberg the snapshot time): one `Partition`
per epoch-aligned, half-open UTC window, carrying the commits it holds.
`--every` sets the width (`1h`, `1d`, `1w`); a commit the log does not date is
omitted, so a window never claims a commit it cannot place. `--last` (`1h`,
`1d`, `1w`) bounds the read to commits within that window of the newest one,
and only windows the horizon covers whole are emitted. A partition is a
lens on the files, not a thing the table stores — see `docs/partition.md`.
`list files` then re-reads only the window's commits and streams the files they
added — the lease rides on every file, and that stream is what `bytemass`
measures.

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ pqbench list tables dbx_samples.nyctaxi --format json |
    pqbench credentials check |
    pqbench credentials get |
    pqbench list partitions
name                  first_time     last_time  commits
--------------------  -----------  ------------  -------
…                     …            …                   1
partitions: 1
```

```console no-run
$ PQB_ENDPOINT=… pqbench get table pqbench.demo.events
name                           format  snapshot_version  columns  uri
-----------------------------  ------  ----------------  -------  ----------------------
catalogs/pqbench/schemas/demo/tables/events  delta  0       2  s3://lakehouse/unity/events
tables: 1
```

List a table's partitions by URI; a terminal prints them:

```console run delta
$ pqbench list partitions docker/e2e-lakehouse/table --format table
name                           first_time      last_time  commits
--------------------------  -------------  -------------  -------
catalogs/local/schemas/local/tables/table/partitions/1789862400000  1789862400000  1789948800000        1
partitions: 1
```

List the catalogs at the endpoint:

```console no-run
$ pqbench list catalogs
name                  catalog_type
--------------------  -----------------
catalogs/dbx_samples  CATALOG_TYPE_MANAGED
catalogs/samples      CATALOG_TYPE_SYSTEM
catalogs: 2
```

Read one catalog's record:

```console no-run
$ pqbench get catalog dbx_samples
name                  catalog_type
--------------------  --------------------
catalogs/dbx_samples  CATALOG_TYPE_MANAGED
catalogs: 1
```

List the schemas in a catalog:

```console no-run
$ pqbench list schemas dbx_samples
name                                   comment  uri
-------------------------------------  -------  ----------------------
catalogs/dbx_samples/schemas/bakehouse  …        s3://lakehouse/bakehouse
catalogs/dbx_samples/schemas/nyctaxi    …        s3://lakehouse/nyctaxi
schemas: 2
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

## Selecting files

There is no built-in file filter: `list files` streams every file the
window's commits added, and `bytemass` measures each, so which files to measure
is a shell job on the stream (`jq`, `sort`, `head`, `xargs`) — see
[demo.md](demo.md). `bytemass` reads its files in flight (`--fan-out`, 128 by
default), so the per-file footer round trips overlap instead of adding up.

## S3 file patterns

With the `aws` feature, `bytemass` expands quoted S3 key patterns before
reading footers. `*` and `?` match within one path component; `**` can span
subdirectories. Listing starts at the literal directory prefix and consumes
all listing pages. Results are sorted and deduplicated. No matches and listing
permission failures are explicit errors; exact URIs never require listing.
Percent-encode literal wildcard characters when addressing an exact key.

```console
$ pqbench bytemass 's3://bucket/table/*.parquet'
$ pqbench bytemass 's3://bucket/table/**/*.parquet'
```

These patterns enumerate physical objects, not a table's active snapshot.
Use `list partitions | list files` for Delta/Iceberg analysis to avoid counting
obsolete files.


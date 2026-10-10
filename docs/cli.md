# pqbench CLI

Command guide for humans and agents. `pqbench --help` is the long page
(auth, documents, format skills). `pqbench <command> --help` is local to
that command and links back. This file is the durable copy.

## What to run

| Goal | Command |
| --- | --- |
| On-disk bytes per column | `pqbench bytemass FILE` |
| List a table's natural partitions | `pqbench table ls DIR` (or v2 refs on stdin) |
| List a partition's files | `pqbench partition ls` (partitions on stdin) |
| Read the endpoint's metastore record | `pqbench metastore info` (`PQB_ENDPOINT`) |
| List the catalogs at a catalog endpoint | `pqbench metastore ls` (`PQB_ENDPOINT`) |
| Read one catalog's record | `pqbench catalog info [CATALOG]` (refs on stdin) |
| List the schemas in a catalog | `pqbench catalog ls [CATALOG]` (refs on stdin) |
| Read one schema's record | `pqbench schema info CATALOG.SCHEMA` (refs on stdin) |
| List the tables in a schema | `pqbench schema ls CATALOG.SCHEMA` (refs on stdin) |
| Read one table's record | `pqbench table info CATALOG.SCHEMA.TABLE` (v2 refs on stdin) |
| Check + vend read credentials (refs) | `pqbench credentials check` / `pqbench credentials get` (v2 refs on stdin) |
| Pace a ref stream to N records/s | `pqbench ratelimit [--rate N]` |
| Visualize a bytemass stream | `pqbench bytemass … \| pqbench viz -o report` |
| Codec speed on raw bytes | `pqbench lz FILE -c zstd@3` |
| Codec speed on Parquet pages | `pqbench compression FILE` (NONE-compressed only) |
| Column facts from row values | `pqbench profile FILE` |
| Rewrite a sample and measure it | `pqbench experiment FILE --rewrite sort:text --aim all` |
| Agent skill (write / DDL / codec recipes) | `pqbench skill parquet-advisor` |

The usual walk:

```console run delta
$ pqbench table ls docker/e2e-lakehouse/table | pqbench partition ls | pqbench bytemass | pqbench viz -o /tmp/report
```

A terminal prints an aligned table; a pipe streams NDJSON. The table keeps
the stream clean: only data rows are shown, bounded to 1000 rows, with the
count of hidden rows reported. Credentials travel on the refs (`AWS_*`) —
`credentials get` writes the vended keys onto the refs it passes on, and
pqbench never writes them into the process environment itself.

## Documents

| `kind` | Produced by | Consumed by |
| --- | --- | --- |
| `pqbench.metastore` | `metastore info` | humans / scripts (`--json`) |
| `pqbench.catalog` | `metastore ls`, `catalog info` | `catalog info`, `catalog ls`, humans / scripts (`--json`) |
| `pqbench.schema` | `catalog ls`, `schema info` | `schema info`, `schema ls` |
| `pqbench.table-ref` v2 | `schema ls`, `table info`, `credentials get` | `table info`, `table ls`, `credentials get` |
| `pqbench.partition` | `table ls` | `partition ls`, humans / scripts (`--json`) |
| `pqbench.table-file` | `partition ls` | `bytemass`, humans / scripts (`--json`) |
| `pqbench.remote-source` | a producer | `bytemass` |
| `pqbench.bytemass-file` / `pqbench.bytemass-row` | `bytemass` | `viz` |
| `pqbench.profile-column` | `profile` | humans / scripts (`--json`) |
| `pqbench.experiment-trial` / `pqbench.experiment-column` | `experiment` | humans / scripts (`--json`) |
| `pqbench.skill` | `skill` (list) | an agent |

The metadata walk's table level exchanges `pqbench.table-ref` version `2`;
`table info` rejects version `1`.

## Flags that repeat

| Flag | Meaning |
| --- | --- |
| `-o` / `--output` | `FILE` for the lz4 NDJSON stream; `PREFIX` on `viz`. Independent of what stdout shows. |
| `--format` | Every streaming command: `auto` (table on a terminal, NDJSON on a pipe), `table`, or `json`. |
| `--json` | `bytemass`, `lz`, `compression`, `profile`, `experiment`: stream NDJSON on stdout (same as `--format json`). |

## Auth — how to reach data

Local paths need no credentials. Remote objects and catalogs do. Credentials
on a document override the process environment and are **not** written back
into it.

### Object storage (`s3://`)

Needs `--features aws` (or `delta-s3` / `iceberg-s3`). `AmazonS3Builder`
reads the default AWS provider chain, then applies document `env`.

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

Only `AWS_*` names are accepted on `pqbench.remote-source` and listed tables.

Process env (instance role, SSO, shared credentials):

```console no-run
AWS_PROFILE=analytics pqbench bytemass s3://bucket/table/part-0.parquet
```

Vended STS on a producer document piped into `bytemass`:

```console no-run
cat <<'EOF' | pqbench bytemass
{"kind":"pqbench.remote-source","version":1,
 "inputs":["s3://bucket/table/part-0.parquet"],
 "env":{"AWS_ACCESS_KEY_ID":"…","AWS_SECRET_ACCESS_KEY":"…",
        "AWS_SESSION_TOKEN":"…","AWS_REGION":"us-east-1"}}
EOF
```

### Catalog config (Unity / Databricks / Iceberg REST)

The walk's config comes from the environment — configuration management sets
it (`pqbench setup` prints it for the shell to eval):

| Variable | Role |
| --- | --- |
| `PQB_ENDPOINT` | Databricks workspace URL, Unity OSS, or Iceberg REST base |
| `PQB_TOKEN` | Bearer PAT (`dapi-…`) or OAuth token; list API only |
| `PQB_TABLE_FORMAT` | `unity` (the default) or `iceberg`; for `iceberg` the endpoint names the catalog base (`{root}/v1` or `{root}/v1/{prefix}`) |

The token is a Bearer on the **list** API only. Only `AWS_*` is copied onto
listed tables. A PAT does not open `s3://`.

`metastore info` reads the same endpoint and reports its metastore
record (name, id, cloud, region) as `pqbench.metastore`; `metastore ls` lists
the catalogs at the endpoint, one `pqbench.catalog` line each (name,
catalog_type). `catalog info` reports one catalog's record (name, catalog_type,
comment, owner) as a single `pqbench.catalog` line. `catalog ls` lists the
schemas in the catalog, one `pqbench.schema` line each (catalog, name): Unity
REST serves `/schemas`; with `PQB_TABLE_FORMAT=iceberg` (or `"table_format":
"iceberg"` in the document) the command speaks Iceberg REST instead — the
endpoint then names the catalog base (`{root}/v1` or `{root}/v1/{prefix}`) and
the command reads `/namespaces` under it. Unity is the default; no config
probe runs.

The metadata levels pipe: `PQB_ENDPOINT` / `PQB_TOKEN` / `PQB_TABLE_FORMAT`
carry the walk's context, and each level reads the parent's refs on standard
input as they arrive — one `pqbench.catalog` line per catalog, then one
`pqbench.schema` line per schema. Each ref's request starts as its record is
read; `--fan-out` (64 on the listing levels, one per core on the per-table
stages) caps the requests in flight — it is a limit,
not a batch: up to that many run at once on one thread, and rows are written as
requests finish, not in ref order. Reading is demand-driven: a slow endpoint or
a slow downstream pipe stops the reads, so the level above backpressures
instead of buffering. A `pqbench ratelimit` stage
paces the refs between two levels at a target rate — records pass through
unchanged, one bucket per kind, nothing dropped. A 429 fails the level with the
endpoint's status and body; pace the walk and retry it at a lower rate in the
shell:

```console
set -o pipefail
rate=15
until pqbench metastore ls | pqbench ratelimit --rate "$rate" | pqbench catalog ls; do
    rate=$((rate / 2))
done
```

`metastore ls | catalog ls` lists every schema at the endpoint; `metastore ls |
catalog info` enriches each catalog instead. `info` emits the same kind as the
`ls` above it, so it can be inserted or skipped; `tee` (or `-o`) writes each
level into the job tree.

```console no-run
$ PQB_ENDPOINT=https://example.cloud.databricks.com PQB_TOKEN=dapi-… \
    pqbench metastore ls | pqbench catalog ls
catalog      name
-----------  ----------------
dbx_samples  bakehouse
dbx_samples  nyctaxi
samples      accuweather
schemas: 3
```

`schema info` reads one schema (catalog, name, comment, location, properties)
from Unity `/schemas/{full_name}` or Iceberg REST `loadNamespace`. `schema ls`
lists the tables in it, one `pqbench.table-ref` version 2 line each — the
document `table info` enriches. Unity's `/tables` pages carry the full name
and storage location, so the ref is complete; Iceberg REST lists identifiers
only, so the ref carries the `loadTable` URL as its `uri` and no storage path.
A view carries no location, so its ref keeps no storage path;
`credentials check` is the stage that drops it. Refs are addresses; the walk
context (endpoint, token, storage options) comes from the environment
(`PQB_*`, `AWS_*`). The walk is a plain pipeline: every command streams refs
and keeps `--fan-out` in flight, `credentials get` writes the vended keys onto
the refs, and `table info` passes them on so `bytemass` reads the files under
the same lease:

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ pqbench schema ls dbx_samples.nyctaxi --format json |
    pqbench credentials check |
    pqbench credentials get |
    pqbench table info --format json
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

`table info` enriches that ref — id, format, snapshot, columns, partition
columns, format properties — and keeps the same kind and version, so
`schema ls | table info` chains. Unity `/tables/{full_name}` names the
storage location whose Delta log is read with `without_files()`, and the
catalog's declared columns and properties are merged over the log's; Iceberg
REST `loadTable` carries the metadata inline, so the Iceberg path runs no
storage read at all. The Delta read is O(1) in files, so a walk can descend to
every table before deciding which files to measure. `table ls` lists a table's
partitions and `partition ls` its files.

The table read knows nothing about credentials: it reads with the env it is
given — the lake source's options, the ref's own, and the process environment
the storage client also reads — and emits that env back on the record, so the
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
`credentials check` stops the walk with the reason and no storage read runs.
Customer-storage tables read under the lease; compatibility mode publishes a
read-only copy for path-based clients. Inside Databricks compute the lease is
not the mechanism: serverless notebooks are refused storage-credential minting
outright (`UC_SERVERLESS_UNTRUSTED_DOMAIN_STORAGE_TOKEN_MINTING`) and refs
pass through, while classic compute reaches storage through its own instance
profile.

`credentials check` and `credentials get` read the same refs. The check is the
walk's single filter: it drops a `system` catalog ref, a view (the catalog
reports no location), and a table on Databricks default storage, with the
reason on standard error; eligible refs pass through unchanged. `credentials get` is the stage that materializes the
credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`) on the refs for the table read and other tools: Unity's
`temporary-table-credentials`, or under `PQB_TABLE_FORMAT=iceberg` the
catalog's `loadCredentials` route (`GET …/tables/{table}/credentials`) and the
`storage-credentials` it returns (the metadata-inline Iceberg read needs no
keys, but the next storage operations do); a catalog that does not serve the
route falls back to the delegated `loadTable`. A table the Iceberg catalog
cannot serve passes through with its own env.

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ pqbench schema ls dbx_samples.nyctaxi --format json |
    pqbench credentials check | pqbench credentials get
```

`table ls` reads each ref's Delta / Iceberg log — never the data files — and
groups the table's commits into **natural partitions** by commit time (Delta
`commitInfo.timestamp`; Iceberg the snapshot time): one `pqbench.partition`
per epoch-aligned, half-open UTC window, carrying the commits it holds.
`--every` sets the width (`1h`, `1d`, `1w`); a commit the log does not date is
omitted, so a window never claims a commit it cannot place. A partition is a
lens on the files, not a thing the table stores — see `docs/partition.md`.
`partition ls` then re-reads the window's commits and lists the files they
added — the env rides on every file, and that stream is what `bytemass`
measures.

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ pqbench schema ls dbx_samples.nyctaxi --format json |
    pqbench credentials check |
    pqbench credentials get |
    pqbench table ls
table                 first_time     last_time  commits
--------------------  -----------  ------------  -------
…                     …            …                   1
partitions: 1
```

```console no-run
$ PQB_ENDPOINT=… pqbench table info pqbench.demo.events
name                 format  snapshot  columns  location
-------------------  ------  --------  -------  ----------------------
pqbench.demo.events  delta          0        2  s3://lakehouse/unity/events
tables: 1
```

List a table's partitions by URI; a terminal prints them:

```console run delta
$ pqbench table ls docker/e2e-lakehouse/table --format table
table                          first_time      last_time  commits
--------------------------  -------------  -------------  -------
docker/e2e-lakehouse/table  1789862400000  1789948800000        1
partitions: 1
```

List the catalogs at the endpoint:

```console no-run
$ pqbench metastore ls < source.json
name         catalog_type
-----------  ---------------
dbx_samples  MANAGED_CATALOG
samples      SYSTEM_CATALOG
catalogs: 2
```

Read one catalog's record:

```console no-run
$ pqbench catalog info dbx_samples < source.json
name         catalog_type     comment         owner
-----------  ---------------  --------------  -----------------
dbx_samples  MANAGED_CATALOG  sample catalog  owner@example.com
catalogs: 1
```

List the schemas in a catalog:

```console no-run
$ pqbench catalog ls dbx_samples < source.json
catalog      name
-----------  ----------------
dbx_samples  bakehouse
dbx_samples  nyctaxi
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
  `credentials check` → `credentials get` → `table info` walk.

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
[profile.md](profile.md), [experiment.md](experiment.md), [skill.md](skill.md),
[demo.md](demo.md).

## File footer metadata

The `bytemass-file` machine record includes a `metadata` object, even for an
empty file. It contains `creator`, the footer `format_version`, ordered
`key_values` (including duplicate keys), and `row_groups`. Metadata values
are UTF-8-safe previews of at most 256 bytes; `value_bytes` and `truncated`
distinguish previews from complete values. Missing values remain null.

Each row group reports its rows and compressed/uncompressed column-chunk
bytes, plus `column_indexes` and `offset_indexes` booleans in leaf-column
order. Presence is available without loading index contents. These facts need
only the footer; `--indexes` still controls the extra index read. Footer
format version does not identify the data-page version or compression level.

## Selecting files

There is no built-in file filter: `partition ls` streams every file the
window's commits added, and `bytemass` measures each, so which files to measure
is a shell job on the stream (`jq`, `sort`, `head`, `xargs`) — see
[demo.md](demo.md).

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
Use `table ls | partition ls` for Delta/Iceberg analysis to avoid counting
obsolete files.

## Page headers without indexes

`bytemass --pages` emits a `bytemass-page` record for every page, including
its row group, leaf path, ordinal, absolute offset, header length, page type,
compressed/uncompressed payload sizes, encoding, and applicable value or
dictionary-entry counts. A v2 page also reports row count. A v1 value count
is not a row count for repeated columns.

```console
$ pqbench bytemass data.parquet --pages --format table
```

This opt-in scan works without ColumnIndex/OffsetIndex and skips compressed
payloads without decoding them. Small header reads can include up to 4 KiB
of payload read-ahead; unusually large headers are capped at 1 MiB. Remote
reads reuse object identity conditions. Ordinary bytemass remains footer-only;
`--indexes` independently loads index contents. Header sizes exclude the
header itself, while chunk sizes include it. Checksums of skipped payloads
are not verified. Encrypted headers are unsupported.

Observed dictionary fallback and page boundaries do not establish which
writer thresholds caused them. `viz` ignores page records and continues to
render the column totals from the same stream.

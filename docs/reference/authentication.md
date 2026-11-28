# Authentication

Local paths need no credentials. Remote objects and catalogs do. A table's
vended lease overrides the process environment for that read and is **not**
written back into it, nor onto a document. The
[Databricks authentication guide](../guides/auth.md) creates the service
principal; this page is the reference for the variables and the credential
flow.

## Object storage (`s3://`)

Needs `--features aws` (or `delta-s3` / `iceberg-s3`). The storage adapter
builds an `AmazonS3Builder` from the default AWS provider chain, then applies
the table's vended lease (or the daemon's answer for it), when there is one.

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

### Archive classes and tiers

An object in S3 `GLACIER` or `DEEP_ARCHIVE`, or in an Intelligent-Tiering
`ARCHIVE_ACCESS` or `DEEP_ARCHIVE_ACCESS` tier, cannot be read until a restore
has finished (`GLACIER_IR` and GCS `ARCHIVE` are online and always readable).
`get partition-price` HEADs the files it samples without reading them, so it
still prices an archived partition; its `restore_required` field records
whether any sampled file needs a restore, and a completed restore clears it.
A command that would read the body — `bytemass`, `download`, `list
suggestions` — fails early with an error naming the storage class and the need
to restore, instead of paying a storage request that can only fail. An absent
or unknown class is read as usual.

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
| `PQB_CLIENT_ID` / `PQB_CLIENT_SECRET` | a service principal, when no static token is set |
| `PQB_TABLE_FORMAT` | `unity` (the default) or `iceberg`; for `iceberg` the endpoint names the catalog base (`{root}/v1` or `{root}/v1/{prefix}`) |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | a lower-priority fallback for the endpoint and bearer |
| `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` | the same fallback for a service principal |

The token is a Bearer on the **catalog** API only. Only `AWS_*` is copied onto
listed tables. A PAT does not open `s3://`.

### Service principal (OAuth M2M)

A service principal authenticates with a client id and secret instead of a
long-lived token. Name them in the profile, or in the environment:

```ini
[DEFAULT]
endpoint      = https://dbc-abc123.cloud.databricks.com
client-id     = 0123-4567
client-secret = …
table_format  = unity
```

With no static `token`, pqbench mints the bearer from
`POST {endpoint}/oidc/v1/token` — HTTP Basic over `client_id:client_secret`,
`grant_type=client_credentials`, `scope=all-apis` — when the daemon needs one.
The daemon holds the token and mints the next one five minutes before it
expires, or at half its lifetime for a token shorter than ten minutes; a walk
therefore mints one token an hour rather than one per request. `pqb setup`
mints one for the exports, and `pqb setup --check` proves one end to end.

A static `token` always wins, so a workspace with both configured uses the
bearer an operator handed it. Neither the client secret nor a minted bearer is
ever printed: `--check` reports the token's lifetime, `pqb setup` exports the
token alone (never the secret), and an error carries only the endpoint's
`error`/`error_description`.

`get metastore` reads the same endpoint and reports its metastore record
(name, uid, cloud_provider, region) as `Metastore`. `list catalogs` lists the
catalogs at the endpoint, one `Catalog` line each (name, catalog_type); a
system catalog (`CATALOG_SYSTEM`) and a catalog whose managed storage is
Databricks default storage (`CATALOG_DB_STORAGE`), which no reader outside the
workspace's compute can open, are not listed. `get catalog` reports one
catalog's record (name, catalog_type, comment, owner) as a single `Catalog`
line. `list schemas` lists the schemas in the catalog, one `Schema` line each
(name, comment, uri); a schema the catalog keeps for itself
(`SCHEMA_SYSTEM`, like `information_schema`) holds views over metadata and is
not listed. Unity REST serves `/schemas`; with
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
the level above backpressures instead of buffering. A ref whose read fails — a
timeout, a dropped connection, an endpoint answer such as a 429, or a storage
fault — is dropped by the level: it names the ref and the cause on standard
error, finishes the stream, and exits non-zero with the count, so one bad ref
does not truncate a walk. The drop is only the read: a ref that does not name a
resource, a record whose fields are wrong, or an output failure still fails the
level. With a daemon running, it makes every catalog request and paces Unity at
15/s, so the levels do not answer 429 in the first place; a transient failure
can still make a level fail, and the shell can retry the pipeline as it is.

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
is not a table: it has no files, so `list tables` does not list it. Refs are
addresses; the
walk context (endpoint, token, storage options) comes from the environment
(`PQB_*`, `AWS_*`). The walk is a plain pipeline: every command streams refs
and keeps `--fan-out` in flight. Each storage-reading command resolves the
table's lease for itself — through the daemon when one is running, else by
calling the catalog's vending route — and keeps it in memory. A view, a system
table, or Databricks default storage has no lease the catalog will mint; that
command fails for that ref, names the cause, drops it, and keeps the stream:

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ pqb daemon start
$ echo '"dbx_samples.nyctaxi"' |
>     pqb ls tables |
>     pqb get table |
>     pqb ls files |
>     pqb bytemass
```

`pqb setup` prints that environment for the shell to evaluate. Point
`PQB_CONFIG_FILE` at the committed [`examples/pqbenchrc`](../../examples/pqbenchrc)
to see the output without a workspace:

```console run
$ PQB_CONFIG_FILE=examples/pqbenchrc pqb setup
export PQB_ENDPOINT='https://dbc-abc123.cloud.databricks.com'
export PQB_TOKEN='dapi-example'
export PQB_TABLE_FORMAT='unity'
export AWS_EC2_METADATA_DISABLED='true'
```

It resolves the endpoint and token the way the Databricks SDKs do — the flag,
then `PQB_*`, then `DATABRICKS_*` — and adds `AWS_REGION` and
`AWS_EC2_METADATA_DISABLED=true` (the AWS SDK otherwise probes EC2 metadata for
the region, which hangs where that endpoint is blackholed). A workspace
configured as a service principal exports a bearer minted for that run. A
notebook kernel's dbutils context is not visible to a subprocess, so a notebook
sets `DATABRICKS_*` from it first.

`pqb setup --check` is the preflight: it runs no exports, and reports three
checks, one line each, then exits non-zero when any failed.

```console no-run
$ pqb setup --check
ok    config   endpoint https://dbc-abc123.cloud.databricks.com, dialect unity, a service principal's client id and secret
ok    auth     service-principal token minted, expires in 3600 s
ok    catalog  GET https://dbc-abc123.cloud.databricks.com/api/2.1/unity-catalog/catalogs?max_results=1 answered 200
```

`config` is what the profile, the environment, and the flags resolved — the
endpoint, the dialect, and which credential the run would send, described and
never shown. `auth` is that credential (a mint for a service principal). `catalog`
is one page of the endpoint's first listing level under that credential: the
Unity catalogs, or the Iceberg REST base's namespaces. A failed check does not
stop the next one, so one run reports every problem.

## The daemon

Each stage of a pipeline is its own process, so a process-local cache would
still ask the catalog for the same table once per stage. `pqbench daemon start`
runs one daemon for the resolved identity and every stage uses it:

```console no-run
$ pqbench daemon start
$ pqbench daemon status
running /run/user/1000/pqbench/d-0123456789abcdef.sock
$ pqbench daemon stop
```

The socket path is derived from the endpoint, dialect, and a fingerprint of
the token, under `$XDG_RUNTIME_DIR` (Linux) or `$TMPDIR`; two principals on the
same workspace get different daemons, and no environment variable is needed.
`PQB_DAEMON_SOCK` overrides the path for a sidecar or a test. The socket is 0600
in a 0700 directory, so only the same user can connect.

The daemon makes every catalog request for the walk — the `list` levels and
every `get` level — and paces Unity at 15/s, the catalog's per-endpoint ceiling,
so a walk with many refs in flight does not trip the rate limit. It caches the
lease per table and refreshes it five minutes before the catalog's expiry — or at half the remaining lifetime when the lease is shorter
than ten minutes. Unity reports `expiration_time` in epoch milliseconds;
Iceberg reports `s3.session-token-expires-at-ms`; when neither is present the
daemon assumes the usual one-hour lifetime and refreshes at 55 minutes. A
catalog that vends nothing is remembered for five minutes, so a file walk does
not ask once per file.

Every catalog call goes through the daemon: a command that needs the catalog
and finds no daemon on its socket fails with a hint to run `pqbench daemon
start`, and makes no call of its own. A notebook, a serverless job, or a
scheduler task starts one first (`pqbench daemon start`, or `pqbench daemon
serve` as a sidecar). A read of local files names no endpoint and needs no
daemon.

A ref the catalog will not vend for is dropped with the cause on standard
error, and that stage exits non-zero; run the walk over a catalog that is
externally readable, or filter the refs first.

### The driver

`pqbench-driver start --store DIR` runs the partition-version state driver as
its own process, with its own socket (`{DIR}/driver.sock`, or the path
`PQB_DRIVER_SOCK` names). Its whole state is one map of partition versions
(`PartitionVersion`, a partition at one etag). The work is plain pqbench
commands, which pqbench-driver runs as child processes and awaits; the driver
decides what runs next for a version and what a finished command means for it.
The catalog daemon is independent: pqbench-driver's commands talk to it when
one is running, and local tables need none.

- **The walk** runs `list catalogs | list schemas | list tables | get table |
  list partitions`: every table the endpoint's metastore shows. With
  `--walk-tables FILE` it runs `list partitions` over the Table records in
  FILE instead (for local tables). It runs once when pqbench-driver
  starts, then every `--walk-period-seconds`. A partition at a new etag is a
  new `PENDING` version, and the etag it replaces leaves the map; so does a
  listed table's version the walk no longer finds, and when every table was
  read, the versions of the tables it did not list. Their results are renamed
  under `DIR/deleted/` and removed from there. A walk where a table failed
  deletes nothing: only a complete listing can say a version is gone.
- **Each version** goes through one command at a time, each from the state
  before it to the state after it: `price` (`get partition-price`),
  `download` (the partition's sample file into the version's directory),
  `bytemass` and `lint` (`bytemass --local` and `list suggestions --local` on
  that copy), then `verify` (`get suggestion --local` on the potential
  suggestions, while the copy is still there; the three partition rules that
  need no file — `tombstones`, `small-files`, and `delete-files` — are added to
  the same record set).
  The fifteen column rules (`uuid-text`, `base64-text`, `int96-timestamp`,
  `text-timestamp`, `numeric-text`, `codec-zstd`, `page-size`, `delta-integer`,
  `int64-narrow`, `decimal-narrow`, `dictionary-unique`, `dictionary-missing`,
  `delta-text-encoding`, `dictionary-fallback`, `stats-truncated`) each name
  their change in every engine pqbench knows, so the driver records one
  suggestion per rule with the `remedies` for spark, pyarrow, parquet-rs, Delta,
  Iceberg, and DuckDB. One command runs per CPU at once,
  taking the newest data first and then the largest partition. A version
  with no live file, or whose sampled files need a restore, is `UP_TO_DATE`
  after its price. With `--low-network` no file is copied: after
  its price a version runs `bytemass` on the sample file in place, which reads
  only the footer, and then verifies from the partition's metadata alone,
  since it has no column suggestion to measure. A failure puts the version
  back with the command's error; after two it waits, and after six it is
  `FAILED` — quarantined, since a record that is permanently bad reads no
  better on the seventh attempt, and retrying it only re-downloads it every
  backoff window. A quarantined version is not rescheduled, and a restart keeps
  it that way: its state, failure count and error are the checkpoint's. A walk
  that finds a new etag — a partition that was written — makes a new version,
  and that one is tried.

A command's output lands in `DIR/results/{partition}/{etag}/` through a
temporary file and is renamed into place when the command succeeds. The map is
written whole to `DIR/checkpoint.ndjson` every 5 minutes while commands run,
at once when none do, and on stop. A restart loads it and puts each running
version back; a crash loses at most the changes since the last checkpoint,
and the commands redo them.

`pqbench list partition-versions [PARENT]` prints the map as `PartitionVersion`
records, named `…/partitions/{partition}/versions/{etag}`: all of them, or those
under a catalog, schema, table or partition name. It reads the map from
pqbench-driver over `PQB_DRIVER_SOCK`; with the variable unset it fails with a
hint to start pqbench-driver. The checkpoint is the same records, and `pqbench
summarize partition-versions` totals them per catalog, schema and table as
`PartitionStats` records.

`pqbench get partition-version NAME` reads one version's stored results: its
`PartitionPrice`, its lint `Suggestion`s, its column masses from `bytemass`,
then its listing, one record per line. NAME is a version
(`…/partitions/{partition}/versions/{etag}`), or a partition for its newest
live version. A version with no results yet reports its state instead.

`pqbench-driver status [--socket PATH]` prints the map as one JSON object:
`state_counts` (the versions in each `PartitionVersion.State`),
`total_versions`, `running_commands` (the worker commands in flight),
`failing_versions` (versions whose current transition failed and that back off
before retrying, or that are quarantined), `last_walk_time`, and
`last_walk_error` when that walk failed. Four counters only ever grow while the
driver runs: `completed_run_count` (runs that reached their transition's done
state), `failed_run_count` (runs that ended in an error, timeouts included),
`timed_out_run_count` (runs whose command was killed at `step-timeout`), and
`quarantined_version_count` (versions that reached `FAILED`).

`pqbench-driver list suggestions [PARENT]` prints the `Suggestion` records the
driver measured, one per line, largest saving first, all of them or those under
a catalog, schema, table or partition name. The driver holds each version's
records in its map as soon as the version is done, so a listing over a whole
lake reads the results it has not read yet, not every version's file. Each record is a version's
potential column suggestion measured against its control, or one of the three
partition rules that need no file: `tombstones` (removed files no active file
holds — Delta's tombstones, and Iceberg's deleted entries and superseded
snapshots — with the log's own byte count as the saving), `small-files` (many
small files, with the compaction remedy each engine spells), and `delete-files`
(Iceberg merge-on-read delete files a scan still applies, with the rewrite that
merges them into the data). A partition rule's `name` is
`{partition}/suggestions/{rule}`, its `partition` names the partition, and its
`column` is empty.

One setting of the driver's own — `step-timeout` — lives only in the same
run-control file; see
[Configuration](configuration.md#driver-settings).

`pqbench-driver stop` sends a stop over the socket. `SIGINT` and `SIGTERM` do
the same: the driver aborts the runs in flight (their commands are killed with
them), checkpoints the map, removes its socket, and exits 0.

```console no-run
$ pqbench-driver start --store /var/lib/pqbench --walk-period-seconds 86400
$ export PQB_DRIVER_SOCK=/var/lib/pqbench/driver.sock
$ pqbench list partition-versions catalogs/main/schemas/sales
$ pqbench get partition-version catalogs/main/schemas/sales/tables/orders/partitions/day%3D1
$ pqbench-driver status
$ pqbench-driver list suggestions catalogs/main/schemas/sales
$ pqbench-driver stop --socket /var/lib/pqbench/driver.sock
```

## Databricks-governed tables

A governed table has two access modes, and pqbench is the second one:

- **Engine-mediated.** Notebooks, SQL, and BI drivers ask Databricks compute to
  read the table; the platform reaches storage with its own identity and no
  cloud credentials reach the caller. Use `spark.sql` / the SQL warehouse for
  this; pqbench does not.
- **Credential-mediated.** An external reader calls the catalog's vending route
  and reads storage itself under a short-lived, downscoped lease — the
  `get table` → `list files` → `bytemass` walk, with the lease held by the
  reader (or the daemon) rather than carried on the documents.

Whether the second mode exists is the storage's property, not the caller's:

| Table storage | External read under a vended lease |
| --- | --- |
| External location (customer S3), external table | yes |
| External location, managed table | yes |
| Databricks default storage, managed table | no — the catalog reports `TABLE_DB_STORAGE`; the vending route refuses, and the objects deny externally issued sessions |
| Managed volume | files via FUSE in compute or the Files API; not a table read |

The vending route is the answer: it mints a lease for an external location and
refuses a view, a system table, or Databricks default storage. There is no
separate eligibility stage; the storage-reading command that asked for the
lease reports the refusal and drops that ref.

Two environment notes:

- The vended response carries the keys, a session token, and the storage URL,
  but no region: set `AWS_REGION` (or the client's equivalent) for the read.
- Inside serverless compute, Unity refuses to mint storage credentials to
  notebook code (`UC_SERVERLESS_UNTRUSTED_DOMAIN_STORAGE_TOKEN_MINTING`); the
  reader falls back to the ambient `AWS_*` and the compute's own engines.
  Classic compute reaches storage through its instance profile.

## References

- Temporary table credentials: <https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials>
- AWS default credential chain: <https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html>
- Iceberg REST catalog: <https://iceberg.apache.org/docs/latest/rest-catalog-spec/>

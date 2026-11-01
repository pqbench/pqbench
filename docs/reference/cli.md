# CLI reference

This document contains the help content for the `pqbench` command-line program.

**Command Overview:**

* [`pqbench`↴](#pqbench)
* [`pqbench diff`↴](#pqbench-diff)
* [`pqbench lz`↴](#pqbench-lz)
* [`pqbench compression`↴](#pqbench-compression)
* [`pqbench bytemass`↴](#pqbench-bytemass)
* [`pqbench get`↴](#pqbench-get)
* [`pqbench get metastore`↴](#pqbench-get-metastore)
* [`pqbench get catalog`↴](#pqbench-get-catalog)
* [`pqbench get schema`↴](#pqbench-get-schema)
* [`pqbench get table`↴](#pqbench-get-table)
* [`pqbench format`↴](#pqbench-format)
* [`pqbench table-file`↴](#pqbench-table-file)
* [`pqbench list`↴](#pqbench-list)
* [`pqbench list catalogs`↴](#pqbench-list-catalogs)
* [`pqbench list schemas`↴](#pqbench-list-schemas)
* [`pqbench list tables`↴](#pqbench-list-tables)
* [`pqbench list files`↴](#pqbench-list-files)
* [`pqbench list columns`↴](#pqbench-list-columns)
* [`pqbench sample`↴](#pqbench-sample)
* [`pqbench sample files`↴](#pqbench-sample-files)
* [`pqbench credentials`↴](#pqbench-credentials)
* [`pqbench credentials get`↴](#pqbench-credentials-get)
* [`pqbench credentials check`↴](#pqbench-credentials-check)
* [`pqbench ratelimit`↴](#pqbench-ratelimit)
* [`pqbench profile`↴](#pqbench-profile)
* [`pqbench summarize`↴](#pqbench-summarize)
* [`pqbench summarize columns`↴](#pqbench-summarize-columns)
* [`pqbench skill`↴](#pqbench-skill)
* [`pqbench setup`↴](#pqbench-setup)

## `pqbench`

pqbench measures how Parquet files spend bytes (bytemass), how well codecs
compress them (lz, compression), what a Delta or Iceberg table's files hold
(list files), and what a decoded row sample holds (profile).
Commands compose on pipes: each writes a resource the next command reads.

```text
  get           → Metastore / Catalog / Schema / Table
                  one resource of the walk
  list          → Catalog / Schema / Table / TableFile / Column
                  a collection of the walk
  sample        → TableFile           keep a rate of them, by size
  credentials   → Table          check + vend read credentials
  bytemass      → Blob + Column        footer byte masses (alias for list columns)
  summarize     → ColumnMass           estimate each column's mass (summarize columns)
  skill         → markdown             bundled agent recipes (write / DDL / codec)
  format        → tables / HTML        render a stream (--html for the treemap)
```

The walk reads with two verbs: `get` reads one resource, `list` lists a
collection; `sample` and `summarize` reduce one.

Standard output is NDJSON; `pqbench format` renders it as tables.
Subcommand help is local (`pqbench get --help`).
Auth, documents, and format skills are here.

Guide: docs/reference/cli.md

**Usage:** `pqbench [OPTIONS] <COMMAND>`

```console no-run
Examples:
  pqbench table-file data.parquet | pqbench bytemass
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass
  pqbench list catalogs | pqbench list schemas | pqbench list tables
  pqbench list catalogs | pqbench list schemas | pqbench list tables \
    | pqbench credentials check | pqbench credentials get | pqbench get table \
    | pqbench list files | pqbench bytemass
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench skill parquet-advisor
  pqbench list catalogs | pqbench format

Documents:
  Metastore              the endpoint's metastore record (get metastore)
  Catalog                the endpoint's catalogs (list catalogs) / one record (get catalog)
  Schema                 a catalog's schemas (list schemas) / one record (get schema)
  Table                  a schema's tables (list tables) / one record (get table, credentials get)
  TableFile              one table file (list files, sample files, table-file)
  Blob                   one file's storage facts (list columns)
  Column                 one file column and its byte mass (list columns)
  ColumnMass             one table column's estimated mass (summarize columns)
  ColumnProfile          one column's profile (profile | format)
  pqbench.skill          name + description (pqbench skill with no args)
```

Features: delta / iceberg to load those logs; aws / delta-s3 / iceberg-s3
for s3://. A missing feature fails at runtime and names itself.

```console no-run
Auth (how to reach data):
  Local paths need no credentials.

  s3://  (needs --features aws / delta-s3 / iceberg-s3)
    Default AWS provider chain (process env, shared config, instance role,
    AWS_PROFILE). A ref's env (a vended lease) overrides that chain; it is
    not exported into the process. Only AWS_* names are accepted on refs.

    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN     STS or catalog-vended temp keys
    AWS_REGION
    AWS_ENDPOINT          S3-compatible host (also set AWS_ENDPOINT_URL)
    AWS_ALLOW_HTTP        true for http://
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST   false for path-style (MinIO, rustfs)
    AWS_SKIP_SIGNATURE    true for a public bucket

  Catalog config  (PQB_ENDPOINT / PQB_TOKEN)
    endpoint + token   a Databricks workspace URL + PAT (dapi-…), or a
    Unity OSS / Iceberg REST URL. The token is a Bearer on the list API
    only. Only AWS_* is copied onto listed tables — a PAT is not an S3 key.

  Databricks-governed tables
    The walk vends storage credentials: `credentials get` materializes the
    vended keys onto the refs, and `get table` reads under the env it is
    given. Reading the objects still needs keys that can GetObject.
    https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials

  Producer pipe
    pqbench table-file s3://bucket/table/part-0.parquet | pqbench bytemass
    (the table-file record names the blob; AWS_* in the environment or a
     vended lease supplies storage access)

Format skills (read these when the input is Parquet or a table):
  Parquet file format      https://parquet.apache.org/docs/file-format/
  Parquet thrift spec      https://github.com/apache/parquet-format
  Delta transaction log    https://github.com/delta-io/delta/blob/master/PROTOCOL.md
  Iceberg table spec       https://iceberg.apache.org/spec/
  Iceberg REST catalog     https://iceberg.apache.org/docs/latest/rest-catalog-spec/
  Unity / Databricks list  https://docs.databricks.com/api/workspace/tables/list
  AWS default credentials  https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html
  pqbench command guide    docs/reference/cli.md
  pqbench table docs       docs/guides/delta.md  docs/guides/iceberg.md  docs/reference/format.md
```

###### **Subcommands:**

* `diff` — Compare compressed column bytes from Parquet files or bytemass streams
* `lz` — Codec sweep over raw file bytes (lzbench-style)
* `compression` — Codec sweep over encoded Parquet pages (NONE-compressed input only)
* `bytemass` — Per-column on-disk bytes from Parquet footers (alias for `list columns`)
* `get` — Read one resource of the walk
* `format` — Render a stream as tables, or a summarize columns stream as HTML
* `table-file` — Name storage paths as table-file records
* `list` — List a collection of the walk
* `sample` — Keep a rate of a table's files, by size
* `credentials` — Check and vend read credentials for table-refs
* `ratelimit` — Pace an NDJSON ref stream to a records-per-second rate
* `profile` — Per-column facts from a decoded row sample (nulls, NDV, entropy, runs)
* `summarize` — Estimate each table column's mass from a list columns stream
* `skill` — Print a bundled agent skill (write / DDL / compression recipes)
* `setup` — Print the walk's environment as shell exports to eval

###### **Options:**

* `--profile <NAME>` — the `~/.pqbenchrc` profile to run under [env: PQB_PROFILE]
* `--endpoint <URL>` — workspace URL; overrides the profile [env: PQB_ENDPOINT]
* `--token <TOKEN>` — bearer token; overrides the profile [env: PQB_TOKEN]
* `--table-format <FORMAT>` — catalog dialect: unity (the default) or iceberg [env: PQB_TABLE_FORMAT]



## `pqbench diff`

Compare compressed column bytes from Parquet files or bytemass streams

**Usage:** `pqbench diff [OPTIONS] <LEFT> <RIGHT>`


###### **Arguments:**

* `<LEFT>`
* `<RIGHT>`

###### **Options:**

* `--depth <DEPTH>` — Roll up dotted physical column paths to N components (positive)



## `pqbench lz`

Compress the whole file as opaque bytes. -c codec@level is repeatable;
omitting -c sweeps every wired codec (zstd, lz4, gzip, snappy). --samples
timed passes after --warmup-iterations. --mode fastest keeps the best pass;
mean averages all. The report is composable NDJSON; `pqbench format`
renders it as a table.

**Usage:** `pqbench lz [OPTIONS] <FILE>`

```console no-run
Examples:
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench lz file.bin | pqbench format

See also:
  pqbench compression --help  same sweep over Parquet pages (NONE input)
  pqbench --help
  docs/reference/cli.md
```

###### **Arguments:**

* `<FILE>` — input file

###### **Options:**

* `-c <codec@level>` — codec@level, repeatable; default: all wired codecs
* `--samples <SAMPLES>` — timed passes to collect per sweep (after warmup)

  Default value: `10`
* `--warmup-iterations <WARMUP_ITERATIONS>` — timed passes to discard before sampling (cold-start effects)

  Default value: `3`
* `--mode <MODE>` — how to reduce the samples: fastest = mean over the best pass, mean = mean over all

  Default value: `fastest`

  Possible values: `fastest`, `mean`




## `pqbench compression`

Same sweep as lz, but over each column chunk's encoded pages. The file must
be NONE-compressed; a compressed file is rejected. --per-column adds one
report row per column chunk. The report is composable NDJSON (report rows,
then per-column rows); `pqbench format` renders it as a table.

**Usage:** `pqbench compression [OPTIONS] <FILE>`

```console no-run
Examples:
  pqbench compression data.parquet
  pqbench compression data.parquet --per-column
  pqbench compression data.parquet -c zstd@3 | pqbench format

See also:
  pqbench lz --help        raw-byte sweep (any file)
  pqbench bytemass --help  on-disk bytes without a codec sweep
  pqbench --help
  docs/reference/cli.md
```

###### **Arguments:**

* `<FILE>` — input file

###### **Options:**

* `-c <codec@level>` — codec@level, repeatable; default: all wired codecs
* `--samples <SAMPLES>` — timed passes to collect per sweep (after warmup)

  Default value: `10`
* `--warmup-iterations <WARMUP_ITERATIONS>` — timed passes to discard before sampling (cold-start effects)

  Default value: `3`
* `--mode <MODE>` — how to reduce the samples: fastest = mean over the best pass, mean = mean over all

  Default value: `fastest`

  Possible values: `fastest`, `mean`

* `--per-column` — report per-column breakdown



## `pqbench bytemass`

Read Parquet footers only and report the on-disk bytes of each column, its
chunks folded over the file's row groups. Works on compressed files
(HEAD + ranged GETs on a URI).

Inputs: a stream of table-file records on standard input. `list files`
names a table's files; `table-file PATH` names one path. A glob is a shell
job on that list, not a bytemass feature. A table-file record carries the
file's blob and, when vended, its lease.

One `Blob` per file and one `Column` per column, with its byte mass nested,
as NDJSON. `pqbench format` renders the columns as a table; `pqbench summarize
columns` folds the stream into one estimate per table column, and `pqbench
format --html` draws those rows. `pqbench bytemass` is an alias for `pqbench
list columns`.

**Usage:** `pqbench bytemass [OPTIONS]`

```console no-run
Examples:
  pqbench table-file data.parquet | pqbench bytemass
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass | pqbench summarize columns | pqbench format --html > report.html

See also:
  pqbench list --help               list the files this command measures
  pqbench summarize columns --help  fold the stream into per-column estimates
  pqbench format --help             draw those rows as a treemap
  pqbench --help           auth, documents, format skills
  docs/reference/cli.md
```

###### **Options:**

* `--fan-out <FAN_OUT>` — files in flight at once

  Default value: `128`



## `pqbench get`

Read one resource of the walk, by type:

```text
  metastore  the endpoint's record (name, uid, cloud, region), one line
  catalog    one catalog's record (name, catalog_type, comment, owner)
  schema     one schema's record (name, uid, display_name, comment, uri,
             properties), from a Schema ref
  table      one table's record (format, snapshot_version, declared_columns,
             partition_columns, properties), from a Table ref
```

The endpoint, bearer, and dialect come from PQB_ENDPOINT / PQB_TOKEN /
PQB_TABLE_FORMAT; a `get` reads a catalog, so it needs an endpoint. `get`
reads the matching resource records on standard input — one per line — so it
enriches a listing: `pqbench list catalogs | pqbench get catalog`. A JSON
record names the resource: `echo '"dbx_samples.nyctaxi"' | pqbench
get schema`.

Unity REST serves /schemas/{full_name} and /tables/{full_name}; the catalog's
declared columns and properties are merged over the Delta log, which is read
with without_files() — O(1) in files — so the table read needs a readable
storage location: the ref's uri or the process environment. Under Iceberg REST
(PQB_TABLE_FORMAT=iceberg) the schema level reads loadNamespace and the table
level reads loadTable, whose metadata is inline, so no storage read runs.

Vended credentials ride the refs from `credentials get`, and `credentials
check` gates the walk on tables the catalog marks readable outside compute. The
emitted record keeps its credentials, so a later stage reads the table's files
under the same lease.

The stream is NDJSON.

**Usage:** `pqbench get <COMMAND>`

```console no-run
Examples:
  pqbench get metastore
  pqbench list catalogs | pqbench get catalog
  echo '"dbx_samples.nyctaxi"' | pqbench list tables | pqbench get table
  pqbench list schemas | pqbench get schema
  pqbench list tables | pqbench get table

See also:
  pqbench list --help      the walk's collections
  pqbench bytemass --help  measure the files the walk names
  pqbench --help           auth (AWS_*, catalog token), documents, format skills
  docs/reference/cli.md  docs/guides/delta.md  docs/guides/iceberg.md
```

###### **Subcommands:**

* `metastore` — Read the endpoint's metastore record
* `catalog` — Read one catalog's record
* `schema` — Read one schema's record (`CATALOG.SCHEMA`)
* `table` — Read one table's record (`CATALOG.SCHEMA.TABLE`)



## `pqbench get metastore`

Read the endpoint's metastore record

**Usage:** `pqbench get metastore`


## `pqbench get catalog`

Read one catalog's record

**Usage:** `pqbench get catalog [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — requests in flight at once

  Default value: `64`



## `pqbench get schema`

Read one schema's record (`CATALOG.SCHEMA`)

**Usage:** `pqbench get schema [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — requests in flight at once

  Default value: `64`



## `pqbench get table`

Read one table's record (`CATALOG.SCHEMA.TABLE`)

**Usage:** `pqbench get table [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — tables in flight at once (default: cores) [env: PQB_TABLE_FAN_OUT]

  Default value: `the number of cores`



## `pqbench format`

Read NDJSON on standard input. Tables are the default, one table per record
kind; a record the curated type cannot hold still renders from its keys, and a
`summarize columns` row renders as a column-mass table. `--html` reads that
same stream and writes the static treemap page to standard output. No other
input is supported for `--html`.

**Usage:** `pqbench format [OPTIONS]`

**Command Alias:** `fmt`

```console no-run
Examples:
  pqbench list catalogs | pqbench format
  pqbench list files | pqbench list columns | pqbench summarize columns | pqbench format
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench list columns | pqbench summarize columns | pqbench format --html > report.html

See also:
  pqbench summarize columns --help  produce the stream --html draws
  docs/reference/format.md  docs/reference/cli.md
```

###### **Options:**

* `--html` — write an HTML treemap for a summarize columns stream



## `pqbench table-file`

Name storage paths as table-file records

**Usage:** `pqbench table-file <PATHS>...`


###### **Arguments:**

* `<PATHS>` — Parquet paths or storage URIs



## `pqbench list`

List a collection of the walk, by name:

```text
  catalogs    the catalogs at the endpoint, one Catalog line each
  schemas     the schemas in a catalog (Catalog refs on standard input), one
              Schema line each
  tables      the tables in a schema (Schema refs on standard input), one Table
              line each — the record `get table` enriches
  files       a table's active files (Table refs on standard input), one
              TableFile line each
  columns     a file's columns and their byte masses (TableFile refs on
              standard input), one Column line each
```

Unity REST serves /catalogs, /schemas, and /tables. Under
PQB_TABLE_FORMAT=iceberg the schema and table levels speak Iceberg REST
instead: the endpoint then names the catalog base ({root}/v1 or
{root}/v1/{prefix}) and they read /namespaces under it. No config probe runs.
Unity fills a table's storage location in the listing; Iceberg leaves it to
`get table`.

`list files` resolves a snapshot and writes its active files, one TableFile per
line, each with the table's lease — that stream is what `list columns` measures;
`bytemass` is an alias for that level. The files are the snapshot's own set, so
a file a later commit removed is absent;
nothing is collected, so a table of any size costs one log batch plus one file.
--last/--partition keep
only the files the log dates in a window (e.g. 1m, 1h, 1d, 1w, or one UTC day), so a
daily run reads the day's writes without walking what the table already held.

Every level reads its parent's refs on standard input and writes one resource
per line, so the walk is a pipe:

```console no-run
  pqbench list catalogs | pqbench list schemas | pqbench list tables |
  pqbench get table | pqbench list files | pqbench list columns
```

Detection: _delta_log is Delta (wins UniForm). Iceberg is
metadata/version-hint.text, metadata/*.metadata.json, or a .metadata.json path.
Needs --features delta and/or iceberg (delta-s3 / iceberg-s3 for s3://).

The stream is NDJSON.

**Usage:** `pqbench list <COMMAND>`

**Command Alias:** `ls`

```console no-run
Examples:
  pqbench list catalogs
  pqbench list catalogs | pqbench list schemas | pqbench list tables
  echo '"dbx_samples"' | pqbench list schemas
  echo '"dbx_samples.nyctaxi"' | pqbench list tables
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench list columns
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files --last 1d | pqbench list columns

See also:
  pqbench get --help          read one resource
  pqbench bytemass --help     alias for `list columns`
  pqbench --help           catalog auth, PQB_* config
  docs/reference/cli.md
```

###### **Subcommands:**

* `catalogs` — List the catalogs at the endpoint
* `schemas` — List the schemas in a catalog (`CATALOG`)
* `tables` — List the tables in a schema (`CATALOG.SCHEMA`)
* `files` — List a table's active files (Table refs on stdin)
* `columns` — List a file's columns and their byte masses (Table-file refs on stdin)



## `pqbench list catalogs`

List the catalogs at the endpoint

**Usage:** `pqbench list catalogs`


## `pqbench list schemas`

List the schemas in a catalog (`CATALOG`)

**Usage:** `pqbench list schemas [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — catalogs in flight at once

  Default value: `64`



## `pqbench list tables`

List the tables in a schema (`CATALOG.SCHEMA`)

**Usage:** `pqbench list tables [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — schemas in flight at once

  Default value: `64`



## `pqbench list files`

List a table's active files (Table refs on stdin)

**Usage:** `pqbench list files [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — tables in flight at once

  Default value: `64`
* `--partition <DATE>` — read only files the log dates in one UTC day: an ISO date, YYYY-MM-DD; e.g. 2026-10-28
* `--last <WINDOW>` — read only files the log dates in the last WINDOW, ending now: e.g. 1m, 1h, 1d, 1w



## `pqbench list columns`

List a file's columns and their byte masses (Table-file refs on stdin)

**Usage:** `pqbench list columns [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — files in flight at once

  Default value: `128`



## `pqbench sample`

Keep a sample of a table's files: read table-file records on standard input and
write the kept ones, each with `sample_rate` set to the probability it was kept
with.

A file is kept with probability rate x size / 256 MiB, capped at one, drawn
from a hash of its name, so the same record always makes the same decision. A
256 MiB file is kept at --rate (1/100 keeps about one in a hundred), a 1 GiB
file four times as often while below the cap, and --rate 1 keeps every file
that has a size. The probability sets the record's weight, its inverse: a sum
over kept records divides each term by `sample_rate`. A record that already
carries a `sample_rate` composes with the smaller of the two probabilities,
because both threshold the same hash.

No storage object is read: `list files` already carries the log's sizes, so the
stage streams and never holds the table. A record with no positive size is an
error, not a silent drop. The coverage line (files and bytes kept) prints on
standard error.

**Usage:** `pqbench sample <COMMAND>`

```console no-run
Examples:
  cat table.ndjson | pqbench list files | pqbench sample files --rate 1/100 | pqbench bytemass
  cat table.ndjson | pqbench list files | pqbench sample files --rate 1/10 | pqbench bytemass

See also:
  pqbench list files --help  the stream this command samples
  pqbench bytemass --help    measure the kept files
  pqbench --help             catalog auth, PQB_* config
  docs/reference/cli.md
```

###### **Subcommands:**

* `files` — Keep a rate of a table's files, by size



## `pqbench sample files`

Keep a rate of a table's files, by size

**Usage:** `pqbench sample files [OPTIONS]`


###### **Options:**

* `--rate <RATE>` — keep probability at 256 MiB; 1 keeps every file

  Default value: `1/100`



## `pqbench credentials`

Check and vend read credentials for table-refs. The endpoint, token, dialect,
and object-store options come from the environment: PQB_ENDPOINT / PQB_TOKEN /
PQB_TABLE_FORMAT and AWS_*.

```text
  check  the walk's single filter: each Table on
         standard input is checked against the catalog. A `system` catalog
         ref, a view (the catalog reports no location), and a table on
         Databricks default storage (`TABLE_DB_STORAGE`) all have no external
         read, so the check drops each with the reason on standard error;
         eligible refs pass through unchanged, so the stage composes ahead of
         `credentials get` and a mixed schema keeps going. Under
         PQB_TABLE_FORMAT=iceberg the metadata read is inline through the
         catalog, so the check passes.

  get    each Table enriched with the table's
         vended read credentials, held on the table — the explicit stage that
         materializes the lease for the table read and other tools. Unity GET /tables/{full_name} reads the
         table id and securable kind; Databricks default storage
         (`TABLE_DB_STORAGE`) passes through with its own env, and any other
         kind (or a catalog that reports none) is attempted; inside serverless
         compute Unity refuses to mint storage credentials, so refs pass
         through there too.
         Under PQB_TABLE_FORMAT=iceberg the command reads the catalog's
         credentials route (loadCredentials) and takes the storage-credentials
         it returns, for the next storage read; a catalog that does not serve
         it falls back to the delegated loadTable, and a table the catalog
         cannot serve via Iceberg passes through with its own env.
```

The stream is NDJSON.

**Usage:** `pqbench credentials <COMMAND>`

```console no-run
Examples:
  export PQB_ENDPOINT=… PQB_TOKEN=…
  echo '{"name":"catalogs/dbx_samples/schemas/nyctaxi"}' | pqbench list tables | pqbench credentials check | pqbench credentials get
  echo '{"name":"catalogs/dbx_samples/schemas/nyctaxi"}' | pqbench list tables | pqbench credentials check | pqbench credentials get | pqbench get table

See also:
  pqbench list --help  the tables in a schema
  pqbench --help          catalog auth, PQB_* config
  docs/reference/cli.md
```

###### **Subcommands:**

* `get` — Put vended read credentials on each table-ref
* `check` — Check each table-ref is readable outside Databricks compute



## `pqbench credentials get`

Put vended read credentials on each table-ref

**Usage:** `pqbench credentials get [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — requests in flight at once

  Default value: `64`



## `pqbench credentials check`

Check each table-ref is readable outside Databricks compute

**Usage:** `pqbench credentials check [OPTIONS]`


###### **Options:**

* `--fan-out <FAN_OUT>` — requests in flight at once

  Default value: `64`



## `pqbench ratelimit`

Pace an NDJSON ref stream: records pass through unchanged, delayed so each
record kind observes at most --rate records per second (15 by default; 0
turns pacing off). Nothing is dropped. One bucket per kind, so catalogs,
schemas, and table refs pace independently — the walk's per-endpoint pace.

```console no-run
  list catalogs | pqbench ratelimit | list schemas
  list schemas | pqbench ratelimit --rate 5 | list tables
```

A consumer that issues one request per record as it arrives sees the same
rate. A 429 fails the level; retry it at a lower --rate.

**Usage:** `pqbench ratelimit [OPTIONS]`

**Command Alias:** `rl`

```console no-run
Examples:
  pqbench list catalogs | pqbench ratelimit | pqbench list schemas
  pqbench list schemas | pqbench ratelimit --rate 5 | pqbench list tables

See also:
  pqbench list --help     the walk's collections
  pqbench get --help      read one resource of it
  docs/reference/cli.md
```

###### **Options:**

* `--rate <RATE>` — records per second per resource type; 0 turns pacing off [env: PQB_RATE]

  Default value: `15`



## `pqbench profile`

Read up to --rows leading rows and decode every value, then report one
column's nulls, distinct values, entropy, top values, min/max, string
lengths, runs, and monotonicity. Unlike bytemass (footer only), this
decodes data and reads row values; integer and number columns compare
numerically.

--columns GLOB is repeatable and matches whole column names; the default
keeps every column, and a glob matching nothing is an error. --rows is
`all` or `first:N` (default first:8192). --top bounds top_values per
column (default 8).

One record per column, as NDJSON.

**Usage:** `pqbench profile [OPTIONS] [INPUTS]...`

```console no-run
Examples:
  pqbench profile data.parquet
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench profile data.parquet --rows all | pqbench format

See also:
  pqbench bytemass --help  footer byte masses without decoding rows
  pqbench compression --help  codec sweep over encoded pages
  pqbench --help
  docs/reference/profile.md  docs/reference/cli.md
```

###### **Arguments:**

* `<INPUTS>` — parquet paths to sample

###### **Options:**

* `--columns <GLOB>` — column name glob, repeatable; default: every column
* `--rows <METHOD>` — how many leading rows to read: `all` or `first:N`

  Default value: `first:8192`
* `--top <TOP>` — number of top values per column

  Default value: `8`



## `pqbench summarize`

Read a `list columns` stream and write one `ColumnMass` row per table column: the
weighted bytes, values, and bytes per value over the files that measured it,
the distinct file count and storage classes, and the monthly S3 storage cost
(one candidate per class, and one per access sub-tier for Intelligent-Tiering).

Each additive field is divided by the record's `sample_rate`, so a
`sample files` stream estimates the table, not the sample. A row has no name:
it is a command document, not a resource. `pqbench format --html` draws the
rows as a static HTML treemap.

**Usage:** `pqbench summarize <COMMAND>`

```console no-run
Examples:
  pqbench table-file data.parquet | pqbench list columns | pqbench summarize columns
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench list columns | pqbench summarize columns
  pqbench list files | pqbench list columns | pqbench summarize columns | pqbench format --html > report.html

See also:
  pqbench list columns --help     produce the stream this command reduces
  pqbench format --help           draw them as a treemap
  docs/reference/cli.md
```

###### **Subcommands:**

* `columns` — Estimate each table column's mass from a bytemass stream



## `pqbench summarize columns`

Estimate each table column's mass from a bytemass stream

**Usage:** `pqbench summarize columns [INPUT]`


###### **Arguments:**

* `<INPUT>` — bytemass NDJSON stream; reads standard input when omitted



## `pqbench skill`

Print an agent skill that is compiled into this binary: this is a
document, like `--help`.

```console no-run
  pqbench skill                       list skills as pqbench.skill lines
  pqbench skill parquet-advisor       the advisor workflow
  pqbench skill parquet-advisor recipes
                                      write-path, table DDL, codec levels
```

The advisor turns bytemass / profile facts into recipes
(ingest settings, Iceberg/Delta/Spark/DuckDB DDL, compression level
with pros and cons). It does not measure files.

**Usage:** `pqbench skill [NAMES]...`

```console no-run
Examples:
  pqbench skill
  pqbench skill parquet-advisor
  pqbench list catalogs | pqbench format
  pqbench skill parquet-advisor recipes
  pqbench table-file data.parquet | pqbench bytemass
  pqbench profile data.parquet --rows first:8192

See also:
  pqbench profile --help     cheap column facts
  pqbench --help             documents
  docs/reference/skill.md  docs/reference/cli.md
```

###### **Arguments:**

* `<NAMES>` — skill name, or `name document` (`parquet-advisor recipes`)



## `pqbench setup`

Print the environment the metadata walk needs, as shell `export` lines, for
the caller to evaluate:

```text
  eval "$(pqbench setup)"
```

The endpoint and token resolve the way the Databricks SDKs do: the flag, then
PQB_ENDPOINT / PQB_TOKEN, then DATABRICKS_HOST / DATABRICKS_TOKEN. A notebook
kernel's dbutils context is not visible to a subprocess, so a notebook sets
DATABRICKS_* from it first. AWS_REGION comes from the flag or AWS_REGION (a
vended lease carries no region); AWS_EC2_METADATA_DISABLED=true keeps the AWS
SDK from probing EC2 metadata, which hangs where that endpoint is blackholed.

```text
  endpoint  --endpoint | PQB_ENDPOINT | DATABRICKS_HOST
  token     --token    | PQB_TOKEN    | DATABRICKS_TOKEN
  format    --table-format | PQB_TABLE_FORMAT (unity default)
  region    --region   | AWS_REGION
```

**Usage:** `pqbench setup [OPTIONS]`

```console no-run
Examples:
  eval "$(pqbench setup --endpoint https://… --token dapi-… --region us-east-1)"
  DATABRICKS_HOST=https://… DATABRICKS_TOKEN=dapi-… eval "$(pqbench setup)"
  pqbench setup --table-format iceberg --region us-west-2

See also:
  pqbench credentials --help  check and vend read credentials
  pqbench get --help          read a listed table's record
  pqbench --help              auth, PQB_* config
  docs/reference/cli.md
```

###### **Options:**

* `--region <REGION>` — bucket region [env: AWS_REGION]



<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>

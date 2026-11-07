//! `--help` copy. The root command is the long guide (auth, documents,
//! format skills). Each subcommand stays local and points back.

/// One-line summary for `pqbench -h`.
pub const ROOT_ABOUT: &str =
    "Measure Parquet storage and codec speed; pipe refs → list files → list columns → summarize";

/// Full summary for `pqbench --help`.
pub const ROOT_LONG_ABOUT: &str = "\
pqbench measures how Parquet files spend bytes (bytemass), how well codecs
compress them (lz, compression), what a Delta or Iceberg table's files hold
(list files), and what a decoded row sample holds (profile).
Commands compose on pipes: each writes a resource the next command reads.

  get           → Metastore / Catalog / Schema / Table
                  one resource of the walk
  list          → Catalog / Schema / Table / TableFile / Column
                  a collection of the walk
  sample        → TableFile           keep a rate of them, by size
  daemon        → leases               cache and refresh catalog-vended leases
  bytemass      → Blob + Column        footer byte masses (alias for list columns)
  summarize     → ColumnMass           estimate each column's mass (summarize columns)
  skill         → markdown             bundled agent recipes (write / DDL / codec)
  format        → tables / HTML        render a stream (--html for the treemap)

The walk reads with two verbs: `get` reads one resource, `list` lists a
collection; `sample` and `summarize` reduce one.

Standard output is NDJSON; `pqbench format` renders it as tables.
Subcommand help is local (`pqbench get --help`).
Auth, documents, and format skills are here.

Guide: docs/reference/cli.md";

/// Extensive after-text for `pqbench --help` only.
pub const ROOT_AFTER: &str = "\
Examples:
  pqbench table-file data.parquet | pqbench bytemass
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass
  pqbench list catalogs | pqbench list schemas | pqbench list tables
  pqbench daemon start
  pqbench list catalogs | pqbench list schemas | pqbench list tables \\
    | pqbench get table | pqbench list files | pqbench bytemass
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench skill parquet-advisor
  pqbench list catalogs | pqbench format

Documents:
  Metastore              the endpoint's metastore record (get metastore)
  Catalog                the endpoint's catalogs (list catalogs) / one record (get catalog)
  Schema                 a catalog's schemas (list schemas) / one record (get schema)
  Table                  a schema's tables (list tables) / one record (get table)
  TableFile              one table file (list files, sample files, table-file)
  Blob                   one file's storage facts (list columns)
  Column                 one file column and its byte mass (list columns)
  ColumnMass             one table column's estimated mass (summarize columns)
  ColumnProfile          one column's profile (profile | format)
  pqbench.skill          name + description (pqbench skill with no args)

Features: delta / iceberg to load those logs; aws / delta-s3 / iceberg-s3
for s3://. A missing feature fails at runtime and names itself.

Auth (how to reach data):
  Local paths need no credentials.

  s3://  (needs --features aws / delta-s3 / iceberg-s3)
    Default AWS provider chain (process env, shared config, instance role,
    AWS_PROFILE). A table's vended lease overrides that chain for its reads;
    it is held in memory and never exported or written to a document.

    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN     STS or catalog-vended temp keys
    AWS_REGION
    AWS_ENDPOINT          S3-compatible host (also set AWS_ENDPOINT_URL)
    AWS_ALLOW_HTTP        true for http://
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST   false for path-style (MinIO, rustfs)
    AWS_SKIP_SIGNATURE    true for a public bucket

  Catalog config  (PQB_ENDPOINT / PQB_TOKEN)
    endpoint + token   a Databricks workspace URL + PAT (dapi-…), or a
    Unity OSS / Iceberg REST URL. The token is a Bearer for catalog reads and
    for storage-lease vending; a PAT is not an S3 key.

  Databricks-governed tables
    Each storage-reading stage resolves the table's vended lease itself —
    through `pqbench daemon` when one is running, else by calling the
    catalog — and reads under it. The keys stay in memory; a table the
    catalog will not vend for is dropped with the cause.
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
  pqbench table docs       docs/guides/delta.md  docs/guides/iceberg.md  docs/reference/format.md";

pub const BYTEMASS_ABOUT: &str =
    "Per-column on-disk bytes from Parquet footers (alias for `list columns`)";

pub const BYTEMASS_LONG_ABOUT: &str = "\
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
list columns`.";

pub const BYTEMASS_AFTER: &str = "\
Examples:
  pqbench table-file data.parquet | pqbench bytemass
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass | pqbench summarize columns | pqbench format --html > report.html

See also:
  pqbench list --help               list the files this command measures
  pqbench summarize columns --help  fold the stream into per-column estimates
  pqbench format --help             draw those rows as a treemap
  pqbench --help           auth, documents, format skills
  docs/reference/cli.md";

pub const GET_ABOUT: &str = "Read one resource of the walk";

pub const GET_LONG_ABOUT: &str = "\
Read one resource of the walk, by type:

  metastore  the endpoint's record (name, uid, cloud, region), one line
  catalog    one catalog's record (name, catalog_type, comment, owner)
  schema     one schema's record (name, uid, display_name, comment, uri,
             properties), from a Schema ref
  table      one table's record (format, snapshot_version, declared_columns,
             partition_columns, properties), from a Table ref

The endpoint, bearer, and dialect come from PQB_ENDPOINT / PQB_TOKEN /
PQB_TABLE_FORMAT; a `get` reads a catalog, so it needs an endpoint. `get`
reads the matching resource records on standard input — one per line — so it
enriches a listing: `pqbench list catalogs | pqbench get catalog`. A JSON
record names the resource: `echo '\"dbx_samples.nyctaxi\"' | pqbench
get schema`.

Unity REST serves /schemas/{full_name} and /tables/{full_name}; the catalog's
declared columns and properties are merged over the Delta log, which is read
with without_files() — O(1) in files — so the table read needs a readable
storage location: the ref's uri or the process environment. Under Iceberg REST
(PQB_TABLE_FORMAT=iceberg) the schema level reads loadNamespace and the table
level reads loadTable, whose metadata is inline, so no storage read runs. A
table loadTable refuses as not Iceberg compatible falls back to the Unity
record and its own log (Delta wins UniForm), as the Unity dialect reads it.

The emitted record carries no credentials. Each storage-reading stage resolves
the table's vended lease itself — through the daemon when one is running, else
directly from the catalog — so a table the catalog will not vend for is dropped
there with the cause, and the level exits non-zero.

The stream is NDJSON.";

pub const GET_AFTER: &str = "\
Examples:
  pqbench get metastore
  pqbench list catalogs | pqbench get catalog
  echo '\"dbx_samples.nyctaxi\"' | pqbench list tables | pqbench get table
  pqbench list schemas | pqbench get schema
  pqbench list tables | pqbench get table

See also:
  pqbench list --help      the walk's collections
  pqbench bytemass --help  measure the files the walk names
  pqbench --help           auth (AWS_*, catalog token), documents, format skills
  docs/reference/cli.md  docs/guides/delta.md  docs/guides/iceberg.md";

pub const LIST_ABOUT: &str = "List a collection of the walk";

pub const LIST_LONG_ABOUT: &str = "\
List a collection of the walk, by name:

  catalogs    the catalogs at the endpoint, one Catalog line each
  schemas     the schemas in a catalog (Catalog refs on standard input), one
              Schema line each
  tables      the tables in a schema (Schema refs on standard input), one Table
              line each — the record `get table` enriches
  files       a table's active files (Table refs on standard input), one
              TableFile line each
  columns     a file's columns and their byte masses (TableFile refs on
              standard input), one Column line each

Unity REST serves /catalogs, /schemas, and /tables. Under
PQB_TABLE_FORMAT=iceberg the schema and table levels speak Iceberg REST
instead: the endpoint then names the catalog base ({root}/v1 or
{root}/v1/{prefix}) and they read /namespaces under it. No config probe runs.
Unity fills a table's storage location in the listing; Iceberg leaves it to
`get table`, and a table the Iceberg catalog will not serve (a Delta table:
'is not an Iceberg compatible table') falls back to the Unity record and its
own storage format, so Delta and UniForm tables walk under either dialect.

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

  pqbench list catalogs | pqbench list schemas | pqbench list tables |
  pqbench get table | pqbench list files | pqbench list columns

Detection: _delta_log is Delta (wins UniForm). Iceberg is
metadata/version-hint.text, metadata/*.metadata.json, or a .metadata.json path.
Needs --features delta and/or iceberg (delta-s3 / iceberg-s3 for s3://).

The stream is NDJSON.";

pub const LIST_AFTER: &str = "\
Examples:
  pqbench list catalogs
  pqbench list catalogs | pqbench list schemas | pqbench list tables
  echo '\"dbx_samples\"' | pqbench list schemas
  echo '\"dbx_samples.nyctaxi\"' | pqbench list tables
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench list columns
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files --last 1d | pqbench list columns

See also:
  pqbench get --help          read one resource
  pqbench bytemass --help     alias for `list columns`
  pqbench --help           catalog auth, PQB_* config
  docs/reference/cli.md";

pub const SAMPLE_ABOUT: &str = "Keep a rate of a table's files, by size";

pub const SAMPLE_LONG_ABOUT: &str = "\
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
standard error.";

pub const SAMPLE_AFTER: &str = "\
Examples:
  cat table.ndjson | pqbench list files | pqbench sample files --rate 1/100 | pqbench bytemass
  cat table.ndjson | pqbench list files | pqbench sample files --rate 1/10 | pqbench bytemass

See also:
  pqbench list files --help  the stream this command samples
  pqbench bytemass --help    measure the kept files
  pqbench --help             catalog auth, PQB_* config
  docs/reference/cli.md";

pub const DAEMON_ABOUT: &str = "Own the catalog calls and cache storage leases";

pub const DAEMON_LONG_ABOUT: &str = "\
Own every catalog request the walk makes and cache the storage lease each table
is read under. One daemon per resolved identity (endpoint, dialect, and a token
fingerprint); its socket is derived from that identity, so a reader finds it
without an environment variable.

  start   run it in the background, or reuse the one already running
  stop    stop it
  status  report whether it is running, and where its socket is
  serve   run it in the foreground, for a sidecar or a service manager

`list catalogs`, `list schemas`, `list tables`, and every `get` level ask the
daemon, which paces Unity at 15 requests a second, the catalog's per-endpoint
ceiling; the storage levels (`list files`, `list columns`) ask it only for the
table's lease. Without a running daemon each command makes its own catalog call
and paces nothing. Vended leases are session state and never ride a Table or
TableFile document.";

pub const DAEMON_AFTER: &str = "\
Examples:
  pqbench daemon start
  pqbench list tables | pqbench get table | pqbench list files | pqbench bytemass
  pqbench daemon status
  pqbench daemon stop

See also:
  pqbench --help  catalog auth, PQB_* config
  docs/reference/authentication.md";

pub const SUMMARIZE_ABOUT: &str = "Estimate each table column's mass from a list columns stream";

pub const SUMMARIZE_LONG_ABOUT: &str = "\
Read a `list columns` stream and write one `ColumnMass` row per table column: the
weighted bytes, values, and bytes per value over the files that measured it,
the distinct file count and storage classes, and the monthly S3 storage cost
(one candidate per class, and one per access sub-tier for Intelligent-Tiering).

Each additive field is divided by the record's `sample_rate`, so a
`sample files` stream estimates the table, not the sample. A row has no name:
it is a command document, not a resource. `pqbench format --html` draws the
rows as a static HTML treemap.";

pub const SUMMARIZE_AFTER: &str = "\
Examples:
  pqbench table-file data.parquet | pqbench list columns | pqbench summarize columns
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench list columns | pqbench summarize columns
  pqbench list files | pqbench list columns | pqbench summarize columns | pqbench format --html > report.html

See also:
  pqbench list columns --help     produce the stream this command reduces
  pqbench format --help           draw them as a treemap
  docs/reference/cli.md";

pub const FORMAT_ABOUT: &str = "Render a stream as tables, or a summarize columns stream as HTML";

pub const FORMAT_LONG_ABOUT: &str = "\
Read NDJSON on standard input. Tables are the default, one table per record
kind; a record the curated type cannot hold still renders from its keys, and a
`summarize columns` row renders as a column-mass table. `--html` reads that
same stream and writes the static treemap page to standard output. No other
input is supported for `--html`.";

pub const FORMAT_AFTER: &str = "\
Examples:
  pqbench list catalogs | pqbench format
  pqbench list files | pqbench list columns | pqbench summarize columns | pqbench format
  cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench list columns | pqbench summarize columns | pqbench format --html > report.html

See also:
  pqbench summarize columns --help  produce the stream --html draws
  docs/reference/format.md  docs/reference/cli.md";

pub const LZ_ABOUT: &str = "Codec sweep over raw file bytes (lzbench-style)";

pub const LZ_LONG_ABOUT: &str = "\
Compress the whole file as opaque bytes. -c codec@level is repeatable;
omitting -c sweeps every wired codec (zstd, lz4, gzip, snappy). --samples
timed passes after --warmup-iterations. --mode fastest keeps the best pass;
mean averages all. The report is composable NDJSON; `pqbench format`
renders it as a table.";

pub const LZ_AFTER: &str = "\
Examples:
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench lz file.bin | pqbench format

See also:
  pqbench compression --help  same sweep over Parquet pages (NONE input)
  pqbench --help
  docs/reference/cli.md";

pub const COMPRESSION_ABOUT: &str =
    "Codec sweep over encoded Parquet pages (NONE-compressed input only)";

pub const COMPRESSION_LONG_ABOUT: &str = "\
Same sweep as lz, but over each column chunk's encoded pages. The file must
be NONE-compressed; a compressed file is rejected. --per-column adds one
report row per column chunk. The report is composable NDJSON (report rows,
then per-column rows); `pqbench format` renders it as a table.";

pub const COMPRESSION_AFTER: &str = "\
Examples:
  pqbench compression data.parquet
  pqbench compression data.parquet --per-column
  pqbench compression data.parquet -c zstd@3 | pqbench format

See also:
  pqbench lz --help        raw-byte sweep (any file)
  pqbench bytemass --help  on-disk bytes without a codec sweep
  pqbench --help
  docs/reference/cli.md";

pub const PROFILE_ABOUT: &str =
    "Per-column facts from a decoded row sample (nulls, NDV, entropy, runs)";

pub const PROFILE_LONG_ABOUT: &str = "\
Read up to --rows leading rows and decode every value, then report one
column's nulls, distinct values, entropy, top values, min/max, string
lengths, runs, and monotonicity. Unlike bytemass (footer only), this
decodes data and reads row values; integer and number columns compare
numerically.

--columns GLOB is repeatable and matches whole column names; the default
keeps every column, and a glob matching nothing is an error. --rows is
`all` or `first:N` (default first:8192). --top bounds top_values per
column (default 8).

One record per column, as NDJSON.";

pub const PROFILE_AFTER: &str = "\
Examples:
  pqbench profile data.parquet
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench profile data.parquet --rows all | pqbench format

See also:
  pqbench bytemass --help  footer byte masses without decoding rows
  pqbench compression --help  codec sweep over encoded pages
  pqbench --help
  docs/reference/profile.md  docs/reference/cli.md";

pub const SKILL_ABOUT: &str = "Print a bundled agent skill (write / DDL / compression recipes)";

pub const SKILL_LONG_ABOUT: &str = "\
Print an agent skill that is compiled into this binary: this is a
document, like `--help`.

  pqbench skill                       list skills as pqbench.skill lines
  pqbench skill parquet-advisor       the advisor workflow
  pqbench skill parquet-advisor recipes
                                      write-path, table DDL, codec levels

The advisor turns bytemass / profile facts into recipes
(ingest settings, Iceberg/Delta/Spark/DuckDB DDL, compression level
with pros and cons). It does not measure files.";

pub const SKILL_AFTER: &str = "\
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
  docs/reference/skill.md  docs/reference/cli.md";

pub const SETUP_ABOUT: &str = "Print the walk's environment as shell exports to eval";

pub const SETUP_LONG_ABOUT: &str = "\
Print the environment the metadata walk needs, as shell `export` lines, for
the caller to evaluate:

  eval \"$(pqbench setup)\"

The endpoint and token resolve the way the Databricks SDKs do: the flag, then
PQB_ENDPOINT / PQB_TOKEN, then DATABRICKS_HOST / DATABRICKS_TOKEN. A notebook
kernel's dbutils context is not visible to a subprocess, so a notebook sets
DATABRICKS_* from it first. AWS_REGION comes from the flag or AWS_REGION (a
vended lease carries no region); AWS_EC2_METADATA_DISABLED=true keeps the AWS
SDK from probing EC2 metadata, which hangs where that endpoint is blackholed.

  endpoint  --endpoint | PQB_ENDPOINT | DATABRICKS_HOST
  token     --token    | PQB_TOKEN    | DATABRICKS_TOKEN
  format    --table-format | PQB_TABLE_FORMAT (unity default)
  region    --region   | AWS_REGION";

pub const SETUP_AFTER: &str = "\
Examples:
  eval \"$(pqbench setup --endpoint https://… --token dapi-… --region us-east-1)\"
  DATABRICKS_HOST=https://… DATABRICKS_TOKEN=dapi-… eval \"$(pqbench setup)\"
  pqbench setup --table-format iceberg --region us-west-2

See also:
  pqbench daemon --help       cache and refresh vended leases
  pqbench get --help          read a listed table's record
  pqbench --help              auth, PQB_* config
  docs/reference/cli.md";

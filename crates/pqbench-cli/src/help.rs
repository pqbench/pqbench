//! `--help` copy. The root command is the long guide (auth, documents,
//! format skills). Each subcommand stays local and points back.

/// One-line summary for `pqbench -h`.
pub const ROOT_ABOUT: &str =
    "Measure Parquet storage and codec speed; pipe refs → list partitions → list files → bytemass";

/// Full summary for `pqbench --help`.
pub const ROOT_LONG_ABOUT: &str = "\
pqbench measures how Parquet files spend bytes (bytemass), how well codecs
compress them (lz, compression), what a Delta or Iceberg table's partitions
and files hold (list partitions, list files), and what a decoded row sample
holds (profile).
Commands compose on pipes: each writes a resource the next command reads.

  get           → Metastore / Catalog / Schema / Table
                  one resource of the walk
  list          → Catalog / Schema / Table / Partition / TableFile
                  a collection of the walk
  credentials   → Table          check + vend read credentials
  bytemass      → Blob + Bytemass      footer byte masses (one line per column)
  viz           → PREFIX.html          collect the stream into a static treemap
  skill         → markdown             bundled agent recipes (write / DDL / codec)

The walk reads with two verbs: `get` reads one resource, `list` lists a
collection.

A terminal prints an aligned table; a pipe streams NDJSON. `--format
table|json` overrides either. `-o` also writes the lz4 NDJSON stream.
Subcommand help is local (`pqbench get --help`).
Auth, documents, and format skills are here.

Guide: docs/cli.md";

/// Extensive after-text for `pqbench --help` only.
pub const ROOT_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench list partitions ./delta-table | pqbench list files | pqbench bytemass
  pqbench list catalogs | pqbench list schemas | pqbench list tables
  pqbench list tables CAT.SCHEMA | pqbench credentials check | pqbench credentials get \\
    | pqbench get table | pqbench list partitions | pqbench list files | pqbench bytemass
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench skill parquet-advisor

Documents:
  Metastore              the endpoint's metastore record (get metastore)
  Catalog                the endpoint's catalogs (list catalogs) / one record (get catalog)
  Schema                 a catalog's schemas (list schemas) / one record (get schema)
  Table                  a schema's tables (list tables) / one record (get table, credentials get)
  Partition              a table's natural partitions (list partitions)
  TableFile              one file of a partition (list files)
  remote-source          one URI + AWS_* from a producer
  Blob                   one file's footer facts (bytemass)
  Bytemass               one column's byte mass (bytemass)
  pqbench.profile-column one column's profile
  pqbench.skill          name + description (pqbench skill with no args)

Features: delta / iceberg to load those logs; aws / delta-s3 / iceberg-s3
for s3://. A missing feature fails at runtime and names itself.

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
    echo '{\"version\":1,\"inputs\":[\"s3://b/t\"],
      \"env\":{\"AWS_ACCESS_KEY_ID\":\"…\",\"AWS_SECRET_ACCESS_KEY\":\"…\",
             \"AWS_SESSION_TOKEN\":\"…\",\"AWS_REGION\":\"us-east-1\"}}' \\
      | pqbench bytemass

Format skills (read these when the input is Parquet or a table):
  Parquet file format      https://parquet.apache.org/docs/file-format/
  Parquet thrift spec      https://github.com/apache/parquet-format
  Delta transaction log    https://github.com/delta-io/delta/blob/master/PROTOCOL.md
  Iceberg table spec       https://iceberg.apache.org/spec/
  Iceberg REST catalog     https://iceberg.apache.org/docs/latest/rest-catalog-spec/
  Unity / Databricks list  https://docs.databricks.com/api/workspace/tables/list
  AWS default credentials  https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html
  pqbench command guide    docs/cli.md
  pqbench table docs       docs/delta.md  docs/iceberg.md  docs/viz.md";

pub const BYTEMASS_ABOUT: &str = "Per-column on-disk bytes from Parquet footers (no page decode)";

pub const BYTEMASS_LONG_ABOUT: &str = "\
Read Parquet footers only and report the on-disk bytes of each column, its
chunks folded over the file's row groups. Works on compressed files
(HEAD + ranged GETs on a URI).

Inputs: parquet paths, quoted globs, s3://, a TableFile or
remote-source document, or '-' / stdin. List a table's files with
`pqbench list partitions | pqbench list files` first.

A terminal prints one row per column as a table; a pipe streams one `Blob`
per file and one `Bytemass` per column as NDJSON. `--format json` (or
`--json`) forces the stream, and `-o` also writes it. Pipe the stream to `viz`.";

pub const BYTEMASS_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench bytemass 'data/*.parquet' -o masses.ndjson.lz4
  pqbench list partitions ./delta-table | pqbench list files | pqbench bytemass
  pqbench list partitions ./delta-table | pqbench list files | pqbench bytemass | pqbench viz -o report

See also:
  pqbench list --help      list the partitions whose files this command measures
  pqbench viz --help       collect the stream into a static HTML page
  pqbench --help           auth, documents, format skills
  docs/cli.md";

pub const GET_ABOUT: &str = "Read one resource of the walk";

pub const GET_LONG_ABOUT: &str = "\
Read one resource of the walk, by type:

  metastore  the endpoint's record (name, uid, cloud, region), one line
  catalog    one catalog's record (name, catalog_type, comment, owner)
  schema     one schema's record (name, comment, uri, properties), from
             CATALOG.SCHEMA
  table      one table's record (format, snapshot, columns, partition columns,
             format properties), from CATALOG.SCHEMA.TABLE

The endpoint, bearer, and dialect come from PQB_ENDPOINT / PQB_TOKEN /
PQB_TABLE_FORMAT; a `get` reads a catalog, so it needs an endpoint. Without a
name the command reads that type's resources on standard input — one per line —
so it enriches a listing: `pqbench list catalogs | pqbench get catalog`.

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

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const GET_AFTER: &str = "\
Examples:
  pqbench get metastore
  pqbench get catalog dbx_samples
  pqbench get schema dbx_samples.nyctaxi
  pqbench get table dbx_samples.nyctaxi.trips
  pqbench list tables dbx_samples.nyctaxi | pqbench get table

See also:
  pqbench list --help      the walk's collections
  pqbench bytemass --help  measure the files the walk names
  pqbench --help           auth (AWS_*, catalog token), documents, format skills
  docs/cli.md  docs/delta.md  docs/iceberg.md";

pub const LIST_ABOUT: &str = "List a collection of the walk";

pub const LIST_LONG_ABOUT: &str = "\
List a collection of the walk, by name:

  catalogs    the catalogs at the endpoint, one Catalog line each
  schemas     the schemas in a catalog (CATALOG), one Schema line each
  tables      the tables in a schema (CATALOG.SCHEMA), one Table line each —
              the record `get table` enriches
  partitions  a table's natural partitions (DIR, or a Table stream on standard
              input): its commits grouped into epoch-aligned, half-open UTC
              windows
  files       a partition's files — the ones its commits added

Unity REST serves /catalogs, /schemas, and /tables. Under
PQB_TABLE_FORMAT=iceberg the schema and table levels speak Iceberg REST
instead: the endpoint then names the catalog base ({root}/v1 or
{root}/v1/{prefix}) and they read /namespaces under it. No config probe runs.
Unity fills a table's storage location in the listing; Iceberg leaves it to
`get table`.

`list partitions` reads the Delta / Iceberg log — never the data files — and
groups the commits by commit time (Delta commitInfo.timestamp; Iceberg the
snapshot time); --every sets the window width (1h, 1d, 1w; default 1d), --last
bounds the read to the newest commits, and a commit the log does not date is
omitted, so a window never claims a commit it cannot place. A partition is a
lens on the files, not a thing the table stores —
see docs/partition.md. `list files` re-reads the window's commits and names the
files they added; each file carries the partition's credentials, and that
stream is what `bytemass` measures.

Every level reads its parent's refs on standard input, or takes the parent as
its argument, and writes one resource per line, so the walk is a pipe:

  pqbench list catalogs | pqbench list schemas | pqbench list tables |
  pqbench get table | pqbench list partitions | pqbench list files | pqbench bytemass

Detection: _delta_log is Delta (wins UniForm). Iceberg is
metadata/version-hint.text, metadata/*.metadata.json, or a .metadata.json path.
Needs --features delta and/or iceberg (delta-s3 / iceberg-s3 for s3://).

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const LIST_AFTER: &str = "\
Examples:
  pqbench list catalogs
  pqbench list schemas dbx_samples
  pqbench list tables dbx_samples.nyctaxi
  pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass

See also:
  pqbench get --help       read one resource
  pqbench bytemass --help  measure the files listed here
  pqbench --help           catalog auth, PQB_* config
  docs/cli.md  docs/partition.md";

pub const CREDENTIALS_ABOUT: &str = "Check and vend read credentials for table-refs";

pub const CREDENTIALS_LONG_ABOUT: &str = "\
Check and vend read credentials for table-refs. The endpoint, token, dialect,
and object-store options come from the environment: PQB_ENDPOINT / PQB_TOKEN /
PQB_TABLE_FORMAT and AWS_*.

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

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const CREDENTIALS_AFTER: &str = "\
Examples:
  PQB_ENDPOINT=… pqbench list tables CAT.SCHEMA | pqbench credentials check | pqbench credentials get
  PQB_ENDPOINT=… pqbench list tables CAT.SCHEMA | pqbench credentials check | pqbench credentials get | pqbench get table

See also:
  pqbench list --help  the tables in a schema
  pqbench --help          catalog auth, PQB_* config
  docs/cli.md";

pub const RATELIMIT_ABOUT: &str = "Pace an NDJSON ref stream to a records-per-second rate";

pub const RATELIMIT_LONG_ABOUT: &str = "\
Pace an NDJSON ref stream: records pass through unchanged, delayed so each
record kind observes at most --rate records per second (15 by default; 0
turns pacing off). Nothing is dropped. One bucket per kind, so catalogs,
schemas, and table refs pace independently — the walk's per-endpoint pace.

  list catalogs | pqbench ratelimit | list schemas
  list schemas | pqbench ratelimit --rate 5 | list tables

A consumer that issues one request per record as it arrives sees the same
rate. A 429 fails the level; retry it at a lower --rate.";

pub const RATELIMIT_AFTER: &str = "\
Examples:
  pqbench list catalogs | pqbench ratelimit | pqbench list schemas
  pqbench list schemas CATALOG | pqbench ratelimit --rate 5 | pqbench list tables

See also:
  pqbench list --help     the walk's collections
  pqbench get --help      read one resource of it
  docs/cli.md";

pub const VIZ_ABOUT: &str = "Collect a bytemass stream into a static HTML treemap";

pub const VIZ_LONG_ABOUT: &str = "\
Read a bytemass stream and write `PREFIX.html`. The page embeds the measured
columns and blobs and loads the d3 modules it uses from a CDN, drawing one
treemap of bytes per value. Does not measure files. `-o PREFIX` is required.";

pub const VIZ_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet | pqbench viz -o report
  pqbench list partitions ./delta-table | pqbench list files | pqbench bytemass | pqbench viz -o report
  xdg-open report.html

See also:
  pqbench bytemass --help  produce the stream this command collects
  pqbench --help           documents
  docs/viz.md  docs/cli.md";

pub const LZ_ABOUT: &str = "Codec sweep over raw file bytes (lzbench-style)";

pub const LZ_LONG_ABOUT: &str = "\
Compress the whole file as opaque bytes. -c codec@level is repeatable;
omitting -c sweeps every wired codec (zstd, lz4, gzip, snappy). --samples
timed passes after --warmup-iterations. --mode fastest keeps the best pass;
mean averages all. A terminal prints the sweep as a table; `--format json`
(or `--json`) streams the same report as composable NDJSON.";

pub const LZ_AFTER: &str = "\
Examples:
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench lz file.bin --json

See also:
  pqbench compression --help  same sweep over Parquet pages (NONE input)
  pqbench --help
  docs/cli.md";

pub const COMPRESSION_ABOUT: &str =
    "Codec sweep over encoded Parquet pages (NONE-compressed input only)";

pub const COMPRESSION_LONG_ABOUT: &str = "\
Same sweep as lz, but over each column chunk's encoded pages. The file must
be NONE-compressed; a compressed file is rejected. --per-column adds one
report row per column chunk. A terminal prints the sweep as a table (report
rows, then per-column rows); `--format json` (or `--json`) streams the same
report as composable NDJSON.";

pub const COMPRESSION_AFTER: &str = "\
Examples:
  pqbench compression data.parquet
  pqbench compression data.parquet --per-column
  pqbench compression data.parquet -c zstd@3 --json

See also:
  pqbench lz --help        raw-byte sweep (any file)
  pqbench bytemass --help  on-disk bytes without a codec sweep
  pqbench --help
  docs/cli.md";

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

A terminal prints one row per column as a table; a pipe streams one
`pqbench.profile-column` per column. `--format json`
(or `--json`) forces the stream, and `-o` also writes it.";

pub const PROFILE_AFTER: &str = "\
Examples:
  pqbench profile data.parquet
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench profile data.parquet --rows first:1024 -o profile.ndjson.lz4
  pqbench profile data.parquet --rows all --json

See also:
  pqbench bytemass --help  footer byte masses without decoding rows
  pqbench compression --help  codec sweep over encoded pages
  pqbench --help
  docs/profile.md  docs/cli.md";

pub const SKILL_ABOUT: &str = "Print a bundled agent skill (write / DDL / compression recipes)";

pub const SKILL_LONG_ABOUT: &str = "\
Print an agent skill that is compiled into this binary. No TTY `-o`
requirement: this is a document, like `--help`.

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
  pqbench skill parquet-advisor recipes
  pqbench bytemass data.parquet
  pqbench profile data.parquet --rows first:8192

See also:
  pqbench profile --help     cheap column facts
  pqbench --help             documents
  docs/skill.md  docs/cli.md";

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
  pqbench credentials --help  check and vend read credentials
  pqbench get --help          read a listed table's record
  pqbench --help              auth, PQB_* config
  docs/cli.md";

//! `--help` copy. The root command is the long guide (auth, documents,
//! format skills). Each subcommand stays local and points back.

/// One-line summary for `pqbench -h`.
pub const ROOT_ABOUT: &str =
    "Measure Parquet storage and codec speed; pipe refs → partition ls → file ls → bytemass";

/// Full summary for `pqbench --help`.
pub const ROOT_LONG_ABOUT: &str = "\
pqbench measures how Parquet files spend bytes (bytemass), how well codecs
compress them (lz, compression), what a Delta or Iceberg table's partitions
and files hold (partition ls, file ls), what a decoded row sample holds
(profile), and how a rewritten sample would store (experiment).
Commands compose on pipes: each writes a resource the next command reads.

  metastore get → Metastore      the endpoint's record
  catalog       → Catalog        the endpoint's catalogs / one catalog's record
  schema        → Schema         a catalog's schemas / one schema's record
  table ls      → Table          a schema's tables
  table get     → Table          enrich a table's record
  credentials   → Table          check + vend read credentials
  partition ls  → Partition      a table's natural partitions
  file ls       → TableFile      a partition's added files
  bytemass      → pqbench.bytemass-row footer byte masses (one line per column)
  viz           → PREFIX.html          collect the stream into a static treemap
  skill         → markdown             bundled agent recipes (write / DDL / codec)

A terminal prints an aligned table; a pipe streams NDJSON. `--format
table|json` overrides either. `-o` also writes the lz4 NDJSON stream.
Subcommand help is local (`pqbench table --help`).
Auth, documents, and format skills are here.

Guide: docs/cli.md";

/// Extensive after-text for `pqbench --help` only.
pub const ROOT_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench partition ls ./delta-table | pqbench file ls | pqbench bytemass
  pqbench schema ls CAT.SCHEMA | pqbench credentials check | pqbench credentials get \\
    | pqbench table get | pqbench partition ls | pqbench file ls | pqbench bytemass
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench experiment data.parquet --rewrite sort:text --aim all
  pqbench skill parquet-advisor

Documents:
  Metastore              the endpoint's metastore record (metastore get)
  Catalog                the endpoint's catalogs (catalog ls) / one record (catalog get)
  Schema                 a catalog's schemas (schema ls) / one record (schema get)
  Table                  a schema's tables (table ls) / one record (table get, credentials get)
  Partition              a table's natural partitions (partition ls)
  TableFile              one file of a partition (file ls)
  remote-source          one URI + AWS_* from a producer
  pqbench.bytemass-file  one file's footer facts; -row one column's mass
  pqbench.experiment-trial  one trial's bytes; -column per column
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
    vended keys onto the refs, and `table get` reads under the env it is
    given. Reading the objects still needs keys that can GetObject.
    https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials

  Producer pipe
    echo '{\"kind\":\"pqbench.remote-source\",\"version\":1,\"inputs\":[\"s3://b/t\"],
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

pub const BYTEMASS_ABOUT: &str =
    "Per-column on-disk bytes/row from Parquet footers (no page decode)";

pub const BYTEMASS_LONG_ABOUT: &str = "\
Read Parquet footers only and report on-disk bytes per column per row.
Works on compressed files (HEAD + ranged GETs on a URI).

Inputs: parquet paths, quoted globs, s3://, a TableFile or
remote-source document, or '-' / stdin. List a table's files with
`pqbench partition ls | pqbench file ls` first.

A terminal prints one row per column chunk as a table; a pipe streams the
same rows as `pqbench.bytemass-row` NDJSON. `--format json` (or `--json`)
forces the stream, and `-o` also writes it. Pipe the stream to `viz`.";

pub const BYTEMASS_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench bytemass 'data/*.parquet' -o masses.ndjson.zst
  pqbench partition ls ./delta-table | pqbench file ls | pqbench bytemass
  pqbench partition ls ./delta-table | pqbench file ls | pqbench bytemass | pqbench viz -o report

See also:
  pqbench partition --help  list the partitions whose files this command measures
  pqbench viz --help       collect the stream into a static HTML page
  pqbench --help           auth, documents, format skills
  docs/cli.md";

pub const TABLE_ABOUT: &str = "Read one table's record, or list the tables in a schema";

pub const TABLE_LONG_ABOUT: &str = "\
Read one table at a catalog endpoint, or list the tables in a schema. The table
is the entity between schemas and partitions. The endpoint, token, dialect, and
object-store options come from the environment: PQB_ENDPOINT / PQB_TOKEN /
PQB_TABLE_FORMAT and AWS_*.

  get    the ref enriched with the table's record (format, snapshot, columns,
         partition columns, format properties) as one Table. Without a
         CATALOG.SCHEMA.TABLE argument the command reads Table resources on
         standard input — one table per line — so `table ls | table get`
         chains. Unity REST serves /tables/{full_name}; the catalog's
         declared columns and properties are merged over the Delta log read
         with without_files(), so that path is O(1) in files and needs a
         readable storage location: the ref's uri or the process environment.
         Vended credentials ride the ref (or the process env) from
         `credentials get`; `credentials check` gates the walk on tables the
         catalog marks readable outside compute. The emitted record carries
         its credentials, so a later stage reads the table's files under the
         same lease. PQB_TABLE_FORMAT=iceberg reads loadTable, whose metadata
         is inline, so no storage read runs.
  ls     the tables in a schema (CATALOG.SCHEMA), one Table line each — the
         record `table get` enriches. Without a CATALOG.SCHEMA argument the
         command reads Schema resources on standard input, so `schema ls |
         table ls` chains. Unity REST serves /tables;
         PQB_TABLE_FORMAT=iceberg lists namespaces' tables; a ref carries its
         name and no storage URI until `table get` resolves it.

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.

Detection: _delta_log is Delta (wins UniForm). Iceberg is
metadata/version-hint.text, metadata/*.metadata.json, or a .metadata.json
path. Needs --features delta and/or iceberg (delta-s3 / iceberg-s3 for s3://).";

pub const TABLE_AFTER: &str = "\
Examples:
  pqbench table get dbx_samples.nyctaxi.trips < source.json
  PQB_ENDPOINT=… pqbench table ls | pqbench table get
  pqbench partition ls ./delta-table | pqbench file ls | pqbench bytemass
  pqbench schema ls CAT.SCHEMA | pqbench table ls

See also:
  pqbench schema --help    the schemas in a catalog
  pqbench partition --help  list a table's natural partitions
  pqbench bytemass --help  measure the files named there
  pqbench --help           auth (AWS_*, catalog token), documents, format skills
  docs/delta.md  docs/iceberg.md  docs/cli.md";

pub const METASTORE_ABOUT: &str = "Read the endpoint's metastore record";

pub const METASTORE_LONG_ABOUT: &str = "\
Read the metastore at a catalog endpoint: the entity above catalogs. The
endpoint and token come from PQB_ENDPOINT / PQB_TOKEN.

  get    the metastore record (name, uid, cloud, region) as one
         Metastore line

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const METASTORE_AFTER: &str = "\
Examples:
  pqbench metastore get < source.json
  PQB_ENDPOINT=… pqbench metastore get | pqbench catalog ls

See also:
  pqbench catalog --help   the endpoint's catalogs
  pqbench schema --help    the schemas in a catalog
  pqbench --help           catalog auth, PQB_* config
  docs/cli.md";

pub const CATALOG_ABOUT: &str = "Read one catalog's record or list the catalogs at the endpoint";

pub const CATALOG_LONG_ABOUT: &str = "\
Read one catalog, or list the catalogs at a catalog endpoint: the entity above
schemas. The endpoint and token come from PQB_ENDPOINT / PQB_TOKEN (and
PQB_TABLE_FORMAT).

  get    the catalog's record (name, catalog_type, comment, owner) as one
         Catalog line. Without a CATALOG argument the command reads Catalog
         resources on standard input — one catalog per line.
  ls     the catalogs at the endpoint, one Catalog line each. A Metastore
         stream on standard input (`metastore get`) affords the walk its shape;
         the endpoint's catalogs are the answer either way.

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const CATALOG_AFTER: &str = "\
Examples:
  pqbench catalog get dbx_samples < source.json
  pqbench catalog ls
  PQB_ENDPOINT=… pqbench metastore get | pqbench catalog ls

See also:
  pqbench metastore --help  the endpoint's metastore record
  pqbench schema --help     the schemas in a catalog
  pqbench --help            catalog auth, PQB_* config
  docs/cli.md";

pub const SCHEMA_ABOUT: &str = "Read one schema's record or list the schemas in a catalog";

pub const SCHEMA_LONG_ABOUT: &str = "\
Read one schema, or list the schemas in a catalog. The schema is the entity
between catalogs and tables. The endpoint and token come from PQB_ENDPOINT /
PQB_TOKEN (and PQB_TABLE_FORMAT).

  get    the schema's record (name, comment, uri, properties) as one Schema
         line. Without a CATALOG.SCHEMA argument the command reads Schema
         resources on standard input — one schema per line.
  ls     the schemas in a catalog (CATALOG), one Schema line each (name,
         comment, uri) — the record `schema get` enriches. Without a CATALOG
         argument the command reads Catalog resources on standard input, so
         `catalog ls | schema ls` chains. Unity REST serves /schemas;
         PQB_TABLE_FORMAT=iceberg speaks Iceberg REST instead, reading
         /namespaces under the endpoint (which then names the catalog base:
         {root}/v1 or {root}/v1/{prefix}).

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const SCHEMA_AFTER: &str = "\
Examples:
  pqbench schema get dbx_samples.nyctaxi < source.json
  pqbench schema ls dbx_samples
  PQB_ENDPOINT=… pqbench catalog ls | pqbench schema ls

See also:
  pqbench catalog --help  the catalogs at the endpoint
  pqbench table --help  the tables in a schema
  pqbench --help          catalog auth, PQB_* config
  docs/cli.md";

pub const PARTITION_ABOUT: &str = "List a table's natural partitions, grouped by commit time";

pub const PARTITION_LONG_ABOUT: &str = "\
List a table's natural partitions: reads the Delta / Iceberg log — never the
data files — and emits one Partition per epoch-aligned, half-open UTC window,
carrying the commits it holds. A partition is a grouping of a table's active
files; the natural key is time.

  ls     Inputs: a table directory or URI, or a Table stream on standard input.
         A path read names its table in a `local` catalog and schema, so every
         partition has a name. A ref from a catalog walk must carry its storage
         URI — run `pqbench table get` first if it does not. --every is the
         window width (1h, 1d, 1w; default 1d). Pipe to `pqbench file ls` for
         each partition's files.

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const PARTITION_AFTER: &str = "\
See `pqbench file ls --help` (the files), `pqbench bytemass --help` (measure
them), and `docs/partition.md`.";

pub const FILE_ABOUT: &str = "List a partition's files — the ones its commits added";

pub const FILE_LONG_ABOUT: &str = "\
List a partition's files. A partition is a grouping of a table's active files
by a key; the natural key is time, so `partition ls` groups a table's commits
into epoch-aligned UTC windows and this lists the files those commits added.

  ls     each Partition record on standard input is re-read: the table's log is
         loaded, and the files the window's commits named are emitted as
         TableFile records — the added-in-window set, so a file a later commit
         removes is still named. Each file names its blob and carries the
         partition's credentials, so a short-lived lease travels with it, and
         there is no begin/end envelope. Pipe it to `bytemass` to measure the
         partition.

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const FILE_AFTER: &str = "\
See `pqbench partition ls --help` (the partitions), `pqbench bytemass --help`
(measure them), and `docs/partition.md`.";

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
  PQB_ENDPOINT=… pqbench table ls | pqbench credentials check | pqbench credentials get
  PQB_ENDPOINT=… pqbench table ls | pqbench credentials check | pqbench credentials get | pqbench table get

See also:
  pqbench table --help  the tables in a schema
  pqbench --help          catalog auth, PQB_* config
  docs/cli.md";

pub const RATELIMIT_ABOUT: &str = "Pace an NDJSON ref stream to a records-per-second rate";

pub const RATELIMIT_LONG_ABOUT: &str = "\
Pace an NDJSON ref stream: records pass through unchanged, delayed so each
record kind observes at most --rate records per second (15 by default; 0
turns pacing off). Nothing is dropped. One bucket per kind, so catalogs,
schemas, and table refs pace independently — the walk's per-endpoint pace.

  catalog ls | pqbench ratelimit | schema ls
  schema ls | pqbench ratelimit --rate 5 | table ls

A consumer that issues one request per record as it arrives sees the same
rate. A 429 fails the level; retry it at a lower --rate.";

pub const RATELIMIT_AFTER: &str = "\
Examples:
  pqbench catalog ls | pqbench ratelimit | pqbench schema ls
  pqbench schema ls | pqbench ratelimit --rate 5 | pqbench table ls

See also:
  pqbench catalog --help  the walk's first level
  pqbench schema --help   the walk's second level
  pqbench table --help    the walk's third level
  docs/cli.md";

pub const VIZ_ABOUT: &str = "Collect a bytemass stream into a static HTML treemap";

pub const VIZ_LONG_ABOUT: &str = "\
Read `pqbench.bytemass-row` lines and write `PREFIX.html`. The page embeds
the measured rows and loads the d3 modules it uses from a CDN, drawing one
treemap per table id. Does not measure files. `-o PREFIX` is required.";

pub const VIZ_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet | pqbench viz -o report
  pqbench table ls ./delta-table | pqbench partition ls | pqbench bytemass | pqbench viz -o report
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
  pqbench profile data.parquet --rows first:1024 -o profile.ndjson.zst
  pqbench profile data.parquet --rows all --json

See also:
  pqbench bytemass --help  footer byte masses without decoding rows
  pqbench compression --help  codec sweep over encoded pages
  pqbench --help
  docs/profile.md  docs/cli.md";

pub const EXPERIMENT_ABOUT: &str =
    "Rewrite a decoded row sample and measure storage and skip locality";

pub const EXPERIMENT_LONG_ABOUT: &str = "\
Read up to --rows leading rows, then for each --rewrite SPEC write the sample
back to Parquet and measure it. A control trial (zstd, dictionary on) is
always measured first; trial ratios are versus that control, not the source
file's original encodings.

--rewrite / --trial is repeatable and each value is one trial; semicolons
compose rewrites (sort:a;codec:zstd@3;dictionary:off;row-group-size:2048).
Supported rewrites: sort:A / sort:A,B (numeric or bytewise), zorder:A,B,
hilbert:A,B (exactly two columns), codec:NAME[@LEVEL] (uncompressed, snappy,
gzip, lz4, zstd), dictionary:on|off|BYTES, row-group-size:N, page-size:BYTES,
encoding:plain|delta|rle|delta_length|delta_byte_array|byte_stream_split,
and cast:COL:int64|double|string. Encodings apply only to compatible columns;
an encoding that applies to no column is an error.

--aim chooses what to measure: storage (bytes, bytes per row), skipping
(row-group min/max locality), or all. skipping/all splits the sample into row
groups of 2048 rows unless a trial sets row-group-size.

A terminal prints the trials and their columns as tables; a pipe streams one
`pqbench.experiment-trial` per trial and one `pqbench.experiment-column` per
column. `--format json` (or `--json`) forces the stream, and `-o` also writes
it. This command does not do drop, index editing, or input row-group selection.
The current sample representation flattens nested columns to strings and
does not preserve all logical types. Casts are not guaranteed lossless; verify
values and schema before applying a trial to production data.";

pub const EXPERIMENT_AFTER: &str = "\
Examples:
  pqbench experiment data.parquet
  pqbench experiment data.parquet --rewrite sort:text --aim all
  pqbench experiment data.parquet --rewrite 'sort:country,city' --rewrite codec:snappy
  pqbench experiment data.parquet --rewrite 'sort:id;dictionary:off' -o trials.ndjson.zst

See also:
  pqbench profile --help  facts to choose a rewrite from
  pqbench bytemass --help  footer byte masses of an existing file
  pqbench --help
  docs/experiment.md  docs/cli.md";

pub const SKILL_ABOUT: &str = "Print a bundled agent skill (write / DDL / compression recipes)";

pub const SKILL_LONG_ABOUT: &str = "\
Print an agent skill that is compiled into this binary. No TTY `-o`
requirement: this is a document, like `--help`.

  pqbench skill                       list skills as pqbench.skill lines
  pqbench skill parquet-advisor       the advisor workflow
  pqbench skill parquet-advisor recipes
                                      write-path, table DDL, codec levels

The advisor turns bytemass / profile / experiment facts into recipes
(ingest settings, Iceberg/Delta/Spark/DuckDB DDL, compression level
with pros and cons). It does not measure files.";

pub const SKILL_AFTER: &str = "\
Examples:
  pqbench skill
  pqbench skill parquet-advisor
  pqbench skill parquet-advisor recipes
  pqbench bytemass data.parquet
  pqbench profile data.parquet --rows first:8192
  pqbench experiment data.parquet --rewrite sort:text --aim skipping

See also:
  pqbench profile --help     cheap column facts
  pqbench experiment --help  measure a rewrite
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
  pqbench table --help        read a listed table's record
  pqbench --help              auth, PQB_* config
  docs/cli.md";

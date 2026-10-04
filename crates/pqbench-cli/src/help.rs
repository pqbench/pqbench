//! `--help` copy. The root command is the long guide (auth, documents,
//! format skills). Each subcommand stays local and points back.

/// One-line summary for `pqbench -h`.
pub const ROOT_ABOUT: &str =
    "Measure Parquet storage and codec speed; pipe lake → table → bytemass → viz";

/// Full summary for `pqbench --help`.
pub const ROOT_LONG_ABOUT: &str = "\
pqbench measures how Parquet files spend bytes (bytemass), how well codecs
compress them (lz, compression), what a Delta or Iceberg snapshot
currently stores (table, lake), what a decoded row sample holds
(profile), and how a rewritten sample would store (experiment).
Commands compose on pipes: each writes a versioned JSON document the
next command reads.

  lake     →  pqbench.table-ref    list tables (or a catalog)
  table    →  pqbench.table        load one snapshot's log and files
  bytemass →  pqbench.bytemass-row footer byte masses (one line per column)
  viz      →  PREFIX.html          collect the stream into a static treemap
  dump     →  Parquet files        copy the files the table names
  skill    →  markdown             bundled agent recipes (write / DDL / codec)

A terminal prints an aligned table; a pipe streams NDJSON. `--format
table|json` overrides either. `-o` also writes the lz4 NDJSON stream.
Subcommand help is local (`pqbench table --help`).
Auth, documents, and format skills are here.

Guide: docs/cli.md";

/// Extensive after-text for `pqbench --help` only.
pub const ROOT_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench table ./delta-table | pqbench bytemass
  pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
  pqbench table ./delta-table | pqbench dump ./sample
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench profile data.parquet --columns 'text' --top 5
  pqbench experiment data.parquet --rewrite sort:text --aim all
  pqbench skill parquet-advisor

Documents (kind + version 1):
  pqbench.experiment     begin/end around trial and column lines
  pqbench.profile        begin/end around pqbench.profile-column lines
  pqbench.lake-source    catalog endpoint + token; lake lists it
  pqbench.metastore      the endpoint's metastore record
  pqbench.catalog        the endpoint's catalogs (metastore ls)
  pqbench.lake           tables (name, uri, env); table loads each log
  pqbench.table-ref      one table's record address + storage path
  pqbench.table          format, snapshot, log, active files (streamed)
  pqbench.remote-source  one URI + AWS_* from a producer
  pqbench.bytemass       begin/end around pqbench.bytemass-row lines
  pqbench.skill          name + description (pqbench skill with no args)

Features: delta / iceberg to load those logs; aws / delta-s3 / iceberg-s3
for s3://. A missing feature fails at runtime and names itself.

Auth (how to reach data):
  Local paths need no credentials.

  s3://  (needs --features aws / delta-s3 / iceberg-s3)
    Default AWS provider chain (process env, shared config, instance role,
    AWS_PROFILE). A document env overrides that chain and is not exported
    back into the process. Only AWS_* names are accepted on tables.

    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN     STS or catalog-vended temp keys
    AWS_REGION
    AWS_ENDPOINT          S3-compatible host (also set AWS_ENDPOINT_URL)
    AWS_ALLOW_HTTP        true for http://
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST   false for path-style (MinIO, rustfs)
    AWS_SKIP_SIGNATURE    true for a public bucket

  Catalog list  (pqbench.lake-source)
    endpoint + token   a Databricks workspace URL + PAT (dapi-…), or a
    Unity OSS / Iceberg REST URL. The token is a Bearer on the list API
    only. Only AWS_* is copied onto listed tables — a PAT is not an S3 key.

  Databricks-governed tables
    lake lists storage_location. Reading objects still needs AWS keys that
    can GetObject. The legacy lake path does not call
    temporary-table-credentials; paste vended STS into AWS_* on env, or use
    a role that already can read the bucket. The tablev2 walk does:
    `credentials get` materializes the vended keys onto the refs, and
    `tablev2 info` reads under the env it is given.
    https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials

  Producer pipe
    echo '{\"kind\":\"pqbench.remote-source\",\"version\":1,\"inputs\":[\"s3://b/t\"],
      \"env\":{\"AWS_ACCESS_KEY_ID\":\"…\",\"AWS_SECRET_ACCESS_KEY\":\"…\",
             \"AWS_SESSION_TOKEN\":\"…\",\"AWS_REGION\":\"us-east-1\"}}' \\
      | pqbench table | pqbench bytemass

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

Inputs: parquet paths, quoted globs, s3://, a pqbench.table or loaded
pqbench.lake, or '-' / stdin. A lake must go through `pqbench table` first.

A terminal prints one row per column chunk as a table; a pipe streams the
same rows as `pqbench.bytemass-row` NDJSON. `--format json` (or `--json`)
forces the stream, and `-o` also writes it. Pipe the stream to `viz`.";

pub const BYTEMASS_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench bytemass 'data/*.parquet' -o masses.ndjson.zst
  pqbench table ./delta-table | pqbench bytemass
  pqbench table ./delta-table | pqbench bytemass | pqbench viz -o report

See also:
  pqbench table --help     load the snapshot this command measures
  pqbench viz --help       collect the stream into a static HTML page
  pqbench dump --help      copy those files to a directory
  pqbench --help           auth, documents, format skills
  docs/cli.md";

pub const TABLE_ABOUT: &str =
    "Detect Delta or Iceberg and load one snapshot's log and active files";

pub const TABLE_LONG_ABOUT: &str = "\
Detect the format, then load metadata. Delta: transaction log and active
files. Iceberg: metadata JSON and Avro manifests (delete files in the log,
not files[]). Does not measure bytes — pipe to bytemass or dump.

Inputs: table directory or URI, Iceberg metadata JSON, a pqbench.table /
pqbench.lake / pqbench.remote-source, or '-' / stdin. A pqbench.table-ref
carries its storage path; a ref from an Iceberg catalog walk does not, so run
`table info` first to fill it.

  info   read each table-ref's record (Iceberg loadTable) and emit the ref
         with its storage path; a ref that already has one passes through

Detection: _delta_log is Delta (wins UniForm). Iceberg is
metadata/version-hint.text, metadata/*.metadata.json, or a .metadata.json
path.

--version is a Delta commit or Iceberg snapshot id (default: latest).

Needs --features delta and/or iceberg (delta-s3 / iceberg-s3 for s3://).";

pub const TABLE_AFTER: &str = "\
Examples:
  pqbench table ./delta-table -o table.ndjson.zst
  pqbench table ./delta-table | pqbench bytemass
  pqbench lake s3://bucket/warehouse | pqbench table | pqbench bytemass
  pqbench table ./iceberg-table | pqbench bytemass

See also:
  pqbench lake --help      list tables into a document this command loads
  pqbench tablev2 --help   read a catalog table's record (the new tree)
  pqbench bytemass --help  measure the files named here
  pqbench dump --help      copy those files to a directory
  pqbench --help           auth (AWS_*, catalog token), documents, format skills
  docs/delta.md  docs/iceberg.md  docs/cli.md";

pub const LAKE_ABOUT: &str = "List Delta/Iceberg tables in a directory, URI, or catalog";

pub const LAKE_LONG_ABOUT: &str = "\
Walk a warehouse or list a catalog and stream `pqbench.table-ref` lines
(name, uri, env). `pqbench table` loads each log.

Inputs: directory, file:// or s3:// prefix, a pqbench.lake to re-select, a
pqbench.lake-source, or '-' / stdin.

Walk: descend until _delta_log or an Iceberg hint / metadata JSON. Do not
search inside a table. --max-depth bounds a tree with no marker. --include /
--exclude are Unix globs on the relative table name and prune the walk.

Catalog: GET /v1/config — a defaults object is Iceberg REST; 200 without
defaults, or HTTP 404, is Unity. A lake-source names endpoint + token.
Only AWS_* is copied onto tables. s3:// listing needs --features aws.";

pub const LAKE_AFTER: &str = "\
Examples:
  pqbench lake ./warehouse
  pqbench lake ./warehouse --include 'sales/**' --exclude tmp
  pqbench lake s3://bucket/warehouse --max-depth 2
  pqbench lake source.json
  pqbench lake ./warehouse | pqbench table | pqbench bytemass

See also:
  pqbench table --help     load each listed table
  pqbench --help           catalog vs object auth, lake-source shape
  docs/cli.md  docs/demos/unity.json";

pub const METASTORE_ABOUT: &str = "Read the endpoint's metastore record and list its catalogs";

pub const METASTORE_LONG_ABOUT: &str = "\
Read the metastore at a catalog endpoint: the entity above catalogs. The
endpoint and token come from a pqbench.lake-source on standard input, or from
PQB_ENDPOINT / PQB_TOKEN when the document leaves them out.

  info   the metastore record (name, id, cloud, region) as one
         pqbench.metastore line
  ls     the catalogs at the endpoint, one pqbench.catalog line each

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const METASTORE_AFTER: &str = "\
Examples:
  pqbench metastore info < source.json
  pqbench metastore ls < source.json
  PQB_ENDPOINT=… pqbench metastore ls | pqbench catalog ls

See also:
  pqbench catalog --help   the schemas of one catalog
  pqbench lake --help      list the tables the endpoint serves
  pqbench --help           catalog auth, lake-source shape
  docs/cli.md";

pub const CATALOG_ABOUT: &str = "Read one catalog's record or list its schemas";

pub const CATALOG_LONG_ABOUT: &str = "\
Read one catalog at a catalog endpoint: the entity above schemas. The
endpoint and token come from a pqbench.lake-source on standard input, or from
PQB_ENDPOINT / PQB_TOKEN (and PQB_TABLE_FORMAT) when the document leaves them
out. Without a CATALOG argument the command reads pqbench.catalog refs on
standard input — one catalog per line — so `metastore ls | catalog ls` chains.

  info   the catalog's record (name, catalog_type, comment, owner) as one
         pqbench.catalog line
  ls     the schemas in the catalog, one pqbench.schema line each (catalog,
         name). Unity REST serves /schemas; PQB_TABLE_FORMAT=iceberg speaks
         Iceberg REST instead, reading /namespaces under the endpoint (which
         then names the catalog base: {root}/v1 or {root}/v1/{prefix}).

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const CATALOG_AFTER: &str = "\
Examples:
  pqbench catalog info dbx_samples < source.json
  pqbench catalog ls dbx_samples < source.json
  PQB_ENDPOINT=… pqbench metastore ls | pqbench catalog ls

See also:
  pqbench metastore --help  the endpoint's metastore and its catalogs
  pqbench --help            catalog auth, lake-source shape
  docs/cli.md";

pub const SCHEMA_ABOUT: &str = "Read one schema's record or list its tables";

pub const SCHEMA_LONG_ABOUT: &str = "\
Read one schema at a catalog endpoint: the entity between catalogs and
tables. The endpoint and token come from a pqbench.lake-source on standard
input, or from PQB_ENDPOINT / PQB_TOKEN (and PQB_TABLE_FORMAT) when the
document leaves them out. Without a CATALOG.SCHEMA argument the command reads
pqbench.schema refs on standard input — one schema per line — so `catalog ls |
schema ls` chains.

  info   the schema's record (catalog, name, comment, location, properties)
         as one pqbench.schema line
  ls     the tables in the schema, one pqbench.table-ref version 2 line each
         (id, uri, storage path, storage options) — the document `tablev2 info`
         enriches. Unity
         REST serves /tables; PQB_TABLE_FORMAT=iceberg lists namespaces'
         tables and carries each loadTable URL (the endpoint names the catalog
         base). The legacy `table` reads version 1 refs only.

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const SCHEMA_AFTER: &str = "\
Examples:
  pqbench schema info dbx_samples.nyctaxi < source.json
  pqbench schema ls dbx_samples.nyctaxi < source.json
  PQB_ENDPOINT=… pqbench catalog ls | pqbench schema ls

See also:
  pqbench catalog --help  the schemas of one catalog
  pqbench tablev2 --help  read a listed table's record
  pqbench --help          catalog auth, lake-source shape
  docs/cli.md";

pub const TABLEV2_ABOUT: &str = "Read one table's record without its files";

pub const TABLEV2_LONG_ABOUT: &str = "\
Read one table at a catalog endpoint: the entity between schemas and
partitions. The endpoint, token, and object-store options come from a
pqbench.lake-source on standard input, or from PQB_ENDPOINT / PQB_TOKEN (and
PQB_TABLE_FORMAT) when the document leaves them out. Without a
CATALOG.SCHEMA.TABLE argument the command reads pqbench.table-ref version 2
refs on standard input — one table per line — so `schema ls | tablev2 info`
chains. Version 1 refs (the legacy `lake` stream) are rejected.

  info   the ref enriched with the table's record (format, snapshot, columns,
         partition columns, format properties) as one pqbench.table-ref
         version 2 line. Unity REST serves /tables/{full_name}; the catalog's
         declared columns and properties are merged over the Delta log read
         with without_files(), so that path is O(1) in files and needs a
         readable storage location: the lake source's env, the ref's env, or
         the process environment. Vended credentials ride the ref (or the
         process env) from `credentials get`; `credentials check` gates the
         walk on tables the catalog marks readable outside compute. The
         emitted record carries the env it read under, so a later stage reads
         the table's files under the same lease. PQB_TABLE_FORMAT=iceberg reads
         loadTable, whose metadata is inline, so the Iceberg path runs no
         storage read.

The name is temporary: the older `pqbench table` still owns `table info`
(filling a ref's storage path) and the file-loading command, and exchanges
version 1 documents only.

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const TABLEV2_AFTER: &str = "\
Examples:
  pqbench tablev2 info dbx_samples.nyctaxi.trips < source.json
  PQB_ENDPOINT=… pqbench schema ls | pqbench tablev2 info

See also:
  pqbench schema --help  the tables of one schema
  pqbench table --help   load a listed table's files
  pqbench --help         catalog auth, lake-source shape
  docs/cli.md";

pub const CREDENTIALS_ABOUT: &str = "Check and vend read credentials for table-refs";

pub const CREDENTIALS_LONG_ABOUT: &str = "\
Check and vend read credentials for table-refs. The endpoint, token, and
object-store options come from a pqbench.lake-source on standard input, or from
PQB_ENDPOINT / PQB_TOKEN (and PQB_TABLE_FORMAT) when the document leaves them
out.

  check  each pqbench.table-ref version 2 ref on standard input is checked
         against the catalog's capability manifest: a table the catalog
         reports without direct-external-engine read or write support (managed
         default storage, a view) has no external read at all, so the check
         drops it with the reason on standard error; eligible refs pass
         through unchanged, so the stage composes ahead of `credentials get`
         and a mixed schema keeps going. Under PQB_TABLE_FORMAT=iceberg the
         metadata read is inline through the catalog, so the check passes.

  get    each pqbench.table-ref version 2 ref enriched with the table's
         vended read credentials (AWS_* on env) as one pqbench.table-ref
         version 2 line — the explicit stage that materializes env for the
         table read and other tools. Unity GET /tables/{full_name} reads the
         table id and capability manifest; a manifest without
         direct-external-engine read or write support (managed default
         storage, a view) passes through with its own env, and a catalog that
         reports no manifest is attempted; inside serverless compute Unity
         refuses to mint storage credentials, so refs pass through there too.
         Under PQB_TABLE_FORMAT=iceberg the command reads the catalog's
         credentials route (loadCredentials) and takes the storage-credentials
         it returns, for the next storage read; a catalog that does not serve
         it falls back to the delegated loadTable, and a table the catalog
         cannot serve via Iceberg passes through with its own env.

A terminal prints an aligned table; a pipe streams NDJSON. `--format json`
forces the stream, and `-o` also writes it.";

pub const CREDENTIALS_AFTER: &str = "\
Examples:
  PQB_ENDPOINT=… pqbench schema ls | pqbench credentials check | pqbench credentials get
  PQB_ENDPOINT=… pqbench schema ls | pqbench credentials check | pqbench credentials get | pqbench tablev2 info

See also:
  pqbench tablev2 --help  read a listed table's record
  pqbench schema --help   the tables of one schema
  pqbench --help          catalog auth, lake-source shape
  docs/cli.md";

pub const RATELIMIT_ABOUT: &str = "Pace an NDJSON ref stream to a records-per-second rate";

pub const RATELIMIT_LONG_ABOUT: &str = "\
Pace an NDJSON ref stream: records pass through unchanged, delayed so each
record kind observes at most --rate records per second (15 by default; 0
turns pacing off). Nothing is dropped. One bucket per kind, so catalogs,
schemas, and table refs pace independently — the walk's per-endpoint pace.

  metastore ls | pqbench ratelimit | catalog ls
  catalog ls | pqbench ratelimit --rate 5 | schema ls

A consumer that issues one request per record as it arrives sees the same
rate. A 429 fails the level; retry it at a lower --rate.";

pub const RATELIMIT_AFTER: &str = "\
Examples:
  pqbench metastore ls | pqbench ratelimit | pqbench catalog ls
  pqbench catalog ls | pqbench ratelimit --rate 5 | pqbench schema ls

See also:
  pqbench metastore --help  the walk's first level
  pqbench catalog --help    the walk's second level
  pqbench schema --help     the walk's third level
  docs/cli.md";

pub const DUMP_ABOUT: &str = "Copy the Parquet files a table names into a directory";
pub const DUMP_LONG_ABOUT: &str = "\
Write the Parquet files named by a pqbench.table or pqbench.lake document
into OUTPUT, each at its table-relative path. A single table keeps its own
layout; a lake nests each table under its name so equal paths do not
collide. A path that would escape OUTPUT is refused. s3:// needs
--features aws.";

pub const DUMP_AFTER: &str = "\
Examples:
  pqbench table ./delta-table | pqbench dump ./sample
  pqbench dump ./sample s3://bucket/table
  pqbench lake ./warehouse | pqbench table | pqbench dump ./mirror

See also:
  pqbench table --help     name the files to copy
  pqbench bytemass --help  measure those files instead
  pqbench --help           auth for s3://
  docs/cli.md";

pub const VIZ_ABOUT: &str = "Collect a bytemass stream into a static HTML treemap";

pub const VIZ_LONG_ABOUT: &str = "\
Read `pqbench.bytemass-row` lines and write `PREFIX.html`. The page embeds
the measured rows and loads the d3 modules it uses from a CDN, drawing one
treemap per table id. Does not measure files. `-o PREFIX` is required.";

pub const VIZ_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet | pqbench viz -o report
  pqbench table ./delta-table | pqbench bytemass | pqbench viz -o report
  pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
  xdg-open report.html

See also:
  pqbench bytemass --help  produce the stream this command collects
  pqbench --help           document kinds
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
`pqbench.profile-column` per column between begin and end. `--format json`
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
  pqbench tablev2 --help      read a listed table's record
  pqbench --help              auth, lake-source shape
  docs/cli.md";

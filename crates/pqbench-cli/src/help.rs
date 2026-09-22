//! `--help` copy. The root command is the long guide (auth, documents,
//! format skills). Each subcommand stays local and points back.

/// One-line summary for `pqbench -h`.
pub const ROOT_ABOUT: &str =
    "Measure Parquet storage and codec speed; pipe lake → table → bytemass → viz";

/// Full summary for `pqbench --help`.
pub const ROOT_LONG_ABOUT: &str = "\
pqbench measures how Parquet files spend bytes (bytemass), how well codecs
compress them (lz, compression), and what a Delta or Iceberg snapshot
currently stores (table, lake). Commands compose on pipes: each writes a
versioned JSON document the next command reads.

  lake     →  pqbench.table-ref    list tables (or a catalog)
  table    →  pqbench.table        load one snapshot's log and files
  bytemass →  pqbench.bytemass-row footer byte masses (one line per column)
  viz      →  PREFIX.sqlite+html   collect the bytemass stream
  dump     →  Parquet              row sample from the same files (zstd)

A TTY prints a short summary and requires `-o` (zstd NDJSON). A pipe
streams NDJSON. Subcommand help is local (`pqbench table --help`).
Auth, documents, and format skills are here.

Guide: docs/cli.md";

/// Extensive after-text for `pqbench --help` only.
pub const ROOT_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench table ./delta-table | pqbench bytemass
  pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
  pqbench table ./delta-table | pqbench dump --row-groups first:1 -o sample.parquet
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column

Documents (kind + version 1):
  pqbench.lake-source    catalog host/token in env; lake lists it
  pqbench.lake           tables (name, uri, env); table loads each log
  pqbench.table-ref      one table name + uri + env
  pqbench.table          format, snapshot, log, active files (streamed)
  pqbench.remote-source  one URI + AWS_* from a producer
  pqbench.bytemass       begin/end around pqbench.bytemass-row lines

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
    DATABRICKS_HOST + DATABRICKS_TOKEN   workspace URL + PAT (dapi-…)
    or CATALOG_ENDPOINT + CATALOG_TOKEN
    in document env or the process environment. Bearer is for the list API
    only. Only AWS_* is copied onto listed tables — a PAT is not an S3 key.

  Databricks-governed tables
    lake lists storage_location. Reading objects still needs AWS keys that
    can GetObject. pqbench does not call temporary-table-credentials; paste
    vended STS into AWS_* on env, or use a role that already can read the
    bucket.
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

A pipe streams one `pqbench.bytemass-row` per column chunk. A TTY needs
`-o`. `--json` is the same stream. Pipe the stream to `viz`.

--include / --exclude / --sample apply to file partition paths."

pub const BYTEMASS_AFTER: &str = "\
Examples:
  pqbench bytemass data.parquet
  pqbench bytemass 'data/*.parquet' -o masses.ndjson.zst
  pqbench table ./delta-table | pqbench bytemass --include 'year=2024/**' --sample every:8
  pqbench table ./delta-table | pqbench bytemass | pqbench viz -o report

See also:
  pqbench table --help     load the snapshot this command measures
  pqbench viz --help       collect the stream into sqlite + html
  pqbench dump --help      row sample from the same files
  pqbench --help           auth, documents, format skills
  docs/cli.md";

pub const TABLE_ABOUT: &str =
    "Detect Delta or Iceberg and load one snapshot's log and active files";

pub const TABLE_LONG_ABOUT: &str = "\
Detect the format, then load metadata. Delta: transaction log and active
files. Iceberg: metadata JSON and Avro manifests (delete files in the log,
not files[]). Does not measure bytes — pipe to bytemass or dump.

Inputs: table directory or URI, Iceberg metadata JSON, a pqbench.table /
pqbench.lake / pqbench.remote-source, or '-' / stdin.

Detection: _delta_log is Delta (wins UniForm). Iceberg is
metadata/version-hint.text, metadata/*.metadata.json, or a .metadata.json
path.

--version is a Delta commit or Iceberg snapshot id (default: latest). Do
not combine it with --exclude-snapshot-*. --exclude-modified-* and
--exclude-version-* are Delta file fields (Iceberg fails). --exclude-snapshot-*
is snapshot creation time (RFC3339 UTC) on both formats.

Needs --features delta and/or iceberg (delta-s3 / iceberg-s3 for s3://).";

pub const TABLE_AFTER: &str = "\
Examples:
  pqbench table ./delta-table -o table.ndjson.zst
  pqbench table ./delta-table | pqbench bytemass
  pqbench table ./iceberg-table --exclude-snapshot-after 2024-01-01T00:00:00Z
  pqbench table ./delta-table --exclude-version-before 10

See also:
  pqbench lake --help      list tables into a document this command loads
  pqbench bytemass --help  measure the files named here
  pqbench dump --help      sample rows from those files
  pqbench --help           auth (AWS_*, catalog token), documents, format skills
  docs/delta.md  docs/iceberg.md  docs/cli.md";

pub const LAKE_ABOUT: &str = "List Delta/Iceberg tables in a directory, URI, or catalog";

pub const LAKE_LONG_ABOUT: &str = "\
Walk a warehouse or list a catalog and stream `pqbench.table-ref` lines
(name, uri, env). `pqbench table` loads each log.

Inputs: directory, file:// or s3:// prefix, a pqbench.lake to re-select, a
pqbench.lake-source, or '-' / stdin.

Walk: descend until _delta_log or an Iceberg hint / metadata JSON. Do not
search inside a table. --include / --exclude are Unix globs on the relative
table name.

Catalog: GET /v1/config — defaults object is Iceberg REST; 200 without
defaults, or HTTP 404, is Unity. Host/token: DATABRICKS_HOST +
DATABRICKS_TOKEN or CATALOG_ENDPOINT + CATALOG_TOKEN (env or process).
Only AWS_* is copied onto tables. s3:// listing needs --features aws.";

pub const LAKE_AFTER: &str = "\
Examples:
  pqbench lake ./warehouse
  pqbench lake ./warehouse --include 'sales/**' --exclude tmp
  pqbench lake s3://bucket/warehouse
  DATABRICKS_TOKEN=dapi-… pqbench lake source.json
  pqbench lake ./warehouse | pqbench table | pqbench bytemass

See also:
  pqbench table --help     load each listed table
  pqbench --help           catalog vs object auth, lake-source shape
  docs/cli.md  docs/demos/unity.json";

pub const DUMP_ABOUT: &str = "Write a row sample from Parquet files or a table document";

pub const DUMP_LONG_ABOUT: &str = "\
Read the same files bytemass would measure and write Parquet (zstd).
`--output` or a redirect; a TTY without `--output` is an error.

--sample picks files after include/exclude (all, every:N, first:N).
--row-groups first:N reads only the leading row groups.";

pub const DUMP_AFTER: &str = "\
Examples:
  pqbench dump data.parquet --output sample.parquet
  pqbench table ./delta-table | pqbench dump --row-groups first:1 --output sample.parquet
  pqbench table ./delta-table | pqbench dump --include 'year=2024/**' --sample first:1 -o sample.parquet

See also:
  pqbench table --help     name the files to dump
  pqbench bytemass --help  measure those files instead
  pqbench --help           auth for s3://
  docs/cli.md";

pub const VIZ_ABOUT: &str = "Collect a bytemass stream into SQLite and a static HTML page";

pub const VIZ_LONG_ABOUT: &str = "\
Read `pqbench.bytemass-row` lines and write `PREFIX.sqlite` plus
`PREFIX.html`. The page loads sql.js and d3 from a CDN, queries the
embedded database, and draws a column treemap. Does not measure files.
`-o PREFIX` is required.";

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
mean averages all. --json emits the same report as composable JSON.";

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
report row per column chunk. --json is the same report as composable JSON.";

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

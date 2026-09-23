# pqbench CLI

Command guide for humans and agents. `pqbench --help` is the long page
(auth, documents, format skills). `pqbench <command> --help` is local to
that command and links back. This file is the durable copy.

## What to run

| Goal | Command |
| --- | --- |
| On-disk bytes per column | `pqbench bytemass FILE` |
| Delta / Iceberg snapshot + files | `pqbench table DIR` |
| List tables in a warehouse or catalog | `pqbench lake DIR` |
| Visualize a bytemass stream | `pqbench bytemass … \| pqbench viz -o report` |
| Row sample | `pqbench dump FILE --output sample.parquet` |
| Sample-level column facts | `pqbench dump FILE \| pqbench profile` |
| Rewrite a sample and measure it | `pqbench dump FILE \| pqbench experiment --rewrite sort:ts --aim skipping` |
| Agent skill (write / DDL / codec recipes) | `pqbench skill parquet-advisor` |
| Codec speed on raw bytes | `pqbench lz FILE -c zstd@3` |
| Codec speed on Parquet pages | `pqbench compression FILE` (NONE-compressed only) |

The usual lake pipe:

```sh
pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
```

A TTY prints a short summary and requires `-o`. A pipe streams NDJSON.
Credentials travel on that document (`AWS_*`; catalog host/token on a
lake-source) and are not exported into the process environment.

## Documents

| `kind` | Produced by | Consumed by |
| --- | --- | --- |
| `pqbench.lake-source` | you / a producer | `lake` |
| `pqbench.table-ref` | `lake` | `table` |
| `pqbench.table` | `table` | `bytemass`, `dump`, `table` (re-apply excludes) |
| `pqbench.remote-source` | a producer | `table`, `bytemass` |
| `pqbench.bytemass` / `pqbench.bytemass-file` / `pqbench.bytemass-row` | `bytemass` | `viz` |
| `pqbench.profile` / `pqbench.profile-column` / `pqbench.profile-dependency` | `profile` | an agent / the next pass |
| `pqbench.experiment` / `pqbench.experiment-trial` / `pqbench.experiment-column` | `experiment` | an agent / the next pass |
| `pqbench.skill` | `skill` (list) | an agent |

All current documents are version `1`.

## Flags that repeat

| Flag | Meaning |
| --- | --- |
| `--include GLOB` / `--exclude GLOB` | Unix globs (`*`, `?`, `**`). On `lake` they match table names; on `table` / `bytemass` / `dump` they match file partition paths. Hive prefixes on `table` (`year=2024/**`) are pushed into the Delta listing. |
| `--no-stats` | On `table`: keep `num_records` / `bytes_per_row`, drop min/max/null maps. |
| `--indexes` | On `bytemass`: also load ColumnIndex/OffsetIndex (one extra range). Off by default. |
| `--dependencies` | On `profile`: pairwise locality analysis. Off by default (`O(pairs · rows)`). |
| `--measures NAME` | On `profile`: request a locality measure (`pair_ndv`, `entropy`, `mutual_information`, `functional_dependency`, `null_cooccurrence`, `numeric_relationship`, `categorical_association`, `all`). Implies `--dependencies`. |
| `--pairs LEFT,RIGHT` | On `profile`: analyze only that column pair. Implies `--dependencies`. |
| `--rewrite SPEC` / `--trial SPEC` | On `experiment`: one empirical rewrite (`sort:A,B`, `codec:zstd@3`, …). |
| `--aim storage\|skipping\|all` | On `experiment`: measure bytes per row, skip locality, or both. |
| `--rows all\|first:N` | On `profile`: cap decoded rows (default `first:8192`). |
| `--columns GLOB` | On `profile`: keep column names matching the glob. |
| `--sample all\|every:N\|first:N` | After include/exclude, keep every file, every Nth, or the first N. |
| `--exclude-modified-before/after TIME` | RFC3339 UTC. Delta log `modificationTime`. Iceberg has none (fails). |
| `--exclude-version-before/after N` | Delta add version. Iceberg has none (fails). |
| `--exclude-snapshot-before/after TIME` | Snapshot creation time. Not together with `--version`. |
| `--version N` | Delta commit or Iceberg snapshot id. Default: latest. |

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

Only `AWS_*` names are accepted on `pqbench.table`, `pqbench.remote-source`,
and listed lake tables.

```sh
# Process env (instance role, SSO, shared credentials):
AWS_PROFILE=analytics pqbench bytemass s3://bucket/table/part-0.parquet

# Vended STS on a producer document:
cat <<'EOF' | pqbench table | pqbench bytemass
{"kind":"pqbench.remote-source","version":1,
 "inputs":["s3://bucket/table"],
 "env":{"AWS_ACCESS_KEY_ID":"…","AWS_SECRET_ACCESS_KEY":"…",
        "AWS_SESSION_TOKEN":"…","AWS_REGION":"us-east-1"}}
EOF
```

### Catalog list (Unity / Databricks / Iceberg REST)

A `pqbench.lake-source` lists tables. Host and token live in `env` or the
process environment:

| Name | Role |
| --- | --- |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | Workspace URL + PAT (`dapi-…`) |
| `CATALOG_ENDPOINT` / `CATALOG_TOKEN` | Unity OSS or Iceberg REST |

The token is a Bearer on the **list** API only. Only `AWS_*` is copied onto
listed tables. A PAT does not open `s3://`.

```sh
DATABRICKS_TOKEN=dapi-… pqbench lake source.json
```

```json
{
  "kind": "pqbench.lake-source",
  "version": 1,
  "env": {
    "DATABRICKS_HOST": "https://example.cloud.databricks.com",
    "DATABRICKS_TOKEN": "dapi-…",
    "AWS_REGION": "us-east-1"
  }
}
```

### Databricks-governed tables

`lake` returns `storage_location`. Reading the objects still needs AWS keys
that can `GetObject` / `HeadObject`. pqbench does **not** call
`temporary-table-credentials` or `temporary-path-credentials`. Paste those
STS keys into `AWS_*` on `env`, or run under a role that already can read
the bucket.

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
[demo.md](demo.md).

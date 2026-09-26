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
| Copy a table's Parquet files | `pqbench table DIR \| pqbench dump ./sample` |
| Codec speed on raw bytes | `pqbench lz FILE -c zstd@3` |
| Codec speed on Parquet pages | `pqbench compression FILE` (NONE-compressed only) |

The usual lake pipe:

```sh
pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
```

A TTY prints a short summary and requires `-o`. A pipe streams NDJSON.
Credentials travel on that document (`AWS_*`; a catalog `token` on a
lake-source) and are not exported into the process environment.

## Documents

| `kind` | Produced by | Consumed by |
| --- | --- | --- |
| `pqbench.lake-source` | you / a producer | `lake` |
| `pqbench.table-ref` | `lake` | `table` |
| `pqbench.table` | `table` | `bytemass`, `dump` |
| `pqbench.remote-source` | a producer | `table`, `bytemass` |
| `pqbench.bytemass` / `pqbench.bytemass-row` | `bytemass` | `viz` |

All current documents are version `1`.

## Flags that repeat

| Flag | Meaning |
| --- | --- |
| `--include GLOB` / `--exclude GLOB` | `lake` only. Unix globs (`*`, `?`, `**`) on the relative table name; a literal leading name prunes the walk. |
| `--max-depth N` | `lake` only. Bound a tree with no table marker (default 8). |
| `--version N` | `table` only. Delta commit or Iceberg snapshot id. Default: latest. |
| `-o` / `--output` | `FILE` on a TTY (zstd NDJSON); `PREFIX` on `viz`. Required on a terminal. |
| `--json` | `bytemass`, `lz`, `compression`: stream NDJSON on stdout. |

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

A `pqbench.lake-source` lists tables. It names the catalog at the top level:
`endpoint` and an optional `token`, plus `catalog` / `schema` to narrow the
list.

| Field | Role |
| --- | --- |
| `endpoint` | Databricks workspace URL, Unity OSS, or Iceberg REST base |
| `token` | Bearer PAT (`dapi-…`) or OAuth token; list API only |
| `catalog` / `schema` | Optional catalog / schema (or glob) to list |

The token is a Bearer on the **list** API only. Only `AWS_*` is copied onto
listed tables. A PAT does not open `s3://`.

```json
{
  "kind": "pqbench.lake-source",
  "version": 1,
  "endpoint": "https://example.cloud.databricks.com",
  "token": "dapi-…",
  "catalog": "main",
  "env": { "AWS_REGION": "us-east-1" }
}
```

```sh
pqbench lake source.json --include 'main.default.*'
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

# External Unity Catalog credentials piped into pqbench

## Scope

`pqbench` does not need a Unity Catalog command, an HTTP client, or a
Databricks dependency. It accepts a versioned source document on stdin:

```sh
external-command | pqbench bytemass --source -
```

The external command resolves the catalog table and vends credentials. `jq`
maps its JSON response to `pqbench.remote-source` v1. `pqbench` passes opaque
storage options only to its isolated `object_store` adapter, which makes
footer-only reads of supplied Parquet objects.

The input is for **Parquet object URIs**, not a Delta table root. Resolve active
Parquet files outside pqbench for this minimal feature. Delta table selection
and `pqbench dump` are separate work.

## pqbench source document

```json
{
  "kind": "pqbench.remote-source",
  "version": 1,
  "inputs": [
    "s3://bucket/path/part-00000.parquet",
    "s3://bucket/path/part-00001.parquet"
  ],
  "object_store_options": {
    "aws_access_key_id": "…",
    "aws_secret_access_key": "…",
    "aws_session_token": "…",
    "aws_region": "us-east-1"
  }
}
```

- `inputs` contains absolute URIs. Each is read using HEAD plus bounded range
  reads for the Parquet trailer and footer; no data pages are fetched.
- `object_store_options` is a generic string map. Bytemass and Parquet parsing
  do not know about S3, UC, AWS, or credential shapes.
- Build with the AWS feature:

  ```sh
  cargo run -p pqbench-cli --features aws -- bytemass --source -
  ```

The source is stdin-only. This avoids command arguments and environment
variables managed by pqbench. Do not log, persist, or echo the generated JSON.

## OSS Unity Catalog + local S3-compatible stack

The local OSS UC POC proved these endpoints:

```text
GET  /api/2.1/unity-catalog/tables/{catalog.schema.table}
POST /api/2.1/unity-catalog/temporary-table-credentials
```

The bundled table uses `file://`, so it returned a URL and null cloud
credentials. Configure UC with MinIO or LocalStack S3 credentials, then
register an external Delta table at `s3://…` for the next experiment.

### 1. Resolve table metadata

```sh
UC=http://127.0.0.1:8080
TABLE=unity.default.events

table=$(curl --fail --silent --show-error \
  "$UC/api/2.1/unity-catalog/tables/$TABLE")
table_id=$(jq -er '.table_id' <<<"$table")
```

### 2. Request temporary read credentials

```sh
credentials=$(curl --fail --silent --show-error \
  -X POST "$UC/api/2.1/unity-catalog/temporary-table-credentials" \
  -H 'Content-Type: application/json' \
  --data "$(jq -nc --arg table_id "$table_id" \
    '{table_id: $table_id, operation: "READ"}')")
```

For S3, inspect one redacted response to confirm exact OSS field names.
Expected material is an S3 URL and `aws_temp_credentials` containing access
key, secret key, session token, region, and expiry.

### 3. Supply real Parquet object URIs

For this POC, obtain active files from the fixture bootstrap and preserve their
order. Do not use arbitrary S3 listing order as Delta semantics.

```sh
parquet_inputs='[
  "s3://pqbench-fixtures/events/part-00000.parquet",
  "s3://pqbench-fixtures/events/part-00001.parquet"
]'
```

### 4. `jq` to pqbench pipe

Adjust `.aws_temp_credentials` paths only if the local UC response differs:

```sh
jq -n \
  --argjson inputs "$parquet_inputs" \
  --argjson credentials "$credentials" \
  '{
    kind: "pqbench.remote-source",
    version: 1,
    inputs: $inputs,
    object_store_options: {
      aws_access_key_id: $credentials.aws_temp_credentials.access_key_id,
      aws_secret_access_key: $credentials.aws_temp_credentials.secret_access_key,
      aws_session_token: $credentials.aws_temp_credentials.session_token,
      aws_region: ($credentials.aws_temp_credentials.region // "us-east-1")
    }
  }' \
| cargo run -q -p pqbench-cli --features aws -- bytemass --source -
```

For MinIO or LocalStack add options known by the fixture setup:

```jq
aws_endpoint: "http://localhost:9000",
aws_allow_http: "true"
```

Use `localhost` from a host process or the Compose service name from a
containerized pqbench process.

## Databricks Unity Catalog variant

The consumer stays identical; only the producer changes.

```sh
WORKSPACE='https://<workspace-host>'
TABLE='main.analytics.events'

table=$(curl --fail --silent --show-error \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "$WORKSPACE/api/2.1/unity-catalog/tables/$TABLE")
table_id=$(jq -er '.table_id' <<<"$table")

credentials=$(curl --fail --silent --show-error \
  -X POST "$WORKSPACE/api/2.1/unity-catalog/temporary-table-credentials" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H 'Content-Type: application/json' \
  --data "$(jq -nc --arg table_id "$table_id" \
    '{table_id: $table_id, operation: "READ"}')")
```

Databricks CLI can be the producer instead:

```sh
table=$(databricks tables get "$TABLE" --output json)
table_id=$(jq -er '.table_id' <<<"$table")
credentials=$(databricks temporary-table-credentials \
  generate-temporary-table-credentials \
  --table-id "$table_id" --operation READ --output json)
```

Follow either producer with the `jq -n … | pqbench bytemass --source -` command
above. Databricks requires external access and the relevant UC privileges.

## Validation

1. Use the vended credentials with an S3 client for HEAD and range GET first.
2. Compare pqbench output with local analysis of the same fixture files.
3. Check S3/MinIO logs: metadata, trailer, and footer reads only; no whole-file
   GET.
4. Confirm invalid credentials fail without displaying key material.

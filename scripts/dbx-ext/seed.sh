#!/usr/bin/env bash
#
# Seed the pqbench external fixture over S3 for the Databricks e2e.
#
# Two layers:
#
#   AWS (pqbench-uc.yaml, CloudFormation): the S3 bucket and the IAM role Unity
#   Catalog assumes. Deploy that once; CloudFormation cannot create Unity
#   Catalog objects.
#
#   Databricks (this script): the fixture schemas and their external Delta
#   tables, created with the `databricks` CLI and SQL on a SQL warehouse. The
#   data is copied server-side from the sample catalog into the fixture's
#   external location:
#
#       CREATE TABLE IF NOT EXISTS pqbench_ext.<schema>.<table>
#       USING DELTA
#       LOCATION 's3://<bucket>/pqbench_ext/<schema>/<table>'
#       AS SELECT * FROM dbx_samples.<schema>.<table>
#
# Nothing crosses the client: the warehouse reads the source and writes the
# destination on S3. Only DDL results come back to this shell.
#
# Every fixture table is external, so Unity Catalog vends read credentials for
# it — what the issue-#58 walk exercises:
#
#       schema ls | credentials check | credentials get
#         | table info | table ls | partition ls | bytemass
#
# Copy set (source -> fixture), sized to fit the workspace's storage budget:
#
#   nyctaxi:trips                 one table
#   bakehouse                     six tables
#   accuweather                   twelve tables
#   healthverity                  one table
#   tpcds_sf1                     twenty-four tables
#   tpch_sf1                      eight tables
#   tpch_sf10                     eight tables
#   clickbench                    one large table (a perf target)
#
# Idempotent and resumable: `CREATE TABLE IF NOT EXISTS` skips a table that is
# already there, so re-running after a failure continues where it stopped.
#
# Usage:
#
#   scripts/dbx-ext/seed.sh
#   DATABRICKS_PROFILE=dbx-samples-sp scripts/dbx-ext/seed.sh
#   PQB_WAREHOUSE=<id> scripts/dbx-ext/seed.sh
#
# Tunables (environment): DATABRICKS_PROFILE, PQB_CATALOG, PQB_SOURCE_CATALOG,
# PQB_BUCKET, PQB_PRINCIPAL, PQB_WAREHOUSE, PQB_COPY_SET.

set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
CATALOG="${PQB_CATALOG:-pqbench_ext}"
SOURCE_CATALOG="${PQB_SOURCE_CATALOG:-dbx_samples}"
BUCKET="${PQB_BUCKET:-pqbench-uc-e2e-us-east-2}"
# The e2e service principal; granted on every schema the walk reads.
PRINCIPAL="${PQB_PRINCIPAL:-c241bd26-b7a7-4c1d-9c35-b4aaf4f431f9}"
WAREHOUSE="${PQB_WAREHOUSE:-}"

# `schema` copies every table in it; `schema:table` copies one table.
COPY_SET="${PQB_COPY_SET:-nyctaxi:trips bakehouse accuweather healthverity tpcds_sf1 tpch_sf1 tpch_sf10 clickbench}"

dbx() { databricks "$@" --profile "$PROFILE"; }

if [ -z "$WAREHOUSE" ]; then
  WAREHOUSE="$(dbx warehouses list -o json | jq -r '.[0].id')"
fi
echo "warehouse $WAREHOUSE, $SOURCE_CATALOG -> $CATALOG, bucket $BUCKET"

# Run one statement and wait for it; retry the transient free-tier failures.
run_sql() {
  local statement="$1" attempt response id state
  for attempt in 1 2 3 4 5 6; do
    response="$(dbx api post /api/2.0/sql/statements --json "$(jq -n \
      --arg warehouse "$WAREHOUSE" --arg statement "$statement" \
      '{warehouse_id:$warehouse, statement:$statement, wait_timeout:"50s"}')")"
    id="$(jq -r '.statement_id' <<<"$response")"
    state="$(jq -r '.status.state' <<<"$response")"
    while [ "$state" = "PENDING" ] || [ "$state" = "RUNNING" ]; do
      sleep 3
      response="$(dbx api get "/api/2.0/sql/statements/$id")"
      state="$(jq -r '.status.state' <<<"$response")"
    done
    if [ "$state" = "SUCCEEDED" ]; then
      return 0
    fi
    echo "sql attempt $attempt: $state" >&2
    sleep 5
  done
  echo "sql failed: $statement" >&2
  jq -r '.status.error.message // empty' <<<"$response" >&2
  return 1
}

create_schema() {
  local schema="$1"
  dbx schemas create "$schema" "$CATALOG" >/dev/null 2>&1 || true
  dbx grants update schema "$CATALOG.$schema" --json "$(jq -n --arg principal "$PRINCIPAL" \
    '{changes:[{principal:$principal, add:["USE_SCHEMA","SELECT","EXTERNAL_USE_SCHEMA"]}]}')" \
    >/dev/null
  echo "schema $CATALOG.$schema"
}

copy_table() {
  local schema="$1" table="$2"
  local location="s3://$BUCKET/$CATALOG/$schema/$table"
  run_sql "CREATE TABLE IF NOT EXISTS $CATALOG.$schema.$table USING DELTA LOCATION '$location' AS SELECT * FROM $SOURCE_CATALOG.$schema.$table"
  echo "  $schema.$table"
}

list_tables() {
  dbx tables list "$SOURCE_CATALOG" "$1" -o json | jq -r '.[].name'
}

for entry in $COPY_SET; do
  schema="${entry%%:*}"
  tables="${entry#*:}"
  create_schema "$schema"
  if [ "$tables" = "$entry" ]; then
    tables="$(list_tables "$schema")"
  else
    tables="$(tr ',' ' ' <<<"$tables")"
  fi
  for table in $tables; do
    copy_table "$schema" "$table"
  done
done

echo "done"

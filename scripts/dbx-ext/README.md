# The Databricks external fixture

The live Databricks half of the e2e: a Unity Catalog catalog whose tables are
**external Delta on S3**, so Unity vends read credentials and the issue-#58 walk
runs end to end —

```console no-run
$ pqbench schema ls <catalog>.<schema> | pqbench credentials check \
    | pqbench credentials get | pqbench table info \
    | pqbench table ls | pqbench partition ls | pqbench bytemass
```

Two layers, because CloudFormation cannot create Unity Catalog objects:

| Layer | What | Where |
| --- | --- | --- |
| AWS | S3 bucket + the IAM role Unity assumes | [`pqbench-uc.yaml`](pqbench-uc.yaml) (CloudFormation) |
| Databricks | storage credential, external location, catalog, schemas, tables | [`seed.sh`](seed.sh) (`databricks` CLI + SQL) |

## AWS: the bucket and the role

```bash
aws cloudformation deploy \
  --template-file scripts/dbx-ext/pqbench-uc.yaml \
  --stack-name pqbench-uc \
  --region us-east-2 \
  --capabilities CAPABILITY_NAMED_IAM
```

That prints a bucket name and a role ARN. The role is self-assuming: it trusts
both the Unity Catalog master role and itself, so same-account external locations
work.

## Databricks: the catalog and the tables

`seed.sh` uses the `databricks` CLI and SQL on a SQL warehouse; the data is
copied **server-side** (`CREATE TABLE ... AS SELECT`), never through the client:

```bash
scripts/dbx-ext/seed.sh
```

It creates the storage credential and external location over the bucket, the
catalog, and — for each source table — an external Delta table:

```sql
CREATE TABLE IF NOT EXISTS pqbench_ext.<schema>.<table>
USING DELTA
LOCATION 's3://<bucket>/pqbench_ext/<schema>/<table>'
AS SELECT * FROM dbx_samples.<schema>.<table>
```

Then it grants the e2e service principal `USE_SCHEMA`, `SELECT`, and
`EXTERNAL_USE_SCHEMA` on each schema (`EXTERNAL_USE_SCHEMA` is what lets the walk
vend credentials; it is not inherited from ownership or `ALL_PRIVILEGES`).

The copy set is real sample data, sized to the workspace's storage budget:

| Source | Fixture | Tables |
| --- | --- | --- |
| `dbx_samples.nyctaxi` | `pqbench_ext.nyctaxi` | `trips` |
| `dbx_samples.bakehouse` | `pqbench_ext.bakehouse` | 6 |
| `dbx_samples.accuweather` | `pqbench_ext.accuweather` | 12 |
| `dbx_samples.healthverity` | `pqbench_ext.healthverity` | `claims_sample_synthetic` |
| `dbx_samples.tpcds_sf1` | `pqbench_ext.tpcds_sf1` | 24 |
| `dbx_samples.tpch_sf1` | `pqbench_ext.tpch_sf1` | 8 |
| `dbx_samples.tpch_sf10` | `pqbench_ext.tpch_sf10` | 8 |
| `dbx_samples.clickbench` | `pqbench_ext.clickbench` | `hits` (large) |

Override the set with `PQB_COPY_SET` (`schema` copies every table in it,
`schema:table` copies one table).

`seed.sh` is idempotent and resumable: a table that already exists is skipped, so
re-running after a failure continues where it stopped.

## Running the e2e against it

`make dbx-e2e` drives the fixture with the service principal's OAuth M2M bearer
(mint one with the `dbx-samples-sp` profile, `experiments/exp33_databricks_credential_vending.md`):

```bash
export DBX_HOST=https://<workspace>.cloud.databricks.com
export DBX_SAMPLES_SP_CLIENT_ID=... DBX_SAMPLES_SP_CLIENT_SECRET=...
make dbx-e2e
```

See [`docs/performance.md`](../../docs/performance.md) for the perf recipe.

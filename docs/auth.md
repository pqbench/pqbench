# Databricks authentication

How pqbench reaches a Databricks workspace for catalog and metastore reads:
a service principal with OAuth M2M credentials. The commands below are the
procedure used for the `dbx_samples` workspace; replace names and ids as
needed.

## Create the service principal

```sh
# 1. Create the SP in the workspace (workspace SCIM).
databricks service-principals create --display-name dbx-samples-sp --profile DEFAULT -o json
# → id <numeric-id>, applicationId <uuid>

# 2. Workspace-level OAuth secret (the SP must be in the workspace).
databricks service-principal-secrets-proxy create <numeric-id> --profile DEFAULT -o json
# → secret + expire_time (the value is shown once)

# 3. Workspace entitlement. Without it every workspace API answers
#    "This API is disabled for users without the databricks-sql-access or
#     workspace-access or workspace-consume entitlements."
databricks api patch /api/2.0/preview/scim/v2/ServicePrincipals/<numeric-id> \
  --profile DEFAULT \
  --json '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
           "Operations":[{"op":"add","path":"entitlements",
                          "value":[{"value":"workspace-access"}]}]}'
```

## Profile and verify

Write an OAuth M2M profile in `~/.databrickscfg`:

```ini
[dbx-samples-sp]
host = https://<workspace>.cloud.databricks.com
client_id = <applicationId>
client_secret = <secret>
auth_type = oauth-m2m
```

Then verify the identity:

```sh
databricks current-user me --profile dbx-samples-sp -o json
# → displayName dbx-samples-sp, userName <applicationId>
```

The bearer for a REST call is a short-lived OAuth token minted from the
profile: `POST /oidc/v1/token` with HTTP Basic `client_id:client_secret`,
`grant_type=client_credentials`, `scope=all-apis`.

## Minimum permissions

Catalog and metadata reads need `external_access_enabled = true` on the
metastore (metastore admin), the `workspace-access` entitlement on the SP
(workspace admin), and `USE CATALOG` / `USE SCHEMA` / `SELECT` grants (catalog
owner). Credential vending additionally needs external storage and
`EXTERNAL USE LOCATION`; default-storage tables are excluded. The full table
and the experiment that verified it are in
[exp-33](../experiments/exp33_databricks_credential_vending.md).

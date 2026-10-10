//! The catalog calls behind [`super::api`].
//!
//! Unity serves `GET /tables/{catalog}.{schema}.{table}`; the record names the
//! table id and the securable kind. Databricks default storage
//! (`TABLE_DB_STORAGE`) is the kind the vending route refuses, so it stops
//! there; for any other kind `POST /temporary-table-credentials` vends read
//! credentials. A missing kind says nothing, so a catalog that reports none
//! (Unity OSS) is attempted. Serverless notebooks are refused
//! (`UC_SERVERLESS_UNTRUSTED_DOMAIN_STORAGE_TOKEN_MINTING`); those refs pass
//! through with no env, since the compute reaches storage itself.
//!
//! Iceberg REST serves `loadCredentials` (`GET
//! …/tables/{table}/credentials`) with the catalog's vended credentials: a
//! list of prefix-scoped configs whose `s3.*` keys become the ref's `AWS_*`
//! env. A catalog that does not serve the route (or cannot mint for the table,
//! like a UniForm Delta table) falls back to `loadTable` with
//! `X-Iceberg-Access-Delegation: vended-credentials`, where the longest prefix
//! covering the table's location wins, and `config` may carry the keys. A
//! table the catalog cannot serve via Iceberg (`is not an Iceberg compatible
//! table`), an unknown table, and an unimplemented operation all pass through
//! with no credentials.
//!
//! The URLs and JSON shapes are this module's; the transport is the
//! third-party facade.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::api::Error;
use crate::dialect;
use crate::third_party::reqwest::{self, Request};

/// The subset of the Unity table record vending needs: the table id and the
/// securable kind that gates the attempt.
#[derive(Deserialize)]
struct TableRecord {
    #[serde(default)]
    table_id: Option<String>,
    #[serde(default)]
    securable_kind: Option<String>,
}

impl TableRecord {
    /// Whether the catalog reports the table readable outside Databricks
    /// compute. Databricks default storage (`TABLE_DB_STORAGE`) is the kind
    /// the vending route refuses; a missing kind says nothing, so the table
    /// is attempted and the route decides.
    fn eligible(&self) -> bool {
        self.securable_kind.as_deref() != Some("TABLE_DB_STORAGE")
    }
}

/// The table's Unity record: the table id and the securable kind.
async fn table_record(root: &str, name: &str, token: Option<&str>) -> Result<TableRecord, Error> {
    dialect::get_json(&format!("{root}/tables/{}", dialect::encode(name)), token)
        .await
        .map_err(Error::from)
}

pub(super) async fn vend_unity(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Option<BTreeMap<String, String>>, Error> {
    let name = format!("{catalog}.{schema}.{table}");
    let root = dialect::api_root(endpoint);
    let record = table_record(&root, &name, token).await?;
    if !record.eligible() {
        return Ok(None);
    }
    let Some(table_id) = record.table_id.filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    let response = reqwest::request(Request::post(
        format!("{root}/temporary-table-credentials"),
        token.map(str::to_owned),
        serde_json::json!({"table_id": table_id, "operation": "READ"}).to_string(),
    ))
    .await
    .map_err(|error| Error::from(error.to_string()))?;
    if matches!(response.status, 404 | 501) {
        return Ok(None);
    }
    if response.status == 403
        && String::from_utf8_lossy(&response.bytes)
            .contains("UC_SERVERLESS_UNTRUSTED_DOMAIN_STORAGE_TOKEN_MINTING")
    {
        // Serverless notebooks cannot mint storage credentials; the compute's
        // engines reach storage, so the ref passes through with no env.
        return Ok(None);
    }
    if response.status != 200 {
        return Err(Error::from(format!(
            "the endpoint returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        )));
    }
    let record: CredentialsRecord = serde_json::from_slice(&response.bytes).map_err(|error| {
        Error::from(format!(
            "the response was not the expected document: {error}"
        ))
    })?;
    Ok(record.aws_temp_credentials.map(|credentials| {
        BTreeMap::from([
            ("AWS_ACCESS_KEY_ID".to_string(), credentials.access_key_id),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                credentials.secret_access_key,
            ),
            ("AWS_SESSION_TOKEN".to_string(), credentials.session_token),
        ])
    }))
}

/// The subset of the temporary-credentials response this command reports.
#[derive(Deserialize)]
struct CredentialsRecord {
    #[serde(default)]
    aws_temp_credentials: Option<AwsTempCredentials>,
}

#[derive(Deserialize)]
struct AwsTempCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
}

/// The catalog's vended credentials for one Iceberg table: `loadCredentials`
/// (`GET …/tables/{table}/credentials`) names the `s3.*` keys directly; a
/// catalog that does not serve the route (or cannot mint for the table, like a
/// UniForm Delta table) falls back to the delegated `loadTable`.
pub(super) async fn vend_iceberg(
    endpoint: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Option<BTreeMap<String, String>>, Error> {
    let url = format!(
        "{}/namespaces/{}/tables/{}/credentials",
        dialect::iceberg_root(endpoint),
        dialect::iceberg_namespace(schema),
        dialect::encode(table)
    );
    let response = reqwest::request(Request::get(url, token.map(str::to_owned)))
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    if response.status == 200 {
        let loaded: LoadedCredentials =
            serde_json::from_slice(&response.bytes).map_err(|error| {
                Error::from(format!(
                    "the response was not the expected document: {error}"
                ))
            })?;
        return Ok(credential_options(&loaded.storage_credentials));
    }
    vend_iceberg_load(endpoint, schema, table, token).await
}

/// The delegated `loadTable` route: `X-Iceberg-Access-Delegation:
/// vended-credentials` may carry `storage-credentials` — the longest prefix
/// covering the table's location wins — or the `s3.*` keys in `config`. A
/// table the catalog cannot serve via Iceberg, an unknown table, and an
/// unimplemented operation all pass through with no credentials.
async fn vend_iceberg_load(
    endpoint: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Option<BTreeMap<String, String>>, Error> {
    let url = format!(
        "{}/namespaces/{}/tables/{}",
        dialect::iceberg_root(endpoint),
        dialect::iceberg_namespace(schema),
        dialect::encode(table)
    );
    let request = Request::get(url, token.map(str::to_owned))
        .header("X-Iceberg-Access-Delegation", "vended-credentials");
    let response = reqwest::request(request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    if matches!(response.status, 404 | 501) {
        return Ok(None);
    }
    if response.status == 400
        && String::from_utf8_lossy(&response.bytes).contains("is not an Iceberg compatible table")
    {
        return Ok(None);
    }
    if response.status != 200 {
        return Err(Error::from(format!(
            "the endpoint returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        )));
    }
    let loaded: LoadedTable = serde_json::from_slice(&response.bytes).map_err(|error| {
        Error::from(format!(
            "the response was not the expected document: {error}"
        ))
    })?;
    let location = loaded
        .metadata
        .and_then(|metadata| metadata.location)
        .unwrap_or_default();
    let config = matching_config(&loaded.storage_credentials, &location).unwrap_or(loaded.config);
    Ok(aws_options(&config))
}

/// The subset of the Iceberg REST `loadTable` response vending needs.
#[derive(Deserialize)]
struct LoadedTable {
    #[serde(default)]
    metadata: Option<TableMetadata>,
    #[serde(default)]
    config: BTreeMap<String, String>,
    #[serde(default, rename = "storage-credentials")]
    storage_credentials: Vec<StorageCredential>,
}

#[derive(Deserialize)]
struct TableMetadata {
    #[serde(default)]
    location: Option<String>,
}

/// The document `loadCredentials` returns: the vended storage credentials.
#[derive(Deserialize)]
struct LoadedCredentials {
    #[serde(default, rename = "storage-credentials")]
    storage_credentials: Vec<StorageCredential>,
}

/// One prefix-scoped credential set from `storage-credentials`.
#[derive(Deserialize)]
struct StorageCredential {
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    config: BTreeMap<String, String>,
}

/// The `AWS_*` options from the first vended credential naming an access key:
/// the credentials route is table-scoped, so any credential it returns can
/// supply the env.
fn credential_options(credentials: &[StorageCredential]) -> Option<BTreeMap<String, String>> {
    credentials
        .iter()
        .find_map(|credential| aws_options(&credential.config))
}

/// The config of the storage credential whose prefix covers `location`; the
/// longest prefix wins. `None` when no prefix matches.
fn matching_config(
    credentials: &[StorageCredential],
    location: &str,
) -> Option<BTreeMap<String, String>> {
    credentials
        .iter()
        .filter(|credential| {
            !credential.prefix.is_empty() && location.starts_with(&credential.prefix)
        })
        .max_by_key(|credential| credential.prefix.len())
        .map(|credential| credential.config.clone())
}

/// The `AWS_*` options an Iceberg `s3.*` config names; `None` without an
/// access key.
fn aws_options(config: &BTreeMap<String, String>) -> Option<BTreeMap<String, String>> {
    let key = config.get("s3.access-key-id")?;
    let secret = config.get("s3.secret-access-key")?;
    let mut options = BTreeMap::from([
        ("AWS_ACCESS_KEY_ID".to_string(), key.clone()),
        ("AWS_SECRET_ACCESS_KEY".to_string(), secret.clone()),
    ]);
    if let Some(token) = config.get("s3.session-token") {
        options.insert("AWS_SESSION_TOKEN".to_string(), token.clone());
    }
    Some(options)
}

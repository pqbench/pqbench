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
//! list of prefix-scoped configs whose `s3.*` keys become the table's `AWS_*`
//! env. A catalog that does not serve the route (or cannot mint for the table,
//! like a UniForm Delta table) falls back to `loadTable` with
//! `X-Iceberg-Access-Delegation: vended-credentials`, where the longest prefix
//! covering the table's location wins, and `config` may carry the keys. A
//! table the catalog cannot serve via Iceberg (`is not an Iceberg compatible
//! table`) falls back to the Unity vending route, so a Delta or UniForm table
//! walks under either dialect; an unknown table and an unimplemented operation
//! return no credentials, so the read keeps its ambient env.
//!
//! The URLs and JSON shapes are this module's; the transport is the
//! third-party facade.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::api::Error;
use crate::dialect;
use crate::third_party::reqwest::Request;
use crate::v1::Credentials;
use crate::Workspace;

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
async fn table_record(workspace: &Workspace, root: &str, name: &str) -> Result<TableRecord, Error> {
    crate::http::get_json(
        workspace,
        &format!("{root}/tables/{}", dialect::encode(name)),
        workspace.config().token.as_deref(),
    )
    .await
    .map_err(Error::Http)
}

pub(super) async fn vend_unity(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Option<Credentials>, Error> {
    let name = format!("{catalog}.{schema}.{table}");
    // Under the Iceberg dialect the endpoint names the Iceberg base; the
    // Unity route is still the root it was built from.
    let root = dialect::unity_root(workspace.config().endpoint.as_str());
    let record = table_record(workspace, &root, &name).await?;
    if !record.eligible() {
        return Ok(None);
    }
    let Some(table_id) = record.table_id.filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    let response = workspace
        .http()
        .request(Request::post(
            format!("{root}/temporary-table-credentials"),
            workspace.config().token.clone(),
            serde_json::json!({"table_id": table_id, "operation": "READ"}).to_string(),
        ))
        .await
        .map_err(|error| Error::Http(crate::http::Error::Transport(error)))?;
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
        return Err(Error::Http(crate::http::Error::Status {
            status: response.status,
            body: String::from_utf8_lossy(&response.bytes).into_owned(),
        }));
    }
    let record: CredentialsRecord = serde_json::from_slice(&response.bytes)
        .map_err(|error| Error::Http(crate::http::Error::Body(error)))?;
    let expire_time = record.expiration_time;
    Ok(record.aws_temp_credentials.map(|credentials| Credentials {
        access_key_id: Some(credentials.access_key_id),
        secret_access_key: Some(credentials.secret_access_key),
        session_token: Some(credentials.session_token),
        expire_time,
    }))
}

/// The subset of the temporary-credentials response this command reports.
#[derive(Deserialize)]
struct CredentialsRecord {
    #[serde(default)]
    aws_temp_credentials: Option<AwsTempCredentials>,
    #[serde(default)]
    expiration_time: Option<u64>,
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
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Option<Credentials>, Error> {
    let url = format!(
        "{}/namespaces/{}/tables/{}/credentials",
        dialect::iceberg_root(workspace.config().endpoint.as_str()),
        dialect::iceberg_namespace(schema),
        dialect::encode(table)
    );
    let response = workspace
        .http()
        .request(Request::get(url, workspace.config().token.clone()))
        .await
        .map_err(|error| Error::Http(crate::http::Error::Transport(error)))?;
    if response.status == 200 {
        let loaded: LoadedCredentials = serde_json::from_slice(&response.bytes)
            .map_err(|error| Error::Http(crate::http::Error::Body(error)))?;
        return Ok(credential_options(&loaded.storage_credentials));
    }
    vend_iceberg_load(workspace, catalog, schema, table).await
}

/// The delegated `loadTable` route: `X-Iceberg-Access-Delegation:
/// vended-credentials` may carry `storage-credentials` — the longest prefix
/// covering the table's location wins — or the `s3.*` keys in `config`. A
/// table the catalog cannot serve via Iceberg falls back to the Unity vending
/// route; an unknown table and an unimplemented operation pass through with no
/// credentials.
async fn vend_iceberg_load(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Option<Credentials>, Error> {
    let url = format!(
        "{}/namespaces/{}/tables/{}",
        dialect::iceberg_root(workspace.config().endpoint.as_str()),
        dialect::iceberg_namespace(schema),
        dialect::encode(table)
    );
    let request = Request::get(url, workspace.config().token.clone())
        .header("X-Iceberg-Access-Delegation", "vended-credentials");
    let response = workspace
        .http()
        .request(request)
        .await
        .map_err(|error| Error::Http(crate::http::Error::Transport(error)))?;
    if matches!(response.status, 404 | 501) {
        return Ok(None);
    }
    if response.status == 400
        && String::from_utf8_lossy(&response.bytes).contains("is not an Iceberg compatible table")
    {
        // Not an Iceberg table: vend through the Unity route instead.
        return vend_unity(workspace, catalog, schema, table).await;
    }
    if response.status != 200 {
        return Err(Error::Http(crate::http::Error::Status {
            status: response.status,
            body: String::from_utf8_lossy(&response.bytes).into_owned(),
        }));
    }
    let loaded: LoadedTable = serde_json::from_slice(&response.bytes)
        .map_err(|error| Error::Http(crate::http::Error::Body(error)))?;
    let location = loaded
        .metadata
        .and_then(|metadata| metadata.location)
        .unwrap_or_default();
    let config = matching_config(&loaded.storage_credentials, &location).unwrap_or(loaded.config);
    Ok(read_credentials(&config))
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
fn credential_options(credentials: &[StorageCredential]) -> Option<Credentials> {
    credentials
        .iter()
        .find_map(|credential| read_credentials(&credential.config))
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

/// The credential set an Iceberg `s3.*` config names, with the token expiry
/// the catalog reports; `None` without an access key.
fn read_credentials(config: &BTreeMap<String, String>) -> Option<Credentials> {
    Some(Credentials {
        access_key_id: Some(config.get("s3.access-key-id")?.clone()),
        secret_access_key: Some(config.get("s3.secret-access-key")?.clone()),
        session_token: config.get("s3.session-token").cloned(),
        expire_time: config
            .get("s3.session-token-expires-at-ms")
            .and_then(|value| value.parse().ok()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unity_expiration_time_is_read() {
        let record: CredentialsRecord = serde_json::from_str(
            r#"{"aws_temp_credentials":{"access_key_id":"a","secret_access_key":"s","session_token":"t"},"expiration_time":1794050862767}"#,
        )
        .expect("the record parses");
        assert_eq!(record.expiration_time, Some(1_794_050_862_767));
    }

    #[test]
    fn iceberg_session_expiry_is_read() {
        let config = BTreeMap::from([
            ("s3.access-key-id".to_string(), "a".to_string()),
            ("s3.secret-access-key".to_string(), "s".to_string()),
            ("s3.session-token".to_string(), "t".to_string()),
            (
                "s3.session-token-expires-at-ms".to_string(),
                "1794050862767".to_string(),
            ),
        ]);
        let credentials = read_credentials(&config).expect("the config has a key");
        assert_eq!(credentials.expire_time, Some(1_794_050_862_767));
        assert_eq!(credentials.access_key_id.as_deref(), Some("a"));
    }

    #[test]
    fn an_iceberg_config_without_a_key_has_no_credentials() {
        assert!(read_credentials(&BTreeMap::new()).is_none());
    }
}

//! The Unity calls behind [`super::api::vend`].
//!
//! Unity serves `GET /tables/{catalog}.{schema}.{table}`; the record names the
//! table id and kind. Only `TABLE_EXTERNAL`, `TABLE_DELTA_EXTERNAL`, and
//! `TABLE_DELTA` can be read outside Databricks compute; for those,
//! `POST /temporary-table-credentials` vends read credentials. A record that
//! names another kind (managed default storage, a view) stops there; one that
//! names no kind is attempted, so a catalog that omits `securable_kind` (Unity
//! OSS) still vends. The URLs and JSON shapes are this command's; the
//! transport is the third-party facade.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::api::Error;
use crate::dialect;
use crate::third_party::reqwest::{self, Request};

/// The subset of the Unity table record vending needs.
#[derive(Deserialize)]
struct TableRecord {
    #[serde(default)]
    table_id: Option<String>,
    #[serde(default)]
    securable_kind: Option<String>,
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

pub(super) async fn vend(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Option<BTreeMap<String, String>>, Error> {
    let name = format!("{catalog}.{schema}.{table}");
    let root = dialect::api_root(endpoint);
    let record: TableRecord =
        dialect::get_json(&format!("{root}/tables/{}", dialect::encode(&name)), token)
            .await
            .map_err(Error::from)?;
    let Some(table_id) = record.table_id.filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    if let Some(kind) = record.securable_kind.as_deref() {
        if !is_vendable(kind) {
            return Ok(None);
        }
    }
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

/// The Unity table kinds readable outside Databricks compute.
fn is_vendable(kind: &str) -> bool {
    matches!(
        kind,
        "TABLE_EXTERNAL" | "TABLE_DELTA_EXTERNAL" | "TABLE_DELTA"
    )
}

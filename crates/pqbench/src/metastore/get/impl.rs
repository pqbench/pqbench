//! The Unity REST call behind [`super::api::read`].
//!
//! The URL and the JSON shape are this command's; the transport is the
//! third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Error, Metastore};
use crate::dialect;
use crate::third_party::reqwest::Request;
use crate::{Dialect, Workspace};

/// The subset of `GET /metastore_summary` this command reports.
#[derive(Deserialize)]
struct Summary {
    name: String,
    metastore_id: String,
    #[serde(default)]
    cloud: String,
    #[serde(default)]
    region: String,
}

pub(crate) async fn get(workspace: &Workspace) -> Result<Metastore, Error> {
    let url = format!(
        "{}/metastore_summary",
        dialect::api_root(workspace.config().endpoint.as_str())
    );
    let response = workspace
        .http()
        .request(Request::get(url, workspace.config().token.clone()))
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    if response.status != 200 {
        return Err(Error::from(format!(
            "the endpoint returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        )));
    }
    let summary: Summary = serde_json::from_slice(&response.bytes).map_err(|error| {
        Error::from(format!("the response was not a metastore summary: {error}"))
    })?;
    Ok(Metastore {
        name: crate::v1::name::metastore(metastore_id(workspace)),
        uid: Some(summary.metastore_id),
        display_name: Some(summary.name),
        cloud_provider: non_empty(summary.cloud),
        region: non_empty(summary.region),
    })
}

/// The metastore id: the dialect names it (`unity`, `iceberg`).
fn metastore_id(workspace: &Workspace) -> &'static str {
    match workspace.config().dialect {
        Dialect::Unity => "unity",
        Dialect::Iceberg => "iceberg",
    }
}

/// `Some` unless the endpoint reported nothing.
fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

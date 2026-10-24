//! The Unity REST call behind [`super::api::read`].
//!
//! The URL and the JSON shape are this command's; the transport is the
//! third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Error, Metastore};
use crate::dialect;
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
    let summary: Summary =
        crate::http::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::Http)?;
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

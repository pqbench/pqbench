//! The Unity REST call behind [`super::api::read`].
//!
//! The URL and the JSON shape are this command's; the transport is the
//! third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Catalog, Error};
use crate::dialect;
use crate::third_party::reqwest::Request;
use crate::v1::name;
use crate::Workspace;

/// The subset of `GET /catalogs/{name}` this command reports.
#[derive(Deserialize)]
struct Record {
    name: String,
    #[serde(default)]
    catalog_type: Option<String>,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    owner: Option<String>,
}

pub(crate) async fn read(workspace: &Workspace, catalog: &str) -> Result<Catalog, Error> {
    let url = format!(
        "{}/catalogs/{}",
        dialect::api_root(workspace.config().endpoint.as_str()),
        catalog.trim_matches('/')
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
    let record: Record = serde_json::from_slice(&response.bytes)
        .map_err(|error| Error::from(format!("the response was not a catalog record: {error}")))?;
    Ok(Catalog {
        name: name::catalog(&record.name),
        catalog_type: record
            .catalog_type
            .as_deref()
            .and_then(dialect::catalog_type),
        comment: record.comment.filter(|comment| !comment.is_empty()),
        owner: record.owner.filter(|owner| !owner.is_empty()),
        ..Catalog::default()
    })
}

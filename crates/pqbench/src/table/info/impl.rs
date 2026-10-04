//! The `loadTable` call behind [`super::api::read`].
//!
//! The URL is the one the listing carried; nothing is built here. The
//! transport is the third-party facade.

use serde::Deserialize;

use super::api::Error;
use crate::third_party::reqwest::{self, Request};

/// The `loadTable` subset this command needs.
#[derive(Deserialize)]
struct LoadedTable {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

pub(crate) async fn read(uri: &str, token: Option<&str>) -> Result<String, Error> {
    let response = reqwest::request(Request::get(uri, token.map(str::to_owned)))
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    if response.status != 200 {
        return Err(Error::from(format!(
            "the endpoint returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        )));
    }
    let loaded: LoadedTable = serde_json::from_slice(&response.bytes)
        .map_err(|error| Error::from(format!("the response was not a loaded table: {error}")))?;
    if loaded.metadata_location.is_empty() {
        return Err(Error::from(
            "the table is missing metadata-location".to_string(),
        ));
    }
    Ok(loaded.metadata_location)
}

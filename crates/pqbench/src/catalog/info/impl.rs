//! The Unity REST call behind [`super::api::read`].
//!
//! The URL and the JSON shape are this command's; the transport is the
//! third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Catalog, Error};
use crate::third_party::reqwest::{self, Request};

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

/// The Unity REST root, whether or not the endpoint already names it.
fn api_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/api/2.1/unity-catalog") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/api/2.1/unity-catalog")
    }
}

pub(crate) async fn read(
    endpoint: &str,
    catalog: &str,
    token: Option<&str>,
) -> Result<Catalog, Error> {
    let url = format!(
        "{}/catalogs/{}",
        api_root(endpoint),
        catalog.trim_matches('/')
    );
    let response = reqwest::request(Request {
        url,
        bearer: token.map(str::to_owned),
    })
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
        name: record.name,
        catalog_type: record.catalog_type,
        comment: record.comment,
        owner: record.owner,
    })
}

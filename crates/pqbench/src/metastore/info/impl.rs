//! The Unity REST call behind [`super::api::read`].
//!
//! The URL and the JSON shape are this command's; the transport is the
//! third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Error, Metastore};
use crate::third_party::reqwest::{self, Request};

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

/// The Unity REST root, whether or not the endpoint already names it.
fn api_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/api/2.1/unity-catalog") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/api/2.1/unity-catalog")
    }
}

pub(crate) async fn read(endpoint: &str, token: Option<&str>) -> Result<Metastore, Error> {
    let url = format!("{}/metastore_summary", api_root(endpoint));
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
    let summary: Summary = serde_json::from_slice(&response.bytes).map_err(|error| {
        Error::from(format!("the response was not a metastore summary: {error}"))
    })?;
    Ok(Metastore {
        name: summary.name,
        id: summary.metastore_id,
        cloud: summary.cloud,
        region: summary.region,
    })
}

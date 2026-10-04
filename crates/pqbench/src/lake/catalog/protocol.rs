//! Choose the catalog protocol from `GET {endpoint}/v1/config`.
//!
//! An Iceberg REST catalog answers `/v1/config` with a 200 and a `defaults`
//! object. Unity Catalog OSS and Databricks do not serve that route: a 200
//! without `defaults`, or a 404, is Unity. Transport failures and other
//! statuses propagate, so a down catalog is not silently listed as Unity.
//!
//! https://iceberg.apache.org/docs/latest/rest-catalog-spec/

use serde::Deserialize;

use super::super::api::Error;
use crate::third_party::reqwest::{self, Request};

/// The catalog dialect the endpoint speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocol {
    Unity,
    IcebergRest,
}

#[derive(Deserialize)]
struct Config {
    #[serde(default)]
    defaults: Option<serde_json::Value>,
}

/// Select Unity or Iceberg REST from `GET {endpoint}/v1/config`.
pub(crate) async fn select(endpoint: &str, token: Option<&str>) -> Result<Protocol, Error> {
    let url = format!("{}/v1/config", endpoint.trim_end_matches('/'));
    let response = reqwest::request(Request::get(url, token.map(str::to_owned)))
        .await
        .map_err(|error| Error::from(format!("catalog request failed: {error}")))?;
    if response.status == 404 {
        return Ok(Protocol::Unity);
    }
    if !(200..300).contains(&response.status) {
        return Err(Error::from(format!(
            "catalog returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        )));
    }
    let config: Config = serde_json::from_slice(&response.bytes).map_err(|error| {
        Error::from(format!(
            "catalog response was not the expected document: {error}"
        ))
    })?;
    Ok(if config.defaults.is_some() {
        Protocol::IcebergRest
    } else {
        Protocol::Unity
    })
}

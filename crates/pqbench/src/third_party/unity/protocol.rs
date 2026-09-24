//! Choose the catalog protocol from `GET {endpoint}/v1/config`.
//!
//! An Iceberg REST catalog answers `/v1/config` with a 200 and a `defaults`
//! object. Unity Catalog OSS and Databricks do not serve that route: a 200
//! without `defaults`, or a 404, is Unity. Transport failures and other
//! statuses propagate, so a down catalog is not silently listed as Unity.
//!
//! This is the only module that reads `/v1/config`; it names `reqwest` and is
//! compiled only with the `unity` feature.
//!
//! https://iceberg.apache.org/docs/latest/rest-catalog-spec/

use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

use crate::third_party::unity::api::Error;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|error| Error::from(format!("catalog client: {error}")))?;
    let url = format!("{}/v1/config", endpoint.trim_end_matches('/'));
    let mut request = client.get(&url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|error| Error::from(format!("catalog request failed: {error}")))?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(Protocol::Unity);
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(Error::from(format!(
            "catalog returned HTTP {status}: {body}"
        )));
    }
    let config: Config = response.json().await.map_err(|error| {
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

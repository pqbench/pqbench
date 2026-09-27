//! The catalog dialect behind the schema commands.
//!
//! `GET {endpoint}/v1/config?warehouse={catalog}` selects it: a 2xx with a
//! `defaults` object is Iceberg REST, whose `prefix` (overrides win over
//! defaults) shapes the namespace URLs; a 404 is Unity REST, which serves
//! `/schemas/...` and `/tables?...`. Transport failures and other statuses
//! propagate, so a down catalog is not silently read as the other dialect.

use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::third_party::reqwest::{self, Request};

/// The catalog dialect the endpoint speaks.
pub(super) enum Dialect {
    Unity,
    IcebergRest { prefix: String },
}

/// The `GET /v1/config` document that picks the dialect.
#[derive(Deserialize)]
struct Config {
    #[serde(default)]
    defaults: Option<Properties>,
    #[serde(default)]
    overrides: Option<Properties>,
}

#[derive(Deserialize)]
struct Properties {
    #[serde(default)]
    prefix: Option<String>,
}

/// Pick Unity or Iceberg REST from `GET {endpoint}/v1/config`.
pub(super) async fn select(
    endpoint: &str,
    catalog: &str,
    token: Option<&str>,
) -> Result<Dialect, String> {
    let url = format!(
        "{}/v1/config?warehouse={}",
        endpoint.trim_end_matches('/'),
        encode(catalog)
    );
    let response = reqwest::request(Request {
        url,
        bearer: token.map(str::to_owned),
    })
    .await
    .map_err(|error| format!("catalog request failed: {error}"))?;
    if response.status == 404 {
        return Ok(Dialect::Unity);
    }
    if !(200..300).contains(&response.status) {
        return Err(format!(
            "the endpoint returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        ));
    }
    let config: Config = serde_json::from_slice(&response.bytes)
        .map_err(|error| format!("the response was not a catalog config: {error}"))?;
    Ok(match config.defaults {
        Some(defaults) => Dialect::IcebergRest {
            prefix: config
                .overrides
                .and_then(|overrides| overrides.prefix)
                .or(defaults.prefix)
                .unwrap_or_default(),
        },
        None => Dialect::Unity,
    })
}

/// The Iceberg REST root for one dialect prefix (`{root}/v1/{prefix}`).
pub(super) fn iceberg_base(endpoint: &str, prefix: &str) -> String {
    let root = endpoint.trim_end_matches('/');
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        format!("{root}/v1")
    } else {
        format!("{root}/v1/{prefix}")
    }
}

/// A percent-encoded Iceberg namespace: parts joined by the unit separator.
pub(super) fn iceberg_namespace(schema: &str) -> String {
    encode(&schema.split('.').collect::<Vec<_>>().join("\u{1f}"))
}

/// One GET returning a parsed JSON document.
pub(super) async fn get_json<T: DeserializeOwned>(
    url: &str,
    token: Option<&str>,
) -> Result<T, String> {
    let response = reqwest::request(Request {
        url: url.to_string(),
        bearer: token.map(str::to_owned),
    })
    .await
    .map_err(|error| error.to_string())?;
    if response.status != 200 {
        return Err(format!(
            "the endpoint returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        ));
    }
    serde_json::from_slice(&response.bytes)
        .map_err(|error| format!("the response was not the expected document: {error}"))
}

/// The Unity REST root, whether or not the endpoint already names it.
pub(super) fn api_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/api/2.1/unity-catalog") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/api/2.1/unity-catalog")
    }
}

/// Percent-encode one path or query value (RFC 3986 unreserved bytes pass).
pub(super) fn encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

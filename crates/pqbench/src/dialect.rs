//! URL helpers shared by the walk's commands.
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`); nothing
//! here probes the endpoint. Iceberg REST endpoints already name the catalog
//! base (`{root}/v1` or `{root}/v1/{prefix}`), so the commands append
//! `/namespaces…` directly.

use serde::de::DeserializeOwned;

use crate::third_party::reqwest::{self, Request};

/// The Unity REST root, whether or not the endpoint already names it.
pub(crate) fn api_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/api/2.1/unity-catalog") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/api/2.1/unity-catalog")
    }
}

/// The Iceberg REST base, whether or not the endpoint has a trailing slash.
pub(crate) fn iceberg_root(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_string()
}

/// Percent-encode one path or query value (RFC 3986 unreserved bytes pass).
pub(crate) fn encode(value: &str) -> String {
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

/// A percent-encoded Iceberg namespace: parts joined by the unit separator.
pub(crate) fn iceberg_namespace(schema: &str) -> String {
    encode(&schema.split('.').collect::<Vec<_>>().join("\u{1f}"))
}

/// One GET returning a parsed JSON document.
pub(crate) async fn get_json<T: DeserializeOwned>(
    url: &str,
    token: Option<&str>,
) -> Result<T, String> {
    let response = reqwest::request(Request::get(url, token.map(str::to_owned)))
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

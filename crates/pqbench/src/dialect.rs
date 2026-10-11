//! Shared helpers for the walk's commands: endpoint URLs and the backend
//! vocabulary they report.
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`); nothing
//! here probes the endpoint. Iceberg REST endpoints already name the catalog
//! base (`{root}/v1` or `{root}/v1/{prefix}`), so the commands append
//! `/namespaces…` directly.

use serde::de::DeserializeOwned;

use crate::third_party::reqwest::Request;
use crate::v1::CatalogType;
use crate::Workspace;

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
    workspace: &Workspace,
    url: &str,
    token: Option<&str>,
) -> Result<T, String> {
    send_json(workspace, Request::get(url, token.map(str::to_owned))).await
}

/// Send one request and parse its JSON body.
pub(crate) async fn send_json<T: DeserializeOwned>(
    workspace: &Workspace,
    request: Request,
) -> Result<T, String> {
    let response = workspace
        .http()
        .request(request)
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

/// The catalog kind the endpoint reports (`MANAGED_CATALOG`, …) as the proto
/// enum. A kind this build does not name is `None`, not an error.
pub(crate) fn catalog_type(kind: &str) -> Option<CatalogType> {
    match kind {
        "MANAGED_CATALOG" => Some(CatalogType::Managed),
        "FOREIGN_CATALOG" => Some(CatalogType::Foreign),
        "SYSTEM_CATALOG" => Some(CatalogType::System),
        _ => None,
    }
}

//! The Unity and Iceberg REST calls behind [`super::api::list`].
//!
//! The dialect comes from `GET {endpoint}/v1/config?warehouse={catalog}`: a
//! 2xx with a `defaults` object is Iceberg REST, whose `prefix` (overrides
//! win over defaults) shapes the namespaces URL; a 404 is Unity REST, which
//! serves `/schemas?catalog_name=`. Transport failures and other statuses
//! propagate, so a down catalog is not silently read as the other dialect.
//!
//! The URLs, the page shapes, and the pagination are this command's; the
//! transport is the third-party HTTP facade. No feature flags live here.

use serde::de::DeserializeOwned;
use serde::Deserialize;

use super::api::{Error, Schema};
use crate::third_party::reqwest::{self, Request};

/// Schema names per page; both walks follow the endpoint's page token.
const PAGE_SIZE: u32 = 1000;

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

/// One page of Unity `GET /schemas`.
#[derive(Deserialize)]
struct SchemasPage {
    #[serde(default)]
    schemas: Vec<Named>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct Named {
    name: String,
}

/// One page of Iceberg REST `GET /v1/{prefix}/namespaces`.
#[derive(Deserialize)]
struct NamespacesPage {
    #[serde(default)]
    namespaces: Vec<Vec<String>>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

/// The catalog dialect the endpoint speaks.
enum Dialect {
    Unity,
    Iceberg { prefix: String },
}

pub(crate) async fn list(
    endpoint: &str,
    catalog: &str,
    token: Option<&str>,
) -> Result<Vec<Schema>, Error> {
    let names = match dialect(endpoint, catalog, token).await? {
        Dialect::Unity => unity_schemas(endpoint, catalog, token).await?,
        Dialect::Iceberg { prefix } => iceberg_namespaces(endpoint, &prefix, token).await?,
    };
    let mut schemas: Vec<Schema> = names
        .into_iter()
        .map(|name| Schema {
            catalog: catalog.to_string(),
            name,
        })
        .collect();
    schemas.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(schemas)
}

/// Pick Unity or Iceberg REST from `GET {endpoint}/v1/config`.
async fn dialect(endpoint: &str, catalog: &str, token: Option<&str>) -> Result<Dialect, Error> {
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
    .map_err(|error| Error::from(format!("catalog request failed: {error}")))?;
    if response.status == 404 {
        return Ok(Dialect::Unity);
    }
    if !(200..300).contains(&response.status) {
        return Err(Error::from(format!(
            "the endpoint returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        )));
    }
    let config: Config = serde_json::from_slice(&response.bytes)
        .map_err(|error| Error::from(format!("the response was not a catalog config: {error}")))?;
    Ok(match config.defaults {
        Some(defaults) => Dialect::Iceberg {
            prefix: config
                .overrides
                .and_then(|overrides| overrides.prefix)
                .or(defaults.prefix)
                .unwrap_or_default(),
        },
        None => Dialect::Unity,
    })
}

async fn unity_schemas(
    endpoint: &str,
    catalog: &str,
    token: Option<&str>,
) -> Result<Vec<String>, Error> {
    let root = api_root(endpoint);
    let mut names = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
            Some(page_token) => format!(
                "{root}/schemas?catalog_name={}&max_results={PAGE_SIZE}&page_token={}",
                encode(catalog),
                encode(page_token)
            ),
            None => format!(
                "{root}/schemas?catalog_name={}&max_results={PAGE_SIZE}",
                encode(catalog)
            ),
        };
        let page: SchemasPage = get_json(&url, token).await?;
        for schema in page.schemas {
            if schema.name.is_empty() {
                return Err(Error::from(
                    "the endpoint listed a nameless schema".to_string(),
                ));
            }
            names.push(schema.name);
        }
        match page.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
    }
    Ok(names)
}

async fn iceberg_namespaces(
    endpoint: &str,
    prefix: &str,
    token: Option<&str>,
) -> Result<Vec<String>, Error> {
    let root = endpoint.trim_end_matches('/');
    let prefix = prefix.trim_matches('/');
    let base = if prefix.is_empty() {
        format!("{root}/v1")
    } else {
        format!("{root}/v1/{prefix}")
    };
    let mut names = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
            Some(page_token) => format!("{base}/namespaces?pageToken={}", encode(page_token)),
            None => format!("{base}/namespaces"),
        };
        let page: NamespacesPage = get_json(&url, token).await?;
        for parts in page.namespaces {
            if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
                return Err(Error::from(
                    "the endpoint listed a nameless namespace".to_string(),
                ));
            }
            names.push(parts.join("."));
        }
        match page.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
    }
    Ok(names)
}

async fn get_json<T: DeserializeOwned>(url: &str, token: Option<&str>) -> Result<T, Error> {
    let response = reqwest::request(Request {
        url: url.to_string(),
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
    serde_json::from_slice(&response.bytes)
        .map_err(|error| Error::from(format!("the response was not a schema page: {error}")))
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

/// Percent-encode one query value (RFC 3986 unreserved bytes pass through).
fn encode(value: &str) -> String {
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

//! Paged JSON over the HTTP wrapper.
//!
//! The Unity and Iceberg REST walks page the same way: build a URL, GET it as
//! JSON, follow a page token until it stops, and refuse loops. Only the URL
//! shape differs, so [`pages`] takes a builder closure.

use std::collections::BTreeSet;

use serde::de::DeserializeOwned;

use super::super::api::Error;
use crate::third_party::reqwest::{self, Request};

/// How many pages one walk may fetch before it is treated as a loop.
pub(super) const PAGE_CAP: usize = 32;

/// Follow a page token until the backend returns none.
pub(super) async fn pages<P: DeserializeOwned>(
    token: Option<&str>,
    build: impl Fn(Option<&str>) -> String,
    next: fn(&P) -> Option<String>,
) -> Result<Vec<P>, Error> {
    let mut page_token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut pages = Vec::new();
    loop {
        if pages.len() >= PAGE_CAP {
            return Err(format!("catalog listed more than {PAGE_CAP} pages").into());
        }
        let url = build(page_token.as_deref());
        let page: P = get_json(&url, token).await?;
        let next = next(&page);
        pages.push(page);
        let Some(next) = next else {
            return Ok(pages);
        };
        if !seen.insert(next.clone()) {
            return Err(Error::from("catalog repeated a page token".to_string()));
        }
        page_token = Some(next);
    }
}

/// GET `url` as JSON, failing on a non-success status.
pub(super) async fn get_json<T: DeserializeOwned>(
    url: &str,
    token: Option<&str>,
) -> Result<T, Error> {
    let response = reqwest::request(Request {
        url: url.to_string(),
        bearer: token.map(str::to_owned),
        body: None,
    })
    .await
    .map_err(|error| Error::from(format!("catalog request failed: {error}")))?;
    if !(200..300).contains(&response.status) {
        return Err(Error::from(format!(
            "catalog returned HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.bytes)
        )));
    }
    serde_json::from_slice(&response.bytes).map_err(|error| {
        Error::from(format!(
            "catalog response was not the expected document: {error}"
        ))
    })
}

/// A page token from a response field, when it is set and non-empty.
pub(super) fn page_token(token: &Option<String>) -> Option<String> {
    token
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

/// Percent-encode one path segment or query value.
pub(super) fn encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

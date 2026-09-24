//! Shared HTTP for the catalog backends.
//!
//! The Unity and Iceberg REST walks page the same way: build a URL, GET it as
//! JSON, follow a page token until it stops, and refuse loops. Only the URL
//! shape differs, so [`pages`] takes a builder closure. This is the only module
//! besides the two backends that names `reqwest`.

use std::collections::BTreeSet;
use std::time::Duration;

use reqwest::Client;
use serde::de::DeserializeOwned;

use super::super::api::Error;

pub(super) const PAGE_CAP: usize = 32;
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Follow a page token until the backend returns none.
///
/// `token` is the bearer credential for auth. `build` gets the current page
/// token and returns the URL to GET. A page that repeats a token, or a walk
/// longer than [`PAGE_CAP`], is an error.
pub(super) async fn pages<P: DeserializeOwned>(
    client: &Client,
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
        let page: P = get_json(client, &url, token).await?;
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

pub(super) async fn get_json<T: DeserializeOwned>(
    client: &Client,
    url: &str,
    token: Option<&str>,
) -> Result<T, Error> {
    let mut request = client.get(url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|error| Error::from(format!("catalog request failed: {error}")))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(Error::from(format!(
            "catalog returned HTTP {status}: {body}"
        )));
    }
    response.json::<T>().await.map_err(|error| {
        Error::from(format!(
            "catalog response was not the expected document: {error}"
        ))
    })
}

pub(super) fn page_token(token: &Option<String>) -> Option<String> {
    token
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

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

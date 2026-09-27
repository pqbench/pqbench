//! The Unity REST calls behind [`super::api::list`].
//!
//! The URL, the page shape, and the pagination are this command's; the
//! transport is the third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Catalog, Error};
use crate::third_party::reqwest::{self, Request};

/// Catalog names per page; the walk follows `next_page_token`.
const PAGE_SIZE: u32 = 1000;

/// One page of `GET /catalogs`.
#[derive(Deserialize)]
struct Page {
    #[serde(default)]
    catalogs: Vec<Entry>,
    #[serde(default)]
    next_page_token: Option<String>,
}

/// The subset of one catalog this command reports.
#[derive(Deserialize)]
struct Entry {
    name: String,
    #[serde(default)]
    catalog_type: Option<String>,
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

pub(crate) async fn list(endpoint: &str, token: Option<&str>) -> Result<Vec<Catalog>, Error> {
    let root = api_root(endpoint);
    let mut catalogs = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
            Some(page_token) => {
                format!("{root}/catalogs?max_results={PAGE_SIZE}&page_token={page_token}")
            }
            None => format!("{root}/catalogs?max_results={PAGE_SIZE}"),
        };
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
        let page: Page = serde_json::from_slice(&response.bytes).map_err(|error| {
            Error::from(format!("the response was not a catalog page: {error}"))
        })?;
        catalogs.extend(page.catalogs.into_iter().map(|entry| Catalog {
            name: entry.name,
            catalog_type: entry.catalog_type,
        }));
        match page.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
    }
    catalogs.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(catalogs)
}

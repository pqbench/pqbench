//! The Unity REST calls behind [`super::api::list`].
//!
//! The URL, the page shape, and the pagination are this command's; the
//! transport is the third-party HTTP facade. No feature flags live here.

use serde::Deserialize;

use super::api::{Catalog, Error};
use crate::dialect;
use crate::third_party::reqwest::Request;
use crate::v1::name;
use crate::Workspace;

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

pub(crate) async fn list(workspace: &Workspace) -> Result<Vec<Catalog>, Error> {
    let root = dialect::api_root(workspace.config().endpoint.as_str());
    let token = workspace.config().token.as_deref();
    let mut catalogs = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
            Some(page_token) => {
                format!("{root}/catalogs?max_results={PAGE_SIZE}&page_token={page_token}")
            }
            None => format!("{root}/catalogs?max_results={PAGE_SIZE}"),
        };
        let response = workspace
            .http()
            .request(Request::get(url, token.map(str::to_owned)))
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
        catalogs.extend(page.catalogs.into_iter().map(|entry| {
            Catalog {
                name: name::catalog(&entry.name),
                catalog_type: entry
                    .catalog_type
                    .as_deref()
                    .and_then(dialect::catalog_type),
                ..Catalog::default()
            }
        }));
        match page.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
    }
    catalogs.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(catalogs)
}

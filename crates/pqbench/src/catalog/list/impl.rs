//! The Unity REST calls behind [`super::api::list`].
//!
//! The URL, the page shape, and the pagination are this command's; the
//! transport is the third-party HTTP facade. No feature flags live here.

use futures_util::stream::Stream;
use serde::Deserialize;

use super::api::{Catalog, Error};
use crate::dialect;
use crate::v1::{name, CatalogType};
use crate::Workspace;

/// Catalog names per page; the walk follows `next_page_token`.
const PAGE_SIZE: u32 = 1000;

/// One page of `GET /catalogs`.
#[derive(Deserialize)]
struct CatalogsPage {
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
    #[serde(default)]
    securable_kind: Option<String>,
}

/// The catalog kinds with no storage of their own to measure: a system
/// catalog, and a catalog whose managed storage is Databricks default
/// storage, which no reader outside the workspace's compute can open.
const UNREADABLE: &[&str] = &["CATALOG_SYSTEM", "CATALOG_DB_STORAGE"];

/// List the endpoint's catalogs, page by page: a page's catalogs are yielded,
/// in name order, before the next page is fetched.
pub(crate) fn list(workspace: &Workspace) -> impl Stream<Item = Result<Catalog, Error>> + '_ {
    crate::http::paginate(None::<String>, move |page_token| async move {
        fetch_page(workspace, page_token.as_deref()).await
    })
}

/// Fetch one page of `GET /catalogs`; `None` is the first page.
async fn fetch_page(
    workspace: &Workspace,
    page_token: Option<&str>,
) -> Result<crate::http::Page<Catalog, String>, Error> {
    let root = dialect::api_root(workspace.config().endpoint.as_str());
    let url = match page_token {
        Some(page_token) => {
            format!("{root}/catalogs?max_results={PAGE_SIZE}&page_token={page_token}")
        }
        None => format!("{root}/catalogs?max_results={PAGE_SIZE}"),
    };
    let page: CatalogsPage =
        crate::http::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::Http)?;
    let mut rows: Vec<Catalog> = page
        .catalogs
        .into_iter()
        .filter_map(measurable_catalog)
        .collect();
    rows.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(crate::http::Page {
        rows,
        next_page_token: page.next_page_token.filter(|token| !token.is_empty()),
    })
}

/// One listed catalog, or `None` for a catalog with no storage to measure.
fn measurable_catalog(entry: Entry) -> Option<Catalog> {
    let catalog_type = entry
        .catalog_type
        .as_deref()
        .and_then(dialect::catalog_type);
    let unreadable = catalog_type == Some(CatalogType::System)
        || entry
            .securable_kind
            .as_deref()
            .is_some_and(|kind| UNREADABLE.contains(&kind));
    if unreadable {
        return None;
    }
    Some(Catalog {
        name: name::catalog(&entry.name),
        catalog_type,
        ..Catalog::default()
    })
}

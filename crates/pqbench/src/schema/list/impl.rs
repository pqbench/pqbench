//! The Unity and Iceberg REST calls behind [`super::api::list`].
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`): Unity
//! REST serves `/schemas?catalog_name=`; Iceberg REST serves
//! `{endpoint}/namespaces`, where the endpoint already names the catalog base
//! (`{root}/v1` or `{root}/v1/{prefix}`). No config probe runs.
//!
//! The URLs, the page shapes, and the pagination are this command's; the
//! transport is the third-party HTTP facade. No feature flags live here.

use futures_util::stream::{self, Stream, StreamExt};
use serde::Deserialize;

use super::api::{Catalog, Error, Schema, TableFormat};
use crate::dialect;
use crate::Workspace;

/// Schema names per page; both walks follow the endpoint's page token.
const PAGE_SIZE: u32 = 1000;

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
    /// `SCHEMA_SYSTEM` for a schema the catalog keeps for itself, like
    /// `information_schema`: it holds views over metadata, no files.
    #[serde(default)]
    securable_kind: Option<String>,
}

/// One page of Iceberg REST `GET {endpoint}/namespaces`.
#[derive(Deserialize)]
struct NamespacesPage {
    #[serde(default)]
    namespaces: Vec<Vec<String>>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

/// List the schemas the catalog `name` names, page by page: a page's schemas
/// are yielded, in name order, before the next page is fetched.
pub(crate) fn list<'a>(
    workspace: &'a Workspace,
    name: String,
) -> impl Stream<Item = Result<Schema, Error>> + 'a {
    let Some(catalog) = Catalog::parse_name(&name) else {
        let error = Error::Name(format!("{name} is not a catalogs/… name"));
        return stream::iter([Err::<Schema, Error>(error)]).left_stream();
    };
    pages(workspace, catalog).right_stream()
}

/// The catalog's schemas, one page at a time.
fn pages<'a>(
    workspace: &'a Workspace,
    catalog: String,
) -> impl Stream<Item = Result<Schema, Error>> + 'a {
    crate::http::paginate(None::<String>, move |page_token| {
        let catalog = catalog.clone();
        async move { fetch_page(workspace, &catalog, page_token.as_deref()).await }
    })
}

/// Fetch one page of the catalog's schemas; `None` is the first page.
async fn fetch_page(
    workspace: &Workspace,
    catalog: &str,
    page_token: Option<&str>,
) -> Result<crate::http::Page<Schema, String>, Error> {
    let mut page = match workspace.config().dialect {
        TableFormat::Unity => unity_page(workspace, catalog, page_token).await?,
        TableFormat::Iceberg => iceberg_page(workspace, catalog, page_token).await?,
    };
    page.rows.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(page)
}

/// Fetch one page of Unity `GET /schemas`.
async fn unity_page(
    workspace: &Workspace,
    catalog: &str,
    page_token: Option<&str>,
) -> Result<crate::http::Page<Schema, String>, Error> {
    let root = dialect::api_root(workspace.config().endpoint.as_str());
    let url = match page_token {
        Some(page_token) => format!(
            "{root}/schemas?catalog_name={}&max_results={PAGE_SIZE}&page_token={}",
            dialect::encode(catalog),
            dialect::encode(page_token)
        ),
        None => format!(
            "{root}/schemas?catalog_name={}&max_results={PAGE_SIZE}",
            dialect::encode(catalog)
        ),
    };
    let page: SchemasPage =
        crate::http::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::Http)?;
    let mut rows = Vec::with_capacity(page.schemas.len());
    for schema in page.schemas {
        if schema.name.is_empty() {
            return Err(Error::Malformed(
                "the endpoint listed a nameless schema".to_string(),
            ));
        }
        // A schema the catalog keeps for itself, like `information_schema`,
        // holds views over metadata: no files.
        if schema.securable_kind.as_deref() != Some("SCHEMA_SYSTEM") {
            rows.push(named_schema(catalog, schema.name));
        }
    }
    Ok(crate::http::Page {
        rows,
        next_page_token: page.next_page_token.filter(|token| !token.is_empty()),
    })
}

/// Fetch one page of Iceberg REST `GET {endpoint}/namespaces`.
async fn iceberg_page(
    workspace: &Workspace,
    catalog: &str,
    page_token: Option<&str>,
) -> Result<crate::http::Page<Schema, String>, Error> {
    let base = workspace.config().endpoint.trim_end_matches('/');
    let url = match page_token {
        Some(page_token) => format!(
            "{base}/namespaces?pageToken={}",
            dialect::encode(page_token)
        ),
        None => format!("{base}/namespaces"),
    };
    let page: NamespacesPage =
        crate::http::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::Http)?;
    let mut rows = Vec::with_capacity(page.namespaces.len());
    for parts in page.namespaces {
        if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
            return Err(Error::Malformed(
                "the endpoint listed a nameless namespace".to_string(),
            ));
        }
        // `information_schema` holds system views over metadata, no files.
        if parts.len() == 1 && parts[0] == "information_schema" {
            continue;
        }
        rows.push(named_schema(catalog, parts.join(".")));
    }
    Ok(crate::http::Page {
        rows,
        next_page_token: page.next_page_token.filter(|token| !token.is_empty()),
    })
}

/// One listed schema, resolved to its resource name.
fn named_schema(catalog: &str, schema: String) -> Schema {
    Schema {
        display_name: Some(schema.clone()),
        name: crate::v1::name::schema(catalog, &schema),
        ..Schema::default()
    }
}

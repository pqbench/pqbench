//! The Unity and Iceberg REST calls behind [`super::api::list`].
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`). Unity
//! serves `/tables?catalog_name=&schema_name=` pages, each entry carrying
//! `full_name`, `table_type` and `storage_location`, so a ref is complete in
//! one page. A view is not a table: it has no files, so it is not listed.
//! Iceberg REST lists `{endpoint}/namespaces/{namespace}/tables` identifiers
//! only; the ref carries its name and no storage URI, so `pqbench get table`
//! derives the `loadTable` URL and reads the metadata inline. No config probe
//! runs. The URLs, the page shapes, and the pagination are this command's;
//! the transport is the third-party facade.

use futures_util::stream::{self, Stream, StreamExt};
use serde::Deserialize;

use super::api::{Error, Schema, Table};
use crate::dialect;
use crate::schema::TableFormat;
use crate::Workspace;

/// Table names per page; both walks follow the endpoint's page token.
const PAGE_SIZE: u32 = 1000;

/// One page of Unity `GET /tables`.
#[derive(Deserialize)]
struct UnityPage {
    #[serde(default)]
    tables: Vec<UnityTable>,
    #[serde(default)]
    next_page_token: Option<String>,
}

/// The subset of one Unity table this command reports.
#[derive(Deserialize)]
struct UnityTable {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    full_name: Option<String>,
    #[serde(default)]
    table_type: Option<String>,
    #[serde(default)]
    storage_location: Option<String>,
}

/// The table types that hold no files: a view reads tables when it is
/// queried, so it has no storage to measure.
const VIEW_TYPES: &[&str] = &["VIEW", "MATERIALIZED_VIEW", "METRIC_VIEW"];

/// One page of Iceberg REST `GET /v1/{prefix}/namespaces/{namespace}/tables`.
#[derive(Deserialize)]
struct IdentifiersPage {
    #[serde(default)]
    identifiers: Vec<Identifier>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct Identifier {
    #[serde(default, rename = "namespace")]
    namespaces: Vec<String>,
    name: String,
}

/// List the tables the schema `name` names, page by page: a page's tables are
/// yielded, in name order, before the next page is fetched.
pub(crate) fn list<'a>(
    workspace: &'a Workspace,
    name: String,
) -> impl Stream<Item = Result<Table, Error>> + 'a {
    let Some((catalog, schema)) = Schema::parse_name(&name) else {
        let error = Error::Name(format!("{name} is not a catalogs/…/schemas/… name"));
        return stream::iter([Err::<Table, Error>(error)]).left_stream();
    };
    pages(workspace, catalog, schema).right_stream()
}

/// The schema's tables, one page at a time.
fn pages<'a>(
    workspace: &'a Workspace,
    catalog: String,
    schema: String,
) -> impl Stream<Item = Result<Table, Error>> + 'a {
    crate::http::paginate(None::<String>, move |page_token| {
        let catalog = catalog.clone();
        let schema = schema.clone();
        async move { fetch_page(workspace, &catalog, &schema, page_token.as_deref()).await }
    })
}

/// Fetch one page of the schema's tables; `None` is the first page.
async fn fetch_page(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
    page_token: Option<&str>,
) -> Result<crate::http::Page<Table, String>, Error> {
    let mut page = match workspace.config().dialect {
        TableFormat::Unity => unity_page(workspace, catalog, schema, page_token).await?,
        TableFormat::Iceberg => iceberg_page(workspace, catalog, schema, page_token).await?,
    };
    page.rows.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(page)
}

/// Fetch one page of Unity `GET /tables`.
async fn unity_page(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
    page_token: Option<&str>,
) -> Result<crate::http::Page<Table, String>, Error> {
    let root = dialect::api_root(workspace.config().endpoint.as_str());
    let url = unity_tables_url(&root, catalog, schema, page_token);
    let page: UnityPage =
        crate::http::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::Http)?;
    let mut rows = Vec::with_capacity(page.tables.len());
    for entry in page.tables {
        let view = entry
            .table_type
            .as_deref()
            .is_some_and(|kind| VIEW_TYPES.contains(&kind));
        if !view {
            rows.push(unity_table(catalog, schema, entry)?);
        }
    }
    Ok(crate::http::Page {
        rows,
        next_page_token: page.next_page_token.filter(|token| !token.is_empty()),
    })
}

/// The `/tables` page URL, with the page token appended when there is one.
fn unity_tables_url(root: &str, catalog: &str, schema: &str, page_token: Option<&str>) -> String {
    let base = format!(
        "{root}/tables?catalog_name={}&schema_name={}&max_results={PAGE_SIZE}&omit_columns=true&omit_properties=true",
        dialect::encode(catalog),
        dialect::encode(schema)
    );
    match page_token {
        Some(token) => format!("{base}&page_token={}", dialect::encode(token)),
        None => base,
    }
}

/// One listed Unity table as a `Table`, resolving its name and storage path.
fn unity_table(catalog: &str, schema: &str, entry: UnityTable) -> Result<Table, Error> {
    let storage_path = entry.storage_location.filter(|path| !path.is_empty());
    let name = entry
        .full_name
        .filter(|name| !name.is_empty())
        .or_else(|| {
            entry
                .name
                .filter(|name| !name.is_empty())
                .map(|name| format!("{catalog}.{schema}.{name}"))
        })
        .ok_or_else(|| Error::Malformed(format!("the table in {catalog}.{schema} has no name")))?;
    let table = name.rsplit('.').next().unwrap_or(name.as_str());
    Ok(Table {
        name: crate::v1::name::table(catalog, schema, table),
        display_name: Some(table.to_string()),
        uri: storage_path,
        ..Table::default()
    })
}

/// Fetch one page of Iceberg REST `GET …/namespaces/{namespace}/tables`.
async fn iceberg_page(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
    page_token: Option<&str>,
) -> Result<crate::http::Page<Table, String>, Error> {
    let base = dialect::iceberg_root(workspace.config().endpoint.as_str());
    let namespace = dialect::iceberg_namespace(schema);
    let url = match page_token {
        Some(page_token) => format!(
            "{base}/namespaces/{namespace}/tables?pageToken={}",
            dialect::encode(page_token)
        ),
        None => format!("{base}/namespaces/{namespace}/tables"),
    };
    let page: IdentifiersPage =
        crate::http::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::Http)?;
    let mut rows = Vec::with_capacity(page.identifiers.len());
    for identifier in page.identifiers {
        if identifier.name.is_empty() {
            return Err(Error::Malformed(
                "the endpoint listed a nameless table".to_string(),
            ));
        }
        let namespace = if identifier.namespaces.is_empty() {
            schema.to_string()
        } else {
            identifier.namespaces.join(".")
        };
        rows.push(Table {
            name: crate::v1::name::table(catalog, &namespace, &identifier.name),
            display_name: Some(identifier.name),
            ..Table::default()
        });
    }
    Ok(crate::http::Page {
        rows,
        next_page_token: page.next_page_token.filter(|token| !token.is_empty()),
    })
}

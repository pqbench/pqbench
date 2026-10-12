//! The Unity and Iceberg REST calls behind [`super::api::list`].
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`). Unity
//! serves `/tables?catalog_name=&schema_name=` pages, each entry carrying
//! `full_name` and `storage_location`, so a ref is complete in one page.
//! Iceberg REST lists `{endpoint}/namespaces/{namespace}/tables` identifiers
//! only; the ref carries the `loadTable` URL as its `uri`, and
//! `pqbench get table` dereferences it later. No config probe runs. A view
//! carries no location: the ref keeps its `uri` unset, and
//! `credentials check` is the stage that drops it. The URLs, the page shapes,
//! and the pagination are this command's; the transport is the third-party
//! facade.

use serde::Deserialize;

use super::api::{Error, Table};
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
    storage_location: Option<String>,
}

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

pub(crate) async fn list(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
) -> Result<Vec<Table>, Error> {
    let mut tables = match workspace.config().dialect {
        TableFormat::Unity => unity_tables(workspace, catalog, schema).await?,
        TableFormat::Iceberg => iceberg_tables(workspace, catalog, schema).await?,
    };
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tables)
}

async fn unity_tables(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
) -> Result<Vec<Table>, Error> {
    let root = dialect::api_root(workspace.config().endpoint.as_str());
    let mut tables = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
            Some(page_token) => format!(
                "{root}/tables?catalog_name={}&schema_name={}&max_results={PAGE_SIZE}&omit_columns=true&omit_properties=true&page_token={}",
                dialect::encode(catalog),
                dialect::encode(schema),
                dialect::encode(page_token)
            ),
            None => format!(
                "{root}/tables?catalog_name={}&schema_name={}&max_results={PAGE_SIZE}&omit_columns=true&omit_properties=true",
                dialect::encode(catalog),
                dialect::encode(schema)
            ),
        };
        let page: UnityPage =
            dialect::get_json(workspace, &url, workspace.config().token.as_deref())
                .await
                .map_err(Error::from)?;
        for table in page.tables {
            let storage_path = table.storage_location.filter(|path| !path.is_empty());
            let name = table
                .full_name
                .filter(|name| !name.is_empty())
                .or_else(|| {
                    table
                        .name
                        .filter(|name| !name.is_empty())
                        .map(|name| format!("{catalog}.{schema}.{name}"))
                })
                .ok_or_else(|| {
                    Error::from(format!("the table in {catalog}.{schema} has no name"))
                })?;
            let table = name.rsplit('.').next().unwrap_or(name.as_str());
            tables.push(Table {
                name: crate::v1::name::table(catalog, schema, table),
                uri: storage_path,
                ..Table::default()
            });
        }
        match page.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
    }
    Ok(tables)
}

async fn iceberg_tables(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
) -> Result<Vec<Table>, Error> {
    let base = dialect::iceberg_root(workspace.config().endpoint.as_str());
    let namespace = dialect::iceberg_namespace(schema);
    let mut tables = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
            Some(page_token) => format!(
                "{base}/namespaces/{namespace}/tables?pageToken={}",
                dialect::encode(page_token)
            ),
            None => format!("{base}/namespaces/{namespace}/tables"),
        };
        let page: IdentifiersPage =
            dialect::get_json(workspace, &url, workspace.config().token.as_deref())
                .await
                .map_err(Error::from)?;
        for identifier in page.identifiers {
            if identifier.name.is_empty() {
                return Err(Error::from(
                    "the endpoint listed a nameless table".to_string(),
                ));
            }
            let namespace = if identifier.namespaces.is_empty() {
                schema.to_string()
            } else {
                identifier.namespaces.join(".")
            };
            tables.push(Table {
                name: crate::v1::name::table(catalog, &namespace, &identifier.name),
                ..Table::default()
            });
        }
        match page.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
    }
    Ok(tables)
}

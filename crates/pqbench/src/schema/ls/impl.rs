//! The Unity and Iceberg REST calls behind [`super::api::list`].
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`). Unity
//! serves `/tables?catalog_name=&schema_name=` pages, each entry carrying
//! `full_name`, `data_source_format`, and `storage_location`. Iceberg REST
//! lists `{endpoint}/namespaces/{namespace}/tables` identifiers, then
//! `loadTable` for each `metadata-location`, where the endpoint already names
//! the catalog base. No config probe runs. Entries with no location (views)
//! are skipped. The URLs, the page shapes, and the pagination are this
//! command's; the transport is the third-party facade.

use serde::Deserialize;

use super::api::{Error, TableRef};
use crate::schema::dialect;
use crate::schema::TableFormat;

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
    data_source_format: Option<String>,
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

/// The `loadTable` subset this command needs.
#[derive(Deserialize)]
struct LoadedTable {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

pub(crate) async fn list(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
    table_format: TableFormat,
) -> Result<Vec<TableRef>, Error> {
    let mut tables = match table_format {
        TableFormat::Unity => unity_tables(endpoint, catalog, schema, token).await?,
        TableFormat::Iceberg => iceberg_tables(endpoint, catalog, schema, token).await?,
    };
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tables)
}

async fn unity_tables(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
) -> Result<Vec<TableRef>, Error> {
    let root = dialect::api_root(endpoint);
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
        let page: UnityPage = dialect::get_json(&url, token).await.map_err(Error::from)?;
        for table in page.tables {
            let Some(uri) = table.storage_location.filter(|uri| !uri.is_empty()) else {
                continue;
            };
            let name = table
                .full_name
                .filter(|name| !name.is_empty())
                .or_else(|| {
                    table
                        .name
                        .filter(|name| !name.is_empty())
                        .map(|name| format!("{catalog}.{schema}.{name}"))
                })
                .ok_or_else(|| Error::from(format!("the table at {uri} has no name")))?;
            tables.push(TableRef {
                name,
                uri,
                format: table.data_source_format.filter(|format| !format.is_empty()),
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
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
) -> Result<Vec<TableRef>, Error> {
    let base = dialect::iceberg_root(endpoint);
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
        let page: IdentifiersPage = dialect::get_json(&url, token).await.map_err(Error::from)?;
        for identifier in page.identifiers {
            if identifier.name.is_empty() {
                return Err(Error::from(
                    "the endpoint listed a nameless table".to_string(),
                ));
            }
            let loaded: LoadedTable = dialect::get_json(
                &format!(
                    "{base}/namespaces/{namespace}/tables/{}",
                    dialect::encode(&identifier.name)
                ),
                token,
            )
            .await
            .map_err(Error::from)?;
            if loaded.metadata_location.is_empty() {
                return Err(Error::from(format!(
                    "Iceberg table {} is missing metadata-location",
                    identifier.name
                )));
            }
            let namespace = if identifier.namespaces.is_empty() {
                schema.to_string()
            } else {
                identifier.namespaces.join(".")
            };
            tables.push(TableRef {
                name: format!("{catalog}.{namespace}.{}", identifier.name),
                uri: loaded.metadata_location,
                format: Some("ICEBERG".to_string()),
            });
        }
        match page.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
    }
    Ok(tables)
}

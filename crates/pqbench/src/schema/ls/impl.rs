//! The Unity and Iceberg REST calls behind [`super::api::list`].
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`): Unity
//! REST serves `/schemas?catalog_name=`; Iceberg REST serves
//! `{endpoint}/namespaces`, where the endpoint already names the catalog base
//! (`{root}/v1` or `{root}/v1/{prefix}`). No config probe runs.
//!
//! The URLs, the page shapes, and the pagination are this command's; the
//! transport is the third-party HTTP facade. No feature flags live here.

use serde::de::DeserializeOwned;
use serde::Deserialize;

use super::api::{Error, Schema, TableFormat};
use crate::dialect;
use crate::third_party::reqwest::Request;
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
}

/// One page of Iceberg REST `GET {endpoint}/namespaces`.
#[derive(Deserialize)]
struct NamespacesPage {
    #[serde(default)]
    namespaces: Vec<Vec<String>>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

pub(crate) async fn list(workspace: &Workspace, catalog: &str) -> Result<Vec<Schema>, Error> {
    let names = match workspace.config().dialect {
        TableFormat::Unity => unity_schemas(workspace, catalog).await?,
        TableFormat::Iceberg => iceberg_namespaces(workspace).await?,
    };
    let mut schemas: Vec<Schema> = names
        .into_iter()
        .map(|schema| Schema {
            name: crate::v1::name::schema(catalog, &schema),
            ..Schema::default()
        })
        .collect();
    schemas.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(schemas)
}

async fn unity_schemas(workspace: &Workspace, catalog: &str) -> Result<Vec<String>, Error> {
    let root = dialect::api_root(workspace.config().endpoint.as_str());
    let mut names = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
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
        let page: SchemasPage = get_json(workspace, &url).await?;
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

async fn iceberg_namespaces(workspace: &Workspace) -> Result<Vec<String>, Error> {
    let base = workspace.config().endpoint.trim_end_matches('/');
    let mut names = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let url = match &page_token {
            Some(page_token) => format!(
                "{base}/namespaces?pageToken={}",
                dialect::encode(page_token)
            ),
            None => format!("{base}/namespaces"),
        };
        let page: NamespacesPage = get_json(workspace, &url).await?;
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

async fn get_json<T: DeserializeOwned>(workspace: &Workspace, url: &str) -> Result<T, Error> {
    let response = workspace
        .http()
        .request(Request::get(url, workspace.config().token.clone()))
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

//! The Iceberg REST list client behind [`super::api::list_tables`].
//!
//! The transport is the `reqwest` wrapper.
//!
//! https://iceberg.apache.org/docs/latest/rest-catalog-spec/

use std::collections::BTreeMap;

use serde::Deserialize;

use super::super::api::{Error, LakeSource, NameFilter};
use super::http::{encode, get_json, page_token, pages};
use crate::lake::LakeTable;

#[derive(Deserialize)]
struct NamespacesPage {
    namespaces: Vec<Vec<String>>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct TablesPage {
    identifiers: Vec<Identifier>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct Identifier {
    name: String,
}

#[derive(Deserialize)]
struct LoadedTable {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

pub(crate) async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    let root = source.endpoint.trim_end_matches('/').to_string();
    let token = source.token.as_deref().filter(|token| !token.is_empty());
    let mut tables = Vec::new();
    for namespace in list_namespaces(&root, token, filter).await? {
        tables.extend(list_namespace_tables(&root, token, &namespace, &source.env, filter).await?);
    }
    if tables.is_empty() {
        return Err(Error::from("catalog listed no Iceberg tables".to_string()));
    }
    Ok(tables)
}

async fn list_namespaces(
    root: &str,
    token: Option<&str>,
    filter: &NameFilter,
) -> Result<Vec<Vec<String>>, Error> {
    let mut namespaces = Vec::new();
    for page in pages::<NamespacesPage>(
        token,
        |page_token| root_path(root, "/v1/namespaces", page_token),
        |page| page_token(&page.next_page_token),
    )
    .await?
    {
        for parts in page.namespaces {
            if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
                return Err(Error::from(
                    "catalog /v1/namespaces listed a nameless namespace".to_string(),
                ));
            }
            if filter.keeps_prefix(&parts.join(".")) {
                namespaces.push(parts);
            }
        }
    }
    Ok(namespaces)
}

async fn list_namespace_tables(
    root: &str,
    token: Option<&str>,
    namespace: &[String],
    env: &BTreeMap<String, String>,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    let encoded = encoded_namespace(namespace);
    let mut tables = Vec::new();
    for page in pages::<TablesPage>(
        token,
        |page_token| {
            root_path(
                root,
                &format!("/v1/namespaces/{encoded}/tables"),
                page_token,
            )
        },
        |page| page_token(&page.next_page_token),
    )
    .await?
    {
        for item in page.identifiers {
            if item.name.is_empty() {
                return Err(format!(
                    "catalog listed a nameless table in namespace {}",
                    namespace.join(".")
                )
                .into());
            }
            let table = load_table(root, token, namespace, &item.name, env).await?;
            if filter.keeps(&table.name) {
                tables.push(table);
            }
        }
    }
    Ok(tables)
}

async fn load_table(
    root: &str,
    token: Option<&str>,
    namespace: &[String],
    name: &str,
    env: &BTreeMap<String, String>,
) -> Result<LakeTable, Error> {
    let url = format!(
        "{root}/v1/namespaces/{}/tables/{}",
        encoded_namespace(namespace),
        encode(name)
    );
    let loaded: LoadedTable = get_json(&url, token).await?;
    if loaded.metadata_location.is_empty() {
        return Err(format!(
            "Iceberg table {}.{} is missing metadata-location",
            namespace.join("."),
            name
        )
        .into());
    }
    let full_name = namespace
        .iter()
        .cloned()
        .chain(std::iter::once(name.to_string()))
        .collect::<Vec<_>>()
        .join(".");
    Ok(LakeTable {
        name: full_name,
        uri: loaded.metadata_location,
        env: env.clone(),
        info: None,
    })
}

fn encoded_namespace(namespace: &[String]) -> String {
    encode(&namespace.join("\u{1f}"))
}

fn root_path(root: &str, path: &str, page_token: Option<&str>) -> String {
    let mut url = format!("{root}{path}");
    if let Some(token) = page_token {
        url.push_str("?pageToken=");
        url.push_str(&encode(token));
    }
    url
}

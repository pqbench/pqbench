//! List tables from an Iceberg REST catalog.
//!
//! The walk is the Iceberg REST dialect the lakehouse stand serves:
//! `GET /v1/namespaces`, `GET /v1/namespaces/{ns}/tables`, then
//! `GET /v1/namespaces/{ns}/tables/{table}` for the metadata location. Listing
//! is sequential: the caller runs one table per later process. `--include` /
//! `--exclude` prune the walk when the leading name is a literal.
//!
//! `GET /v1/config` picked this protocol (see [`super::protocol`]); this module
//! names `reqwest` and is compiled only with the `unity` feature.
//!
//! https://iceberg.apache.org/docs/latest/rest-catalog-spec/

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::third_party::unity::api::{Error, LakeSource, NameFilter};

use super::filter;
use crate::lake::LakeTable;

const PAGE_CAP: usize = 32;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|error| Error::from(format!("catalog client: {error}")))?;
    let root = source.endpoint.trim_end_matches('/').to_string();
    let token = source.token.clone().filter(|token| !token.is_empty());
    let mut tables = Vec::new();
    for namespace in list_namespaces(&client, &root, token.as_deref(), filter).await? {
        tables.extend(
            list_namespace_tables(
                &client,
                &root,
                token.as_deref(),
                &namespace,
                &source.env,
                filter,
            )
            .await?,
        );
    }
    if tables.is_empty() {
        return Err(Error::from("catalog listed no Iceberg tables".to_string()));
    }
    Ok(tables)
}

async fn list_namespaces(
    client: &Client,
    root: &str,
    token: Option<&str>,
    filter: &NameFilter,
) -> Result<Vec<Vec<String>>, Error> {
    let mut namespaces = Vec::new();
    for page in pages::<NamespacesPage>(client, root, token, "/v1/namespaces", |page| {
        page_token(&page.next_page_token)
    })
    .await?
    {
        for parts in page.namespaces {
            if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
                return Err(Error::from(
                    "catalog /v1/namespaces listed a nameless namespace".to_string(),
                ));
            }
            if filter::keeps_prefix(filter, &parts.join(".")) {
                namespaces.push(parts);
            }
        }
    }
    Ok(namespaces)
}

async fn list_namespace_tables(
    client: &Client,
    root: &str,
    token: Option<&str>,
    namespace: &[String],
    env: &BTreeMap<String, String>,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    let encoded = encoded_namespace(namespace);
    let mut tables = Vec::new();
    for page in pages::<TablesPage>(
        client,
        root,
        token,
        &format!("/v1/namespaces/{encoded}/tables"),
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
            let table = load_table(client, root, token, namespace, &item.name, env).await?;
            if filter.keeps(&table.name) {
                tables.push(table);
            }
        }
    }
    Ok(tables)
}

async fn load_table(
    client: &Client,
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
    let loaded: LoadedTable = get_json(client, &url, token).await?;
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

fn page_token(token: &Option<String>) -> Option<String> {
    token
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

async fn pages<P: DeserializeOwned>(
    client: &Client,
    root: &str,
    token: Option<&str>,
    path: &str,
    next: fn(&P) -> Option<String>,
) -> Result<Vec<P>, Error> {
    let mut page_token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut pages = Vec::new();
    loop {
        if pages.len() >= PAGE_CAP {
            return Err(format!("catalog listed more than {PAGE_CAP} pages at {path}").into());
        }
        let mut url = format!("{root}{path}");
        if let Some(token) = &page_token {
            url.push_str("?pageToken=");
            url.push_str(&encode(token));
        }
        let page: P = get_json(client, &url, token).await?;
        let next = next(&page);
        pages.push(page);
        let Some(next) = next else {
            return Ok(pages);
        };
        if !seen.insert(next.clone()) {
            return Err(format!("catalog repeated page token at {path}").into());
        }
        page_token = Some(next);
    }
}

async fn get_json<T: DeserializeOwned>(
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

fn encode(value: &str) -> String {
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

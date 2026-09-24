//! The async HTTP client behind [`super::super::api::list_tables`].
//!
//! This is the only module that names the third-party `reqwest` crate. It is
//! compiled only with the `unity` feature.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::third_party::unity::api::{is_glob, Error, LakeSource, NameFilter};

use super::filter;
use crate::lake::LakeTable;

const PAGE_CAP: usize = 32;
const PAGE_SIZE: u32 = 50;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
struct Named {
    name: String,
}

#[derive(Deserialize)]
struct CatalogsPage {
    #[serde(default)]
    catalogs: Vec<Named>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct SchemasPage {
    #[serde(default)]
    schemas: Vec<Named>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct TablesPage {
    #[serde(default)]
    tables: Vec<TableEntry>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct TableEntry {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    full_name: Option<String>,
    #[serde(default)]
    table_type: Option<String>,
    #[serde(default)]
    data_source_format: Option<String>,
    #[serde(default)]
    storage_location: Option<String>,
}

pub(crate) async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|error| Error::from(format!("catalog client: {error}")))?;
    let root = api_root(&source.endpoint);
    let token = source.token.clone().filter(|token| !token.is_empty());
    let mut tables = Vec::new();
    for catalog in list_catalogs(&client, &root, token.as_deref(), source, filter).await? {
        for schema in
            list_schemas(&client, &root, token.as_deref(), &catalog, source, filter).await?
        {
            tables.extend(
                list_schema_tables(
                    &client,
                    &root,
                    token.as_deref(),
                    &catalog,
                    &schema,
                    &source.env,
                    filter,
                )
                .await?,
            );
        }
    }
    if tables.is_empty() {
        return Err(Error::from("catalog listed no Delta tables".to_string()));
    }
    Ok(tables)
}

async fn list_catalogs(
    client: &Client,
    root: &str,
    token: Option<&str>,
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<String>, Error> {
    if let Some(catalog) = nonempty(&source.catalog) {
        if !is_glob(catalog) {
            return Ok(vec![catalog.to_string()]
                .into_iter()
                .filter(|name| filter::keeps_prefix(filter, name))
                .collect());
        }
    }
    if source.catalog.is_none() {
        if let Some(scoped) = filter::catalog_scope(filter) {
            return Ok(scoped
                .into_iter()
                .filter(|catalog| filter::keeps_prefix(filter, catalog))
                .collect());
        }
    }
    let names = names::<CatalogsPage>(
        client,
        root,
        token,
        "/catalogs",
        &[],
        |page| page.catalogs.iter().map(|item| item.name.clone()).collect(),
        |page| page_token(&page.next_page_token),
    )
    .await?;
    Ok(names
        .into_iter()
        .filter(|catalog| {
            source
                .catalog
                .as_deref()
                .filter(|pattern| is_glob(pattern))
                .is_none_or(|pattern| {
                    glob::Pattern::new(pattern).is_ok_and(|glob| glob.matches(catalog))
                })
                && filter::keeps_prefix(filter, catalog)
        })
        .collect())
}

async fn list_schemas(
    client: &Client,
    root: &str,
    token: Option<&str>,
    catalog: &str,
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<String>, Error> {
    if let Some(schema) = nonempty(&source.schema) {
        if !is_glob(schema) {
            let fqn = format!("{catalog}.{schema}");
            return Ok(if filter::keeps_prefix(filter, &fqn) {
                vec![schema.to_string()]
            } else {
                Vec::new()
            });
        }
    }
    if source.schema.is_none() {
        if let Some(scoped) = filter::schema_scope(filter, catalog) {
            return Ok(scoped
                .into_iter()
                .filter(|schema| filter::keeps_prefix(filter, &format!("{catalog}.{schema}")))
                .collect());
        }
    }
    let names = names::<SchemasPage>(
        client,
        root,
        token,
        "/schemas",
        &[("catalog_name", catalog)],
        |page| page.schemas.iter().map(|item| item.name.clone()).collect(),
        |page| page_token(&page.next_page_token),
    )
    .await?;
    Ok(names
        .into_iter()
        .filter(|schema| {
            let fqn = format!("{catalog}.{schema}");
            source
                .schema
                .as_deref()
                .filter(|pattern| is_glob(pattern))
                .is_none_or(|pattern| {
                    glob::Pattern::new(pattern).is_ok_and(|glob| glob.matches(schema))
                })
                && filter::keeps_prefix(filter, &fqn)
        })
        .collect())
}

fn api_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/api/2.1/unity-catalog") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/api/2.1/unity-catalog")
    }
}

async fn names<P: DeserializeOwned>(
    client: &Client,
    root: &str,
    token: Option<&str>,
    path: &str,
    query: &[(&str, &str)],
    field: fn(&P) -> Vec<String>,
    next: fn(&P) -> Option<String>,
) -> Result<Vec<String>, Error> {
    let mut names = Vec::new();
    for page in pages::<P>(client, root, token, path, query, next).await? {
        for name in field(&page) {
            if name.is_empty() {
                return Err(format!("{path} listed a nameless entry").into());
            }
            names.push(name);
        }
    }
    Ok(names)
}

fn page_token(token: &Option<String>) -> Option<String> {
    token
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

async fn list_schema_tables(
    client: &Client,
    root: &str,
    token: Option<&str>,
    catalog: &str,
    schema: &str,
    env: &BTreeMap<String, String>,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    let mut tables = Vec::new();
    for page in pages::<TablesPage>(
        client,
        root,
        token,
        "/tables",
        &[("catalog_name", catalog), ("schema_name", schema)],
        |page| page_token(&page.next_page_token),
    )
    .await?
    {
        for item in page.tables {
            if let Some(table) = lake_table(item, catalog, schema, env)? {
                if filter.keeps(&table.name) {
                    tables.push(table);
                }
            }
        }
    }
    Ok(tables)
}

fn lake_table(
    item: TableEntry,
    catalog: &str,
    schema: &str,
    env: &BTreeMap<String, String>,
) -> Result<Option<LakeTable>, Error> {
    if let Some(kind) = item.table_type.as_deref() {
        if !kind.eq_ignore_ascii_case("MANAGED") && !kind.eq_ignore_ascii_case("EXTERNAL") {
            return Ok(None);
        }
    }
    let Some(format) = item.data_source_format.filter(|format| !format.is_empty()) else {
        return Ok(None);
    };
    if !format.eq_ignore_ascii_case("DELTA") {
        return Ok(None);
    }
    let Some(uri) = item.storage_location.filter(|uri| !uri.is_empty()) else {
        return Ok(None);
    };
    let name = item
        .full_name
        .filter(|name| !name.is_empty())
        .or_else(|| item.name.filter(|name| !name.is_empty()))
        .map(|name| {
            if name.contains('.') {
                name
            } else {
                format!("{catalog}.{schema}.{name}")
            }
        })
        .ok_or_else(|| Error::from(format!("Delta table at {uri} has no name")))?;
    Ok(Some(LakeTable {
        name,
        uri,
        env: env.clone(),
        info: None,
    }))
}

async fn pages<P: DeserializeOwned>(
    client: &Client,
    root: &str,
    token: Option<&str>,
    path: &str,
    query: &[(&str, &str)],
    next: fn(&P) -> Option<String>,
) -> Result<Vec<P>, Error> {
    let mut page_token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut pages = Vec::new();
    loop {
        if pages.len() >= PAGE_CAP {
            return Err(format!("catalog listed more than {PAGE_CAP} pages at {path}").into());
        }
        let mut url = format!("{root}{path}?max_results={PAGE_SIZE}");
        if path == "/tables" {
            url.push_str("&omit_columns=true&omit_properties=true");
        }
        for (key, value) in query {
            url.push('&');
            url.push_str(key);
            url.push('=');
            url.push_str(&encode(value));
        }
        if let Some(token) = &page_token {
            url.push_str("&page_token=");
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

fn nonempty(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|value| !value.is_empty())
}

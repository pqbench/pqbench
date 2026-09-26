//! The Unity Catalog list client behind [`super::super::api::list_tables`].
//!
//! Compiled only with the `unity` feature; it names `reqwest`, as does
//! [`super::http`], which holds the shared page walk.

use std::collections::BTreeMap;

use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::third_party::unity::api::{is_glob, Error, LakeSource, NameFilter};

use super::filter;
use super::http::{self, encode, page_token, REQUEST_TIMEOUT};
use crate::lake::LakeTable;

const PAGE_SIZE: u32 = 50;

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
    let root = api_root(&source.catalog_endpoint()?);
    let token = source.catalog_token();
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
                    &source.storage_env(),
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
    for page in http::pages::<P>(
        client,
        token,
        |page_token| unity_path(root, path, query, page_token),
        next,
    )
    .await?
    {
        for name in field(&page) {
            if name.is_empty() {
                return Err(format!("{path} listed a nameless entry").into());
            }
            names.push(name);
        }
    }
    Ok(names)
}

fn unity_path(root: &str, path: &str, query: &[(&str, &str)], page_token: Option<&str>) -> String {
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
    if let Some(token) = page_token {
        url.push_str("&page_token=");
        url.push_str(&encode(token));
    }
    url
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
    let query = [("catalog_name", catalog), ("schema_name", schema)];
    for page in http::pages::<TablesPage>(
        client,
        token,
        |page_token| unity_path(root, "/tables", &query, page_token),
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

fn nonempty(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|value| !value.is_empty())
}

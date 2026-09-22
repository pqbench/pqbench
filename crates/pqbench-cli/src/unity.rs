//! List Delta tables from a Unity Catalog endpoint.
//!
//! Unity Catalog OSS and Databricks expose the same list routes. Pagination
//! follows the Databricks REST guide: `max_results=0`, then repeat while
//! `next_page_token` is present. An empty page can still carry a token.
//!
//! https://docs.databricks.com/api/workspace/tables/list
//! https://docs.databricks.com/aws/en/dev-tools/rest-api

use std::collections::{BTreeMap, BTreeSet};

use pqbench::lake::{Lake, LakeTable};
use serde::Deserialize;

use crate::document::LakeSource;
use crate::CliError;

const PAGE_CAP: usize = 32;

#[derive(Deserialize)]
struct Named {
    name: String,
}

#[derive(Deserialize)]
struct CatalogsPage {
    catalogs: Vec<Named>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct SchemasPage {
    schemas: Vec<Named>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct TablesPage {
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
    data_source_format: Option<String>,
    #[serde(default)]
    storage_location: Option<String>,
}

/// List every Delta table the catalog will show, in name order.
pub(crate) fn list_tables(source: &LakeSource) -> Result<Lake, CliError> {
    let root = api_root(&source.endpoint);
    let token = source.token.as_deref().filter(|token| !token.is_empty());
    let mut tables = Vec::new();
    for catalog in names::<CatalogsPage>(
        &root,
        token,
        "/catalogs",
        &[],
        |page| page.catalogs.iter().map(|item| item.name.clone()).collect(),
        |page| page_token(&page.next_page_token),
    )? {
        let schemas = names::<SchemasPage>(
            &root,
            token,
            "/schemas",
            &[("catalog_name", catalog.as_str())],
            |page| page.schemas.iter().map(|item| item.name.clone()).collect(),
            |page| page_token(&page.next_page_token),
        )?;
        for schema in schemas {
            for table in tables_in(&root, token, &catalog, &schema, &source.env)? {
                tables.push(table);
            }
        }
    }
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    if tables.is_empty() {
        return Err("catalog listed no Delta tables".into());
    }
    Ok(Lake {
        kind: "pqbench.lake".into(),
        version: 1,
        name: Some(root),
        tables,
    })
}

fn api_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/api/2.1/unity-catalog") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/api/2.1/unity-catalog")
    }
}

fn names<P: for<'de> Deserialize<'de>>(
    root: &str,
    token: Option<&str>,
    path: &str,
    query: &[(&str, &str)],
    field: fn(&P) -> Vec<String>,
    next: fn(&P) -> Option<String>,
) -> Result<Vec<String>, CliError> {
    let mut names = Vec::new();
    for page in pages::<P>(root, token, path, query, next)? {
        for name in field(&page) {
            if name.is_empty() {
                return Err(format!("catalog {path} listed a nameless entry").into());
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

fn tables_in(
    root: &str,
    token: Option<&str>,
    catalog: &str,
    schema: &str,
    env: &BTreeMap<String, String>,
) -> Result<Vec<LakeTable>, CliError> {
    let mut tables = Vec::new();
    for page in pages::<TablesPage>(
        root,
        token,
        "/tables",
        &[("catalog_name", catalog), ("schema_name", schema)],
        |page| page_token(&page.next_page_token),
    )? {
        for item in page.tables {
            if let Some(table) = lake_table(item, catalog, schema, env)? {
                tables.push(table);
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
) -> Result<Option<LakeTable>, CliError> {
    let format = item.data_source_format.as_deref().unwrap_or("DELTA");
    if !format.eq_ignore_ascii_case("DELTA") {
        return Ok(None);
    }
    let uri = item
        .storage_location
        .filter(|uri| !uri.is_empty())
        .ok_or_else(|| format!("Delta table {catalog}.{schema} is missing storage_location"))?;
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
        .ok_or_else(|| format!("Delta table at {uri} has no name"))?;
    Ok(Some(LakeTable {
        name,
        uri,
        env: env.clone(),
        info: None,
    }))
}

fn pages<P: for<'de> Deserialize<'de>>(
    root: &str,
    token: Option<&str>,
    path: &str,
    query: &[(&str, &str)],
    next: fn(&P) -> Option<String>,
) -> Result<Vec<P>, CliError> {
    let mut page_token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut pages = Vec::new();
    loop {
        if pages.len() >= PAGE_CAP {
            return Err(format!("catalog listed more than {PAGE_CAP} pages at {path}").into());
        }
        let mut url = format!("{root}{path}?max_results=0");
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
        let page: P = get_json(&url, token)?;
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

fn get_json<T: for<'de> Deserialize<'de>>(url: &str, token: Option<&str>) -> Result<T, CliError> {
    let request = ureq::get(url);
    let request = match token {
        Some(token) => request.set("Authorization", &format!("Bearer {token}")),
        None => request,
    };
    let response = request.call().map_err(catalog_error)?;
    let body = response
        .into_string()
        .map_err(|error| format!("catalog response was not text: {error}"))?;
    serde_json::from_str(&body)
        .map_err(|error| format!("catalog response was not the expected document: {error}").into())
}

fn catalog_error(error: ureq::Error) -> CliError {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response
                .into_string()
                .map_err(|error| {
                    format!("catalog returned HTTP {code} and the body could not be read: {error}")
                })
                .unwrap_or_else(|error| error.to_string());
            format!("catalog returned HTTP {code}: {body}").into()
        }
        other => format!("catalog request failed: {other}").into(),
    }
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

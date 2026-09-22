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
use serde_json::Value;

use crate::document::LakeSource;
use crate::CliError;

/// List every Delta table the catalog will show, in name order.
pub(crate) fn list_tables(source: &LakeSource) -> Result<Lake, CliError> {
    let root = api_root(&source.endpoint);
    let token = source.token.as_deref().filter(|token| !token.is_empty());
    let mut tables = Vec::new();
    for catalog in names(&root, token, "/catalogs", &[], "catalogs")? {
        let schemas = names(
            &root,
            token,
            "/schemas",
            &[("catalog_name", catalog.as_str())],
            "schemas",
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

fn names(
    root: &str,
    token: Option<&str>,
    path: &str,
    query: &[(&str, &str)],
    field: &str,
) -> Result<Vec<String>, CliError> {
    let mut names = Vec::new();
    for page in pages(root, token, path, query)? {
        let Some(items) = page.get(field).and_then(|value| value.as_array()) else {
            continue;
        };
        for item in items {
            if let Some(name) = item.get("name").and_then(|name| name.as_str()) {
                if !name.is_empty() {
                    names.push(name.to_string());
                }
            }
        }
    }
    Ok(names)
}

fn tables_in(
    root: &str,
    token: Option<&str>,
    catalog: &str,
    schema: &str,
    env: &BTreeMap<String, String>,
) -> Result<Vec<LakeTable>, CliError> {
    let mut tables = Vec::new();
    for page in pages(
        root,
        token,
        "/tables",
        &[("catalog_name", catalog), ("schema_name", schema)],
    )? {
        let Some(items) = page.get("tables").and_then(|value| value.as_array()) else {
            continue;
        };
        for item in items {
            if let Some(table) = lake_table(item, catalog, schema, env) {
                tables.push(table);
            }
        }
    }
    Ok(tables)
}

fn lake_table(
    item: &Value,
    catalog: &str,
    schema: &str,
    env: &BTreeMap<String, String>,
) -> Option<LakeTable> {
    let format = item
        .get("data_source_format")
        .and_then(|value| value.as_str())
        .unwrap_or("DELTA");
    if !format.eq_ignore_ascii_case("DELTA") {
        return None;
    }
    let uri = item.get("storage_location")?.as_str()?.to_string();
    if uri.is_empty() {
        return None;
    }
    let name = item
        .get("full_name")
        .and_then(|value| value.as_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            let name = item
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("table");
            format!("{catalog}.{schema}.{name}")
        });
    Some(LakeTable {
        name,
        uri,
        env: env.clone(),
        info: None,
    })
}

/// Follow `next_page_token` until it is absent. `max_results=0` asks the server
/// to choose the page size, which is what the Databricks list guide recommends.
fn pages(
    root: &str,
    token: Option<&str>,
    path: &str,
    query: &[(&str, &str)],
) -> Result<Vec<Value>, CliError> {
    let mut page_token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut pages = Vec::new();
    loop {
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
        let page = get_json(&url, token)?;
        let next = page
            .get("next_page_token")
            .and_then(|value| value.as_str())
            .filter(|token| !token.is_empty())
            .map(str::to_string);
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

fn get_json(url: &str, token: Option<&str>) -> Result<Value, CliError> {
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
        .map_err(|error| format!("catalog response was not JSON: {error}").into())
}

fn catalog_error(error: ureq::Error) -> CliError {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
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

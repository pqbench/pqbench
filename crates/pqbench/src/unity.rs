//! List Delta tables from a Unity Catalog endpoint.
//!
//! Unity Catalog OSS and Databricks expose the same list routes. Pagination
//! follows `next_page_token`. Pages are bounded (`max_results=50`). Table list
//! requests omit columns and properties. `--include` / `--exclude` prune the
//! walk when the leading name is a literal. Listing is sequential: the caller
//! runs one table per later process, not one thread per table. The HTTP client
//! is behind the optional `unity` feature: without it [`list_tables`] fails at
//! runtime, so callers need no `cfg`.
//!
//! https://docs.databricks.com/api/workspace/tables/list
//! https://docs.databricks.com/aws/en/dev-tools/rest-api

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::lake::filter::NameFilter;
use crate::lake::LakeTable;

/// Credentials for listing a Unity Catalog, OSS or Databricks. `endpoint` is
/// the server origin (`http://localhost:8080` or
/// `https://example.cloud.databricks.com`). `token` is the Databricks bearer
/// token; Unity OSS often has none. `env` is copied onto each listed table so
/// `pqbench table` can read its files.
#[derive(Deserialize)]
pub struct LakeSource {
    pub version: u32,
    pub endpoint: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// List this catalog, or a catalog-name glob. A literal skips `/catalogs`.
    #[serde(default)]
    pub catalog: Option<String>,
    /// List this schema, or a schema-name glob. A literal skips `/schemas`.
    /// Requires `catalog`.
    #[serde(default)]
    pub schema: Option<String>,
}

/// Errors listing a Unity Catalog.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unity: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// List Delta tables, sending each to `on_table` as it is found.
///
/// Without the `unity` feature, this returns an error instead of listing.
pub fn list_tables<E: From<Error>>(
    source: &LakeSource,
    filter: &NameFilter,
    on_table: impl FnMut(LakeTable) -> Result<(), E>,
) -> Result<usize, E> {
    #[cfg(feature = "unity")]
    {
        client::list_tables(source, filter, on_table)
    }
    #[cfg(not(feature = "unity"))]
    {
        let _ = (source, filter, on_table);
        Err(Error::from(
            "this build lists directories only; rebuild with --features unity for a Unity Catalog"
                .to_string(),
        )
        .into())
    }
}

#[cfg(feature = "unity")]
mod client {
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Read;

    use serde::Deserialize;

    use super::{Error, LakeSource};
    use crate::lake::filter::{is_glob, NameFilter};
    use crate::lake::LakeTable;

    const PAGE_CAP: usize = 32;
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

    pub(super) fn list_tables<E: From<Error>>(
        source: &LakeSource,
        filter: &NameFilter,
        mut on_table: impl FnMut(LakeTable) -> Result<(), E>,
    ) -> Result<usize, E> {
        let root = api_root(&source.endpoint);
        let token = source.token.clone().filter(|token| !token.is_empty());
        let mut tables = 0usize;
        for catalog in list_catalogs(&root, token.as_deref(), source, filter)? {
            for schema in list_schemas(&root, token.as_deref(), &catalog, source, filter)? {
                for table in list_schema_tables(
                    &root,
                    token.as_deref(),
                    &catalog,
                    &schema,
                    &source.env,
                    filter,
                )? {
                    on_table(table)?;
                    tables += 1;
                }
            }
        }
        if tables == 0 {
            return Err(Error::from("catalog listed no Delta tables".to_string()).into());
        }
        Ok(tables)
    }

    fn list_catalogs(
        root: &str,
        token: Option<&str>,
        source: &LakeSource,
        filter: &NameFilter,
    ) -> Result<Vec<String>, Error> {
        if let Some(catalog) = nonempty(&source.catalog) {
            if !is_glob(catalog) {
                return Ok(vec![catalog.to_string()]
                    .into_iter()
                    .filter(|name| filter.keeps_prefix(name))
                    .collect());
            }
        }
        if source.catalog.is_none() {
            if let Some(scoped) = filter.catalog_scope() {
                return Ok(scoped
                    .into_iter()
                    .filter(|catalog| filter.keeps_prefix(catalog))
                    .collect());
            }
        }
        let names = names::<CatalogsPage>(
            root,
            token,
            "/catalogs",
            &[],
            |page| page.catalogs.iter().map(|item| item.name.clone()).collect(),
            |page| page_token(&page.next_page_token),
        )?;
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
                    && filter.keeps_prefix(catalog)
            })
            .collect())
    }

    fn list_schemas(
        root: &str,
        token: Option<&str>,
        catalog: &str,
        source: &LakeSource,
        filter: &NameFilter,
    ) -> Result<Vec<String>, Error> {
        if let Some(schema) = nonempty(&source.schema) {
            if !is_glob(schema) {
                let fqn = format!("{catalog}.{schema}");
                return Ok(if filter.keeps_prefix(&fqn) {
                    vec![schema.to_string()]
                } else {
                    Vec::new()
                });
            }
        }
        if source.schema.is_none() {
            if let Some(scoped) = filter.schema_scope(catalog) {
                return Ok(scoped
                    .into_iter()
                    .filter(|schema| filter.keeps_prefix(&format!("{catalog}.{schema}")))
                    .collect());
            }
        }
        let names = names::<SchemasPage>(
            root,
            token,
            "/schemas",
            &[("catalog_name", catalog)],
            |page| page.schemas.iter().map(|item| item.name.clone()).collect(),
            |page| page_token(&page.next_page_token),
        )?;
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
                    && filter.keeps_prefix(&fqn)
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

    fn names<P: for<'de> Deserialize<'de>>(
        root: &str,
        token: Option<&str>,
        path: &str,
        query: &[(&str, &str)],
        field: fn(&P) -> Vec<String>,
        next: fn(&P) -> Option<String>,
    ) -> Result<Vec<String>, Error> {
        let mut names = Vec::new();
        for page in pages::<P>(root, token, path, query, next)? {
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

    fn list_schema_tables(
        root: &str,
        token: Option<&str>,
        catalog: &str,
        schema: &str,
        env: &BTreeMap<String, String>,
        filter: &NameFilter,
    ) -> Result<Vec<LakeTable>, Error> {
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

    fn pages<P: for<'de> Deserialize<'de>>(
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

    fn get_json<T: for<'de> Deserialize<'de>>(url: &str, token: Option<&str>) -> Result<T, Error> {
        let request = ureq::get(url);
        let request = match token {
            Some(token) => request.set("Authorization", &format!("Bearer {token}")),
            None => request,
        };
        let response = request.call().map_err(catalog_error)?;
        let mut body = String::new();
        response
            .into_reader()
            .read_to_string(&mut body)
            .map_err(|error| Error::from(format!("catalog response was not text: {error}")))?;
        serde_json::from_str(&body).map_err(|error| {
            Error::from(format!(
                "catalog response was not the expected document: {error}"
            ))
        })
    }

    fn catalog_error(error: ureq::Error) -> Error {
        match error {
            ureq::Error::Status(code, response) => {
                let mut body = String::new();
                let _ = response.into_reader().read_to_string(&mut body);
                Error::from(format!("catalog returned HTTP {code}: {body}"))
            }
            other => Error::from(format!("catalog request failed: {other}")),
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

    fn nonempty(value: &Option<String>) -> Option<&str> {
        value.as_deref().filter(|value| !value.is_empty())
    }
}

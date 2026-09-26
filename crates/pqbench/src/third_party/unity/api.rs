//! List tables from a catalog endpoint: Unity Catalog or Iceberg REST.
//!
//! [`list_tables`] is the only entry point. `GET {endpoint}/v1/config` chooses
//! the dialect: a 200 with a `defaults` object is Iceberg REST; a 200 without
//! `defaults`, or a 404, is Unity. Unity Catalog OSS and Databricks expose the
//! same list routes (catalogs, then schemas, then tables, following
//! `next_page_token`); an Iceberg REST catalog lists namespaces and tables,
//! then `loadTable` for each metadata location. Pages are bounded, and
//! `--include` / `--exclude` prune the walk when the leading name is a literal.
//! Listing is sequential: the caller runs one table per later process, not one
//! thread per table.
//!
//! All HTTP interaction lives in the private `impl` module. The `unity` feature
//! compiles it; without it [`list_tables`] fails and names the feature. There
//! are no feature flags outside this folder.
//!
//! https://docs.databricks.com/api/workspace/tables/list
//! https://docs.databricks.com/aws/en/dev-tools/rest-api
//! https://iceberg.apache.org/docs/latest/rest-catalog-spec/

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::lake::LakeTable;

/// Credentials for listing a catalog. `endpoint` is the server origin
/// (`http://localhost:8080` for Unity, `http://localhost:8181` for Iceberg
/// REST, or `https://example.cloud.databricks.com`). `GET /v1/config` chooses
/// the dialect. `token` is the bearer token; Unity OSS often has none. `env` is
/// copied onto each listed table so `pqbench table` can read its files.
#[derive(Deserialize)]
pub struct LakeSource {
    pub version: u32,
    pub endpoint: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// List this catalog, or a catalog-name glob. A literal skips `/catalogs`.
    /// Unity only; Iceberg REST lists every namespace at `endpoint`.
    #[serde(default)]
    pub catalog: Option<String>,
    /// List this schema, or a schema-name glob. A literal skips `/schemas`.
    /// Requires `catalog`. Unity only.
    #[serde(default)]
    pub schema: Option<String>,
}

/// Errors listing a catalog.
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

/// Keep names that match `--include` and do not match `--exclude`.
///
/// A pattern with `*`, `?`, or `[` is a glob, matched against the whole FQN or
/// component-wise (`main.*.events`). Anything else is an exact FQN or a prefix
/// (`main` keeps `main.default.events`). A name is a Unity FQN
/// (`catalog.schema.table`) or a lake-relative directory path.
#[derive(Clone, Default)]
pub struct NameFilter {
    pub(crate) include: Vec<String>,
    pub(crate) exclude: Vec<String>,
}

impl NameFilter {
    /// Build a filter from the include and exclude patterns.
    #[must_use]
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self { include, exclude }
    }

    /// A complete table FQN, or a directory lake name.
    #[must_use]
    pub fn keeps(&self, name: &str) -> bool {
        self.included(name) && !self.excluded(name)
    }

    fn included(&self, name: &str) -> bool {
        self.include.is_empty()
            || self
                .include
                .iter()
                .any(|pattern| matches_fqn(name, pattern))
    }

    fn excluded(&self, name: &str) -> bool {
        self.exclude
            .iter()
            .any(|pattern| matches_fqn(name, pattern))
    }

    /// Whether a partial `catalog`/`catalog.schema` name — or a directory
    /// prefix below a lake root — can still lead to a kept table FQN. False
    /// means the prefix and everything under it can be skipped.
    #[must_use]
    pub fn keeps_prefix(&self, name: &str) -> bool {
        let included =
            self.include.is_empty() || self.include.iter().any(|pattern| can_reach(name, pattern));
        included
            && !self
                .exclude
                .iter()
                .any(|pattern| prunes_prefix(name, pattern))
    }
}

/// Whether `pattern` can still match `prefix` or something below it.
pub(crate) fn can_reach(prefix: &str, pattern: &str) -> bool {
    if is_glob(pattern) && glob_matches(pattern, prefix) {
        return true;
    }
    components_match(prefix, pattern, true)
}

/// Whether `pattern` (a literal or a prefix) drops `prefix` entirely.
pub(crate) fn prunes_prefix(prefix: &str, pattern: &str) -> bool {
    let prefix_parts = split_fqn(prefix);
    let pattern_parts = split_fqn(pattern);
    if pattern_parts.len() > prefix_parts.len() {
        return false;
    }
    matches_fqn(prefix, pattern)
}

pub(crate) fn is_glob(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?') || pattern.contains('[')
}

pub(crate) fn matches_fqn(name: &str, pattern: &str) -> bool {
    if is_glob(pattern) {
        if glob_matches(pattern, name) {
            return true;
        }
        return components_match(name, pattern, false);
    }
    name == pattern
        || name.starts_with(&format!("{pattern}."))
        || name.starts_with(&format!("{pattern}/"))
}

pub(crate) fn components_match(name: &str, pattern: &str, prefix: bool) -> bool {
    let name_parts = split_fqn(name);
    let pattern_parts = split_fqn(pattern);
    if name_parts.is_empty() || pattern_parts.is_empty() {
        return false;
    }
    if !prefix && name_parts.len() < pattern_parts.len() {
        return false;
    }
    let shared = name_parts.len().min(pattern_parts.len());
    name_parts
        .iter()
        .zip(pattern_parts.iter())
        .take(shared)
        .all(|(name, pattern)| component_matches(name, pattern))
}

fn component_matches(name: &str, pattern: &str) -> bool {
    if is_glob(pattern) {
        glob_matches(pattern, name)
    } else {
        name == pattern
    }
}

pub(crate) fn glob_matches(pattern: &str, name: &str) -> bool {
    glob::Pattern::new(pattern)
        .map(|glob| glob.matches(name))
        .unwrap_or(false)
}

pub(crate) fn split_fqn(name: &str) -> Vec<&str> {
    name.split(['.', '/'])
        .filter(|part| !part.is_empty())
        .collect()
}

/// List the Delta tables the source names, in catalog, schema, table order.
///
/// # Errors
/// Fails when the `unity` feature is off, the endpoint cannot be reached, a
/// catalog page is malformed, or no Delta table is found.
pub async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    super::r#impl::list_tables(source, filter).await
}

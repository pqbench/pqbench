//! List Delta tables from a Unity Catalog endpoint.
//!
//! [`list_tables`] is the only entry point. Unity Catalog OSS and Databricks
//! expose the same list routes: catalogs, then schemas, then tables, following
//! `next_page_token`. Pages are bounded (`max_results=50`), table list requests
//! omit columns and properties, and `--include` / `--exclude` prune the walk
//! when the leading name is a literal. Listing is sequential: the caller runs
//! one table per later process, not one thread per table.
//!
//! All `reqwest` interaction lives in the private `unity_helpers` module. The
//! `unity` feature compiles the client and that module; without it
//! [`list_tables`] fails and names the feature. There are no feature flags
//! outside these two modules.
//!
//! https://docs.databricks.com/api/workspace/tables/list
//! https://docs.databricks.com/aws/en/dev-tools/rest-api

use std::collections::BTreeMap;

use serde::Deserialize;

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

/// Keep names that match `--include` and do not match `--exclude`.
///
/// A pattern with `*`, `?`, or `[` is a glob, matched against the whole FQN or
/// component-wise (`main.*.events`). Anything else is an exact FQN or a prefix
/// (`main` keeps `main.default.events`). A name is a Unity FQN
/// (`catalog.schema.table`) or a lake-relative directory path.
#[derive(Clone, Default)]
pub struct NameFilter {
    include: Vec<String>,
    exclude: Vec<String>,
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

    /// A catalog or `catalog.schema` still worth walking.
    #[cfg(feature = "unity")]
    pub(crate) fn keeps_prefix(&self, name: &str) -> bool {
        let included =
            self.include.is_empty() || self.include.iter().any(|pattern| can_reach(name, pattern));
        included
            && !self
                .exclude
                .iter()
                .any(|pattern| prunes_prefix(name, pattern))
    }

    /// Catalogs `--include` can name without walking `/catalogs`.
    #[cfg(feature = "unity")]
    pub(crate) fn catalog_scope(&self) -> Option<Vec<String>> {
        literal_heads(&self.include, 0)
    }

    /// Schemas `--include` can name inside `catalog` without walking `/schemas`.
    #[cfg(feature = "unity")]
    pub(crate) fn schema_scope(&self, catalog: &str) -> Option<Vec<String>> {
        let patterns: Vec<String> = self
            .include
            .iter()
            .filter(|pattern| can_reach(catalog, pattern))
            .cloned()
            .collect();
        if patterns.is_empty() {
            return None;
        }
        literal_heads(&patterns, 1)
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
}

pub(crate) fn is_glob(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?') || pattern.contains('[')
}

fn matches_fqn(name: &str, pattern: &str) -> bool {
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

#[cfg(feature = "unity")]
fn can_reach(prefix: &str, pattern: &str) -> bool {
    if is_glob(pattern) && glob_matches(pattern, prefix) {
        return true;
    }
    components_match(prefix, pattern, true)
}

#[cfg(feature = "unity")]
fn prunes_prefix(prefix: &str, pattern: &str) -> bool {
    let prefix_parts = split_fqn(prefix);
    let pattern_parts = split_fqn(pattern);
    if pattern_parts.len() > prefix_parts.len() {
        return false;
    }
    matches_fqn(prefix, pattern)
}

fn components_match(name: &str, pattern: &str, prefix: bool) -> bool {
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

fn glob_matches(pattern: &str, name: &str) -> bool {
    glob::Pattern::new(pattern)
        .map(|glob| glob.matches(name))
        .unwrap_or(false)
}

fn split_fqn(name: &str) -> Vec<&str> {
    name.split(['.', '/'])
        .filter(|part| !part.is_empty())
        .collect()
}

#[cfg(feature = "unity")]
fn literal_heads(patterns: &[String], index: usize) -> Option<Vec<String>> {
    if patterns.is_empty() {
        return None;
    }
    let mut heads = Vec::new();
    for pattern in patterns {
        let parts = split_fqn(pattern);
        let Some(part) = parts.get(index) else {
            continue;
        };
        if is_glob(part) {
            return None;
        }
        if !heads.iter().any(|have| have == part) {
            heads.push((*part).to_string());
        }
    }
    if heads.is_empty() {
        None
    } else {
        Some(heads)
    }
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
    #[cfg(feature = "unity")]
    {
        crate::unity_helpers::list_tables(source, filter).await
    }
    #[cfg(not(feature = "unity"))]
    {
        let _ = (source, filter);
        Err(Error::from(
            "this build lists directories only; rebuild with --features unity for a Unity Catalog"
                .to_string(),
        ))
    }
}

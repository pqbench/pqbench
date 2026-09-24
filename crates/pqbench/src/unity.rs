//! List Delta tables from a Unity Catalog endpoint.
//!
//! [`list_tables`] is the only entry point. Unity Catalog OSS and Databricks
//! expose the same list routes: catalogs, then schemas, then tables, following
//! `next_page_token`. Pages are bounded (`max_results=50`), table list requests
//! omit columns and properties, and `--include` / `--exclude` prune the walk
//! when the leading name is a literal. Listing is sequential: the caller runs
//! one table per later process, not one thread per table.
//!
//! All `ureq` interaction lives in the private `unity_helpers` module. The
//! `unity` feature compiles the client and that module; without it
//! [`list_tables`] fails and names the feature. There are no feature flags
//! outside these two modules.
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

/// List the Delta tables the source names, in catalog, schema, table order.
///
/// # Errors
/// Fails when the `unity` feature is off, the endpoint cannot be reached, a
/// catalog page is malformed, or no Delta table is found.
pub fn list_tables(source: &LakeSource, filter: &NameFilter) -> Result<Vec<LakeTable>, Error> {
    #[cfg(feature = "unity")]
    {
        crate::unity_helpers::list_tables(source, filter)
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

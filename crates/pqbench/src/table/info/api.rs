//! `table info`: one table's record.
//!
//! [`read`] is the only public function. It returns the plain
//! [`TableInfo`](crate::table::TableInfo); the CLI owns the document it
//! becomes.

use std::collections::BTreeMap;
use std::fmt;

use crate::table::TableInfo;

/// The catalog dialect a `table info` read speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// Unity REST: `/tables`.
    Unity,
    /// Iceberg REST: `/namespaces…/tables`; the endpoint already names the
    /// catalog base (`{root}/v1` or `{root}/v1/{prefix}`), so no config probe
    /// runs.
    Iceberg,
}

/// Errors reading a table's record.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "table: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Where and how a table's metadata is read: the storage path a parent ref
/// already carries (when it does), and the object-store options the read needs.
pub struct Storage {
    /// The storage path a parent ref already carries, when it does. The catalog
    /// stays authoritative; this only lets the storage read start earlier.
    pub location: Option<String>,
    /// The object-store options (`AWS_*` names) the read needs.
    pub env: BTreeMap<String, String>,
}

/// Read the record of `table` in `schema` of `catalog` at `endpoint`.
///
/// The catalog names the table and the location to load; the table's own
/// metadata is read without files (`without_files()` for Delta, the metadata
/// JSON only for Iceberg), so the cost is O(1) in files. `storage` carries the
/// object-store options that read needs and, when the parent ref already named
/// it, the location, so the storage read starts alongside the catalog call; the
/// catalog's own location stays authoritative.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or the table's metadata cannot be read.
pub async fn read(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
    table_format: TableFormat,
    storage: &Storage,
) -> Result<TableInfo, Error> {
    super::r#impl::read(
        endpoint,
        catalog,
        schema,
        table,
        token,
        table_format,
        storage,
    )
    .await
}

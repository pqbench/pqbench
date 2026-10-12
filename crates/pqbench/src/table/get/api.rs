//! `table::get`: one table's record.
//!
//! [`read`] is the only public function. It returns the [`Table`] resource —
//! the wire form of the loaded [`TableInfo`](crate::table::TableInfo) — and the
//! CLI forwards it as-is.

use std::collections::BTreeMap;
use std::fmt;

pub use crate::config::Dialect as TableFormat;
pub use crate::v1::Table;

use crate::{Lake, Workspace};

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

/// Read the record of `table` in `schema` of `catalog` at the workspace's
/// endpoint.
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
    workspace: &Workspace,
    lake: &Lake,
    catalog: &str,
    schema: &str,
    table: &str,
    storage: &Storage,
) -> Result<Table, Error> {
    super::r#impl::read(workspace, lake, catalog, schema, table, storage).await
}

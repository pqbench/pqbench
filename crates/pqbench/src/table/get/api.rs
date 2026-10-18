//! `table::get`: one table's record.
//!
//! [`get`] is the only public function. It returns the [`Table`] resource —
//! the wire form of the loaded [`TableInfo`](crate::table::TableInfo) — and the
//! CLI forwards it as-is.

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

/// Read the record of the table `table` names — the ref
/// [`credentials::get`](crate::credentials::get::get) returns, or a bare
/// `catalogs/{catalog}/schemas/{schema}/tables/{table}` name.
///
/// The catalog names the table and the location to load; the table's own
/// metadata is read without files (`without_files()` for Delta, the metadata
/// JSON only for Iceberg), so the cost is O(1) in files. The ref's `uri`, when
/// it has one, starts the storage read alongside the catalog call — the
/// catalog's own location stays authoritative — and its `credentials` are the
/// object-store options that read uses.
///
/// # Errors
/// Fails when the ref is not a table name, the endpoint cannot be reached,
/// answers with an unexpected status, or the table's metadata cannot be read.
pub async fn get(workspace: &Workspace, lake: &Lake, table: &Table) -> Result<Table, Error> {
    super::r#impl::get(workspace, lake, table).await
}

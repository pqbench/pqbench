//! `tablev2 info`: one table's record.
//!
//! [`read`] is the only public function. It returns the plain
//! [`TableInfo`](crate::table::TableInfo); the CLI owns the document it
//! becomes.

use std::collections::BTreeMap;
use std::fmt;

use crate::table::TableInfo;
use crate::tablev2::TableFormat;

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

/// Read the record of `table` in `schema` of `catalog` at `endpoint`.
///
/// The catalog names the table and the location to load; the table's own
/// metadata is read without files (`without_files()` for Delta, the metadata
/// JSON only for Iceberg), so the cost is O(1) in files. `env` carries the
/// object-store options that read needs.
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
    env: &BTreeMap<String, String>,
) -> Result<TableInfo, Error> {
    super::r#impl::read(endpoint, catalog, schema, table, token, table_format, env).await
}

//! `schema ls`: the tables in one schema.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::collections::BTreeMap;
use std::fmt;

use crate::schema::TableFormat;

/// One table in a schema: the `pqbench.table-ref` the CLI emits.
#[derive(Debug, Clone)]
pub struct TableRef {
    /// The table's full name (`catalog.schema.table`).
    pub name: String,
    /// The endpoint URL that reads this table's record.
    pub uri: String,
    /// The table's storage location, when the listing reports it.
    pub storage_path: Option<String>,
    /// Storage options for reading the table, copied from the source; empty
    /// when the caller has none.
    pub env: BTreeMap<String, String>,
}

/// Errors listing a schema's tables.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "schema: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// List the tables in `schema` of `catalog` at `endpoint`.
///
/// Unity pages carry a storage location; Iceberg REST lists identifiers only,
/// so `storage_path` is `None` there until `pqbench table info` resolves it.
/// Every ref carries `env`; the CLI writes it onto the wire ref.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed table page.
pub async fn list(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
    table_format: TableFormat,
    env: &BTreeMap<String, String>,
) -> Result<Vec<TableRef>, Error> {
    super::r#impl::list(endpoint, catalog, schema, token, table_format, env).await
}

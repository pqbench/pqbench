//! `schema ls`: the tables in one schema.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::fmt;

use crate::Context;

/// One table in a schema: the `pqbench.table-ref` the CLI emits.
#[derive(Debug, Clone)]
pub struct TableRef {
    /// The table's full name (`catalog.schema.table`).
    pub name: String,
    /// The endpoint URL that reads this table's record.
    pub uri: String,
    /// The table's storage location, when the listing reports it.
    pub storage_path: Option<String>,
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

/// List the tables in `schema` of `catalog` at the context's endpoint.
///
/// Unity pages carry a storage location; Iceberg REST lists identifiers only,
/// so `storage_path` is `None` there until `pqbench table info` resolves it.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed table page.
pub async fn list(context: &Context, catalog: &str, schema: &str) -> Result<Vec<TableRef>, Error> {
    super::r#impl::list(context, catalog, schema).await
}

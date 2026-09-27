//! `schema info`: one schema's record.
//!
//! [`read`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::collections::BTreeMap;
use std::fmt;

/// One schema record.
#[derive(Debug, Clone)]
pub struct Schema {
    /// The catalog the schema belongs to.
    pub catalog: String,
    /// The schema name (an Iceberg namespace may have several parts).
    pub name: String,
    /// The schema comment, when it has one.
    pub comment: Option<String>,
    /// The schema's storage location, when it has one.
    pub location: Option<String>,
    /// The schema properties the endpoint reports.
    pub properties: BTreeMap<String, String>,
}

/// Errors reading a schema.
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

/// Read the record of `schema` in `catalog` at `endpoint`.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed schema record.
pub async fn read(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
) -> Result<Schema, Error> {
    super::r#impl::read(endpoint, catalog, schema, token).await
}

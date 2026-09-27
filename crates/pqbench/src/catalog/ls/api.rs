//! `catalog ls`: the schemas in one catalog.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::fmt;

/// One schema in a catalog.
#[derive(Debug, Clone)]
pub struct Schema {
    /// The catalog the schema belongs to.
    pub catalog: String,
    /// The schema name.
    pub name: String,
}

/// Errors listing a catalog's schemas.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "catalog: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// List the schemas in `catalog` at `endpoint`.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed schema page.
pub async fn list(
    endpoint: &str,
    catalog: &str,
    token: Option<&str>,
) -> Result<Vec<Schema>, Error> {
    super::r#impl::list(endpoint, catalog, token).await
}

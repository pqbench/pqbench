//! `catalog info`: one catalog's record.
//!
//! [`read`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::fmt;

/// One catalog record.
#[derive(Debug, Clone)]
pub struct Catalog {
    /// The catalog name.
    pub name: String,
    /// The catalog type the endpoint reports (`MANAGED_CATALOG`, …), when it
    /// reports one.
    pub catalog_type: Option<String>,
    /// The catalog comment, when it has one.
    pub comment: Option<String>,
    /// The catalog owner, when the endpoint reports one.
    pub owner: Option<String>,
}

/// Errors reading a catalog.
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

/// Read the record of `catalog` from `endpoint`.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed catalog record.
pub async fn read(endpoint: &str, catalog: &str, token: Option<&str>) -> Result<Catalog, Error> {
    super::r#impl::read(endpoint, catalog, token).await
}

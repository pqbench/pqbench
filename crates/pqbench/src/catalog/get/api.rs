//! `catalog get`: one catalog's record.
//!
//! [`read`] is the only public function. It returns the [`Catalog`] resource;
//! the CLI forwards it as-is.

use std::fmt;

pub use crate::v1::Catalog;

use crate::Workspace;

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

/// Read the record of `catalog` from the workspace's endpoint.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed catalog record.
pub async fn read(workspace: &Workspace, catalog: &str) -> Result<Catalog, Error> {
    super::r#impl::read(workspace, catalog).await
}

//! `catalog::get`: one catalog's record.
//!
//! [`get`] is the only public function. It returns the [`Catalog`] resource;
//! the CLI forwards it as-is.

use std::fmt;

pub use crate::v1::Catalog;

use crate::Workspace;

/// Errors reading a catalog.
#[derive(Debug)]
pub enum Error {
    /// The name argument was not a resource name.
    Name(String),
    /// The endpoint request failed.
    Http(crate::http::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) => write!(f, "{message}"),
            Error::Http(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            _ => None,
        }
    }
}

/// Read the record of the catalog `name` — a `catalogs/{catalog}` resource
/// name — from the workspace's endpoint.
///
/// # Errors
/// Fails when `name` is not a catalog name, the endpoint cannot be reached,
/// answers with an unexpected status, or returns a malformed catalog record.
pub async fn get(workspace: &Workspace, name: &str) -> Result<Catalog, Error> {
    super::r#impl::get(workspace, name).await
}

//! `catalog::list`: the catalogs at the endpoint.
//!
//! [`list`] is the only public function. It returns [`Catalog`] resources; the
//! CLI forwards them as-is.

use std::fmt;

pub use crate::v1::Catalog;

use crate::Workspace;

/// Errors listing catalogs.
#[derive(Debug)]
#[non_exhaustive]
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
            Error::Name(_) => None,
        }
    }
}

/// List the catalogs at the workspace's endpoint, in name order.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed catalog page.
pub async fn list(workspace: &Workspace) -> Result<Vec<Catalog>, Error> {
    super::r#impl::list(workspace).await
}

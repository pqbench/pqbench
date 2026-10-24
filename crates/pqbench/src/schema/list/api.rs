//! `schema::list`: the schemas in one catalog.
//!
//! [`list`] is the only public function. It returns [`Schema`] resources; the
//! CLI forwards them as-is.

use std::fmt;

pub use crate::config::Dialect as TableFormat;
pub use crate::v1::Schema;

use crate::Workspace;

/// Errors listing a catalog's schemas.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name argument was not a resource name.
    Name(String),
    /// The endpoint request failed.
    Http(crate::http::Error),
    /// The endpoint returned a structurally invalid document.
    Malformed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) | Error::Malformed(message) => write!(f, "{message}"),
            Error::Http(_) => write!(f, "the endpoint request failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            Error::Name(_) | Error::Malformed(_) => None,
        }
    }
}

/// List the schemas in the catalog `name` — a `catalogs/{catalog}` resource
/// name — at the workspace's endpoint.
///
/// # Errors
/// Fails when `name` is not a catalog name, the endpoint cannot be reached,
/// answers with an unexpected status, or returns a malformed schema page.
pub async fn list(workspace: &Workspace, name: &str) -> Result<Vec<Schema>, Error> {
    super::r#impl::list(workspace, name).await
}

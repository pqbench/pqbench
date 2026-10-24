//! `table::list`: the tables in one schema.
//!
//! [`list`] is the only public function. It returns [`Table`] resources; the
//! CLI forwards them as-is.

use std::fmt;

pub use crate::v1::Table;

use crate::Workspace;

/// Errors listing a schema's tables.
#[derive(Debug)]
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

/// List the tables in the schema `name` — a
/// `catalogs/{catalog}/schemas/{schema}` resource name — at the workspace's
/// endpoint.
///
/// Unity pages carry a storage location; Iceberg REST lists identifiers only,
/// so `uri` is unset there until `pqbench get table` resolves it.
///
/// # Errors
/// Fails when `name` is not a schema name, the endpoint cannot be reached,
/// answers with an unexpected status, or returns a malformed table page.
pub async fn list(workspace: &Workspace, name: &str) -> Result<Vec<Table>, Error> {
    super::r#impl::list(workspace, name).await
}

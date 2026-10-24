//! `schema::get`: one schema's record.
//!
//! [`get`] is the only public function. It returns the [`Schema`] resource;
//! the CLI forwards it as-is.

use std::fmt;

pub use crate::v1::Schema;

use crate::Workspace;

/// Errors reading a schema.
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
            Error::Name(_) => None,
        }
    }
}

/// Read the record of the schema `name` — a
/// `catalogs/{catalog}/schemas/{schema}` resource name — at the workspace's
/// endpoint.
///
/// # Errors
/// Fails when `name` is not a schema name, the endpoint cannot be reached,
/// answers with an unexpected status, or returns a malformed schema record.
pub async fn get(workspace: &Workspace, name: &str) -> Result<Schema, Error> {
    super::r#impl::get(workspace, name).await
}

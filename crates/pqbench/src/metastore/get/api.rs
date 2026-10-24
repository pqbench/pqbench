//! `metastore::get`: the endpoint's metastore record.
//!
//! [`get`] is the only public function. It returns the [`Metastore`] resource;
//! the CLI forwards it as-is.

use std::fmt;

pub use crate::v1::Metastore;

use crate::Workspace;

/// Errors reading a metastore.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The endpoint request failed.
    Http(crate::http::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Http(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
        }
    }
}

/// Read the metastore record from the workspace's endpoint.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed summary.
pub async fn get(workspace: &Workspace) -> Result<Metastore, Error> {
    super::r#impl::get(workspace).await
}

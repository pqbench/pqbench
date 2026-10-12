//! `metastore get`: the endpoint's metastore record.
//!
//! [`read`] is the only public function. It returns the [`Metastore`] resource;
//! the CLI forwards it as-is.

use std::fmt;

pub use crate::v1::Metastore;

use crate::Workspace;

/// Errors reading a metastore.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metastore: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Read the metastore record from the workspace's endpoint.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed summary.
pub async fn read(workspace: &Workspace) -> Result<Metastore, Error> {
    super::r#impl::read(workspace).await
}

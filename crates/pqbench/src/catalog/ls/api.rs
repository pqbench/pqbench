//! `catalog ls`: the catalogs at the endpoint.
//!
//! [`list`] is the only public function. It returns [`Catalog`] resources; the
//! CLI forwards them as-is.

use std::fmt;

pub use crate::v1::Catalog;

use crate::Workspace;

/// Errors listing catalogs.
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

/// List the catalogs at the workspace's endpoint, in name order.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed catalog page.
pub async fn list(workspace: &Workspace) -> Result<Vec<Catalog>, Error> {
    super::r#impl::list(workspace).await
}

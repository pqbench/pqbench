//! `schema ls`: the schemas in one catalog.
//!
//! [`list`] is the only public function. It returns [`Schema`] resources; the
//! CLI forwards them as-is.

use std::fmt;

pub use crate::config::Dialect as TableFormat;
pub use crate::v1::Schema;

use crate::Workspace;

/// Errors listing a catalog's schemas.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "schema: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// List the schemas in `catalog` at the workspace's endpoint.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed schema page.
pub async fn list(workspace: &Workspace, catalog: &str) -> Result<Vec<Schema>, Error> {
    super::r#impl::list(workspace, catalog).await
}

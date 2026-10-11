//! `metastore ls`: the catalogs at the endpoint.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::fmt;

use crate::Context;

/// One catalog record.
#[derive(Debug, Clone)]
pub struct Catalog {
    /// The catalog name.
    pub name: String,
    /// The catalog type the endpoint reports (`MANAGED_CATALOG`, …), when it
    /// reports one.
    pub catalog_type: Option<String>,
}

/// Errors listing catalogs.
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

/// List the catalogs at the context's endpoint, in name order.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed catalog page.
pub async fn list(context: &Context) -> Result<Vec<Catalog>, Error> {
    super::r#impl::list(context).await
}

//! `schema::get`: one schema's record.
//!
//! [`read`] is the only public function. It returns the [`Schema`] resource;
//! the CLI forwards it as-is.

use std::fmt;

pub use crate::v1::Schema;

use crate::Workspace;

/// Errors reading a schema.
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

/// Read the record of `schema` in `catalog` at the workspace's endpoint.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed schema record.
pub async fn read(workspace: &Workspace, catalog: &str, schema: &str) -> Result<Schema, Error> {
    super::r#impl::read(workspace, catalog, schema).await
}

//! `table info`: one table's storage path.
//!
//! [`read`] is the only public function. It returns the plain path the caller
//! fills into a `pqbench.table-ref`; the wire document belongs to the CLI.

use std::fmt;

/// Errors resolving a table's storage path.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "table: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Read the storage path a table's record address names.
///
/// The address is the Iceberg REST `loadTable` URL a `pqbench.table-ref`
/// carries; its `metadata-location` is what `pqbench table` loads.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or reports no metadata location.
pub async fn read(uri: &str, token: Option<&str>) -> Result<String, Error> {
    super::r#impl::read(uri, token).await
}

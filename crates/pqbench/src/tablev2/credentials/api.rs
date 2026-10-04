//! `credentials get`: one table's vended storage options.
//!
//! [`vend`] is the only public function. It returns the `AWS_*` storage
//! options Unity vends for the table, or `None` when the table's kind cannot
//! be read outside Databricks compute or the catalog does not serve the
//! route, so the caller keeps its own `env`.

use std::collections::BTreeMap;
use std::fmt;

/// Errors vending a table's credentials.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "credentials: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Vend read credentials for `table` in `schema` of `catalog` at `endpoint`.
///
/// The returned options are `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
/// `AWS_SESSION_TOKEN`. `None` means the table cannot be read outside
/// Databricks compute (managed default storage, a view) or the catalog does
/// not implement `temporary-table-credentials`; the caller keeps its own
/// `env`. A catalog that reports no kind is attempted: the route itself
/// decides.
///
/// # Errors
/// Fails when the endpoint cannot be reached or answers with an unexpected
/// status.
pub async fn vend(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Option<BTreeMap<String, String>>, Error> {
    super::r#impl::vend(endpoint, catalog, schema, table, token).await
}

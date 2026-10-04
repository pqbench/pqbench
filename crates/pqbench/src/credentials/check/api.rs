//! `credentials check`: whether a table is readable outside Databricks compute.

use std::fmt;

/// Errors checking a table's eligibility.
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

/// Whether a catalog reports the table readable outside Databricks compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eligibility {
    /// The catalog reports direct external engine read or write support, or
    /// reports no capability manifest at all: the table is attempted. A
    /// catalog that reports none (Unity OSS) leaves the decision to the
    /// vending route.
    Eligible,
    /// The catalog's capability manifest lists capabilities without
    /// `HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT` or
    /// `HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT` (managed default storage, a
    /// view): only Databricks compute reads the table.
    Ineligible,
}

/// Check whether Unity reports `table` readable outside Databricks compute.
///
/// Reads the `GET /tables/{full_name}?include_manifest_capabilities=true`
/// record without asking for credentials. The check separates eligibility
/// from vending: a caller that needs the table itself (`tablev2 info`) can
/// report an [`Eligibility::Ineligible`] table with the reason instead of
/// running a storage read that cannot succeed.
///
/// # Errors
/// Fails when the endpoint cannot be reached or answers with an unexpected
/// status.
pub async fn check_unity(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Eligibility, Error> {
    super::r#impl::check_unity(endpoint, catalog, schema, table, token).await
}

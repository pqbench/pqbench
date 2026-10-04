//! `credentials get`: one table's vended storage options.
//!
//! [`check_unity`] reports whether Unity marks the table readable outside
//! Databricks compute; [`vend_unity`] asks Unity for temporary read
//! credentials; [`vend_iceberg`] takes the Iceberg REST catalog's
//! `storage-credentials`. The vend functions return the `AWS_*` storage
//! options for the table, or `None` when the catalog does not vend (managed
//! default storage, a view, no vending support), so the caller keeps its own
//! `env`.

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
/// Reads the same `GET /tables/{full_name}?include_manifest_capabilities=true`
/// record [`vend_unity`] gates on, without asking for credentials. The check
/// separates eligibility from vending: a caller that needs the table itself
/// (`tablev2 info`) can report an [`Eligibility::Ineligible`] table with the
/// reason instead of running a storage read that cannot succeed.
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

/// Vend Unity read credentials for `table` in `schema` of `catalog` at `endpoint`.
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
pub async fn vend_unity(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Option<BTreeMap<String, String>>, Error> {
    super::r#impl::vend_unity(endpoint, catalog, schema, table, token).await
}

/// Vend Iceberg REST read credentials for `table` in `schema` at `endpoint`.
///
/// The endpoint names the catalog base. The request asks for delegation, and
/// the returned options are the `storage-credentials` config covering the
/// table's location, as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
/// `AWS_SESSION_TOKEN`. `None` means the catalog vends nothing for the table
/// (managed default storage, or no vending support); the caller keeps its own
/// `env`.
///
/// # Errors
/// Fails when the endpoint cannot be reached or answers with an unexpected
/// status.
pub async fn vend_iceberg(
    endpoint: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Option<BTreeMap<String, String>>, Error> {
    super::r#impl::vend_iceberg(endpoint, schema, table, token).await
}

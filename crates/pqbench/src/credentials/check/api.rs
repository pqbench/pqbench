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
    /// The table is not readable outside Databricks compute, for [`Reason`].
    Ineligible(Reason),
}

/// Why a table is not readable outside Databricks compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// The ref names a `system` catalog table; Databricks compute reads it,
    /// not an external client.
    SystemTable,
    /// The catalog reports a view (or another non-table securable), which has
    /// no data files of its own.
    NotATable,
    /// The catalog reports Databricks default storage (`TABLE_DB_STORAGE`),
    /// the kind the vending route refuses: only Databricks compute reads the
    /// table.
    NoExternalRead,
}

/// The catalog dialect a check speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// Unity Catalog REST: the capability manifest gates external read.
    Unity,
    /// Iceberg REST: table metadata is read inline through the catalog, so
    /// there is no manifest to gate — the table is eligible.
    Iceberg,
}

/// Whether `table` is readable outside Databricks compute, by dialect.
///
/// The check owns the format: Unity reads the `GET /tables/{full_name}` record
/// without asking for credentials; Iceberg REST reads its metadata inline
/// through the catalog, so the table is eligible and no catalog call is made.
/// Unity also drops a `system` catalog ref without a call and reports a view
/// as [`Reason::NotATable`], so `schema ls` can list every securable and the
/// check is the single filter. The caller passes the dialect it runs under and
/// gets the same [`Eligibility`] either way. The check separates eligibility
/// from vending: a caller that needs the table itself (`tablev2 info`) can
/// report an [`Eligibility::Ineligible`] table with the reason instead of
/// running a storage read that cannot succeed.
///
/// # Errors
/// Fails when the endpoint cannot be reached or answers with an unexpected
/// status.
pub async fn check(
    format: TableFormat,
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Eligibility, Error> {
    match format {
        TableFormat::Unity => {
            if catalog == "system" {
                return Ok(Eligibility::Ineligible(Reason::SystemTable));
            }
            super::r#impl::check_unity(endpoint, catalog, schema, table, token).await
        }
        TableFormat::Iceberg => Ok(Eligibility::Eligible),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Iceberg REST reads its metadata through the catalog, so the check is
    /// eligible without a catalog call — a bogus endpoint is never reached.
    #[tokio::test]
    async fn iceberg_check_is_eligible_without_a_catalog_call() {
        let eligibility = check(
            TableFormat::Iceberg,
            "http://127.0.0.1:1",
            "catalog",
            "schema",
            "table",
            None,
        )
        .await
        .expect("the Iceberg check does not call the catalog");
        assert_eq!(eligibility, Eligibility::Eligible);
    }
}

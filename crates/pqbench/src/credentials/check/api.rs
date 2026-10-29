//! `credentials check`: whether a table is readable outside Databricks compute.

use std::fmt;

use crate::v1::{EligibilityReason, ReadEligibility};

/// Errors checking a table's eligibility.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name argument was not a resource name.
    Name(String),
    /// The endpoint request failed.
    Http(crate::http::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) => write!(f, "{message}"),
            Error::Http(_) => write!(f, "the endpoint request failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            Error::Name(_) => None,
        }
    }
}

pub use crate::config::Dialect as TableFormat;

use crate::Workspace;

/// Whether the table `name` — a
/// `catalogs/{catalog}/schemas/{schema}/tables/{table}` resource name — is
/// readable outside Databricks compute, by dialect.
///
/// The check owns the format: Unity reads the `GET /tables/{full_name}` record
/// without asking for credentials; Iceberg REST reads its metadata inline
/// through the catalog, so the table is eligible and no catalog call is made.
/// Unity also drops a `system` catalog ref without a call and reports a view
/// as [`EligibilityReason::NotATable`], so `table::list` can list every
/// securable and the check is the single filter. The caller passes the dialect
/// it runs under and gets the same [`ReadEligibility`] either way. The check
/// separates eligibility from vending: a caller that needs the table itself
/// (`table::get`) can report an ineligible table with its reason instead of
/// running a storage read that cannot succeed.
///
/// # Errors
/// Fails when `name` is not a table name, the endpoint cannot be reached, or it
/// answers with an unexpected status.
pub async fn check(workspace: &Workspace, name: &str) -> Result<ReadEligibility, Error> {
    let (catalog, schema, table) = crate::v1::Table::parse_name(name)
        .ok_or_else(|| Error::Name(format!("{name} is not a catalogs/…/tables/… name")))?;
    match workspace.config().dialect {
        TableFormat::Unity => {
            if catalog == "system" {
                return Ok(ineligible(EligibilityReason::SystemTable));
            }
            super::r#impl::check_unity(workspace, &catalog, &schema, &table).await
        }
        TableFormat::Iceberg => Ok(eligible()),
    }
}

/// A table the catalog reports readable outside Databricks compute.
pub(super) fn eligible() -> ReadEligibility {
    ReadEligibility {
        eligible: true,
        reason: None,
    }
}

/// A table only Databricks compute reads, for `reason`.
pub(super) fn ineligible(reason: EligibilityReason) -> ReadEligibility {
    ReadEligibility {
        eligible: false,
        reason: Some(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Iceberg REST reads its metadata through the catalog, so the check is
    /// eligible without a catalog call — a bogus endpoint is never reached.
    #[tokio::test]
    async fn iceberg_check_is_eligible_without_a_catalog_call() {
        let workspace = Workspace::new(crate::Config {
            endpoint: "http://127.0.0.1:1".into(),
            dialect: crate::Dialect::Iceberg,
            ..crate::Config::default()
        })
        .expect("the workspace builds");
        let eligibility = check(&workspace, "catalogs/catalog/schemas/schema/tables/table")
            .await
            .expect("the Iceberg check does not call the catalog");
        assert_eq!(eligibility, eligible());
    }
}

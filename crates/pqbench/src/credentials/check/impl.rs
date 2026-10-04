//! The Unity table record behind [`super::api::check_unity`].
//!
//! Unity serves `GET /tables/{catalog}.{schema}.{table}`; the record names the
//! securable kind. Databricks default storage is `TABLE_DB_STORAGE`, the kind
//! the vending route refuses, so it has no external read; a view has no data
//! files of its own. Every other kind is attempted, and a missing kind says
//! nothing, so a catalog that reports none (Unity OSS) is eligible.
//!
//! The gate is the kind, not the capability manifest: a managed table in an
//! external location can carry an incomplete capability list, so keying on it
//! would drop a readable table. The URLs and JSON shapes are this module's;
//! the transport is the third-party facade.

use serde::Deserialize;

use super::api::{Eligibility, Error, Reason};
use crate::dialect;

/// The subset of the Unity table record the eligibility check needs.
#[derive(Deserialize)]
struct TableRecord {
    #[serde(default)]
    table_type: Option<String>,
    #[serde(default)]
    storage_location: Option<String>,
    #[serde(default)]
    securable_kind: Option<String>,
}

impl TableRecord {
    /// Whether the catalog reports the table readable outside Databricks
    /// compute. A view (or another location-less securable) has no data files
    /// to read; `TABLE_DB_STORAGE` is Databricks default storage, which the
    /// vending route refuses. A missing kind says nothing, so the table is
    /// attempted.
    fn eligibility(&self) -> Eligibility {
        if self.is_view() {
            return Eligibility::Ineligible(Reason::NotATable);
        }
        if self.is_default_storage() {
            return Eligibility::Ineligible(Reason::NoExternalRead);
        }
        Eligibility::Eligible
    }

    /// Whether the record is a view rather than a table: the type names a
    /// view, or the record carries no storage location.
    fn is_view(&self) -> bool {
        matches!(
            self.table_type.as_deref(),
            Some("VIEW" | "MATERIALIZED_VIEW")
        ) || self.storage_location.as_deref().is_none_or(str::is_empty)
    }

    /// Whether the catalog reports Databricks default (managed) storage — the
    /// kind the vending route refuses to mint for.
    fn is_default_storage(&self) -> bool {
        self.securable_kind.as_deref() == Some("TABLE_DB_STORAGE")
    }
}

/// Whether Unity reports the table readable outside Databricks compute,
/// without asking for credentials.
pub(super) async fn check_unity(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
) -> Result<Eligibility, Error> {
    let name = format!("{catalog}.{schema}.{table}");
    let record: TableRecord = dialect::get_json(
        &format!(
            "{}/tables/{}",
            dialect::api_root(endpoint),
            dialect::encode(&name)
        ),
        token,
    )
    .await
    .map_err(Error::from)?;
    Ok(record.eligibility())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(json: &str) -> TableRecord {
        serde_json::from_str(json).expect("the sample record parses")
    }

    #[test]
    fn a_view_is_not_a_table() {
        let record = record(r#"{"table_type":"VIEW","storage_location":"s3://b/t"}"#);
        assert_eq!(
            record.eligibility(),
            Eligibility::Ineligible(Reason::NotATable)
        );
    }

    #[test]
    fn a_locationless_record_is_not_a_table() {
        let record = record(r#"{"table_type":"MANAGED"}"#);
        assert_eq!(
            record.eligibility(),
            Eligibility::Ineligible(Reason::NotATable)
        );
    }

    #[test]
    fn databricks_default_storage_is_ineligible() {
        let record =
            record(r#"{"storage_location":"s3://b/t","securable_kind":"TABLE_DB_STORAGE"}"#);
        assert_eq!(
            record.eligibility(),
            Eligibility::Ineligible(Reason::NoExternalRead)
        );
    }

    #[test]
    fn a_managed_table_in_an_external_location_is_eligible() {
        // Its capability list may be incomplete; the kind still reads
        // externally, so the check must not drop it.
        let record = record(
            r#"{"storage_location":"s3://b/t","securable_kind":"TABLE_DELTA_ICEBERG_MANAGED",
                "securable_kind_manifest":{"capabilities":["HAS_STORAGE"]}}"#,
        );
        assert_eq!(record.eligibility(), Eligibility::Eligible);
    }

    #[test]
    fn a_customer_table_is_eligible() {
        let record =
            record(r#"{"storage_location":"s3://b/t","securable_kind":"TABLE_DELTA_EXTERNAL"}"#);
        assert_eq!(record.eligibility(), Eligibility::Eligible);
    }
}

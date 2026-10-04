//! The Unity table record behind [`super::api::check_unity`].
//!
//! Unity serves `GET /tables/{catalog}.{schema}.{table}` with
//! `include_manifest_capabilities=true`; the record names the capability
//! manifest. A manifest that lists capabilities without
//! `HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT` or
//! `HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT` (managed default storage, a
//! view) means only Databricks compute reads the table; a missing or empty
//! manifest says nothing, so a catalog that reports none (Unity OSS) is
//! eligible. The URLs and JSON shapes are this module's; the transport is the
//! third-party facade.

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
    securable_kind_manifest: Option<SecurableKindManifest>,
}

impl TableRecord {
    /// Whether the catalog reports the table readable outside Databricks
    /// compute. A view (or another location-less securable) has no data files
    /// to read; a manifest that lists capabilities without direct support
    /// (managed default storage) is not readable either. A missing or empty
    /// manifest says nothing, so the table is attempted.
    fn eligibility(&self) -> Eligibility {
        if self.is_view() {
            return Eligibility::Ineligible(Reason::NotATable);
        }
        match &self.securable_kind_manifest {
            Some(manifest)
                if !manifest.capabilities.is_empty()
                    && !manifest
                        .capabilities
                        .iter()
                        .any(|capability| grants_direct_access(capability)) =>
            {
                Eligibility::Ineligible(Reason::NoExternalRead)
            }
            _ => Eligibility::Eligible,
        }
    }

    /// Whether the record is a view rather than a table: the type names a
    /// view, or the record carries no storage location.
    fn is_view(&self) -> bool {
        matches!(
            self.table_type.as_deref(),
            Some("VIEW" | "MATERIALIZED_VIEW")
        ) || self.storage_location.as_deref().is_none_or(str::is_empty)
    }
}

/// The capability manifest Unity returns for
/// `include_manifest_capabilities=true`.
#[derive(Deserialize)]
struct SecurableKindManifest {
    #[serde(default)]
    capabilities: Vec<String>,
}

/// Whether a manifest capability marks the table readable or writable
/// outside Databricks compute.
fn grants_direct_access(capability: &str) -> bool {
    matches!(
        capability,
        "HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT" | "HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT"
    )
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
            "{}/tables/{}?include_manifest_capabilities=true",
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
    fn a_manifest_without_direct_support_is_ineligible() {
        let record = record(
            r#"{"storage_location":"s3://b/t",
                "securable_kind_manifest":{"capabilities":["HAS_MANAGED_STORAGE"]}}"#,
        );
        assert_eq!(
            record.eligibility(),
            Eligibility::Ineligible(Reason::NoExternalRead)
        );
    }

    #[test]
    fn a_customer_table_with_direct_support_is_eligible() {
        let record = record(
            r#"{"storage_location":"s3://b/t",
                "securable_kind_manifest":{"capabilities":["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]}}"#,
        );
        assert_eq!(record.eligibility(), Eligibility::Eligible);
    }
}

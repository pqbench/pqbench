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

use super::api::{Eligibility, Error};
use crate::dialect;

/// The subset of the Unity table record the eligibility check needs.
#[derive(Deserialize)]
struct TableRecord {
    #[serde(default)]
    securable_kind_manifest: Option<SecurableKindManifest>,
}

impl TableRecord {
    /// Whether the manifest marks the table readable outside Databricks
    /// compute. A missing or empty manifest says nothing, so the table is
    /// attempted; a manifest that lists capabilities without direct support
    /// (managed default storage, a view) is not.
    fn eligibility(&self) -> Eligibility {
        match &self.securable_kind_manifest {
            Some(manifest)
                if !manifest.capabilities.is_empty()
                    && !manifest
                        .capabilities
                        .iter()
                        .any(|capability| grants_direct_access(capability)) =>
            {
                Eligibility::Ineligible
            }
            _ => Eligibility::Eligible,
        }
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

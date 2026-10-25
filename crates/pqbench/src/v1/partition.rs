use serde::{Deserialize, Serialize};

use super::name;
use super::{Credentials, Parent};

/// A natural partition: a half-open commit-time window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Partition {
    /// `…/tables/{table}/partitions/{partition}`.
    pub name: String,
    /// Inclusive start of the window, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<u64>,
    /// Exclusive end of the window, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<u64>,
    /// Commits in the window, in version order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_versions: Vec<u64>,
    /// The commits' `commitInfo` actions, index-aligned with
    /// [`commit_versions`](Self::commit_versions): the operation, its
    /// parameters and metrics, and the writer's identity, as the log stores
    /// them. Empty when no commit in the window has one — Iceberg carries none,
    /// and a Delta writer may leave it out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_info: Vec<serde_json::Value>,
    /// The table's storage root URI. Denormalized onto the partition so the
    /// files stage reads the log without a catalog call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The table's vended credentials, when the caller holds them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
}

impl Parent for Partition {
    const NOUN: &'static str = "partition";

    const SHAPE: &'static str = "catalogs/…/tables/…/partitions/…";

    fn from_name(name: &str) -> Option<Self> {
        let (catalog, schema, table, partition) = name::parse_partition(name)?;
        Some(Self {
            name: name::partition(&catalog, &schema, &table, &partition),
            ..Self::default()
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn name_mut(&mut self) -> &mut String {
        &mut self.name
    }
}

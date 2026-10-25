use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::json::{self, FromJson};
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

impl FromJson for Partition {
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(
            value,
            "partition",
            "catalogs/…/tables/…/partitions/…",
            Self::named,
            |partition| &partition.name,
            |partition| &mut partition.name,
        )
    }
}

impl Partition {
    fn named(name: &str) -> Option<Self> {
        if name::parse_partition(name).is_some() {
            return Some(Self {
                name: name.to_owned(),
                ..Self::default()
            });
        }
        let (catalog, rest) = name.split_once('.')?;
        let (rest, partition) = rest.rsplit_once('.')?;
        let (schema, table) = rest.rsplit_once('.')?;
        if catalog.is_empty() || schema.is_empty() || table.is_empty() || partition.is_empty() {
            return None;
        }
        Some(Self {
            name: name::partition(catalog, schema, table, partition),
            ..Self::default()
        })
    }
}

impl Parent for Partition {
    const NOUN: &'static str = "partition";

    fn name(&self) -> &str {
        &self.name
    }
}

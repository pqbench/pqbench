use serde::{Deserialize, Serialize};

use super::Resource;

/// A partition at one etag and where it is in the pipeline: the master's
/// record of it. A partition has a version per etag the scan found; the
/// previous one is kept, `DELETED`, until it is purged.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct PartitionVersion {
    /// `…/tables/{table}/partitions/{partition}/versions/{etag}`.
    #[resource(
        name = "catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions/{partition}/versions/{version}"
    )]
    pub name: String,
    pub state: State,
    /// When the scan found this version, milliseconds since the Unix epoch.
    pub create_time: u64,
    /// When this version reached `UP_TO_DATE`.
    // aipnaming: allow(aip-140/prepositions)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub up_to_date_time: Option<u64>,
    /// The partition's first scan, carried from version to version.
    pub first_listed_time: u64,
    /// The partition's versions before this one.
    pub etag_change_count: u64,
    /// Failures of the current step on this version.
    pub failure_count: u64,
    /// The last failure's error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The version's files, from its listing.
    pub file_count: u64,
    /// The version's bytes, from its listing.
    pub size_bytes: u64,
    /// The bytes of the files removed from the log but not vacuumed, from its
    /// listing, when the log reports them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed_size_bytes: Option<u64>,
    /// When the version's newest file was written, from its listing; absent
    /// when the log records no per-file time (Iceberg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modification_time: Option<u64>,
    /// The version's monthly storage cost, from its price: the cheapest and
    /// the dearest class or access tier it may be in. Absent until priced,
    /// and for files with no storage class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lowest_cost_per_month: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highest_cost_per_month: Option<f64>,
}

/// Where a partition version is in the pipeline: each step's job takes a
/// version from the state before it, through its own working state, to the
/// state after it.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum State {
    /// The scan no longer finds this version; its results wait for purge.
    Deleted,
    /// Found, not processed yet.
    #[default]
    Pending,
    Pricing,
    Priced,
    Bytemassing,
    Bytemassed,
    Linting,
    /// Every step done for this version.
    UpToDate,
}

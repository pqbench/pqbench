use serde::{Deserialize, Serialize};

use super::Resource;

/// A partition at one etag and where it is in the pipeline: the driver's
/// record of it, and the engine's whole state. A partition has one version in
/// the map per etag the newest walk found; a version an etag change supersedes
/// leaves the map and its results are deleted.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct PartitionVersion {
    /// `…/tables/{table}/partitions/{partition}/versions/{etag}`.
    #[resource(
        name = "catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions/{partition}/versions/{version}"
    )]
    pub name: String,
    pub state: State,
    /// When a walk first found this version, milliseconds since the Unix
    /// epoch.
    pub create_time: u64,
    /// When a walk last found this version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_time: Option<u64>,
    /// The partition's first scan, carried from version to version.
    pub first_listed_time: u64,
    /// The partition's versions before this one.
    pub etag_change_count: u64,
    /// Failures of the current transition on this version.
    pub failure_count: u64,
    /// The last failure's error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Earliest time (epoch ms) to retry after repeated failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_time: Option<u64>,
    /// The version's files, from its listing.
    pub file_count: u64,
    /// The version's bytes, from its listing.
    pub size_bytes: u64,
    /// The bytes of the files removed from the log but not vacuumed, from its
    /// listing, when the log reports them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed_size_bytes: Option<u64>,
    /// When the version's newest file was written, from its listing; absent
    /// when the log records no per-file time.
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

/// Where a partition version is in the pipeline: each transition takes a
/// version from the state before it, through its running state, to the state
/// after it.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum State {
    /// Found, not processed yet.
    #[default]
    Pending,
    Pricing,
    Priced,
    /// The version's sample file is being copied into its directory.
    Downloading,
    /// The sample file is on disk, for the commands that read its data.
    Downloaded,
    Bytemassing,
    Bytemassed,
    Linting,
    /// The potential suggestions are on disk, waiting for the verification
    /// that measures them against their control.
    Listed,
    Verifying,
    /// Every transition the version needs is done: a version with no live file,
    /// or whose files need a restore, is done after its price.
    UpToDate,
    /// The version failed too many times: it is quarantined, so no transition
    /// runs on it again. Only a walk that finds a new etag — a partition that
    /// was written — makes a version that is tried afresh.
    Failed,
}

impl State {
    /// Every state: what `status` counts by.
    pub const ALL: [State; 12] = [
        State::Pending,
        State::Pricing,
        State::Priced,
        State::Downloading,
        State::Downloaded,
        State::Bytemassing,
        State::Bytemassed,
        State::Linting,
        State::Listed,
        State::Verifying,
        State::UpToDate,
        State::Failed,
    ];

    /// The state's name as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            State::Pending => "PENDING",
            State::Pricing => "PRICING",
            State::Priced => "PRICED",
            State::Downloading => "DOWNLOADING",
            State::Downloaded => "DOWNLOADED",
            State::Bytemassing => "BYTEMASSING",
            State::Bytemassed => "BYTEMASSED",
            State::Linting => "LINTING",
            State::Listed => "LISTED",
            State::Verifying => "VERIFYING",
            State::UpToDate => "UP_TO_DATE",
            State::Failed => "FAILED",
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

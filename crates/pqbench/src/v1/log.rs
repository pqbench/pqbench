use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One commit of a table's log.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Commit {
    /// `…/tables/{table}/commits/{commit}`.
    pub name: String,
    /// The log version.
    pub version: u64,
    /// Commit time, milliseconds since the Unix epoch, when the log records
    /// one. Delta reads `commitInfo.timestamp`; Iceberg the snapshot time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_time: Option<i64>,
    /// The commit's `commitInfo` action as the log stores it, when it has one:
    /// the operation, its parameters and metrics, and the writer's identity.
    /// Delta reads it from the first line of the commit file; Iceberg has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_info: Option<serde_json::Value>,
    /// Actions from the commit file, in file order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<LogAction>,
}

/// Statistics copied from a log's `add` action.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FileStats {
    /// `numRecords` from the log.
    pub record_count: u64,
    /// On-disk file size divided by [`Self::record_count`]. Absent at zero rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_row: Option<f64>,
    /// Per-column first value, JSON as stored (nested structs stay objects).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub first_values: BTreeMap<String, serde_json::Value>,
    /// Per-column last value, JSON as stored.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub last_values: BTreeMap<String, serde_json::Value>,
    /// Per-column `nullCount`. Nested struct columns are flattened to dotted
    /// keys (`struct.inner.x`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub null_count: BTreeMap<String, u64>,
    /// The log's `tightBounds`, when it sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tight_bounds: Option<bool>,
}

/// One action from a commit file, in file order.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LogAction {
    /// Action kind as stored (`add`, `remove`, `metaData`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Data path when the action names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The size the log declares, when the action names one (an `add`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

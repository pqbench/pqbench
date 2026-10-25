use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

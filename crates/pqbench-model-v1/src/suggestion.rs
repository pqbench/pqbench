use serde::{Deserialize, Serialize};

use super::Resource;

/// One engine's setting that applies a suggestion's rewrite.
///
/// A rewrite is written by whichever engine compacts the table, so the same
/// change has a setting per engine. `engine` names the library or table format
/// the setting belongs to (`spark`, `pyarrow`, `parquet-rs`, `delta`,
/// `iceberg`), and `setting` spells it that engine's way.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Remedy {
    /// The engine the setting belongs to.
    pub engine: String,
    /// The setting, as that engine spells it.
    pub setting: String,
}

impl Remedy {
    /// One engine's setting.
    pub fn new(engine: impl Into<String>, setting: impl Into<String>) -> Self {
        Self {
            engine: engine.into(),
            setting: setting.into(),
        }
    }
}

/// One storage suggestion for a column: a rewrite `list suggestions` finds
/// plausible and `get suggestion` measures against a control.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct Suggestion {
    /// `{blob}/columns/{column}/suggestions/{rule}`.
    #[resource(name = "blobs/{service}/{locator}/columns/{column}/suggestions/{rule}")]
    pub name: String,
    /// The stable rule id.
    pub rule: String,
    /// The column's dotted leaf path.
    pub column: String,
    /// The table whose file was measured, when measured through a table file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// The partition a partition-level rule measured,
    /// `…/tables/{table}/partitions/{partition}`; absent on a column rule. The
    /// record's name is `{partition}/suggestions/{rule}` and its `column` is
    /// empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    /// The probability the table file was kept with, when a sample stage chose
    /// it. Absent means unsampled, weight one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<f64>,
    /// The size in bytes of the file the column was measured in, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The application that wrote the file, as the footer's `created_by` says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<String>,
    /// One-line explanation: the detection evidence, then the control's verdict.
    pub message: String,
    /// The change that recovers the bytes, in the terms of the file's writer.
    pub remedy: String,
    /// The same change as one setting per engine, when the rule has one for
    /// that engine. Ordered by engine; `remedy` is the detected writer's.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remedies: Vec<Remedy>,
    /// Whether the controlled rewrite confirms a saving. Absent on the
    /// potential `list suggestions` emits; `true` when `get suggestion`
    /// measured a win; `false` when it measured none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified: Option<bool>,
    /// Estimated bytes saved, conservative across compression levels. Present
    /// only when `verified` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub savings_bytes: Option<u64>,
    /// Rows the detection sampled, so `get suggestion` measures the same
    /// values. Absent means the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_row_count: Option<u64>,
    /// Whole row groups the detection sampled: the one group whose row count is
    /// closest to the file's average. `get suggestion` re-reads it, so a
    /// verification measures the values `list` detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_row_group_count: Option<u64>,
}

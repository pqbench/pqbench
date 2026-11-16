use serde::{Deserialize, Serialize};

use super::Resource;

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
    /// Whether the controlled rewrite confirms a saving. Absent on the
    /// potential `list suggestions` emits; `true` when `get suggestion`
    /// measured a win; `false` when it measured none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified: Option<bool>,
    /// Estimated bytes saved, conservative across compression levels. Present
    /// only when `verified` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub savings_bytes: Option<u64>,
    /// Leading rows the detection sampled, so `get suggestion` measures the
    /// same values. Absent means the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_row_count: Option<u64>,
}

use serde::{Deserialize, Serialize};

use super::{Bytemass, Resource};

/// A file's copy of a declared column: the physical unit the analyses read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct Column {
    /// `…/blobs/{service}/{locator}/columns/{column}`.
    #[resource(name = "blobs/{service}/{locator}/columns/{column}")]
    pub name: String,
    /// The declared column this file column is a copy of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_column: Option<String>,
    /// The table whose file was measured, when measured through a table file.
    /// The blob is storage-rooted, so a bare file names no table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// The column's position in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    /// The physical type of the leaf column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_data_type: Option<String>,
    /// Row groups the column spans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
    /// The column's on-disk byte mass, when the footer was measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mass: Option<Bytemass>,
    /// The probability the table file naming this column was kept with, when a
    /// `sample files` stage chose it. Absent means unsampled, weight one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<f64>,
}

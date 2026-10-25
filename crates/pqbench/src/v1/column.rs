use serde::{Deserialize, Serialize};

/// A file's copy of a declared column: the physical unit the analyses read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Column {
    /// `…/blobs/{service}/{locator}/columns/{column}`.
    pub name: String,
    /// The declared column this file column is a copy of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_column: Option<String>,
    /// The column's position in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    /// The physical type of the leaf column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_data_type: Option<String>,
    /// Row groups the column spans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
}

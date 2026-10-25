use serde::{Deserialize, Serialize};

/// A storage blob: the bytes a data file lives in — a local file or an S3
/// object, named by its service.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Blob {
    /// `blobs/{service}/{locator}`.
    pub name: String,
    /// The object's size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The storage class or tier (`STANDARD`, `STANDARD_IA`, …), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Row groups in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
    /// Leaf columns in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_count: Option<u64>,
}

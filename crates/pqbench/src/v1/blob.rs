use serde::{Deserialize, Serialize};

use super::Resource;

/// A storage blob: the bytes a data file lives in — a local file or an S3
/// object, named by its service.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct Blob {
    /// `blobs/{service}/{locator}`.
    #[resource(name = "blobs/{service}/{locator}")]
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
    /// The probability the table file naming this blob was kept with, when a
    /// `sample files` stage chose it. Absent means unsampled, weight one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::Blob;

    #[test]
    fn a_blob_has_no_dotted_shorthand() {
        assert_eq!(Blob::parse_name("a.b.c"), None);
        assert_eq!(
            Blob::parse_name("blobs/file/x"),
            Some(("file".to_string(), "x".to_string()))
        );
    }
}

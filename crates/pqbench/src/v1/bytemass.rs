use serde::{Deserialize, Serialize};

/// A column's byte mass (the footer's `ColumnMetaData`), folded over the
/// column's chunks. A value on [`Column`](super::Column), not a resource.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Bytemass {
    /// On-disk (compressed) bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_bytes: Option<u64>,
    /// Encoded bytes before compression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncompressed_bytes: Option<u64>,
    /// The compression codec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Encodings listed on the column chunks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encodings: Vec<String>,
    /// A dictionary page is present.
    #[serde(default)]
    pub dictionary: bool,
    /// Values in the column (including nulls).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_count: Option<u64>,
    /// Footer `null_count`, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    /// Footer `distinct_count`, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_count: Option<u64>,
    /// Footer first value, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_value: Option<String>,
    /// Footer last value, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_value: Option<String>,
    /// Compressed bytes divided by [`Self::value_count`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_value: Option<f64>,
    /// Uncompressed divided by compressed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// [`Self::null_count`] divided by [`Self::value_count`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_fraction: Option<f64>,
}

use serde::{Deserialize, Serialize};

/// A codec sweep of a column.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionReport {
    /// `…/columns/{column}/compression/{codec}`.
    pub name: String,
    /// The codec swept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Compressed bytes for this codec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_bytes: Option<u64>,
    /// Uncompressed divided by compressed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio: Option<f64>,
    /// Compress throughput, in megabytes per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compress_megabytes_per_second: Option<f64>,
    /// Decompress throughput, in megabytes per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decompress_megabytes_per_second: Option<f64>,
}

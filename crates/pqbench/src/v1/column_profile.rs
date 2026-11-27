use serde::{Deserialize, Serialize};

use super::Resource;
use crate::profile::ValueCount;

/// A decoded-sample profile of a column.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct ColumnProfile {
    /// `…/columns/{column}/profile`.
    #[resource(name = "blobs/{service}/{locator}/columns/{column}/profile")]
    pub name: String,
    /// Column path in the file.
    pub column: String,
    /// Physical kind of the column's values.
    pub physical_kind: String,
    /// Values sampled (including nulls).
    // aipnaming: allow(aip-141/count-suffix)
    pub num_values: u64,
    /// Nulls among the sampled values.
    pub null_count: u64,
    /// `null_count / num_values`.
    pub null_fraction: f64,
    /// Distinct non-null values among the sampled values.
    pub ndv: u64,
    /// `ndv / num_values`.
    pub ndv_ratio: f64,
    /// Shannon entropy, in bits.
    pub entropy: f64,
    /// The most frequent values.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_values: Vec<ValueCount>,
    /// Minimum non-null value.
    // aipnaming: allow(aip-145/ranges)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_value: Option<String>,
    /// Maximum non-null value.
    // aipnaming: allow(aip-145/ranges)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_value: Option<String>,
    /// Mean string length in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_mean: Option<f64>,
    /// Median string length in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_p50: Option<u64>,
    /// 90th-percentile string length in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_p90: Option<u64>,
    /// Maximum string length in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_max: Option<u64>,
    /// Fraction of adjacent value pairs that are equal.
    pub adjacent_equal_fraction: f64,
    /// Mean run length across all sampled values.
    pub run_length_mean: f64,
    /// Longest run of identical adjacent values.
    pub run_length_max: u64,
    /// Monotonicity classification of non-null values.
    pub monotonic: String,
}

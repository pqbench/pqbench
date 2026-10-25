use serde::{Deserialize, Serialize};

/// A decoded-sample profile of a column.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ColumnProfile {
    /// `…/columns/{column}/profile`.
    pub name: String,
    /// Nulls among the sampled rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    /// Distinct values among the sampled rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_count: Option<u64>,
    /// Shannon entropy, in bits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entropy_bits: Option<f64>,
    /// The most frequent values.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_values: Vec<String>,
}

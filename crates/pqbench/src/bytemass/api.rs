//! The `bytemass` command: one typed request in, one table out.
//!
//! The table is a `Vec<MassRow>`: one row per (file, column chunk), each row
//! self-contained. The CLI streams those rows; `pqbench viz` collects them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::table::FileStats;
use crate::third_party::parquet::api::Error;

use super::collection;

/// A measured file's identity and the table-log statistics proxied with it.
///
/// One `pqbench.bytemass-file` record: the object identity (path, URI, size,
/// storage class) plus the Delta add action's partition values and statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FileStat {
    /// Table id from the stream, or empty for a bare parquet input.
    #[serde(default)]
    pub id: String,
    /// Table-relative path, when known.
    #[serde(default)]
    pub path: String,
    /// URI or filesystem path that was measured.
    pub file: String,
    /// Log or object size in bytes.
    pub size: u64,
    /// Storage class or tier (`STANDARD`, `STANDARD_IA`, …), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Hive partition values from the Delta add action.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_values: BTreeMap<String, Option<String>>,
    /// Delta `add.stats` proxied with the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<FileStats>,
}

impl FileStat {
    /// Assemble a file record from its identity; stats and storage class are
    /// filled in when known.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        path: impl Into<String>,
        file: impl Into<String>,
        size: u64,
    ) -> Self {
        Self {
            id: id.into(),
            path: path.into(),
            file: file.into(),
            size,
            storage_class: None,
            partition_values: BTreeMap::new(),
            stats: None,
        }
    }
}

/// Arguments for the `bytemass` command.
#[derive(Debug, Clone, Default)]
pub struct BytemassRequest {
    /// Parquet paths or glob masks; quote masks to prevent shell expansion.
    pub inputs: Vec<String>,
    /// Storage options (`AWS_*` names) for remote inputs.
    pub env: BTreeMap<String, String>,
    /// Load ColumnIndex/OffsetIndex (one extra range). Off by default.
    pub indexes: bool,
}

/// One column chunk's measured byte mass: a row of the `bytemass` table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MassRow {
    /// Input path or URI as given.
    pub uri: String,
    /// On-disk file size in bytes.
    pub size_bytes: u64,
    /// Number of rows in the file (denominator for the per-row measure).
    pub row_count: u64,
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub compressed_bytes: u64,
    /// Encoded bytes before compression for this column chunk.
    pub uncompressed_bytes: u64,
    /// Compression codec recorded in the column chunk metadata.
    pub codec: String,
    /// Storage class or tier (`STANDARD`, `STANDARD_IA`, …), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Encodings listed on the column chunk.
    #[serde(default)]
    pub encodings: Vec<String>,
    /// Values in this chunk (including nulls).
    // aipnaming: allow(aip-141/count-suffix)
    #[serde(default)]
    pub num_values: u64,
    /// Dictionary page offset is present.
    #[serde(default)]
    pub dictionary: bool,
    /// Footer `null_count`, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    /// Footer `distinct_count`, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_count: Option<u64>,
    /// Footer min, when statistics exist.
    // aipnaming: allow(aip-145/ranges)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_value: Option<String>,
    /// Footer max, when statistics exist.
    // aipnaming: allow(aip-145/ranges)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_value: Option<String>,
    /// Physical type of the leaf column.
    #[serde(default)]
    pub physical_type: String,
    /// Row-group index (0-based).
    #[serde(default)]
    pub row_group: u32,
    /// Rows in this row group.
    #[serde(default)]
    pub row_group_rows: u64,
    /// Compressed bytes / row-group rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_bytes_per_row: Option<f64>,
    /// Uncompressed bytes / row-group rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncompressed_bytes_per_row: Option<f64>,
    /// Uncompressed / compressed, when compressed > 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// `null_count` / `num_values`, when both are known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_fraction: Option<f64>,
    /// Data pages in the OffsetIndex, when `--indexes` loaded one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_count: Option<u64>,
    /// Sum of OffsetIndex `compressed_page_size`, when loaded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_compressed_bytes: Option<u64>,
}

/// Measure the per-column byte masses of Parquet files.
///
/// Inputs are local paths or storage URIs, each possibly a glob mask for local
/// files. Only file footers are read. The result is one [`MassRow`] per column
/// chunk, in input order; call `render_text`, `render_json`, or `aggregate`
/// on it, or pipe the stream to `viz`.
///
/// # Errors
/// Fails when there are no inputs, a mask matches no files, an input cannot be
/// read, or a byte total overflows.
pub async fn bytemass(request: &BytemassRequest) -> Result<Vec<MassRow>, Error> {
    if request.inputs.is_empty() {
        return Err(Error("no inputs".into()));
    }
    collection::measure_inputs(&request.inputs, &request.env, request.indexes).await
}

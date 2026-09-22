//! The `bytemass` command: one typed request in, one table out.
//!
//! The table is a `Vec<MassRow>`: one row per (file, column chunk), each row
//! self-contained. The CLI owns the rendering decision: it awaits [`bytemass`]
//! and calls `render_text`, `render_json`, or `render_html` on the rows.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::parquet_helpers::Error;

use super::collection;

/// Arguments for the `bytemass` command.
#[derive(Debug, Clone, Default)]
pub struct BytemassRequest {
    /// Parquet paths or glob masks; quote masks to prevent shell expansion.
    pub inputs: Vec<String>,
    /// Storage options (`AWS_*` names) for remote inputs.
    pub env: BTreeMap<String, String>,
}

/// One column chunk's measured byte mass: a row of the `bytemass` table.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct MassRow {
    /// Input path or URI as given.
    pub file: String,
    /// On-disk file size in bytes.
    pub size: u64,
    /// Number of rows in the file (denominator for the per-row measure).
    pub num_rows: u64,
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub compressed_bytes: u64,
    /// Encoded bytes before compression for this column chunk.
    pub uncompressed_bytes: u64,
    /// Compression codec recorded in the column chunk metadata.
    pub codec: String,
}

/// Measure the per-column byte masses of Parquet files.
///
/// Inputs are local paths or storage URIs, each possibly a glob mask for local
/// files. Only file footers are read. The result is one [`MassRow`] per column
/// chunk, in input order; call `render_text`, `render_json`, `render_html`, or
/// `aggregate` on it to present or fold it.
///
/// # Errors
/// Fails when there are no inputs, a mask matches no files, an input cannot be
/// read, or a byte total overflows.
pub async fn bytemass(request: &BytemassRequest) -> Result<Vec<MassRow>, Error> {
    if request.inputs.is_empty() {
        return Err(Error("no inputs".into()));
    }
    collection::measure_inputs(&request.inputs, &request.env).await
}

//! Iceberg snapshot resolution: the metadata JSON and Avro manifests.
//!
//! [`load`] reads a table's header and files; [`stream_commits`] streams its
//! commit times. Neither reads Parquet footers; pipe the record to `bytemass`
//! to measure. All Iceberg and Avro interaction lives in the private `impl`
//! module. Run inside a Tokio runtime.
//!
//! The `iceberg` feature compiles the loader; without it both entry points fail
//! at runtime and name the feature.

use crate::table::{LoadRequest, TableInfo};
use crate::Lake;

/// Load the current or requested Iceberg snapshot into a table record.
///
/// `request.uri` is a table root (`metadata/version-hint.text` or
/// `metadata/*.metadata.json`) or a metadata JSON path/URI.
///
/// # Errors
/// Fails when the `iceberg` feature is off, for invalid metadata, missing
/// snapshots, non-Parquet data files, or data paths outside the table location.
pub async fn load(lake: &Lake, request: &LoadRequest) -> Result<TableInfo, crate::table::Error> {
    super::r#impl::load(lake, request).await
}

/// Stream the table's commits, newest first.
///
/// # Errors
/// As [`load`]; an error raised after the first commit is a stream item.
pub async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::CommitStream<'static>, crate::table::Error> {
    super::r#impl::stream_commits(lake, request).await
}

//! Iceberg snapshot resolution: the metadata JSON and Avro manifests.
//!
//! [`load`] reads a table's header and files; [`stream_commits`] streams its
//! commit times and [`stream_files`] the files its snapshots name. None of them
//! reads Parquet footers; pipe the record to `bytemass` to measure. All Iceberg
//! and Avro interaction lives in the private `impl` module. Run inside a Tokio
//! runtime.
//!
//! The `iceberg` feature compiles the loader; without it every entry point fails
//! at runtime and names the feature.

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

/// Stream the files the selected snapshot's manifests name.
///
/// An Iceberg snapshot's files ride on its manifest list, not on the log, so the
/// files are the resolved snapshot's, and a request naming other snapshots than
/// the selected one yields none.
///
/// # Errors
/// As [`load`]; an error raised after the first file is a stream item.
pub async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::FileStream<'static>, crate::table::Error> {
    super::r#impl::stream_files(lake, request).await
}

/// Stream the selected snapshot's active files, without collecting them.
///
/// Each manifest is read and consumed on its own, so the walk holds one
/// manifest's entries rather than the table's files.
///
/// # Errors
/// As [`load`]; an error raised after the first file is a stream item.
pub async fn stream_active_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::FileStream<'static>, crate::table::Error> {
    super::r#impl::stream_active_files(lake, request).await
}

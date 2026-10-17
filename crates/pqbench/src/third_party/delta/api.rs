//! Delta snapshot resolution: the transaction log and the active files.
//!
//! [`load`] reads a table's header and files; [`stream_commits`] streams its
//! commit times. Neither reads Parquet footers; pipe the record to `bytemass`
//! to measure. All delta-rs interaction lives in the private `impl` module.
//! Drive the load from a current-thread runtime: delta-rs then selects its own
//! executor instead of borrowing the caller's.
//!
//! The `delta` feature compiles the loader; without it both entry points fail
//! at runtime and name the feature.

use crate::table::{LoadRequest, TableInfo};
use crate::Lake;

/// Load the transaction log and the active files of a Delta table.
///
/// # Errors
/// Fails when the `delta` feature is off, the log cannot be read, the snapshot
/// is invalid, or a data path leaves the table root.
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

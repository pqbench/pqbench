//! Delta snapshot resolution: the transaction log and the active files.
//!
//! [`load`] reads a table's header and files; [`stream_commits`] streams its
//! commit times and [`stream_files`] the files its commits added. None of them
//! reads Parquet footers; pipe the record to `bytemass` to measure. All delta-rs
//! interaction lives in the private `impl` module. Drive the load from a
//! current-thread runtime: delta-rs then selects its own executor instead of
//! borrowing the caller's.
//!
//! The `delta` feature compiles the loader; without it every entry point fails
//! at runtime and names the feature.

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

/// Stream the files the requested commits added, in log order.
///
/// The read is the commits' own `add` actions, so no snapshot and no
/// active-file replay is resolved. A version the log no longer holds
/// contributes no files.
///
/// # Errors
/// As [`load`]; an error raised after the first file is a stream item.
pub async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::FileStream<'static>, crate::table::Error> {
    super::r#impl::stream_files(lake, request).await
}

/// Visit the requested snapshot's active files one at a time, without
/// collecting them, and return the version they are active at.
///
/// The walk is the kernel scan, a record batch at a time, over a lazy snapshot:
/// no `EagerSnapshot` is built, so the active set is never resident.
///
/// # Errors
/// As [`load`]; an error raised after the first file aborts the walk there.
pub async fn visit_active_files(
    lake: &Lake,
    request: &LoadRequest,
    visit: &mut (dyn FnMut(crate::v1::TableFile) -> bool + Send),
) -> Result<u64, crate::table::Error> {
    super::r#impl::visit_active_files(lake, request, visit).await
}

//! Delta snapshot resolution: the transaction log and the active files.
//!
//! [`load`] is the only entry point. It does not read Parquet footers; pipe the
//! document to `bytemass` to measure. All delta-rs interaction lives in the
//! private `impl` module. Drive the load from a current-thread runtime:
//! delta-rs then selects its own executor instead of borrowing the caller's.
//!
//! The `delta` feature compiles the loader; without it [`load`] fails at
//! runtime and names the feature.

use crate::table::{LoadEvent, LoadRequest, TableInfo};

/// Load the transaction log and the active files of a Delta table.
///
/// # Errors
/// Fails when the `delta` feature is off, the log cannot be read, the snapshot
/// is invalid, or a data path leaves the table root.
pub async fn load(request: &LoadRequest) -> Result<TableInfo, crate::table::Error> {
    super::r#impl::load(request).await
}

/// Load a Delta snapshot, visiting the header then each active file.
///
/// # Errors
/// Same as [`load`].
pub async fn visit_load(
    request: &LoadRequest,
    mut visit: impl FnMut(LoadEvent<'_>) -> Result<(), crate::table::Error>,
) -> Result<TableInfo, crate::table::Error> {
    super::r#impl::visit_load(request, &mut visit).await
}

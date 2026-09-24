//! Delta snapshot resolution: the transaction log and the active files.
//!
//! [`load`] is the only entry point. It does not read Parquet footers; pipe the
//! document to `bytemass` to measure. All delta-rs interaction lives in the
//! private `delta_helpers` module. Drive the load from a current-thread runtime:
//! delta-rs then selects its own executor instead of borrowing the caller's.
//!
//! The `delta` feature compiles the loader and its private helper module.
//! Without it [`load`] fails and names the feature.

use super::{LoadRequest, TableInfo};

/// Load the transaction log and the active files of a Delta table.
///
/// # Errors
/// Fails when the `delta` feature is off, the log cannot be read, the snapshot
/// is invalid, or a data path leaves the table root.
pub async fn load(request: &LoadRequest) -> Result<TableInfo, super::Error> {
    #[cfg(feature = "delta")]
    {
        super::delta_helpers::load(request)
            .await
            .map_err(|error| super::Error(error.to_string()))
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = request;
        Err(super::Error(
            "delta tables require the `delta` feature (`delta-s3` for S3)".into(),
        ))
    }
}

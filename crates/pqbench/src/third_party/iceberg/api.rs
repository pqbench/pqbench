//! Iceberg snapshot resolution: the metadata JSON and Avro manifests.
//!
//! [`load`] is the only entry point. It does not read Parquet footers; pipe the
//! document to `bytemass` to measure. All Iceberg and Avro interaction lives in
//! the private `impl` module. Run inside a Tokio runtime.
//!
//! The `iceberg` feature compiles the loader; without it [`load`] fails at
//! runtime and names the feature.

use crate::table::{LoadEvent, LoadRequest, TableInfo};

/// Load the current or requested Iceberg snapshot into a table document.
///
/// `request.uri` is a table root (`metadata/version-hint.text` or
/// `metadata/*.metadata.json`) or a metadata JSON path/URI.
///
/// # Errors
/// Fails when the `iceberg` feature is off, for invalid metadata, missing
/// snapshots, non-Parquet data files, or data paths outside the table location.
pub async fn load(request: &LoadRequest) -> Result<TableInfo, crate::table::Error> {
    super::r#impl::load(request).await
}

/// Load the snapshot, visiting the header then each active file.
///
/// # Errors
/// Same as [`load`].
pub async fn visit_load(
    request: &LoadRequest,
    mut visit: impl FnMut(LoadEvent<'_>) -> Result<(), crate::table::Error>,
) -> Result<TableInfo, crate::table::Error> {
    super::r#impl::visit_load(request, &mut visit).await
}

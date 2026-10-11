//! Iceberg snapshot resolution: the metadata JSON and Avro manifests.
//!
//! [`load`] is the only entry point. It does not read Parquet footers; pipe the
//! record to `bytemass` to measure. All Iceberg and Avro interaction lives in
//! the private `impl` module. Run inside a Tokio runtime.
//!
//! The `iceberg` feature compiles the loader; without it [`load`] fails at
//! runtime and names the feature.

use crate::table::{LoadRequest, TableInfo};
use crate::Context;

/// Load the current or requested Iceberg snapshot into a table record.
///
/// `request.uri` is a table root (`metadata/version-hint.text` or
/// `metadata/*.metadata.json`) or a metadata JSON path/URI.
///
/// # Errors
/// Fails when the `iceberg` feature is off, for invalid metadata, missing
/// snapshots, non-Parquet data files, or data paths outside the table location.
pub async fn load(
    context: &Context,
    request: &LoadRequest,
) -> Result<TableInfo, crate::table::Error> {
    super::r#impl::load(context, request).await
}

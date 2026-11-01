//! `bytemass`: a blob's columns, each with its byte mass.
//!
//! [`measure_file`] returns the blob's storage facts and one [`v1::Column`] per
//! leaf column; [`list_columns`] is the columns-only form. The caller scopes
//! the lake with the storage lease.

use crate::v1;
use crate::Lake;

use super::Error;

/// One measured blob: its storage facts and one record per leaf column.
#[derive(Debug, Clone)]
pub struct MeasuredFile {
    /// The blob's name, size, storage class, and file-level counts.
    pub blob: v1::Blob,
    /// One record per leaf column, in file order.
    pub columns: Vec<v1::Column>,
}

/// Measure the blob `name`: its storage facts and its columns.
///
/// The HEAD that reports the blob's size and storage class is reused by the
/// footer read, so a remote blob costs one HEAD rather than two.
///
/// # Errors
/// Fails when `name` is not a blob name, the object cannot be read, or its
/// footer is not valid Parquet.
pub async fn measure_file(lake: &Lake, name: &str) -> Result<MeasuredFile, Error> {
    super::collection::measure_file(lake, name).await
}

/// List the columns of the blob `name`, each with its byte mass.
///
/// Only the file's footer is read, never its data pages. The result is one
/// [`v1::Column`] per leaf column, in file order; `mass` is set on each.
///
/// # Errors
/// Fails when `name` is not a blob name, the object cannot be read, or its
/// footer is not valid Parquet.
pub async fn list_columns(lake: &Lake, name: &str) -> Result<Vec<v1::Column>, Error> {
    super::collection::list_columns(lake, name).await
}

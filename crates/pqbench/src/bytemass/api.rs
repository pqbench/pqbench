//! `bytemass`: a blob's columns, each with its byte mass.
//!
//! [`measure_file`] returns the blob's storage facts and one [`v1::Column`] per
//! leaf column; [`list_columns`] is the columns-only form. The caller scopes
//! the lake with the storage lease.
//!
//! The fold that turns a footer into per-column masses is local work and lives
//! in [`pqbench_parquet::bytemass`]; this module adds the reads — a HEAD for
//! the storage class, a bounded range GET for the footer.

use crate::v1;
use crate::Lake;

use super::{Error, MeasuredFile};

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

/// Measure the blob `name` from a local copy of its bytes at `path`: the
/// records name the blob, not the copy. The blob's storage class still comes
/// from one HEAD.
///
/// # Errors
/// Fails when `name` is not a blob name, the object cannot be read, or the
/// local file's footer is not valid Parquet.
pub async fn measure_local(
    lake: &Lake,
    name: &str,
    path: &std::path::Path,
) -> Result<MeasuredFile, Error> {
    super::collection::measure_local(lake, name, path).await
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

//! `bytemass`: a blob's columns, each with its byte mass.
//!
//! [`list_columns`] is the only public function. It takes the blob name and
//! returns one [`v1::Column`] per leaf column; the caller scopes the lake with
//! the storage lease.

use crate::v1;
use crate::Lake;

use super::Error;

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

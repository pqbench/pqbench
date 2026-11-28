//! Read a blob's footer — over the object store, or from a local copy of it —
//! and hand the folder the footer masses.

use std::path::Path;

use crate::third_party::parquet::api::read_file_masses;
use crate::v1;
use crate::Lake;

use super::remote;
use super::{Error, MeasuredFile};

/// List the blob's columns, each with its byte mass.
pub(super) async fn list_columns(lake: &Lake, name: &str) -> Result<Vec<v1::Column>, Error> {
    Ok(measure_file(lake, name).await?.columns)
}

/// Measure the blob's storage facts and columns from one footer read.
pub(super) async fn measure_file(lake: &Lake, name: &str) -> Result<MeasuredFile, Error> {
    let uri = v1::name::blob_uri(name)
        .ok_or_else(|| Error::Name(format!("{name} is not a blobs/… name")))?;
    if uri.contains("://") {
        let (stat, mass) = remote::read_remote(lake, &uri).await?;
        return measured(name, stat.size_bytes, stat.storage_class, mass);
    }
    let mass = read_file_masses(Path::new(&uri))?;
    let size_bytes = mass.size_bytes;
    measured(name, size_bytes, None, mass)
}

/// Measure the blob from a local copy: the footer comes from `path`, the
/// storage facts from the object's HEAD.
pub(super) async fn measure_local(
    lake: &Lake,
    name: &str,
    path: &Path,
) -> Result<MeasuredFile, Error> {
    let uri = v1::name::blob_uri(name)
        .ok_or_else(|| Error::Name(format!("{name} is not a blobs/… name")))?;
    let mass = read_file_masses(path)?;
    let size_bytes = mass.size_bytes;
    if uri.contains("://") {
        let stat = crate::third_party::object_store::open(lake, &uri)
            .map_err(storage_error)?
            .stat()
            .await
            .map_err(storage_error)?;
        return measured(name, stat.size_bytes, stat.storage_class, mass);
    }
    measured(name, size_bytes, None, mass)
}

/// The fold, with a storage error mapped into this crate's error.
fn measured(
    name: &str,
    size_bytes: u64,
    storage_class: Option<String>,
    mass: crate::third_party::parquet::api::FileMass,
) -> Result<MeasuredFile, Error> {
    pqbench_parquet::bytemass::from_file_mass(name, size_bytes, storage_class, mass)
}

fn storage_error(error: crate::third_party::object_store::Error) -> Error {
    Error::Storage(Box::new(error))
}

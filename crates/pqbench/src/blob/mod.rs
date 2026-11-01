//! `blob`: one file's storage facts.
//!
//! [`get`] reads the object's size and storage class without reading its body.

use std::fmt;

use crate::v1;
use crate::Lake;

/// Errors reading a blob.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name argument was not a blob name.
    Name(String),
    /// The local file could not be read.
    Io {
        /// The path involved.
        path: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
    /// The object store failed.
    Storage(crate::third_party::object_store::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) => write!(f, "{message}"),
            Error::Io { path, .. } => write!(f, "cannot read {path}"),
            Error::Storage(_) => write!(f, "object storage failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io { source, .. } => Some(source),
            Error::Storage(error) => Some(error),
            Error::Name(_) => None,
        }
    }
}

/// Read the storage facts of the blob `name`.
///
/// # Errors
/// Fails when `name` is not a blob name or the object cannot be read.
pub async fn get(lake: &Lake, name: &str) -> Result<v1::Blob, Error> {
    let uri = v1::name::blob_uri(name)
        .ok_or_else(|| Error::Name(format!("{name} is not a blobs/… name")))?;
    if !uri.contains("://") {
        let metadata = std::fs::metadata(&uri).map_err(|source| Error::Io {
            path: uri.clone(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(Error::Name(format!("{name} is not a file")));
        }
        return Ok(v1::Blob {
            name: name.to_string(),
            size_bytes: Some(metadata.len()),
            ..v1::Blob::default()
        });
    }
    let reader = crate::third_party::object_store::open(lake, &uri).map_err(Error::Storage)?;
    let stat = reader.stat().await.map_err(Error::Storage)?;
    Ok(v1::Blob {
        name: name.to_string(),
        size_bytes: Some(stat.size_bytes),
        storage_class: stat.storage_class,
        ..v1::Blob::default()
    })
}

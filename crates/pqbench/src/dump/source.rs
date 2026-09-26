//! Assemble a Parquet [`ObjectSource`] for one dump file.
//!
//! This is the only dump module that composes two wrappers: a local path is
//! passed through in place, while a remote URI is fetched as its footer and the
//! kept row-group byte ranges through [`crate::third_party::object_store`]. The
//! Parquet wrapper sees only plain bytes.

use std::path::{Path, PathBuf};

use crate::third_party::object_store::{self, is_remote};
use crate::third_party::parquet::{self, Error, ObjectSource};

use super::DumpFile;

/// Explain a remote byte fetch with the object it came from.
fn storage_error(uri: &str) -> impl Fn(object_store::Error) -> Error + '_ {
    move |error| Error(format!("object {uri}: {error}"))
}

/// A local path, or the footer and row-group ranges of a remote object.
///
/// # Errors
/// Fails for a remote object that cannot be read, is too small to be Parquet,
/// or returns a truncated footer.
pub(super) async fn open(
    file: &DumpFile,
    row_groups: Option<usize>,
) -> Result<ObjectSource, Error> {
    if !is_remote(&file.uri) {
        return Ok(ObjectSource::Path(local_path(&file.uri)));
    }
    let options: Vec<(String, String)> = file
        .env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let reader = object_store::open(&file.uri, &options).map_err(storage_error(&file.uri))?;
    let stat = reader.stat().await.map_err(storage_error(&file.uri))?;
    let size = stat.size_bytes;
    if size < 8 {
        return Err(Error(format!(
            "object {} is too small to be a Parquet file: {size} bytes",
            file.uri
        )));
    }
    let identity = stat.identity.as_deref();
    let trailer = reader
        .read_range(size - 8..size, identity)
        .await
        .map_err(storage_error(&file.uri))?;
    let span = parquet::footer_range(size, &trailer)
        .map_err(|error| Error(format!("object {}: {error}", file.uri)))?;
    let footer = reader
        .read_range(span.clone(), identity)
        .await
        .map_err(storage_error(&file.uri))?;
    if footer.len() as u64 != span.end - span.start {
        return Err(Error(format!(
            "object {} returned truncated Parquet metadata",
            file.uri
        )));
    }
    let ranges = parquet::data_ranges(&footer, row_groups)
        .map_err(|error| Error(format!("object {}: {error}", file.uri)))?;
    let mut parts = vec![(span.start, footer)];
    for range in ranges {
        let bytes = reader
            .read_range(range.clone(), identity)
            .await
            .map_err(storage_error(&file.uri))?;
        parts.push((range.start, bytes));
    }
    Ok(ObjectSource::Partial { size, parts })
}

fn local_path(uri: &str) -> PathBuf {
    if let Some(path) = uri.strip_prefix("file://") {
        return Path::new(path).to_path_buf();
    }
    Path::new(uri).to_path_buf()
}

//! Footer-only Parquet object reads.
//!
//! Storage access goes through [`crate::third_party::object_store::api::ObjectReader`], so no
//! third-party storage type appears here and there are no feature flags: a URI
//! whose backend is not compiled in fails at runtime through the reader.

use crate::third_party::object_store;
use crate::third_party::object_store::api::ObjectStat;
use crate::third_party::parquet::api::{read_footer_masses, FileMass};
use crate::Lake;

use super::Error;

const PARQUET_FOOTER_SIZE: u64 = 8;

/// Read an individual Parquet object's size and byte masses from a URI.
///
/// Only the trailer and serialized footer metadata are fetched, never data
/// pages. `options` override process-environment backend defaults.
///
/// # Errors
/// Fails for unsupported URIs, unreadable objects, or invalid Parquet footers.
pub(super) async fn read_remote(
    lake: &Lake,
    uri: &str,
    options: impl IntoIterator<Item = (String, String)>,
) -> Result<(ObjectStat, FileMass), Error> {
    let options: Vec<(String, String)> = options.into_iter().collect();
    let reader = object_store::open(lake, uri, &options).map_err(storage_error)?;
    let stat = reader.stat().await.map_err(storage_error)?;
    let size = stat.size_bytes;
    if size < PARQUET_FOOTER_SIZE {
        return Err(Error::Invalid(format!(
            "object {uri} is too small to be a Parquet file: {size} bytes"
        )));
    }
    let identity = stat.identity.as_deref();

    let trailer = reader
        .read_range(size - PARQUET_FOOTER_SIZE..size, identity)
        .await
        .map_err(storage_error)?;
    let trailer: [u8; 8] = trailer.as_slice().try_into().map_err(|_| {
        Error::Invalid(format!("object {uri} returned a truncated Parquet trailer"))
    })?;
    let [a, b, c, d, ..] = trailer;
    if &trailer[4..] != b"PAR1" {
        return Err(Error::Invalid(format!(
            "object {uri} has no Parquet footer magic"
        )));
    }

    let metadata_size = u64::from(u32::from_le_bytes([a, b, c, d]));
    let metadata_start = size
        .checked_sub(PARQUET_FOOTER_SIZE + metadata_size)
        .filter(|start| *start >= 4)
        .ok_or_else(|| {
            Error::Invalid(format!("object {uri} has an invalid Parquet footer size"))
        })?;
    let metadata_len = usize::try_from(metadata_size)
        .map_err(|_| Error::Invalid(format!("object {uri} metadata size exceeds usize")))?;
    let mut footer = reader
        .read_range(metadata_start..size - PARQUET_FOOTER_SIZE, identity)
        .await
        .map_err(storage_error)?;
    if footer.len() != metadata_len {
        return Err(Error::Invalid(format!(
            "object {uri} returned truncated Parquet metadata"
        )));
    }
    footer.extend_from_slice(&trailer);
    Ok((stat, read_footer_masses(&footer, size)?))
}

fn storage_error(error: object_store::Error) -> Error {
    Error::Storage(error)
}

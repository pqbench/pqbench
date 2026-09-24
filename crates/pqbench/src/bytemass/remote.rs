//! Footer-only Parquet object reads.
//!
//! Storage access goes through [`crate::object_store::ObjectReader`], so no
//! third-party storage type appears here and there are no feature flags: a URI
//! whose backend is not compiled in fails at runtime through the reader.

use crate::object_store;
use crate::parquet_helpers::{read_footer_masses, Error, FileMass};

const PARQUET_FOOTER_SIZE: u64 = 8;

/// Read an individual Parquet object's size and byte masses from a URI.
///
/// Only the trailer and serialized footer metadata are fetched, never data
/// pages or indexes. `options` override process-environment backend defaults.
///
/// # Errors
/// Fails for unsupported URIs, unreadable objects, or invalid Parquet footers.
pub(super) async fn read_remote_with_options(
    uri: &str,
    options: impl IntoIterator<Item = (String, String)>,
) -> Result<(u64, FileMass), Error> {
    let options: Vec<(String, String)> = options.into_iter().collect();
    let reader = object_store::open(uri, &options).map_err(storage_error)?;
    let stat = reader.stat().await.map_err(storage_error)?;
    let size = stat.size_bytes;
    if size < PARQUET_FOOTER_SIZE {
        return Err(Error(format!(
            "object {uri} is too small to be a Parquet file: {size} bytes"
        )));
    }
    let identity = stat.identity.as_deref();

    let trailer = reader
        .read_range(size - PARQUET_FOOTER_SIZE..size, identity)
        .await
        .map_err(storage_error)?;
    let trailer: [u8; 8] = trailer
        .as_slice()
        .try_into()
        .map_err(|_| Error(format!("object {uri} returned a truncated Parquet trailer")))?;
    let [a, b, c, d, ..] = trailer;
    if &trailer[4..] != b"PAR1" {
        return Err(Error(format!("object {uri} has no Parquet footer magic")));
    }

    let metadata_size = u64::from(u32::from_le_bytes([a, b, c, d]));
    let metadata_start = size
        .checked_sub(PARQUET_FOOTER_SIZE + metadata_size)
        .filter(|start| *start >= 4)
        .ok_or_else(|| Error(format!("object {uri} has an invalid Parquet footer size")))?;
    let metadata_len = usize::try_from(metadata_size)
        .map_err(|_| Error(format!("object {uri} metadata size exceeds usize")))?;
    let mut footer = reader
        .read_range(metadata_start..size - PARQUET_FOOTER_SIZE, identity)
        .await
        .map_err(storage_error)?;
    if footer.len() != metadata_len {
        return Err(Error(format!(
            "object {uri} returned truncated Parquet metadata"
        )));
    }
    footer.extend_from_slice(&trailer);
    Ok((size, read_footer_masses(&footer)?))
}

fn storage_error(error: object_store::Error) -> Error {
    Error(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::read_remote_with_options;
    use crate::parquet_helpers::{default_metadata_parser, MetadataParser};

    #[tokio::test]
    async fn reads_a_parquet_uri_through_the_public_api() {
        let path = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        ));
        let expected = default_metadata_parser().read_masses(path).unwrap();
        let uri = url::Url::from_file_path(path).unwrap();
        let (size, actual) = read_remote_with_options(uri.as_str(), []).await.unwrap();
        assert_eq!(size, std::fs::metadata(path).unwrap().len());
        assert_eq!(actual.num_rows, expected.num_rows);
        assert_eq!(actual.columns.len(), expected.columns.len());
    }
}

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

/// How much of the object tail one GET asks for. It must cover the trailer and
/// the serialized footer for the read to stay one request; a footer larger than
/// this falls back to a second range GET.
const FOOTER_WINDOW: u64 = 64 * 1024;

/// Read an individual Parquet object's byte masses from a URI.
///
/// One HEAD reports the size, ETag, and storage class. One suffix GET then
/// carries the trailer and, when the footer fits the window, the serialized
/// metadata too; a larger footer costs a second range GET.
///
/// # Errors
/// Fails for unsupported URIs, unreadable objects, or invalid Parquet footers.
pub(super) async fn read_remote(lake: &Lake, uri: &str) -> Result<(ObjectStat, FileMass), Error> {
    let reader = object_store::open(lake, uri).map_err(storage_error)?;
    let stat = reader.stat().await.map_err(storage_error)?;
    let size = stat.size_bytes;
    if size < PARQUET_FOOTER_SIZE {
        return Err(Error::Invalid(format!(
            "object {uri} is too small to be a Parquet file: {size} bytes"
        )));
    }
    let identity = stat.identity.as_deref();

    // One suffix GET carries the trailer and, for a footer within the window,
    // the serialized metadata too.
    let tail = reader
        .read_range(size.saturating_sub(FOOTER_WINDOW)..size, identity)
        .await
        .map_err(storage_error)?;
    let (layout, footer_at) = footer_layout(size, &tail, uri)?;
    let footer = match footer_at {
        // The window already holds the whole footer, trailer included.
        Some(index) => tail[index..].to_vec(),
        None => {
            let mut footer = reader
                .read_range(layout.metadata_start..size - PARQUET_FOOTER_SIZE, identity)
                .await
                .map_err(storage_error)?;
            footer.extend_from_slice(&layout.trailer_bytes);
            footer
        }
    };
    if footer.len().checked_sub(PARQUET_FOOTER_SIZE as usize) != Some(layout.metadata_len) {
        return Err(Error::Invalid(format!(
            "object {uri} returned truncated Parquet metadata"
        )));
    }
    Ok((stat, read_footer_masses(&footer, size)?))
}

/// Where a suffix body's serialized footer starts and how long it is.
struct FooterLayout {
    metadata_start: u64,
    metadata_len: usize,
    trailer_bytes: [u8; PARQUET_FOOTER_SIZE as usize],
}

/// Parse the trailer at the end of `tail` and locate the footer in it.
///
/// The returned index is where the footer begins inside `tail`; `None` means
/// the footer starts before the body and needs a second range GET.
fn footer_layout(
    size: u64,
    tail: &[u8],
    uri: &str,
) -> Result<(FooterLayout, Option<usize>), Error> {
    if tail.len() < PARQUET_FOOTER_SIZE as usize {
        return Err(Error::Invalid(format!(
            "object {uri} returned a truncated Parquet trailer"
        )));
    }
    if tail.len() as u64 > size {
        return Err(Error::Invalid(format!(
            "object {uri} returned more bytes than the object"
        )));
    }
    let tail_start = size - tail.len() as u64;
    let trailer: [u8; PARQUET_FOOTER_SIZE as usize] = tail
        [tail.len() - PARQUET_FOOTER_SIZE as usize..]
        .try_into()
        .map_err(|_| {
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
    let footer_at = (metadata_start >= tail_start).then(|| (metadata_start - tail_start) as usize);
    Ok((
        FooterLayout {
            metadata_start,
            metadata_len,
            trailer_bytes: trailer,
        },
        footer_at,
    ))
}

fn storage_error(error: object_store::Error) -> Error {
    Error::Storage(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tail(metadata_len: usize, body_len: usize) -> Vec<u8> {
        let mut body = vec![0xAA; body_len];
        let mut trailer = [0u8; 8];
        trailer[..4].copy_from_slice(&(metadata_len as u32).to_le_bytes());
        trailer[4..].copy_from_slice(b"PAR1");
        body[body_len - 8..].copy_from_slice(&trailer);
        body
    }

    #[test]
    fn a_footer_inside_the_window_is_sliced_from_the_suffix() {
        let (layout, at) = footer_layout(1_000, &tail(100, 200), "object").unwrap();
        assert_eq!(layout.metadata_start, 892);
        assert_eq!(layout.metadata_len, 100);
        assert_eq!(at, Some(92));
    }

    #[test]
    fn a_whole_small_object_is_its_own_footer() {
        let (layout, at) = footer_layout(100, &tail(20, 100), "object").unwrap();
        assert_eq!(layout.metadata_start, 72);
        assert_eq!(at, Some(72));
    }

    #[test]
    fn a_footer_that_exactly_fills_the_window_starts_at_zero() {
        let (layout, at) = footer_layout(1_000, &tail(100, 108), "object").unwrap();
        assert_eq!(layout.metadata_start, 892);
        assert_eq!(at, Some(0));
    }

    #[test]
    fn a_footer_larger_than_the_window_needs_a_second_read() {
        let (layout, at) = footer_layout(1_000, &tail(100, 50), "object").unwrap();
        assert_eq!(layout.metadata_start, 892);
        assert_eq!(at, None);
    }

    #[test]
    fn a_body_shorter_than_the_trailer_is_rejected() {
        assert!(footer_layout(1_000, &[0; 7], "object").is_err());
    }

    #[test]
    fn a_body_longer_than_the_object_is_rejected() {
        assert!(footer_layout(100, &tail(10, 101), "object").is_err());
    }

    #[test]
    fn missing_footer_magic_is_rejected() {
        let mut body = tail(100, 200);
        let last = body.len() - 1;
        body[last] = b'X';
        assert!(footer_layout(1_000, &body, "object").is_err());
    }

    #[test]
    fn a_footer_size_past_the_start_of_the_object_is_rejected() {
        assert!(footer_layout(1_000, &tail(994, 100), "object").is_err());
    }
}

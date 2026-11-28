//! The Zstandard implementation, backed by the `zstd` crate.
//!
//! This module is private; callers use [`super::api`]. It is the only file that
//! names the `zstd` crate.

use super::api::Error;

/// Compress `src` at `level`.
pub(crate) fn compress(src: &[u8], level: u8) -> Result<Vec<u8>, Error> {
    zstd::bulk::compress(src, i32::from(level)).map_err(Error::Compress)
}

/// Decompress `src` into a buffer of `out_cap` bytes.
pub(crate) fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    zstd::bulk::decompress(src, out_cap).map_err(Error::Decompress)
}

/// Compress `src` into one Zstandard frame at `level`: the standalone stream
/// `zstdcat` reads.
pub(crate) fn compress_frame(src: &[u8], level: u8) -> Result<Vec<u8>, Error> {
    zstd::stream::encode_all(src, i32::from(level)).map_err(Error::Compress)
}

/// Decompress one Zstandard frame, whatever length it decodes to.
pub(crate) fn decompress_frame(src: &[u8]) -> Result<Vec<u8>, Error> {
    zstd::stream::decode_all(src).map_err(Error::Decompress)
}

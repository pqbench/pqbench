//! The LZ4 implementation, backed by the `lz4` crate.
//!
//! This module is private; callers use [`super::api`]. It is the only file that
//! names the `lz4` crate.

use super::api::Error;

/// Compress `src` as an LZ4 block.
pub(crate) fn compress(src: &[u8]) -> Result<Vec<u8>, Error> {
    lz4::block::compress(src, None, false).map_err(|e| Error(e.to_string()))
}

/// Decompress `src` into a buffer of `out_cap` bytes.
pub(crate) fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    lz4::block::decompress(src, Some(i32::try_from(out_cap).unwrap_or(i32::MAX)))
        .map_err(|e| Error(e.to_string()))
}

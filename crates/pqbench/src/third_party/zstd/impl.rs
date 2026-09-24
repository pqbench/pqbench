//! The Zstandard implementation, backed by the `zstd` crate.
//!
//! This module is private; callers use [`super::api`]. It is the only file that
//! names the `zstd` crate.

use super::api::Error;

/// Compress `src` at `level`.
pub(crate) fn compress(src: &[u8], level: u8) -> Result<Vec<u8>, Error> {
    zstd::bulk::compress(src, i32::from(level)).map_err(|e| Error(e.to_string()))
}

/// Decompress `src` into a buffer of `out_cap` bytes.
pub(crate) fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    zstd::bulk::decompress(src, out_cap).map_err(|e| Error(e.to_string()))
}

//! The gzip implementation, backed by the `flate2` crate.
//!
//! This module is private; callers use [`super::api`]. It is the only file that
//! names the `flate2` crate.

use std::io::{Read, Write};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use super::api::Error;

/// gzip-compress `src` at `level`.
pub(crate) fn compress(src: &[u8], level: u8) -> Result<Vec<u8>, Error> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::new(u32::from(level)));
    enc.write_all(src).map_err(|e| Error(e.to_string()))?;
    enc.finish().map_err(|e| Error(e.to_string()))
}

/// Decompress a gzip stream `src` into at most `out_cap` bytes.
pub(crate) fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(out_cap);
    GzDecoder::new(src)
        .take(out_cap as u64)
        .read_to_end(&mut out)
        .map_err(|e| Error(e.to_string()))?;
    Ok(out)
}

//! The LZ4 implementation, backed by the `lz4` crate.
//!
//! This module is private; callers use [`super::api`]. It is the only file that
//! names the `lz4` crate.

use std::io::{Read, Write};

use super::api::Error;

/// Compress `src` as an LZ4 block.
pub(crate) fn compress(src: &[u8]) -> Result<Vec<u8>, Error> {
    lz4::block::compress(src, None, false).map_err(Error::Compress)
}

/// Decompress `src` into a buffer of `out_cap` bytes.
pub(crate) fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    lz4::block::decompress(src, Some(i32::try_from(out_cap).unwrap_or(i32::MAX)))
        .map_err(Error::Decompress)
}

/// Compress `src` as one LZ4 frame: the standalone stream a frame decoder
/// (`lz4cat`) reads, with its own header and end mark.
pub(crate) fn compress_frame(src: &[u8]) -> Result<Vec<u8>, Error> {
    let mut encoder = lz4::EncoderBuilder::new()
        .build(Vec::new())
        .map_err(Error::Compress)?;
    encoder.write_all(src).map_err(Error::Compress)?;
    let (frame, ended) = encoder.finish();
    ended.map_err(Error::Compress)?;
    Ok(frame)
}

/// Decompress one LZ4 frame, whatever length it decodes to.
pub(crate) fn decompress_frame(src: &[u8]) -> Result<Vec<u8>, Error> {
    let mut decoder = lz4::Decoder::new(src).map_err(Error::Decompress)?;
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).map_err(Error::Decompress)?;
    Ok(out)
}

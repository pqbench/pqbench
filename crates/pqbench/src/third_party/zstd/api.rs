//! Zstandard buffer compression.
//!
//! Both functions take and return plain byte buffers; no `zstd` type appears
//! here, so the backing crate can change without touching callers.

use std::fmt;

/// Errors from the Zstandard layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The underlying zstd compression call failed.
    Compress(std::io::Error),
    /// The underlying zstd decompression call failed.
    Decompress(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Compress(_) => write!(f, "zstd compression failed"),
            Error::Decompress(_) => write!(f, "zstd decompression failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Compress(error) | Error::Decompress(error) => Some(error),
        }
    }
}

/// Compress `src` at `level` into a new buffer.
///
/// # Errors
/// Returns [`Error`] if the underlying zstd call fails.
pub fn compress(src: &[u8], level: u8) -> Result<Vec<u8>, Error> {
    super::r#impl::compress(src, level)
}

/// Decompress `src` into a new buffer with room for `out_cap` bytes.
///
/// # Errors
/// Returns [`Error`] if `src` is not valid Zstandard or `out_cap` is too small.
pub fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    super::r#impl::decompress(src, out_cap)
}

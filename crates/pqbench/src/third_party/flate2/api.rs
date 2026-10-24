//! gzip buffer compression (RFC 1952).
//!
//! Both functions take and return plain byte buffers; no `flate2` type appears
//! here, so the backing crate can change without touching callers.

use std::fmt;

/// Errors from the gzip layer.
#[derive(Debug)]
pub enum Error {
    /// The underlying gzip compression call failed.
    Compress(std::io::Error),
    /// The underlying gzip decompression call failed.
    Decompress(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Compress(_) => write!(f, "gzip compression failed"),
            Error::Decompress(_) => write!(f, "gzip decompression failed"),
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

/// gzip-compress `src` at `level` into a new buffer.
///
/// # Errors
/// Returns [`Error`] if the underlying gzip call fails.
pub fn compress(src: &[u8], level: u8) -> Result<Vec<u8>, Error> {
    super::r#impl::compress(src, level)
}

/// Decompress a gzip stream `src` into at most `out_cap` bytes.
///
/// # Errors
/// Returns [`Error`] if `src` is not a valid gzip stream.
pub fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    super::r#impl::decompress(src, out_cap)
}

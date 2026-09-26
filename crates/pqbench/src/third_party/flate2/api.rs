//! gzip buffer compression (RFC 1952).
//!
//! Both functions take and return plain byte buffers; no `flate2` type appears
//! here, so the backing crate can change without touching callers.

/// Errors from the gzip layer.
#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gzip: {}", self.0)
    }
}

impl std::error::Error for Error {}

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

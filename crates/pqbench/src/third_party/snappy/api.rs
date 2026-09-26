//! Snappy buffer compression.
//!
//! Both functions take and return plain byte buffers; no `snappy_src` type
//! appears here, so the backing crate can change without touching callers.

/// Errors from the Snappy layer.
#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "snappy: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// Compress `src` into a new buffer.
///
/// # Errors
/// Returns [`Error`] if the underlying Snappy call fails.
pub fn compress(src: &[u8]) -> Result<Vec<u8>, Error> {
    super::r#impl::compress(src)
}

/// Decompress `src` into a new buffer with room for `out_cap` bytes.
///
/// # Errors
/// Returns [`Error`] if `src` is not valid Snappy or `out_cap` is too small.
pub fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    super::r#impl::decompress(src, out_cap)
}

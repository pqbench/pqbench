//! The JSON surface pqbench uses: parse one value, write one value.
//!
//! Feature selection lives in the private `impl` module, so this file compiles
//! under every feature set: with no SIMD parser compiled in, the surface is
//! unchanged and the backend is `serde_json`.
//!
//! A caller that has to hold a whole document as a tree keeps using
//! `serde_json::Value`; these entry points parse straight into the type the
//! caller wants, which is what avoids a tree between the bytes and the record.

use std::fmt;
use std::io::Write;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// A JSON failure: what the parser objected to, and where.
///
/// The message is the backend's own, so it reads as the parser would put it;
/// [`Error::line`] and [`Error::column`] are the position, which a caller that
/// reports its own diagnostics needs without parsing the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    message: String,
    line: usize,
    column: usize,
}

impl Error {
    /// A failure at `line` and `column`, both 1-based and counted in bytes.
    pub(crate) fn new(message: String, line: usize, column: usize) -> Self {
        Self {
            message,
            line,
            column,
        }
    }

    /// The 1-based line the parser stopped at.
    pub fn line(&self) -> usize {
        self.line
    }

    /// The 1-based column the parser stopped at, counted in bytes.
    pub fn column(&self) -> usize {
        self.column
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

/// Parse one JSON value from `bytes`, in place.
///
/// This is the fast path: the SIMD backend unescapes strings where they lie
/// instead of copying them, so the buffer's contents are unspecified
/// afterwards. A caller that must keep its bytes uses [`from_slice`].
///
/// # Errors
/// Fails when `bytes` is not exactly one JSON value of `T`'s shape.
pub fn from_slice_mut<T: DeserializeOwned>(bytes: &mut [u8]) -> Result<T, Error> {
    super::r#impl::from_slice_mut(bytes)
}

/// Parse one JSON value from bytes the caller keeps.
///
/// # Errors
/// As [`from_slice_mut`].
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    super::r#impl::from_slice(bytes)
}

/// Parse one JSON value from text.
///
/// # Errors
/// As [`from_slice_mut`].
pub fn from_str<T: DeserializeOwned>(text: &str) -> Result<T, Error> {
    super::r#impl::from_str(text)
}

/// Write one JSON value, compact and with no trailing newline.
///
/// # Errors
/// Fails when the value cannot be serialized or the writer fails.
pub fn to_writer<W: Write, T: Serialize + ?Sized>(writer: W, value: &T) -> Result<(), Error> {
    super::r#impl::to_writer(writer, value)
}

/// One JSON value as compact bytes, with no trailing newline.
///
/// # Errors
/// Fails when the value cannot be serialized.
pub fn to_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
    super::r#impl::to_vec(value)
}

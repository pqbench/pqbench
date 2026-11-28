//! The JSON parser dispatch: the one place a feature picks a backend.
//!
//! Writing is always `serde_json`: the SIMD parser has no writer, so the NDJSON
//! writers would gain nothing from dispatching. Parsing is `serde_json` without
//! the `json-simd` feature and `simd-json` with it.

use std::io::Write;

use serde::Serialize;

use super::api::Error;

#[cfg(not(feature = "json-simd"))]
mod serde_json;
#[cfg(feature = "json-simd")]
mod simd_json;

#[cfg(not(feature = "json-simd"))]
pub(crate) use self::serde_json::{from_slice, from_slice_mut, from_str};
#[cfg(feature = "json-simd")]
pub(crate) use self::simd_json::{from_slice, from_slice_mut, from_str};

pub(crate) fn to_writer<W: Write, T: Serialize + ?Sized>(
    writer: W,
    value: &T,
) -> Result<(), Error> {
    ::serde_json::to_writer(writer, value).map_err(failed)
}

pub(crate) fn to_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
    ::serde_json::to_vec(value).map_err(failed)
}

/// A value that could not be written.
fn failed(error: ::serde_json::Error) -> Error {
    Error::new(error.to_string(), error.line(), error.column())
}

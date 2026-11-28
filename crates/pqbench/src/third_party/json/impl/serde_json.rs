//! The `serde_json` parser: the backend without the `json-simd` feature.
//!
//! It parses from a shared slice, so `from_slice_mut` is `from_slice`: nothing
//! is rewritten in place.

use serde::de::DeserializeOwned;

use crate::third_party::json::api::Error;

pub(crate) fn from_slice_mut<T: DeserializeOwned>(bytes: &mut [u8]) -> Result<T, Error> {
    from_slice(bytes)
}

pub(crate) fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    ::serde_json::from_slice(bytes).map_err(failed)
}

pub(crate) fn from_str<T: DeserializeOwned>(text: &str) -> Result<T, Error> {
    ::serde_json::from_str(text).map_err(failed)
}

/// `serde_json` reports the line and column in its message and through its
/// accessors, so both are taken as they are.
fn failed(error: ::serde_json::Error) -> Error {
    Error::new(error.to_string(), error.line(), error.column())
}

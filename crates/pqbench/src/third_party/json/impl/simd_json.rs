//! The `simd-json` parser: the backend under the `json-simd` feature.
//!
//! `simd-json` dispatches at runtime to AVX2 + PCLMULQDQ, SSE4.2, or a portable
//! stage 2, so the binary is not tuned to the machine that built it. It parses
//! in place — a string with escapes is unescaped where it lies — which is why
//! its whole surface takes `&mut [u8]`; the entry points that take a shared
//! slice copy it first.
//!
//! It has no stream deserializer and no writer: a multi-value document is split
//! by `serde_json`'s, and writing stays `serde_json`'s.

use serde::de::DeserializeOwned;

use crate::third_party::json::api::Error;

pub(crate) fn from_slice_mut<T: DeserializeOwned>(bytes: &mut [u8]) -> Result<T, Error> {
    match ::simd_json::serde::from_slice::<T>(bytes) {
        Ok(value) => Ok(value),
        Err(error) => Err(failed(bytes, &error)),
    }
}

pub(crate) fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    let mut buffer = bytes.to_vec();
    from_slice_mut(&mut buffer)
}

pub(crate) fn from_str<T: DeserializeOwned>(text: &str) -> Result<T, Error> {
    from_slice(text.as_bytes())
}

/// A parse failure at the parser's byte index, mapped to a line and a column.
///
/// The index is an offset into the buffer as the parse left it, so a column
/// after a rewritten string can drift; a diagnostic does not need more.
fn failed(input: &[u8], error: &::simd_json::Error) -> Error {
    let index = error.index().min(input.len());
    let before = &input[..index];
    let line = before.iter().filter(|byte| **byte == b'\n').count() + 1;
    let column = index
        - before
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |at| at + 1)
        + 1;
    let what = format!("{:?}", error.error());
    Error::new(
        format!("{what} at line {line} column {column}"),
        line,
        column,
    )
}

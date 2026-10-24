//! The Avro record reader, isolated behind the `iceberg` feature.
//!
//! [`read_avro`] is the runtime dispatch: without the feature it returns an
//! error naming it, so callers never see a `#[cfg]`.

use serde::Deserialize;

use super::api::Error;

#[cfg(feature = "iceberg")]
pub(crate) fn read_avro<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    kind: &str,
) -> Result<Vec<T>, Error> {
    use std::io::Cursor;

    use apache_avro::{from_value, Reader};

    let reader = Reader::new(Cursor::new(bytes)).map_err(|error| Error::Read {
        kind: kind.to_string(),
        source: Box::new(error),
    })?;
    reader
        .map(|value| {
            let value = value.map_err(|error| Error::Read {
                kind: kind.to_string(),
                source: Box::new(error),
            })?;
            from_value::<T>(&value).map_err(|error| Error::Read {
                kind: kind.to_string(),
                source: Box::new(error),
            })
        })
        .collect()
}

#[cfg(not(feature = "iceberg"))]
pub(crate) fn read_avro<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    kind: &str,
) -> Result<Vec<T>, Error> {
    let _ = (bytes, kind);
    Err(Error::Unsupported)
}

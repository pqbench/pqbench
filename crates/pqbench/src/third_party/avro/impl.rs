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

    let reader =
        Reader::new(Cursor::new(bytes)).map_err(|e| Error(format!("cannot read {kind}: {e}")))?;
    reader
        .map(|value| {
            let value = value.map_err(|e| Error(format!("cannot read {kind}: {e}")))?;
            from_value::<T>(&value).map_err(|e| Error(format!("cannot parse {kind}: {e}")))
        })
        .collect()
}

#[cfg(not(feature = "iceberg"))]
pub(crate) fn read_avro<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    kind: &str,
) -> Result<Vec<T>, Error> {
    let _ = (bytes, kind);
    Err(Error::from(
        "reading Avro requires the `iceberg` feature".to_string(),
    ))
}

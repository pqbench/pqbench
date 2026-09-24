//! Read Avro object-container records into Rust values.
//!
//! [`read_avro`] is the only entry point: it decodes every record in an Avro
//! object container byte buffer into `T`. Iceberg manifests use it to turn
//! manifest entries into typed structs.
//!
//! All `apache-avro` interaction lives in the private `impl` module. The
//! `iceberg` feature compiles it; without it [`read_avro`] fails and names the
//! feature. There are no feature flags outside this folder.

use serde::Deserialize;

/// Errors reading Avro records.
#[derive(Debug)]
pub struct Error(pub(crate) String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "avro: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Decode every record in an Avro object container into `T`.
///
/// `kind` names what is being read (`"manifest entry"`, `"manifest list"`) so
/// errors identify the caller's context.
///
/// # Errors
/// Fails when the `iceberg` feature is off, `bytes` is not a valid Avro object
/// container, or a record does not deserialize into `T`.
pub fn read_avro<T: for<'de> Deserialize<'de>>(bytes: &[u8], kind: &str) -> Result<Vec<T>, Error> {
    super::r#impl::read_avro(bytes, kind)
}

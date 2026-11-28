//! Read Avro object-container records into [`Value`] trees.
//!
//! [`read_avro_values`] decodes every record into [`Value`], a minimal
//! format-agnostic tree, so a caller reads Iceberg manifest lists and manifest
//! entries without a Rust struct per Avro schema version or partition spec.
//!
//! All `apache-avro` interaction lives in the private `impl` module. The
//! `iceberg` feature compiles it; without it the entry point fails and names
//! the feature. There are no feature flags outside this folder.

use std::fmt;

/// Errors reading Avro records.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The `iceberg` feature is off.
    Unsupported,
    /// The Avro container could not be decoded.
    Read {
        /// What was being read (`"manifest"`, `"manifest list"`).
        kind: String,
        /// The underlying Avro failure.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Unsupported => write!(f, "reading Avro requires the `iceberg` feature"),
            Error::Read { kind, .. } => write!(f, "cannot read {kind}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Read { source, .. } => Some(source.as_ref()),
            Error::Unsupported => None,
        }
    }
}

/// One Avro value, reduced to the primitives an Iceberg manifest carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `null` (or an unused container type).
    Null,
    /// A 32-bit or 64-bit integer.
    Int(i64),
    /// Text, or a scalar formatted by its Avro logical type (`date`,
    /// `timestamp-*`, `uuid`, `boolean`, floating-point).
    Text(String),
    /// Raw `bytes`, `fixed`, or unscaled big-endian `decimal` bytes.
    Bytes(Vec<u8>),
    /// A record's `(name, value)` fields in schema order.
    Record(Vec<(String, Value)>),
}

impl Value {
    /// Look up a named field when this value is a [`Value::Record`].
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Self> {
        self.fields()?
            .iter()
            .find_map(|(field, value)| (field == name).then_some(value))
    }

    /// Read an integer field by name.
    #[must_use]
    pub fn int(&self, name: &str) -> Option<i64> {
        match self.get(name)? {
            Self::Int(number) => Some(*number),
            _ => None,
        }
    }

    /// Read a text field by name.
    #[must_use]
    pub fn str(&self, name: &str) -> Option<&str> {
        match self.get(name)? {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }

    /// The record's fields, when this value is a [`Value::Record`].
    #[must_use]
    pub fn fields(&self) -> Option<&[(String, Self)]> {
        match self {
            Self::Record(fields) => Some(fields),
            _ => None,
        }
    }
}

/// Decode every record in an Avro object container into [`Value`].
///
/// # Errors
/// Fails when the `iceberg` feature is off or `bytes` is not a valid Avro
/// object container.
pub fn read_avro_values(bytes: &[u8], kind: &str) -> Result<Vec<Value>, Error> {
    super::r#impl::read_avro_values(bytes, kind)
}

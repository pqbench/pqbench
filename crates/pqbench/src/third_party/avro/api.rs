//! Read Avro object-container records into [`Value`] trees.
//!
//! [`read_avro_values`] decodes every record into [`Value`], a format-agnostic
//! tree, so a caller interprets both Iceberg manifest lists (whose v1 and v2/v3
//! schemas differ in whether integer fields are nullable unions) and manifest
//! `partition` records (whose fields and types follow the table's partition
//! spec).
//!
//! All `apache-avro` interaction lives in the private `impl` module. The
//! `iceberg` feature compiles it; without it the entry point fails and names
//! the feature. There are no feature flags outside this folder.

use std::collections::BTreeMap;
use std::fmt;

/// Errors reading Avro records.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The `iceberg` feature is off.
    Unsupported,
    /// The Avro container could not be decoded.
    Read {
        /// What was being read (`"manifest entry"`, `"manifest list"`).
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

/// One Avro value, decoded without a caller-supplied schema.
///
/// The tree keeps the value's own type, so a reader decides what a value means
/// from the record it sits in: a `Date` is the same 32-bit count as an `Int`,
/// but only the schema knows it counts days.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A `null`.
    Null,
    /// A `boolean`.
    Bool(bool),
    /// An `int`.
    Int(i32),
    /// A `long`.
    Long(i64),
    /// A `float`.
    Float(f32),
    /// A `double`.
    Double(f64),
    /// A `string`.
    String(String),
    /// `bytes`, and the body of a `fixed`.
    Bytes(Vec<u8>),
    /// The days since the Unix epoch of a `date`.
    Date(i32),
    /// The microseconds since the Unix epoch of a `timestamp-micros`.
    TimestampMicros(i64),
    /// The milliseconds since the Unix epoch of a `timestamp-millis`.
    TimestampMillis(i64),
    /// The nanoseconds since the Unix epoch of a `timestamp-nanos`.
    TimestampNanos(i64),
    /// The microseconds after midnight of a `time-micros`.
    TimeMicros(i64),
    /// The milliseconds after midnight of a `time-millis`.
    TimeMillis(i32),
    /// A decimal's unscaled value, big-endian two's complement.
    Decimal(Vec<u8>),
    /// The canonical text of a `uuid`.
    Uuid(String),
    /// A record's fields, in schema order.
    Record(Vec<(String, Value)>),
    /// An array's items, in order.
    Array(Vec<Value>),
    /// A map's entries, by key. Sorted so a walk does not depend on the
    /// container's hash order.
    Map(BTreeMap<String, Value>),
}

/// Decode every record in an Avro object container into [`Value`].
///
/// `kind` names what is being read (`"manifest"`, `"manifest list"`) so errors
/// identify the caller's context.
///
/// # Errors
/// Fails when the `iceberg` feature is off or `bytes` is not a valid Avro
/// object container.
pub fn read_avro_values(bytes: &[u8], kind: &str) -> Result<Vec<Value>, Error> {
    super::r#impl::read_avro_values(bytes, kind)
}

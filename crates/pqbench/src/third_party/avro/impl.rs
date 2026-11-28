//! The Avro record reader, isolated behind the `iceberg` feature.
//!
//! [`read_avro_values`] is the runtime dispatch: without the feature it returns
//! an error naming it, so callers never see a `#[cfg]`.

use super::api::{Error, Value};

#[cfg(feature = "iceberg")]
pub(crate) fn read_avro_values(bytes: &[u8], kind: &str) -> Result<Vec<Value>, Error> {
    use std::io::Cursor;

    use apache_avro::Reader;

    let reader = Reader::new(Cursor::new(bytes)).map_err(|error| Error::Read {
        kind: kind.to_string(),
        source: Box::new(error),
    })?;
    reader
        .map(|value| {
            value.map(convert).map_err(|error| Error::Read {
                kind: kind.to_string(),
                source: Box::new(error),
            })
        })
        .collect()
}

/// One `apache-avro` value as this crate's [`Value`].
///
/// A union is transparent: its index is the writer's schema detail, and the
/// value it wraps already says whether it is null. `fixed` and `enum` are their
/// bodies (bytes and symbol), and the local timestamp variants are their UTC
/// counterparts — the same count, which is what a reader formats.
#[cfg(feature = "iceberg")]
fn convert(value: apache_avro::types::Value) -> Value {
    use apache_avro::types::Value as Avro;

    match value {
        Avro::Null => Value::Null,
        Avro::Boolean(value) => Value::Bool(value),
        Avro::Int(value) => Value::Int(value),
        Avro::Long(value) => Value::Long(value),
        Avro::Float(value) => Value::Float(value),
        Avro::Double(value) => Value::Double(value),
        Avro::String(value) => Value::String(value),
        Avro::Bytes(value) => Value::Bytes(value),
        Avro::Fixed(_, value) => Value::Bytes(value),
        Avro::Enum(_, symbol) => Value::String(symbol),
        Avro::Union(_, value) => convert(*value),
        Avro::Array(items) => Value::Array(items.into_iter().map(convert).collect()),
        Avro::Map(entries) => Value::Map(
            entries
                .into_iter()
                .map(|(key, value)| (key, convert(value)))
                .collect(),
        ),
        Avro::Record(fields) => Value::Record(
            fields
                .into_iter()
                .map(|(name, value)| (name, convert(value)))
                .collect(),
        ),
        Avro::Date(days) => Value::Date(days),
        Avro::TimeMillis(value) => Value::TimeMillis(value),
        Avro::TimeMicros(value) => Value::TimeMicros(value),
        Avro::TimestampMillis(value) => Value::TimestampMillis(value),
        Avro::TimestampMicros(value) => Value::TimestampMicros(value),
        Avro::TimestampNanos(value) => Value::TimestampNanos(value),
        Avro::LocalTimestampMillis(value) => Value::TimestampMillis(value),
        Avro::LocalTimestampMicros(value) => Value::TimestampMicros(value),
        Avro::LocalTimestampNanos(value) => Value::TimestampNanos(value),
        Avro::Decimal(value) => Value::Decimal(Vec::<u8>::try_from(value).unwrap_or_default()),
        Avro::BigDecimal(value) => Value::String(value.to_string()),
        Avro::Duration(value) => Value::Bytes(<[u8; 12]>::from(value).to_vec()),
        Avro::Uuid(value) => Value::Uuid(value.to_string()),
    }
}

#[cfg(not(feature = "iceberg"))]
pub(crate) fn read_avro_values(bytes: &[u8], kind: &str) -> Result<Vec<Value>, Error> {
    let _ = (bytes, kind);
    Err(Error::Unsupported)
}

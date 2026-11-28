//! The Avro record reader, isolated behind the `iceberg` feature.

use super::api::{Error, Value};

#[cfg(feature = "iceberg")]
pub(crate) fn read_avro_values(bytes: &[u8], kind: &str) -> Result<Vec<Value>, Error> {
    use std::io::Cursor;

    let read_err = |source| Error::Read {
        kind: kind.to_string(),
        source: Box::new(source),
    };
    apache_avro::Reader::new(Cursor::new(bytes))
        .map_err(read_err)?
        .map(|item| item.map(convert).map_err(read_err))
        .collect()
}

#[cfg(feature = "iceberg")]
fn convert(value: apache_avro::types::Value) -> Value {
    use apache_avro::types::Value as Avro;
    use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};

    let iso = |time: DateTime<Utc>| Value::Text(time.to_rfc3339_opts(SecondsFormat::AutoSi, true));
    match value {
        Avro::Null | Avro::Array(_) | Avro::Map(_) | Avro::Duration(_) => Value::Null,
        Avro::Union(_, inner) => convert(*inner),
        Avro::Int(number) | Avro::TimeMillis(number) => Value::Int(i64::from(number)),
        Avro::Long(number) | Avro::TimeMicros(number) => Value::Int(number),
        Avro::Boolean(flag) => Value::Text(flag.to_string()),
        Avro::Float(number) => Value::Text(number.to_string()),
        Avro::Double(number) => Value::Text(number.to_string()),
        Avro::String(text) | Avro::Enum(_, text) => Value::Text(text),
        Avro::Uuid(uuid) => Value::Text(uuid.to_string()),
        Avro::BigDecimal(decimal) => Value::Text(decimal.to_string()),
        Avro::Bytes(bytes) | Avro::Fixed(_, bytes) => Value::Bytes(bytes),
        Avro::Decimal(decimal) => Value::Bytes(Vec::<u8>::try_from(decimal).unwrap_or_default()),
        Avro::Date(days) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|epoch| epoch.checked_add_signed(Duration::days(i64::from(days))))
            .map_or_else(
                || Value::Int(i64::from(days)),
                |date| Value::Text(date.format("%Y-%m-%d").to_string()),
            ),
        Avro::TimestampMillis(millis) | Avro::LocalTimestampMillis(millis) => {
            DateTime::from_timestamp_millis(millis).map_or(Value::Int(millis), iso)
        }
        Avro::TimestampMicros(micros) | Avro::LocalTimestampMicros(micros) => {
            DateTime::from_timestamp_micros(micros).map_or(Value::Int(micros), iso)
        }
        Avro::TimestampNanos(nanos) | Avro::LocalTimestampNanos(nanos) => {
            iso(DateTime::from_timestamp_nanos(nanos))
        }
        Avro::Record(fields) => Value::Record(
            fields
                .into_iter()
                .map(|(name, field)| (name, convert(field)))
                .collect(),
        ),
    }
}

#[cfg(not(feature = "iceberg"))]
pub(crate) fn read_avro_values(bytes: &[u8], kind: &str) -> Result<Vec<Value>, Error> {
    let _ = (bytes, kind);
    Err(Error::Unsupported)
}

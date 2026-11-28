//! `text-timestamp`: ISO-8601 timestamp text priced as a microsecond `INT64`.
//!
//! The rule reads the text itself: a column is a candidate only when every
//! sampled non-null value is an RFC-3339/ISO-8601 timestamp and nothing else,
//! so a column that merely starts with a date never reaches a rewrite. The
//! control then writes the same instants as `INT64` microseconds, Delta binary
//! packed, against the source's own text encoding and dictionary state.

use std::borrow::Cow;

use parquet::basic::Encoding;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{price, Settings};
use super::{ColumnSample, Error, Finding, RemedyAdvice};

/// The rule id a suggestion carries.
pub const TEXT_TIMESTAMP_RULE: &str = "text-timestamp";

/// The remedy: cast the text to a timestamp and write it as microseconds.
const REMEDY: RemedyAdvice = RemedyAdvice::new("store the instants as a timestamp", "spark");

/// The finding this column earns when every sampled non-null value parses as a
/// timestamp.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.byte_array() || sample.cells().is_empty() {
        return None;
    }
    let mut parsed = 0usize;
    for bytes in sample.cells().iter().flatten() {
        parse_timestamp(bytes.data())?;
        parsed += 1;
    }
    if parsed == 0 {
        return None;
    }
    Some(Finding {
        message: format!("ISO-8601 timestamp text at {:.2} B/value", sample.text_bpv),
        remedies: remedies(sample.leaf),
        advice: REMEDY,
        measure,
    })
}

/// Price the same instants as microsecond `INT64` against the source's text,
/// both written by the same writer at the file's codec and level. The integer
/// arm keeps the smaller of Delta binary packing and PLAIN: text with no
/// pattern can still beat its own length prefix, but never by the packed
/// deltas' margin.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let mut micros: Vec<Option<i64>> = Vec::with_capacity(sample.cells().len());
    for cell in sample.cells() {
        match cell {
            Some(bytes) => match parse_timestamp(bytes.data()) {
                Some(value) => micros.push(Some(value)),
                None => return Ok(None),
            },
            None => micros.push(None),
        }
    }
    let baseline = Arm {
        values: sample
            .values()
            .ok_or_else(|| Error::Invalid("the sample's text was not decoded".into()))?,
        settings: sample.source_settings(control::source_encoding(sample.mass)),
    };
    let integers = super::price::Values::Int64(&micros);
    control::measure_arms(sample, |compression| {
        let baseline_bytes = price(baseline.values, compression, baseline.settings)?;
        let mut candidate_bytes = u64::MAX;
        for encoding in [Encoding::DELTA_BINARY_PACKED, Encoding::PLAIN] {
            let bytes = price(integers, compression, Settings::candidate(Some(encoding)))?;
            candidate_bytes = candidate_bytes.min(bytes);
        }
        Ok(Some((baseline_bytes, candidate_bytes)))
    })
}

/// Parse RFC-3339/ISO-8601 timestamp text as microseconds since the Unix epoch.
///
/// The accepted form is `YYYY-MM-DD[T ]HH:MM:SS[.fraction][Z|±HH:MM|±HHMM]`,
/// the shape every engine writes: a `T` or a space between date and time, an
/// optional fraction of up to nine digits, and an optional offset (a value
/// without one is read as UTC). Anything else — a date alone, a number, a
/// timestamp with trailing text — is `None`, so a column is only a candidate
/// when its every value is a timestamp.
fn parse_timestamp(text: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(text).ok()?;
    let date_time = chrono::DateTime::parse_from_rfc3339(&rfc3339(text)?).ok()?;
    // RFC 3339 allows a leap second; chrono reads `:60` as the next minute's
    // first second, which is not the instant the text names.
    (date_time.timestamp_subsec_nanos() < 1_000_000_000).then(|| date_time.timestamp_micros())
}

/// `text` in the RFC 3339 spelling chrono reads: a value without an offset is
/// read as UTC, an offset written `±HHMM` gains its colon, and a fraction of
/// more than nine digits (which chrono would truncate rather than reject) is
/// refused. The date, the clock, and the offset are chrono's to validate.
fn rfc3339(text: &str) -> Option<Cow<'_, str>> {
    if !text.is_ascii() {
        return None;
    }
    let bytes = text.as_bytes();
    // The date-time body is fixed width; the fraction and offset follow it.
    let mut end = 19;
    if bytes.get(19) == Some(&b'.') {
        let digits = bytes[20..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if digits > 9 {
            return None;
        }
        end += 1 + digits;
    }
    match bytes.get(end) {
        // A value without an offset is UTC.
        None => Some(Cow::Owned(format!("{text}Z"))),
        // `±HHMM`: the offset some engines write without its colon.
        Some(b'+' | b'-') if end + 5 == text.len() => {
            let mut coloned = text.to_string();
            coloned.insert(end + 3, ':');
            Some(Cow::Owned(coloned))
        }
        Some(_) => Some(Cow::Borrowed(text)),
    }
}

/// The settings that store one column's timestamps as microsecond `INT64`, one
/// per engine. The column's type changes, so the format's own type promotion
/// cannot do it in place: the remedy is the cast plus the writer's timestamp
/// type.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            format!(
                "CAST({leaf} AS TIMESTAMP) before write, with \
                 spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS"
            ),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!(
                "cast column {} to pa.timestamp('us') before write (pc.strptime for text, \
                 then Table.set_column)",
                super::py_string(leaf)
            ),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!("write {leaf} as INT64 TIMESTAMP(MICROS), not UTF-8 text"),
        ),
        v1::Remedy::new(
            "delta",
            format!(
                "rewrite with CAST({leaf} AS TIMESTAMP): Delta does not promote string to \
                 timestamp in place"
            ),
        ),
        v1::Remedy::new(
            "iceberg",
            format!(
                "rewrite with CAST({leaf} AS TIMESTAMP): Iceberg does not promote string to \
                 timestamp in place"
            ),
        ),
        v1::Remedy::new(
            "duckdb",
            format!("CAST({leaf} AS TIMESTAMP) in the SELECT of COPY ... (FORMAT parquet)"),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::parse_timestamp;

    /// 2024-02-29T12:34:56.789Z, and its microseconds since the epoch.
    #[test]
    fn a_zoned_timestamp_parses_to_microseconds() {
        assert_eq!(
            parse_timestamp(b"1970-01-01T00:00:00Z"),
            Some(0),
            "the epoch"
        );
        assert_eq!(parse_timestamp(b"1970-01-01 00:00:01"), Some(1_000_000));
        assert_eq!(
            parse_timestamp(b"2024-02-29T12:34:56.789Z"),
            Some(1_709_210_096_789_000)
        );
        assert_eq!(
            parse_timestamp(b"2024-02-29T12:34:56.789123456Z"),
            Some(1_709_210_096_789_123)
        );
        // 13:34:56+01:00 is the same instant as 12:34:56Z.
        assert_eq!(
            parse_timestamp(b"2024-02-29T13:34:56+01:00"),
            Some(1_709_210_096_000_000)
        );
        assert_eq!(
            parse_timestamp(b"2024-02-29T11:34:56-0100"),
            Some(1_709_210_096_000_000)
        );
    }

    /// Anything that is not exactly a timestamp is rejected.
    #[test]
    fn other_text_is_rejected() {
        for text in [
            &b"2024-02-29"[..],
            b"2024-02-30T00:00:00Z",
            b"2023-02-29T00:00:00Z",
            b"2024-13-01T00:00:00Z",
            b"2024-02-29T24:00:00Z",
            b"2024-02-29T00:60:00Z",
            b"2024-02-29T00:00:60Z",
            b"2024-02-29T00:00:00Z ",
            b"2024-02-29T00:00:00+24:00",
            b"2024-02-29T00:00:00.1234567890Z",
            b"2024-02-29T00:00:00.",
            b"value-2024-02-29T00:00:00Z",
            b"1709210096",
        ] {
            assert_eq!(
                parse_timestamp(text),
                None,
                "{}",
                String::from_utf8_lossy(text)
            );
        }
    }
}

//! `text-timestamp`: ISO-8601 timestamp text priced as a microsecond `INT64`.
//!
//! The rule reads the text itself: a column is a candidate only when every
//! sampled non-null value is an RFC-3339/ISO-8601 timestamp and nothing else,
//! so a column that merely starts with a date never reaches a rewrite. The
//! control then writes the same instants as `INT64` microseconds, Delta binary
//! packed, against the source's own text encoding and dictionary state.

use chrono::NaiveDateTime;
use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{price, Settings};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const TEXT_TIMESTAMP_RULE: &str = "text-timestamp";

/// The shortest timestamp text: `YYYY-MM-DDTHH:MM:SS`.
const MINIMUM_LENGTH: usize = 19;

/// Microseconds in a second, a minute, and an hour.
const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_MINUTE: i64 = 60 * MICROSECONDS_PER_SECOND;
const MICROSECONDS_PER_HOUR: i64 = 60 * MICROSECONDS_PER_MINUTE;

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
        remedy: remedy(sample.writer_kind(), sample.leaf),
        remedies: remedies(sample.leaf),
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
        settings: Settings {
            dictionary: sample.chunk_has_dictionary,
            dictionary_page_size_limit: sample.dictionary_page_size_limit(),
            data_page_size_limit: None,
            encoding: control::source_encoding(sample.mass),
            version: WriterVersion::PARQUET_1_0,
        },
    };
    let integers = super::price::Values::Int64(&micros);
    control::measure_arms(sample, |compression| {
        let baseline_bytes = price(baseline.values, compression, baseline.settings)?;
        let mut candidate_bytes = u64::MAX;
        for encoding in [Encoding::DELTA_BINARY_PACKED, Encoding::PLAIN] {
            let bytes = price(
                integers,
                compression,
                Settings {
                    dictionary: false,
                    dictionary_page_size_limit: None,
                    data_page_size_limit: None,
                    encoding: Some(encoding),
                    version: WriterVersion::PARQUET_1_0,
                },
            )?;
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
    if text.len() < MINIMUM_LENGTH || !text.is_ascii() {
        return None;
    }
    let text = std::str::from_utf8(text).ok()?;
    let (date_time, rest) = text.split_at(MINIMUM_LENGTH);
    let date_time = date_time_body(date_time)?;
    let (fraction_micros, rest) = fraction(rest)?;
    let offset_micros = offset(rest)?;
    // chrono validates the civil date (leap years included) and the clock, at
    // the fixed widths a timestamp has.
    let date_time = std::str::from_utf8(&date_time).ok()?;
    let naive = NaiveDateTime::parse_from_str(date_time, "%Y-%m-%dT%H:%M:%S").ok()?;
    Some(naive.and_utc().timestamp_micros() + fraction_micros - offset_micros)
}

/// The 19-byte date and time `YYYY-MM-DD[T ]HH:MM:SS`, with the separator
/// spelled `T` so one format reads it, or `None` when a separator is not where
/// a timestamp has it or a digit is not a digit. The widths are fixed: a
/// one-digit month or a three-digit year is not a timestamp this rule reads.
fn date_time_body(date_time: &str) -> Option<[u8; MINIMUM_LENGTH]> {
    let mut body = [0u8; MINIMUM_LENGTH];
    body.copy_from_slice(date_time.as_bytes());
    match body[10] {
        b'T' | b't' => body[10] = b'T',
        b' ' => body[10] = b'T',
        _ => return None,
    }
    let shaped = body.iter().enumerate().all(|(at, byte)| match at {
        4 | 7 => *byte == b'-',
        13 | 16 => *byte == b':',
        10 => true,
        _ => byte.is_ascii_digit(),
    });
    // A leap second is a minute's 61st second: chrono reads `:60` as the next
    // minute's first, which is not the instant the text names.
    let leap_second = &body[17..19] == b"60";
    (shaped && !leap_second).then_some(body)
}

/// The microseconds a fraction `text` starts with, and the text after it. A
/// fraction is a dot and one to nine digits; the digits past the sixth are
/// sub-microsecond and truncate, as a microsecond `INT64` stores them.
fn fraction(text: &str) -> Option<(i64, &str)> {
    let Some(digits) = text.strip_prefix('.') else {
        return Some((0, text));
    };
    let length = digits.bytes().take_while(u8::is_ascii_digit).count();
    if length == 0 || length > 9 {
        return None;
    }
    let mut micros = 0i64;
    for at in 0..6 {
        let digit = match at < length {
            true => i64::from(digits.as_bytes()[at] - b'0'),
            false => 0,
        };
        micros = micros * 10 + digit;
    }
    Some((micros, &digits[length..]))
}

/// The offset `text` names, in microseconds east of UTC: nothing or `Z` is
/// UTC, and `±HH:MM` and `±HHMM` are read with an hour under 24 and a minute
/// under 60.
fn offset(text: &str) -> Option<i64> {
    let sign = match text.as_bytes() {
        [] | [b'Z' | b'z'] => return Some(0),
        [b'+', ..] => 1,
        [b'-', ..] => -1,
        _ => return None,
    };
    let (hour, minute) = match text.as_bytes() {
        [_, hour_high, hour_low, b':', minute_high, minute_low] => (
            pair(*hour_high, *hour_low)?,
            pair(*minute_high, *minute_low)?,
        ),
        [_, hour_high, hour_low, minute_high, minute_low] => (
            pair(*hour_high, *hour_low)?,
            pair(*minute_high, *minute_low)?,
        ),
        _ => return None,
    };
    if hour > 23 || minute > 59 {
        return None;
    }
    let magnitude =
        i64::from(hour) * MICROSECONDS_PER_HOUR + i64::from(minute) * MICROSECONDS_PER_MINUTE;
    Some(sign * magnitude)
}

/// Two decimal digits as a number, or `None` when either is not a digit.
fn pair(high: u8, low: u8) -> Option<u32> {
    if !high.is_ascii_digit() || !low.is_ascii_digit() {
        return None;
    }
    Some(u32::from(high - b'0') * 10 + u32::from(low - b'0'))
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

/// The setting that stores one column's timestamps as `INT64` microseconds, in
/// the detected writer's terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    let remedies = remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    let lead = "store the instants as a timestamp";
    match writer {
        Writer::ParquetMr => format!("{lead}: {}", setting("spark")),
        Writer::Photon => format!(
            "{lead}: {} (Photon forwarding unverified)",
            setting("spark")
        ),
        Writer::ArrowCpp => format!("{lead}: {}", setting("pyarrow")),
        Writer::ArrowRs => format!("{lead}: {}", setting("parquet-rs")),
        Writer::Other => format!("{lead}: {}", setting("iceberg")),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_timestamp, remedy};
    use crate::lint::writer::Writer;

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

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "ts").contains("TIMESTAMP_MICROS"));
        assert!(remedy(Writer::ArrowCpp, "ts").contains("pa.timestamp('us')"));
        assert!(remedy(Writer::ArrowRs, "ts").contains("TIMESTAMP(MICROS)"));
        assert!(remedy(Writer::Photon, "ts").contains("unverified"));
        assert!(remedy(Writer::Other, "ts").contains("CAST(ts AS TIMESTAMP)"));
    }
}

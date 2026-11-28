//! `text-timestamp`: ISO-8601 timestamp text priced as a microsecond `INT64`.
//!
//! The rule reads the text itself: a column is a candidate only when every
//! sampled non-null value is an RFC-3339/ISO-8601 timestamp and nothing else,
//! so a column that merely starts with a date never reaches a rewrite. The
//! control then writes the same instants as `INT64` microseconds, Delta binary
//! packed, against the source's own text encoding and dictionary state.

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

/// Microseconds in a second, a minute, an hour, and a day.
const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_MINUTE: i64 = 60 * MICROSECONDS_PER_SECOND;
const MICROSECONDS_PER_HOUR: i64 = 60 * MICROSECONDS_PER_MINUTE;
const MICROSECONDS_PER_DAY: i64 = 24 * MICROSECONDS_PER_HOUR;

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
    let year = number(text, 0, 4)?;
    if text[4] != b'-' {
        return None;
    }
    let month = number(text, 5, 7)?;
    if text[7] != b'-' {
        return None;
    }
    let day = number(text, 8, 10)?;
    if !matches!(text[10], b'T' | b't' | b' ') {
        return None;
    }
    let hour = number(text, 11, 13)?;
    if text[13] != b':' {
        return None;
    }
    let minute = number(text, 14, 16)?;
    if text[16] != b':' {
        return None;
    }
    let second = number(text, 17, 19)?;
    if !(1..=12).contains(&month)
        || day < 1
        || day > month_days(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let mut at = MINIMUM_LENGTH;
    let mut fraction_micros = 0i64;
    if text.get(at) == Some(&b'.') {
        at += 1;
        let start = at;
        let mut digits = 0usize;
        while let Some(byte) = text.get(at).copied() {
            if !byte.is_ascii_digit() {
                break;
            }
            if digits < 6 {
                fraction_micros = fraction_micros * 10 + i64::from(byte - b'0');
            }
            digits += 1;
            at += 1;
        }
        if at == start || digits > 9 {
            return None;
        }
        for _ in digits..6 {
            fraction_micros *= 10;
        }
    }
    let offset_micros = match text.get(at).copied() {
        None => 0,
        Some(b'Z' | b'z') if at + 1 == text.len() => 0,
        Some(sign @ (b'+' | b'-')) => {
            let (offset_hour, offset_minute) = match (text.len() - at, text.get(at + 3)) {
                (6, Some(b':')) => (number(text, at + 1, at + 3)?, number(text, at + 4, at + 6)?),
                (5, _) => (number(text, at + 1, at + 3)?, number(text, at + 3, at + 5)?),
                _ => return None,
            };
            if offset_hour > 23 || offset_minute > 59 {
                return None;
            }
            let magnitude = i64::from(offset_hour) * MICROSECONDS_PER_HOUR
                + i64::from(offset_minute) * MICROSECONDS_PER_MINUTE;
            if sign == b'-' {
                -magnitude
            } else {
                magnitude
            }
        }
        Some(_) => return None,
    };
    let days = epoch_days(i64::from(year), i64::from(month), i64::from(day));
    let seconds = i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    Some(
        days * MICROSECONDS_PER_DAY + seconds * MICROSECONDS_PER_SECOND + fraction_micros
            - offset_micros,
    )
}

/// The digits in `text[start..end]` as a number, or `None` when any is not one.
fn number(text: &[u8], start: usize, end: usize) -> Option<u32> {
    let digits = text.get(start..end)?;
    let mut value = 0u32;
    for digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(*digit - b'0');
    }
    Some(value)
}

/// Whether `year` is a leap year in the Gregorian calendar.
fn leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

/// The days in a month of the Gregorian calendar.
fn month_days(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year(year) => 29,
        _ => 28,
    }
}

/// Days from 1970-01-01 to the given proleptic Gregorian date.
fn epoch_days(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_of_year = (month + 9) % 12;
    let day_of_year = (153 * month_of_year + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
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
                "cast {leaf} to pa.timestamp('us') before write (pc.strptime for text, \
                 then Table.set_column)"
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

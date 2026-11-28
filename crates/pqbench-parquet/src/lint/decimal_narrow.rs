//! `decimal-narrow`: a `DECIMAL(precision, scale)` stored in a
//! `FIXED_LEN_BYTE_ARRAY` whose precision fits an integer, priced as `INT32`
//! (precision ≤ 9) or `INT64` (precision 10..=18) against the same
//! `FIXED_LEN_BYTE_ARRAY` values.
//!
//! A decimal's width is its precision: a `DECIMAL(9,2)` needs four bytes, but
//! writers that predate the integer decimal types store it in a 16-byte
//! `FIXED_LEN_BYTE_ARRAY`. Parquet allows a decimal of precision 9 or less on
//! `INT32` and one of precision 18 or less on `INT64`, so the stored integers
//! can move to a narrower physical type without changing a value. The control
//! writes the same two's-complement integers at both widths, at the file's own
//! codec and level, so the saving is the footer's fixed-width declaration
//! removed rather than a guessed ratio.

use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{Settings, Values};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DECIMAL_NARROW_RULE: &str = "decimal-narrow";

/// The widest precision an `INT32` decimal may declare.
const INT32_PRECISION: u32 = 9;

/// The widest precision an `INT64` decimal may declare.
const INT64_PRECISION: u32 = 18;

/// Whether a `DECIMAL` of this precision is stored on `INT32` or `INT64`.
#[derive(Clone, Copy)]
enum Width {
    Int32,
    Int64,
}

/// The finding this column earns when its leaf is a `FIXED_LEN_BYTE_ARRAY`
/// decimal whose precision fits an integer and whose values fit that integer.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if sample.physical() != "FIXED_LEN_BYTE_ARRAY"
        || sample
            .column
            .max_repetition_level
            .is_some_and(|level| level != 0)
    {
        return None;
    }
    let (precision, scale) = decimal_type(sample)?;
    let width = narrow_width(precision)?;
    narrowed_values(sample, width)?;
    let physical = match width {
        Width::Int32 => "INT32",
        Width::Int64 => "INT64",
    };
    Some(Finding {
        message: format!(
            "DECIMAL({precision},{scale}) stored at {:.2} B/value fits in {physical}",
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf, width),
        remedies: remedies(sample.leaf, width),
        measure,
    })
}

/// The leaf's `DECIMAL(precision,scale)`, when the footer records one this
/// build reads.
fn decimal_type(sample: &ColumnSample<'_>) -> Option<(u32, u32)> {
    parse_decimal_type(sample.mass.logical_type.as_deref()?)
}

/// `DECIMAL(precision,scale)` as the footer spells it.
fn parse_decimal_type(logical_type: &str) -> Option<(u32, u32)> {
    let (precision, scale) = logical_type
        .strip_prefix("DECIMAL(")?
        .strip_suffix(')')?
        .split_once(',')?;
    Some((precision.parse().ok()?, scale.parse().ok()?))
}

/// The sample's values as integers of `width`, when every one of them fits.
fn narrowed_values(sample: &ColumnSample<'_>, width: Width) -> Option<Narrowed> {
    let mut int32_values: Vec<Option<i32>> = Vec::with_capacity(sample.cells().len());
    let mut int64_values: Vec<Option<i64>> = Vec::with_capacity(sample.cells().len());
    for cell in sample.cells() {
        let value = match cell {
            Some(bytes) => Some(decode_decimal(bytes.data())?),
            None => None,
        };
        match width {
            Width::Int32 => int32_values.push(value.map(i32::try_from).transpose().ok()?),
            Width::Int64 => int64_values.push(value.map(i64::try_from).transpose().ok()?),
        }
    }
    if int32_values.is_empty() && int64_values.is_empty() {
        return None;
    }
    Some(match width {
        Width::Int32 => Narrowed::Int32(int32_values),
        Width::Int64 => Narrowed::Int64(int64_values),
    })
}

/// One integer value per row, at the width the rule narrows to.
enum Narrowed {
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
}

impl Narrowed {
    /// One pricing arm over these values.
    fn arm(&self, settings: Settings) -> Arm<'_> {
        match self {
            Self::Int32(values) => Arm {
                values: Values::Int32(values),
                settings,
            },
            Self::Int64(values) => Arm {
                values: Values::Int64(values),
                settings,
            },
        }
    }
}

/// Price the narrowed integers against the source's `FIXED_LEN_BYTE_ARRAY`,
/// both written by the same writer at the file's codec and level.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let Some(width) = decimal_width(sample) else {
        return Ok(None);
    };
    let Some(narrow) = narrowed_values(sample, width) else {
        return Ok(None);
    };
    // A `FIXED_LEN_BYTE_ARRAY` dictionary exists only under the 2.0 page
    // version, so both arms write v2 pages when the source had a dictionary.
    let version = match sample.chunk_has_dictionary {
        true => WriterVersion::PARQUET_2_0,
        false => WriterVersion::PARQUET_1_0,
    };
    let settings = Settings {
        dictionary: sample.chunk_has_dictionary,
        dictionary_page_size_limit: sample.dictionary_page_size_limit(),
        data_page_size_limit: None,
        encoding: control::source_encoding(sample.mass),
        version,
    };
    let candidate = narrow.arm(settings);
    control::compare(
        sample,
        &Arm {
            values: sample
                .values()
                .ok_or_else(|| Error::Invalid("the sample's bytes were not decoded".into()))?,
            settings,
        },
        &candidate,
    )
}

/// The width the rule narrows to, from the leaf's decimal precision.
fn decimal_width(sample: &ColumnSample<'_>) -> Option<Width> {
    let (precision, _) = decimal_type(sample)?;
    narrow_width(precision)
}

/// The integer a decimal of this precision fits: `INT32` up to nine digits,
/// `INT64` up to eighteen, and nothing wider.
fn narrow_width(precision: u32) -> Option<Width> {
    match precision {
        precision if precision <= INT32_PRECISION => Some(Width::Int32),
        precision if precision <= INT64_PRECISION => Some(Width::Int64),
        _ => None,
    }
}

/// Decode a big-endian two's-complement decimal, wide as it is declared.
fn decode_decimal(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut value: i128 = if negative { -1 } else { 0 };
    for byte in bytes {
        value = (value << 8) | i128::from(*byte);
    }
    Some(value)
}

/// The settings that store one column's decimals on an integer, one per engine.
fn remedies(leaf: &str, width: Width) -> Vec<v1::Remedy> {
    let (sql_type, arrow_type) = match width {
        Width::Int32 => ("INT", "pa.int32()"),
        Width::Int64 => ("BIGINT", "pa.int64()"),
    };
    vec![
        v1::Remedy::new(
            "spark",
            format!("CAST(`{leaf}` AS {sql_type}) after scaling by its scale"),
        ),
        v1::Remedy::new(
            "duckdb",
            format!("CAST(\"{leaf}\" AS {sql_type}) after scaling by its scale"),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!(
                "column_types={{{}: {arrow_type}}} with a decimal precision of at most \
                 {INT64_PRECISION}",
                super::py_string(leaf)
            ),
        ),
        v1::Remedy::new(
            "iceberg",
            format!("ALTER TABLE <table> ALTER COLUMN {leaf} TYPE {sql_type}"),
        ),
    ]
}

/// The setting that stores one column's decimals on an integer, in the
/// detected writer's terms.
fn remedy(writer: Writer, leaf: &str, width: Width) -> String {
    let remedies = remedies(leaf, width);
    let setting = |engine: &str| super::setting(&remedies, engine);
    match writer {
        Writer::ParquetMr => setting("spark"),
        Writer::Photon => format!("{} (Photon forwarding unverified)", setting("spark")),
        Writer::ArrowCpp => setting("pyarrow"),
        Writer::ArrowRs => format!("write {leaf} as an integer decimal"),
        Writer::Other => setting("iceberg"),
    }
}

#[cfg(test)]
mod tests {
    use parquet::data_type::ByteArray;

    use super::{decode_decimal, detect, measure, parse_decimal_type, remedy, Width};
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(logical_type: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some("FIXED_LEN_BYTE_ARRAY".into()),
            max_repetition_level: Some(0),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                logical_type: Some(logical_type.into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// `value` as a 16-byte big-endian two's-complement decimal.
    fn fixed16(value: i64) -> ByteArray {
        let mut bytes = vec![u8::from(value < 0) * 0xFF; 8];
        bytes.extend_from_slice(&value.to_be_bytes());
        ByteArray::from(bytes)
    }

    /// A `DECIMAL(9,2)` in 16 bytes narrows to `INT32`, and the control confirms
    /// the saving: four bytes of value against sixteen.
    #[test]
    fn a_small_decimal_earns_an_int32() {
        let leaf = column("DECIMAL(9,2)");
        let cells: Vec<Option<ByteArray>> = (0..2_000)
            .map(|value| Some(fixed16(1_000_000 + value * 7)))
            .collect();
        let sample = ColumnSample::for_test(&leaf, &cells, false);

        let finding = detect(&sample).expect("a DECIMAL(9,2) is a candidate");
        assert!(finding.message.contains("INT32"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.5,
            "four bytes against sixteen, got {}",
            measurement.ratio_low
        );
    }

    /// A `DECIMAL(18,4)` needs `INT64`, and a `DECIMAL(38,10)` fits neither.
    #[test]
    fn the_precision_picks_the_width_and_stops_at_eighteen() {
        assert_eq!(parse_decimal_type("DECIMAL(18,4)"), Some((18, 4)));
        assert_eq!(parse_decimal_type("DECIMAL(38,10)"), Some((38, 10)));
        assert_eq!(parse_decimal_type("STRING"), None);

        let leaf = column("DECIMAL(18,4)");
        let cells: Vec<Option<ByteArray>> = (0..2_000)
            .map(|value| Some(fixed16(1_000_000_000_000 + value)))
            .collect();
        let sample = ColumnSample::for_test(&leaf, &cells, false);
        let finding = detect(&sample).expect("a DECIMAL(18,4) is a candidate");
        assert!(finding.message.contains("INT64"));
        assert!(measure(&sample).expect("measure").is_some());

        let wide = column("DECIMAL(38,10)");
        assert!(detect(&ColumnSample::for_test(&wide, &cells, false)).is_none());
        assert!(measure(&ColumnSample::for_test(&wide, &cells, false))
            .expect("measure")
            .is_none());
    }

    /// A `FIXED_LEN_BYTE_ARRAY` that is not a decimal is not a candidate, and a
    /// value the wider integer cannot hold keeps the column out.
    #[test]
    fn only_a_narrow_decimal_is_candidate() {
        let cells: Vec<Option<ByteArray>> = (0..2_000).map(|value| Some(fixed16(value))).collect();

        let mut plain = column("DECIMAL(9,2)");
        plain.mass = Some(v1::Bytemass {
            codec: Some("UNCOMPRESSED".into()),
            ..v1::Bytemass::default()
        });
        assert!(detect(&ColumnSample::for_test(&plain, &cells, false)).is_none());

        let leaf = column("DECIMAL(9,2)");
        let out_of_range: Vec<Option<ByteArray>> = (0..2_000)
            .map(|value| Some(fixed16(i64::from(i32::MAX) + 1 + value)))
            .collect();
        assert!(detect(&ColumnSample::for_test(&leaf, &out_of_range, false)).is_none());
    }

    /// Big-endian two's complement decodes at any width up to sixteen bytes.
    #[test]
    fn a_decimal_decodes_with_its_sign() {
        assert_eq!(decode_decimal(&[0x01]), Some(1));
        assert_eq!(decode_decimal(&[0xFF]), Some(-1));
        assert_eq!(
            decode_decimal(&[0x7F, 0xFF, 0xFF, 0xFF]),
            Some(2_147_483_647)
        );
        assert_eq!(
            decode_decimal(&[0x80, 0x00, 0x00, 0x00]),
            Some(i128::from(i32::MIN))
        );
        assert_eq!(decode_decimal(&[0x00; 16]), Some(0));
        assert_eq!(decode_decimal(&[]), None);
        assert_eq!(decode_decimal(&[0x00; 17]), None);
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "amount", Width::Int32).contains("INT"));
        assert!(remedy(Writer::ArrowCpp, "amount", Width::Int64).contains("pa.int64()"));
        assert!(remedy(Writer::ArrowCpp, "amount", Width::Int32).contains("'amount'"));
        assert!(remedy(Writer::Photon, "amount", Width::Int32).contains("unverified"));
        assert!(remedy(Writer::Other, "amount", Width::Int64).contains("BIGINT"));
    }
}

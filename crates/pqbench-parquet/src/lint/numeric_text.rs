//! `numeric-text`: canonical integer and decimal text priced as `INT64` or
//! `DOUBLE`.
//!
//! A number stored as text pays a four-byte length prefix and one character per
//! digit, and a codec cannot recover either. The rule reads the text itself: a
//! column is a candidate only when nearly every value is the canonical spelling
//! of an integer or a decimal, so a ZIP code, a padded identifier, or a
//! formatted string never reaches a rewrite. The control then writes the same
//! values at their numeric width, under the file's own codec and level, and the
//! difference is what the rewrite saves.

use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{price, Settings, Values};
use super::writer::Writer;
use super::{py_string, ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const NUMERIC_TEXT_RULE: &str = "numeric-text";

/// The fewest sampled non-null values a column decision rests on.
const MINIMUM_VALUES: usize = 16;

/// The fraction of sampled non-null values that must be canonical numbers.
const VALID_FRACTION: f64 = 0.99;

/// What the column's text spells: integers alone, or decimals beside them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Numbers {
    /// Every value is a canonical integer: the rewrite is `INT64`.
    Integers,
    /// At least one value is a canonical decimal: the rewrite is `DOUBLE`.
    Doubles,
}

/// One canonical number as its text spells it.
#[derive(Clone, Copy)]
enum Number {
    Integer(i64),
    Double(f64),
}

/// The finding this column earns when nearly every sampled non-null value is
/// canonical numeric text.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    let (numbers, fraction) = numeric_text(sample)?;
    let spelling = match numbers {
        Numbers::Integers => "integers",
        Numbers::Doubles => "numbers with a decimal point",
    };
    Some(Finding {
        message: format!(
            "numeric text at {:.2} B/value ({:.0}% of values are canonical {spelling})",
            sample.text_bpv,
            fraction * 100.0
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf, numbers),
        remedies: remedies(sample.leaf, numbers),
        measure,
    })
}

/// What the column's values spell, and the fraction that spells it, when the
/// leaf is a scalar text leaf whose values are nearly all canonical numbers.
fn numeric_text(sample: &ColumnSample<'_>) -> Option<(Numbers, f64)> {
    if !sample.byte_array() || sample.cells().is_empty() || !text_leaf(sample) {
        return None;
    }
    let mut counted = 0usize;
    let mut integers = 0usize;
    let mut numbers = 0usize;
    for bytes in sample.cells().iter().flatten() {
        counted += 1;
        match parse_number(bytes.data()) {
            Some(Number::Integer(_)) => {
                integers += 1;
                numbers += 1;
            }
            Some(Number::Double(_)) => numbers += 1,
            None => {}
        }
    }
    if counted < MINIMUM_VALUES {
        return None;
    }
    let fraction = numbers as f64 / counted as f64;
    if fraction < VALID_FRACTION {
        return None;
    }
    // A column of integers alone is narrowed to `INT64`; one that holds any
    // decimal is a `DOUBLE` column, so the integers become doubles too.
    let numbers = match integers == numbers {
        true => Numbers::Integers,
        false => Numbers::Doubles,
    };
    Some((numbers, fraction))
}

/// Whether the leaf is a scalar text leaf: a repeated leaf's values are nested,
/// and a logical type that is not text (JSON, a UUID, a binary payload) spells
/// something other than a number.
fn text_leaf(sample: &ColumnSample<'_>) -> bool {
    sample.scalar() && matches!(sample.mass.logical_type.as_deref(), None | Some("STRING"))
}

/// Parse one value as a canonical number.
///
/// Every accepted spelling is the one Rust itself writes for the value, so a
/// leading `+`, a leading zero, surrounding whitespace, `-0`, `NaN`, an
/// infinity, or scientific notation is rejected: those are identifiers,
/// formatted strings, or a spelling that would change on the way back out.
fn parse_number(text: &[u8]) -> Option<Number> {
    let text = std::str::from_utf8(text).ok()?;
    let integer = text.parse::<i64>().ok();
    if let Some(integer) = integer {
        if integer.to_string() == text {
            return Some(Number::Integer(integer));
        }
    }
    // A canonical decimal always spells its point: `-0`, `NaN`, and `1e5` are
    // not the form a value round-trips through.
    if !text.contains('.') {
        return None;
    }
    let double = text.parse::<f64>().ok()?;
    if !double.is_finite() {
        return None;
    }
    // Rust renders the shortest round-tripping form, so this is exact.
    (double.to_string() == text).then_some(Number::Double(double))
}

/// Price the numeric arm against the source's text, both written by the same
/// writer at the file's codec and level.
///
/// The control needs every value: a column with a stray non-numeric value is not
/// the same column rewritten, so it stays unmeasured rather than credited with
/// the numeric values' bytes. An integer wider than a `DOUBLE` can hold exactly
/// is not converted either, because the rewrite would change its value.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let Some((numbers, _)) = numeric_text(sample) else {
        return Ok(None);
    };
    let mut integers: Vec<Option<i64>> = Vec::with_capacity(sample.cells().len());
    let mut doubles: Vec<Option<f64>> = Vec::with_capacity(sample.cells().len());
    for cell in sample.cells() {
        let number = match cell {
            Some(bytes) => parse_number(bytes.data()),
            None => None,
        };
        match (cell.is_none(), numbers, number) {
            (true, Numbers::Integers, _) => integers.push(None),
            (true, Numbers::Doubles, _) => doubles.push(None),
            (false, Numbers::Integers, Some(Number::Integer(value))) => integers.push(Some(value)),
            (false, Numbers::Doubles, Some(Number::Integer(value))) => {
                let exact = value as f64;
                if exact as i64 != value {
                    return Ok(None);
                }
                doubles.push(Some(exact));
            }
            (false, Numbers::Doubles, Some(Number::Double(value))) => doubles.push(Some(value)),
            (false, _, _) => return Ok(None),
        }
    }
    let baseline = Arm {
        values: sample
            .values()
            .ok_or_else(|| Error::Invalid("the sample's text was not decoded".into()))?,
        settings: source_settings(sample),
    };
    control::measure_arms(sample, |compression| {
        let baseline_bytes = price(baseline.values, compression, baseline.settings)?;
        let candidate_bytes = match numbers {
            Numbers::Doubles => price(
                Values::Double(&doubles),
                compression,
                Settings {
                    encoding: None,
                    ..source_settings(sample)
                },
            )?,
            Numbers::Integers => {
                let mut bytes = u64::MAX;
                for encoding in [Encoding::DELTA_BINARY_PACKED, Encoding::PLAIN] {
                    bytes = bytes.min(price(
                        Values::Int64(&integers),
                        compression,
                        Settings {
                            encoding: Some(encoding),
                            ..source_settings(sample)
                        },
                    )?);
                }
                bytes
            }
        };
        Ok(Some((baseline_bytes, candidate_bytes)))
    })
}

/// The source's own dictionary state and encoding: the rewrite changes what a
/// value is, not how the column is stored, so both arms keep it.
fn source_settings(sample: &ColumnSample<'_>) -> Settings {
    Settings {
        dictionary: sample.chunk_has_dictionary,
        dictionary_page_size_limit: sample.dictionary_page_size_limit(),
        data_page_size_limit: None,
        encoding: control::source_encoding(sample.mass),
        version: WriterVersion::PARQUET_1_0,
    }
}

/// The settings that store one column's numbers as numbers, one per engine.
fn remedies(leaf: &str, numbers: Numbers) -> Vec<v1::Remedy> {
    let (sql_type, arrow_type) = match numbers {
        Numbers::Integers => ("BIGINT", "pa.int64()"),
        Numbers::Doubles => ("DOUBLE", "pa.float64()"),
    };
    vec![
        v1::Remedy::new(
            "spark",
            format!("CAST(`{leaf}` AS {sql_type}) in the write's SELECT"),
        ),
        v1::Remedy::new(
            "duckdb",
            format!("CAST(\"{leaf}\" AS {sql_type}) in the COPY ... SELECT"),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!("column_types={{{}: {arrow_type}}}", py_string(leaf)),
        ),
    ]
}

/// The setting that stores one column's numbers as numbers, in the detected
/// writer's terms.
fn remedy(writer: Writer, leaf: &str, numbers: Numbers) -> String {
    let remedies = remedies(leaf, numbers);
    let setting = |engine: &str| super::setting(&remedies, engine);
    match writer {
        Writer::ParquetMr => setting("spark"),
        Writer::Photon => format!("{} (Photon forwarding unverified)", setting("spark")),
        Writer::ArrowCpp => setting("pyarrow"),
        Writer::ArrowRs => "parse the text with `str::parse` before the writer sees it".to_string(),
        Writer::Other => setting("duckdb"),
    }
}

#[cfg(test)]
mod tests {
    use super::{detect, measure, parse_number, remedy, Number, NUMERIC_TEXT_RULE};
    use crate::lint::writer::Writer;
    use crate::lint::{cells, ColumnSample};
    use crate::v1;

    fn column(codec: &str, logical_type: Option<&str>) -> v1::Column {
        v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            max_repetition_level: Some(0),
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                logical_type: logical_type.map(str::to_string),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// Only the spellings a value round-trips through are numbers.
    #[test]
    fn only_canonical_numbers_parse() {
        assert!(matches!(
            parse_number(b"100001"),
            Some(Number::Integer(100_001))
        ));
        assert!(matches!(parse_number(b"0"), Some(Number::Integer(0))));
        assert!(matches!(parse_number(b"-42"), Some(Number::Integer(-42))));
        assert!(matches!(parse_number(b"12.34"), Some(Number::Double(_))));
        assert!(matches!(parse_number(b"-0.5"), Some(Number::Double(_))));

        for text in [
            "00123",
            "+123",
            " 123",
            "123 ",
            "-0",
            "1e5",
            "1E5",
            "NaN",
            "inf",
            "-inf",
            "0x10",
            "1,234",
            "12.",
            ".5",
            "1.5.5",
            "",
            "123abc",
            "99999999999999999999",
        ] {
            assert!(
                parse_number(text.as_bytes()).is_none(),
                "{text} is not canonical numeric text"
            );
        }
    }

    /// Canonical integer text is a candidate, and the integer arm is smaller
    /// than the text's length prefix and digits.
    #[test]
    fn integer_text_earns_an_int64() {
        let column = column("UNCOMPRESSED", Some("STRING"));
        let values: Vec<String> = (0..2_000)
            .map(|value| (100_001 + value).to_string())
            .collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);

        let finding = detect(&sample).expect("canonical integer text is a candidate");
        assert!(finding.message.contains("B/value"));
        assert!(finding.message.contains("integers"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.1,
            "eight bytes beat a length prefix and six digits, got {}",
            measurement.ratio_low
        );
    }

    /// Canonical decimal text is a candidate for a `DOUBLE`.
    #[test]
    fn decimal_text_earns_a_double() {
        let column = column("UNCOMPRESSED", Some("STRING"));
        // Every value is its own shortest round-tripping spelling, and most
        // carry a point: `1000000.25`, `1000000.26`, …, `1000000.5`, …
        let values: Vec<String> = (0..2_000)
            .map(|value| (1_000_000.25 + f64::from(value) * 0.01).to_string())
            .collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);

        let finding = detect(&sample).expect("canonical decimal text is a candidate");
        assert!(finding.message.contains("decimal point"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.25,
            "eight bytes beat the point and its characters, got {}",
            measurement.ratio_low
        );
    }

    /// Identifiers and formatted strings are not numbers: a leading zero, a
    /// sign, padding, or mixed text keeps the column out.
    #[test]
    fn padded_ids_and_mixed_text_are_not_candidates() {
        let column = column("UNCOMPRESSED", Some("STRING"));

        let padded: Vec<String> = (0..2_000).map(|value| format!("{value:06}")).collect();
        let padded_cells = cells(&padded);
        assert!(detect(&ColumnSample::for_test(&column, &padded_cells, false)).is_none());

        let signed: Vec<String> = (0..2_000).map(|value| format!("+{value}")).collect();
        let signed_cells = cells(&signed);
        assert!(detect(&ColumnSample::for_test(&column, &signed_cells, false)).is_none());

        let mut mixed: Vec<String> = (0..2_000).map(|value| value.to_string()).collect();
        mixed[17] = "unknown".to_string();
        let mixed_cells = cells(&mixed);
        assert!(detect(&ColumnSample::for_test(&column, &mixed_cells, false)).is_some());
        assert!(
            measure(&ColumnSample::for_test(&column, &mixed_cells, false))
                .expect("measure")
                .is_none(),
            "a stray text value is not the same column rewritten"
        );
    }

    /// A JSON or binary leaf spells something other than a number, and a
    /// repeated leaf's values are nested.
    #[test]
    fn only_scalar_text_leaves_are_candidates() {
        let values: Vec<String> = (0..2_000)
            .map(|value| (100_001 + value).to_string())
            .collect();
        let cells = cells(&values);

        let json = column("UNCOMPRESSED", Some("JSON"));
        assert!(detect(&ColumnSample::for_test(&json, &cells, false)).is_none());

        let mut repeated = column("UNCOMPRESSED", Some("STRING"));
        repeated.max_repetition_level = Some(1);
        assert!(detect(&ColumnSample::for_test(&repeated, &cells, false)).is_none());

        // A text leaf whose logical type the footer omits is still text.
        let untyped = column("UNCOMPRESSED", None);
        assert!(detect(&ColumnSample::for_test(&untyped, &cells, false)).is_some());
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "id", super::Numbers::Integers).contains("BIGINT"));
        assert!(remedy(Writer::ParquetMr, "id", super::Numbers::Doubles).contains("DOUBLE"));
        assert!(remedy(Writer::ArrowCpp, "id", super::Numbers::Doubles).contains("pa.float64()"));
        assert!(remedy(Writer::Photon, "id", super::Numbers::Integers).contains("unverified"));
        assert!(remedy(Writer::Other, "id", super::Numbers::Integers).contains("BIGINT"));
        assert_eq!(NUMERIC_TEXT_RULE, "numeric-text");
    }
}

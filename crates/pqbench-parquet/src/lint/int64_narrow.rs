//! `int64-narrow`: an `INT64` column whose whole value range fits `INT32`,
//! priced as `INT32` against the same `INT64` values.
//!
//! Parquet has no `INT16`, and nothing in the format lets an `INT64` declaration
//! cost less than eight bytes a value, so a count, a year, or a shop id stored
//! as `INT64` pays twice what it needs to. The rule reads the sample's values:
//! only a column whose minimum and maximum both fit `INT32` can be narrowed, and
//! only a plain integer is a candidate — a timestamp, a time, a date, or a
//! decimal is an encoded integer, and narrowing it would change what it means.
//! The control writes the same values at both widths, so a dictionary or a
//! Delta encoding that already shrinks the `INT64` column reports no saving and
//! the rule is suppressed.

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{Settings, Values};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const INT64_NARROW_RULE: &str = "int64-narrow";

/// The finding this column earns when every sampled `INT64` value fits `INT32`.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if sample.physical() != "INT64" || !plain_integer(sample) {
        return None;
    }
    let (first_value, last_value) = narrowable(sample.int64_values)?;
    Some(Finding {
        message: format!(
            "INT64 values between {first_value} and {last_value} at {:.2} B/value \
             fit in INT32",
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf),
        remedies: remedies(sample.leaf),
        measure,
    })
}

/// Whether the leaf is a plain integer: an omitted logical type, or the
/// integer annotation a writer adds to a narrowed column. A timestamp, time,
/// date, decimal, or UUID is an encoded value, not a count.
fn plain_integer(sample: &ColumnSample<'_>) -> bool {
    sample.scalar()
        && matches!(
            sample.mass.logical_type.as_deref(),
            None | Some("INT(64,true)")
                | Some("INT(32,true)")
                | Some("INT(64,false)")
                | Some("INT(32,false)")
        )
}

/// The sample's range, when every non-null value fits `INT32`.
fn narrowable(values: &[Option<i64>]) -> Option<(i64, i64)> {
    let mut first_value = i64::MAX;
    let mut last_value = i64::MIN;
    let mut seen = false;
    for value in values.iter().flatten() {
        first_value = first_value.min(*value);
        last_value = last_value.max(*value);
        seen = true;
    }
    seen.then_some((first_value, last_value))
        .filter(|(first, last)| *first >= i64::from(i32::MIN) && *last <= i64::from(i32::MAX))
}

/// Price the narrowed values against the source's own values, both written by
/// the same writer at the file's codec and level and with its own encoding.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    if narrowable(sample.int64_values).is_none() {
        return Ok(None);
    }
    let mut narrowed: Vec<Option<i32>> = Vec::with_capacity(sample.int64_values.len());
    for value in sample.int64_values {
        match value {
            Some(value) => match i32::try_from(*value) {
                Ok(value) => narrowed.push(Some(value)),
                Err(_) => return Ok(None),
            },
            None => narrowed.push(None),
        }
    }
    let settings = Settings {
        dictionary: sample.chunk_has_dictionary,
        dictionary_page_size_limit: sample.dictionary_page_size_limit(),
        data_page_size_limit: None,
        encoding: control::integer_encoding(sample.mass),
        version: parquet::file::properties::WriterVersion::PARQUET_1_0,
    };
    control::compare(
        sample,
        &Arm {
            values: Values::Int64(sample.int64_values),
            settings,
        },
        &Arm {
            values: Values::Int32(&narrowed),
            settings,
        },
    )
}

/// The settings that store one column's integers at their narrowest width, one
/// per engine.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            format!("ALTER TABLE <table> ALTER COLUMN `{leaf}` TYPE INT"),
        ),
        v1::Remedy::new(
            "duckdb",
            format!("CAST(\"{leaf}\" AS INTEGER) in the COPY ... SELECT"),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!("column_types={{{}: pa.int32()}}", super::py_string(leaf)),
        ),
        v1::Remedy::new(
            "iceberg",
            format!("ALTER TABLE <table> ALTER COLUMN {leaf} TYPE int"),
        ),
    ]
}

/// The setting that narrows one column's integers, in the detected writer's
/// terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    let remedies = remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    match writer {
        Writer::ParquetMr => setting("spark"),
        Writer::Photon => format!("{} (Photon forwarding unverified)", setting("spark")),
        Writer::ArrowCpp => setting("pyarrow"),
        Writer::ArrowRs => format!("write {leaf} as INT32"),
        Writer::Other => setting("iceberg"),
    }
}

#[cfg(test)]
mod tests {
    use super::{detect, measure, narrowable, remedy};
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(logical_type: Option<&str>) -> v1::Column {
        v1::Column {
            physical_data_type: Some("INT64".into()),
            max_repetition_level: Some(0),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                logical_type: logical_type.map(str::to_string),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// An `INT64` column of small values is a candidate, and the controlled
    /// rewrite halves its width.
    #[test]
    fn a_narrow_int64_earns_an_int32() {
        let leaf = column(None);
        let values: Vec<Option<i64>> = (0..2_000).map(|value| Some(40_000 + value)).collect();
        let sample = ColumnSample::for_test_int64(&leaf, &values, false);

        let finding = detect(&sample).expect("a narrow INT64 is a candidate");
        assert!(finding.message.contains("INT32"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.45,
            "four bytes against eight, got {}",
            measurement.ratio_low
        );
    }

    /// A value past the `INT32` range keeps the column an `INT64`, whatever the
    /// rest of its values are.
    #[test]
    fn a_wide_value_keeps_the_column_wide() {
        let leaf = column(None);
        let mut values: Vec<Option<i64>> = (0..2_000).map(Some).collect();
        values[1_000] = Some(i64::from(i32::MAX) + 1);
        let sample = ColumnSample::for_test_int64(&leaf, &values, false);
        assert!(detect(&sample).is_none());
        assert!(measure(&sample).expect("measure").is_none());

        let negative: Vec<Option<i64>> = (0..2_000)
            .map(|value| Some(i64::from(i32::MIN) - 1 - value))
            .collect();
        assert!(detect(&ColumnSample::for_test_int64(&leaf, &negative, false)).is_none());
    }

    /// A timestamp, time, date, or decimal is an encoded integer whose width is
    /// part of its meaning, so it is never narrowed.
    #[test]
    fn an_encoded_integer_is_not_candidate() {
        let values: Vec<Option<i64>> = (0..2_000).map(|value| Some(1_700_000 + value)).collect();
        for logical_type in [
            "TIMESTAMP(MICROS,UTC)",
            "TIMESTAMP(MILLIS,LOCAL)",
            "TIME(MICROS,LOCAL)",
            "DATE",
            "DECIMAL(9,2)",
            "UUID",
        ] {
            assert!(
                detect(&ColumnSample::for_test_int64(
                    &column(Some(logical_type)),
                    &values,
                    false
                ))
                .is_none(),
                "{logical_type}"
            );
        }
        // The integer annotation a narrowed column carries is still a plain
        // integer.
        assert!(detect(&ColumnSample::for_test_int64(
            &column(Some("INT(64,true)")),
            &values,
            false
        ))
        .is_some());
    }

    /// The range check is over the non-null values, and needs at least one.
    #[test]
    fn only_the_values_in_range_are_narrowable() {
        assert_eq!(narrowable(&[None, None]), None);
        assert_eq!(narrowable(&[None, Some(7), Some(-9)]), Some((-9, 7)));
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "id").contains("INT"));
        assert!(remedy(Writer::ArrowCpp, "id").contains("pa.int32()"));
        assert!(remedy(Writer::ArrowCpp, "request.ext").contains("'request.ext'"));
        assert!(remedy(Writer::Photon, "id").contains("unverified"));
        assert!(remedy(Writer::Other, "id").contains("TYPE int"));
    }
}

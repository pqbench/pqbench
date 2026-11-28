//! `delta-integer`: dense or monotone `INT32`/`INT64` values written without
//! `DELTA_BINARY_PACKED`, priced against the source's own encoding.
//!
//! Delta binary packing stores the difference between neighbours, so a counter,
//! an identifier, or a timestamp costs a few bits a value where `PLAIN` costs
//! four or eight bytes. The rule only proposes the encoding for values whose
//! neighbours are close: a column of random identifiers has deltas as wide as
//! its values, and the control then prices the change at no saving.

use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::Settings;
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DELTA_INTEGER_RULE: &str = "delta-integer";

/// Values are dense when at least this fraction of the sampled non-null values
/// are distinct: a column of a few repeated values belongs to the dictionary
/// rules, not to an encoding for wide ranges.
const DENSE_FRACTION: f64 = 0.5;

/// The mean absolute value must be at least this many times the mean absolute
/// step between neighbours. Sequential ids and timestamps pass by orders of
/// magnitude; random identifiers sit at about one.
const DELTA_MAGNITUDE_DIVISOR: i128 = 16;

/// The finding this column earns when its leaf is an `INT32` or `INT64` whose
/// neighbours are close and whose chunk is not already Delta binary packed.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !matches!(sample.physical(), "INT32" | "INT64") {
        return None;
    }
    if control::records(sample.mass, Encoding::DELTA_BINARY_PACKED) {
        return None;
    }
    let values = sample.integers()?;
    if values.iter().filter(|value| value.is_some()).count() < 2
        || sample.distinct_fraction() < DENSE_FRACTION
    {
        return None;
    }
    if !neighbours_are_close(&values) {
        return None;
    }
    Some(Finding {
        message: format!(
            "dense {} values at {:.2} B/value without DELTA_BINARY_PACKED",
            sample.physical(),
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf),
        remedies: remedies(sample.leaf),
        measure,
    })
}

/// Whether the non-null values' steps are small against their magnitudes.
///
/// Both sums are taken over the same neighbours, so the comparison does not
/// depend on the sample's length. An `i64` magnitude is summed in `i128` so
/// `i64::MIN` cannot overflow.
fn neighbours_are_close(values: &[Option<i64>]) -> bool {
    let mut previous: Option<i64> = None;
    let mut step_total: i128 = 0;
    let mut magnitude_total: i128 = 0;
    let mut steps = 0i128;
    for value in values.iter().flatten() {
        magnitude_total += i128::from(*value).abs();
        if let Some(previous) = previous {
            step_total += (i128::from(*value) - i128::from(previous)).abs();
            steps += 1;
        }
        previous = Some(*value);
    }
    if steps == 0 || magnitude_total == 0 {
        return false;
    }
    // Compare the means without dividing: `step_total / steps` against
    // `magnitude_total / steps / DELTA_MAGNITUDE_DIVISOR`.
    step_total.saturating_mul(DELTA_MAGNITUDE_DIVISOR) <= magnitude_total
}

/// Price `DELTA_BINARY_PACKED` with the dictionary off against the source's own
/// encoding and dictionary state, both written at the file's codec and level.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let Some(values) = sample.values() else {
        return Ok(None);
    };
    let baseline = Arm {
        values,
        settings: Settings {
            dictionary: sample.chunk_has_dictionary,
            dictionary_page_size_limit: sample.dictionary_page_size_limit(),
            data_page_size_limit: None,
            // The rule never detects a chunk that already records
            // `DELTA_BINARY_PACKED`, so the source's data pages are PLAIN (or
            // dictionary encoded, which the dictionary arm reproduces).
            encoding: None,
            version: WriterVersion::PARQUET_1_0,
        },
    };
    let candidate = Arm {
        values,
        settings: Settings {
            dictionary: false,
            dictionary_page_size_limit: None,
            data_page_size_limit: None,
            encoding: Some(Encoding::DELTA_BINARY_PACKED),
            version: WriterVersion::PARQUET_1_0,
        },
    };
    control::compare(sample, &baseline, &candidate)
}

/// The settings that write one integer column with `DELTA_BINARY_PACKED`, one
/// per engine. Every engine that has a page version uses its 2.0 version, which
/// is what selects Delta encodings, and turns the dictionary off for the column
/// so the encoding is what the values leave.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new("spark", "spark.hadoop.parquet.writer.version=PARQUET_2_0"),
        v1::Remedy::new(
            "pyarrow",
            format!("use_dictionary=False, column_encoding={{{leaf}: 'DELTA_BINARY_PACKED'}}"),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!("set_column_encoding({leaf:?}, Encoding::DELTA_BINARY_PACKED)"),
        ),
        v1::Remedy::new("delta", "parquet.writer.version=PARQUET_2_0"),
        v1::Remedy::new(
            "iceberg",
            format!(
                "write.parquet.page-version=v2 with \
                 write.parquet.dict-encoding-enabled.column.{leaf}=false"
            ),
        ),
        v1::Remedy::new("duckdb", "parquet_version='V2'"),
    ]
}

/// The setting that Delta-encodes one integer column, in the detected writer's
/// terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    let remedies = remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    match writer {
        Writer::ParquetMr => format!("write the file at {}", setting("spark")),
        Writer::Photon => format!(
            "write the file at {} (Photon forwarding unverified)",
            setting("delta")
        ),
        Writer::ArrowCpp => setting("pyarrow"),
        Writer::ArrowRs => setting("parquet-rs"),
        Writer::Other => format!(
            "write the column Delta binary packed ({})",
            setting("iceberg")
        ),
    }
}

#[cfg(test)]
mod tests {
    use parquet::basic::Encoding;

    use super::{detect, measure, neighbours_are_close, remedy};
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(physical_data_type: &str, codec: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some(physical_data_type.into()),
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    fn sequential(len: usize) -> Vec<Option<i64>> {
        (0..len).map(|value| Some(value as i64)).collect()
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "id").contains("PARQUET_2_0"));
        assert!(!remedy(Writer::ParquetMr, "id").contains("spark.sql"));
        assert!(remedy(Writer::ArrowCpp, "id").contains("DELTA_BINARY_PACKED"));
        assert!(remedy(Writer::ArrowRs, "id").contains("set_column_encoding"));
        assert!(remedy(Writer::Other, "id").contains("write.parquet.page-version=v2"));
    }

    /// Sequential and monotone values have close neighbours; random ones do
    /// not.
    #[test]
    fn only_close_neighbours_are_candidates() {
        assert!(neighbours_are_close(&sequential(1_000)));
        let descending: Vec<Option<i64>> = (0..1_000).map(|value| Some(-value)).collect();
        assert!(neighbours_are_close(&descending));
        let timestamps: Vec<Option<i64>> = (0..1_000)
            .map(|value| Some(1_700_000_000_000_000 + value * 1_000_000))
            .collect();
        assert!(neighbours_are_close(&timestamps));

        let mut state = 0x9e37_79b9_u64;
        let random: Vec<Option<i64>> = (0..1_000)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                Some(state as i64)
            })
            .collect();
        assert!(!neighbours_are_close(&random));
        assert!(!neighbours_are_close(&[Some(7)]));
    }

    /// A dense integer column is a candidate, and Delta binary packing beats
    /// PLAIN's eight bytes a value.
    #[test]
    fn dense_integers_without_delta_are_priced() {
        let leaf = column("INT64", "UNCOMPRESSED");
        let values = sequential(10_000);
        let sample = ColumnSample::for_test_int64(&leaf, &values, false);

        let finding = detect(&sample).expect("dense integers are a candidate");
        assert!(finding.message.contains("DELTA_BINARY_PACKED"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.8,
            "packed deltas must beat eight bytes a value, got {}",
            measurement.ratio_low
        );
    }

    /// An `INT32` leaf is a candidate too.
    #[test]
    fn int32_values_are_priced_as_int32() {
        let column = column("INT32", "UNCOMPRESSED");
        let values: Vec<Option<i32>> = (0..10_000).map(Some).collect();
        let sample = ColumnSample::for_test_int32(&column, &values, false);
        assert!(detect(&sample).is_some());
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(measurement.ratio_low > 0.8, "{}", measurement.ratio_low);
    }

    /// Repeated, random, and already-Delta columns are not candidates.
    #[test]
    fn repeated_random_and_delta_columns_are_not_candidates() {
        let leaf = column("INT64", "UNCOMPRESSED");
        let repeated: Vec<Option<i64>> = (0..1_000).map(|value| Some(value % 3)).collect();
        assert!(detect(&ColumnSample::for_test_int64(&leaf, &repeated, false)).is_none());

        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let random: Vec<Option<i64>> = (0..1_000)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                Some(state as i64)
            })
            .collect();
        assert!(detect(&ColumnSample::for_test_int64(&leaf, &random, false)).is_none());

        let mut delta = column("INT64", "UNCOMPRESSED");
        delta.mass = Some(v1::Bytemass {
            codec: Some("UNCOMPRESSED".into()),
            encodings: vec!["PLAIN".into(), Encoding::DELTA_BINARY_PACKED.to_string()],
            ..v1::Bytemass::default()
        });
        assert!(detect(&ColumnSample::for_test_int64(
            &delta,
            &sequential(1_000),
            false
        ))
        .is_none());

        let text = column("BYTE_ARRAY", "UNCOMPRESSED");
        let values = sequential(1_000);
        let cells: Vec<Option<parquet::data_type::ByteArray>> = values
            .iter()
            .map(|value| {
                Some(parquet::data_type::ByteArray::from(
                    value.unwrap().to_string().into_bytes(),
                ))
            })
            .collect();
        assert!(detect(&ColumnSample::for_test(&text, &cells, false)).is_none());
    }
}

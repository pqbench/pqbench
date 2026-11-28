//! `dictionary-unique`: a dictionary page on nearly-unique values, priced
//! against the same writer with the dictionary off. `dictionary-missing`: the
//! same values written without a dictionary, priced with one on.
//!
//! Both rules cover every physical type a dictionary can pay on — `BYTE_ARRAY`,
//! `FIXED_LEN_BYTE_ARRAY`, `INT32`, `INT64`, `FLOAT`, and `DOUBLE` — because a
//! dictionary is a per-chunk value table whatever the values' width. The
//! pricing arms write each type at its own physical width, so a numeric column
//! is compared as numbers, not as bytes.

use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{Settings, Values};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DICTIONARY_UNIQUE_RULE: &str = "dictionary-unique";

/// A dictionary page on values that are nearly unique costs more than it saves.
pub const DICTIONARY_MISSING_RULE: &str = "dictionary-missing";

/// Values are nearly unique when at least this fraction of the sampled
/// non-null values are distinct within the chunk.
const UNIQUE_FRACTION: f64 = 0.5;

/// Values repeat enough for a dictionary when fewer than this fraction of the
/// sampled non-null values are distinct within the chunk.
const REPEATED_FRACTION: f64 = 0.1;

/// The fewest sampled non-null values a dictionary decision rests on. A chunk
/// with a handful of values has no repetition for a dictionary page to
/// amortize over, so the page alone would decide the comparison and every such
/// chunk would earn the same, degenerate finding.
const MINIMUM_VALUES: usize = 32;

/// The finding this column earns when its own chunk carries a dictionary page
/// and the values are nearly unique.
pub(super) fn detect_unique(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !dictionary_leaf(sample)
        || !sample.chunk_has_dictionary
        || sample.distinct_fraction() < UNIQUE_FRACTION
    {
        return None;
    }
    let leaf = sample.leaf;
    Some(Finding {
        message: format!(
            "a dictionary on nearly-unique {} values at {:.2} B/value",
            sample.physical(),
            sample.text_bpv
        ),
        remedy: unique_remedy(sample.writer_kind(), leaf),
        remedies: unique_remedies(leaf),
        measure: measure_unique,
    })
}

/// The finding this column earns when its own chunk has no dictionary page and
/// the values repeat enough for one to pay.
pub(super) fn detect_missing(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !dictionary_leaf(sample) || sample.chunk_has_dictionary {
        return None;
    }
    let fraction = sample.distinct_fraction();
    if fraction >= REPEATED_FRACTION {
        return None;
    }
    let leaf = sample.leaf;
    Some(Finding {
        message: format!(
            "only {:.1}% of sampled {} values are distinct at {:.2} B/value",
            fraction * 100.0,
            sample.physical(),
            sample.text_bpv
        ),
        remedy: missing_remedy(sample.writer_kind(), leaf),
        remedies: missing_remedies(leaf),
        measure: measure_missing,
    })
}

/// Whether the leaf is a physical type a dictionary can pay on, over enough
/// values for the comparison to mean anything. A `BOOLEAN` leaf is not: Parquet
/// has no dictionary for one, and parquet-rs writes `RLE` whatever the settings
/// say.
fn dictionary_leaf(sample: &ColumnSample<'_>) -> bool {
    matches!(
        sample.physical(),
        "BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY" | "INT32" | "INT64" | "FLOAT" | "DOUBLE"
    ) && sample.values().is_some()
        && sample.sampled_values() >= MINIMUM_VALUES
}

/// Price the dictionary arm against the same values with the dictionary off,
/// both written by the same writer at the file's codec and level.
fn measure_unique(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let (Some(baseline), Some(candidate)) = (sheet(sample, true), sheet(sample, false)) else {
        return Ok(None);
    };
    control::compare(sample, &baseline, &candidate)
}

/// Price the same values without a dictionary against the same values with
/// one, both written by the same writer at the file's codec and level.
fn measure_missing(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let (Some(baseline), Some(candidate)) = (sheet(sample, false), sheet(sample, true)) else {
        return Ok(None);
    };
    control::compare(sample, &baseline, &candidate)
}

/// One arm over the sample's values, with the dictionary on or off.
///
/// A `FIXED_LEN_BYTE_ARRAY` leaf writes both arms under the 2.0 page version:
/// parquet-rs mirrors parquet-mr in having no `FIXED_LEN_BYTE_ARRAY` dictionary
/// under the 1.0 version, so a v1 pair would silently compare PLAIN against
/// PLAIN.
fn sheet<'a>(sample: &ColumnSample<'a>, dictionary: bool) -> Option<Arm<'a>> {
    let values = sample.values()?;
    let version = match values {
        Values::FixedLen(..) => WriterVersion::PARQUET_2_0,
        _ => WriterVersion::PARQUET_1_0,
    };
    let encoding = if dictionary {
        None
    } else {
        match (control::source_encoding(sample.mass), version) {
            // A v2 writer chooses DELTA_BYTE_ARRAY for a fixed-length leaf when
            // no encoding is named; the source's PLAIN pages are reproduced
            // explicitly so the pair differs only in the dictionary.
            (None, WriterVersion::PARQUET_2_0) => Some(Encoding::PLAIN),
            (recorded, _) => recorded,
        }
    };
    Some(Arm {
        values,
        settings: Settings {
            dictionary,
            dictionary_page_size_limit: if dictionary {
                sample.dictionary_page_size_limit()
            } else {
                None
            },
            data_page_size_limit: None,
            encoding,
            version,
        },
    })
}

/// The settings that disable one column's dictionary, one per engine.
fn unique_remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            format!("spark.hadoop.parquet.enable.dictionary#{leaf}=false"),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!("use_dictionary=[<other columns>] (leave {leaf} out)"),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!("set_column_dictionary_enabled({leaf:?}, false)"),
        ),
        v1::Remedy::new("iceberg", "write.parquet.dict-size-bytes=8192"),
        v1::Remedy::new(
            "duckdb",
            "no per-column switch: DuckDB enables the dictionary itself when it pays",
        ),
    ]
}

/// The settings that enable one column's dictionary, one per engine.
fn missing_remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            format!("spark.hadoop.parquet.enable.dictionary#{leaf}=true"),
        ),
        v1::Remedy::new("pyarrow", format!("use_dictionary=[{leaf}]")),
        v1::Remedy::new(
            "parquet-rs",
            format!("set_column_dictionary_enabled({leaf:?}, true)"),
        ),
        v1::Remedy::new("iceberg", "write.parquet.dict-size-bytes=2097152"),
        v1::Remedy::new(
            "duckdb",
            "no per-column switch: DuckDB enables the dictionary itself when it pays",
        ),
    ]
}

/// The setting that disables one column's dictionary, in the detected writer's
/// terms.
fn unique_remedy(writer: Writer, leaf: &str) -> String {
    let remedies = unique_remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    primary_remedy(writer, "disable", &setting)
}

/// The setting that enables one column's dictionary, in the detected writer's
/// terms.
fn missing_remedy(writer: Writer, leaf: &str) -> String {
    let remedies = missing_remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    primary_remedy(writer, "enable", &setting)
}

/// The one-line remedy for a writer: the column's dictionary, then the
/// writer's own setting.
fn primary_remedy(writer: Writer, verb: &str, setting: &dyn Fn(&str) -> String) -> String {
    let lead = format!("{verb} the dictionary for this column");
    match writer {
        Writer::ParquetMr => format!("{lead}: {}", setting("spark")),
        Writer::ArrowCpp => format!("{lead}: {}", setting("pyarrow")),
        Writer::ArrowRs => format!("{lead}: {}", setting("parquet-rs")),
        Writer::Photon => format!(
            "{lead}: {} (Photon forwarding unverified)",
            setting("spark")
        ),
        Writer::Other => format!("{lead} at write"),
    }
}

#[cfg(test)]
mod tests {
    use parquet::data_type::ByteArray;

    use super::{detect_missing, detect_unique, measure_missing, missing_remedy, unique_remedy};
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

    fn cells(values: &[String]) -> Vec<Option<ByteArray>> {
        values
            .iter()
            .map(|value| Some(ByteArray::from(value.clone().into_bytes())))
            .collect()
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert_eq!(
            unique_remedy(Writer::ParquetMr, "id"),
            "disable the dictionary for this column: spark.hadoop.parquet.enable.dictionary#id=false"
        );
        assert_eq!(
            unique_remedy(Writer::ArrowRs, "id"),
            "disable the dictionary for this column: set_column_dictionary_enabled(\"id\", false)"
        );
        assert!(unique_remedy(Writer::ArrowCpp, "id").contains("use_dictionary"));
        assert!(unique_remedy(Writer::Photon, "id").contains("parquet.enable.dictionary#id=false"));
        assert!(unique_remedy(Writer::Photon, "id").contains("unverified"));
        assert!(unique_remedy(Writer::Other, "id").contains("at write"));
        assert!(missing_remedy(Writer::ParquetMr, "id").contains("=true"));
        assert_eq!(
            missing_remedy(Writer::ArrowRs, "id"),
            "enable the dictionary for this column: set_column_dictionary_enabled(\"id\", true)"
        );
    }

    /// The rule reads the sample chunk's own dictionary page, not any chunk's.
    #[test]
    fn a_chunk_without_a_dictionary_page_is_not_a_candidate() {
        let column = column("BYTE_ARRAY", "ZSTD");
        let values: Vec<String> = (0..100).map(|value| format!("value-{value}")).collect();
        let cells = cells(&values);

        let with_dictionary = ColumnSample::for_test(&column, &cells, true);
        assert!(detect_unique(&with_dictionary).is_some());

        let without_dictionary = ColumnSample::for_test(&column, &cells, false);
        assert!(detect_unique(&without_dictionary).is_none());
    }

    /// A column without a dictionary whose values repeat within a chunk is a
    /// candidate, and the control shows the dictionary pays.
    #[test]
    fn repeated_values_without_a_dictionary_earn_one() {
        let column = v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                compressed_bytes: Some(400_000),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        };
        let values: Vec<String> = (0..10_000)
            .map(|value| format!("region-{:02}", value % 50))
            .collect();
        let cells = cells(&values);

        let sample = ColumnSample::for_test(&column, &cells, false);
        let finding = detect_missing(&sample).expect("repeated values are a candidate");
        assert!(finding.message.contains("distinct"));
        let measurement = measure_missing(&sample)
            .expect("measure")
            .expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.5,
            "a dictionary on 50 distinct values saves most of the bytes, got {}",
            measurement.ratio_low
        );
    }

    /// A chunk of a handful of values is no dictionary candidate either way:
    /// there is no repetition to amortize a page over, so the page alone would
    /// decide the comparison.
    #[test]
    fn a_handful_of_values_is_not_a_dictionary_candidate() {
        let column = column("INT64", "UNCOMPRESSED");
        let few: Vec<Option<i64>> = (0..8).map(|value| Some(i64::from(value))).collect();

        let with_dictionary = ColumnSample::for_test_int64(&column, &few, true);
        assert!(detect_unique(&with_dictionary).is_none());

        let repeated: Vec<Option<i64>> = (0..8).map(|value| Some(value % 2)).collect();
        let without_dictionary = ColumnSample::for_test_int64(&column, &repeated, false);
        assert!(detect_missing(&without_dictionary).is_none());
    }

    /// Nearly-unique values are not a candidate for a dictionary.
    #[test]
    fn nearly_unique_values_do_not_earn_a_dictionary() {
        let column = column("BYTE_ARRAY", "ZSTD");
        let values: Vec<String> = (0..500).map(|value| format!("value-{value}")).collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);
        assert!(detect_missing(&sample).is_none());
    }

    /// An `INT64` column of a few repeated values earns a dictionary, and the
    /// control prices it as numbers rather than as text.
    #[test]
    fn repeated_integers_earn_a_dictionary() {
        let column = v1::Column {
            physical_data_type: Some("INT64".into()),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                compressed_bytes: Some(160_000),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        };
        let values: Vec<Option<i64>> = (0..10_000).map(|value| Some(value % 50)).collect();
        let sample = ColumnSample::for_test_int64(&column, &values, false);

        assert!(detect_missing(&sample).is_some());
        let measurement = measure_missing(&sample)
            .expect("measure")
            .expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.5,
            "a dictionary on 50 distinct integers saves most of eight bytes a value, got {}",
            measurement.ratio_low
        );
    }

    /// A `DOUBLE` dictionary on nearly-unique values costs more than it saves,
    /// so the rule detects it and the control proves removing it pays.
    #[test]
    fn a_dictionary_on_unique_doubles_is_priced_as_removable() {
        let column = column("DOUBLE", "UNCOMPRESSED");
        let values: Vec<Option<f64>> = (0..2_000).map(|value| Some(value as f64 * 1.5)).collect();
        let sample = ColumnSample::for_test_double(&column, &values, true);

        assert!(detect_unique(&sample).is_some());
        let measurement = super::measure_unique(&sample)
            .expect("measure")
            .expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.0,
            "a dictionary of 2,000 doubles must cost more than the values, got {}",
            measurement.ratio_low
        );
    }

    /// A `FLOAT` column of a few repeated values earns a dictionary at its own
    /// four-byte width.
    #[test]
    fn repeated_floats_earn_a_dictionary() {
        let column = v1::Column {
            physical_data_type: Some("FLOAT".into()),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                compressed_bytes: Some(160_000),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        };
        let values: Vec<Option<f32>> = (0..10_000).map(|value| Some((value % 50) as f32)).collect();
        let sample = ColumnSample::for_test_float(&column, &values, false);

        assert!(detect_missing(&sample).is_some());
        let measurement = measure_missing(&sample)
            .expect("measure")
            .expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.5,
            "a dictionary on 50 distinct floats saves most of four bytes a value, got {}",
            measurement.ratio_low
        );
    }

    /// A `FIXED_LEN_BYTE_ARRAY` dictionary pays on repeated values, and the
    /// control writes v2 pages so the dictionary exists at all.
    #[test]
    fn repeated_fixed_length_values_earn_a_dictionary() {
        let column = v1::Column {
            physical_data_type: Some("FIXED_LEN_BYTE_ARRAY".into()),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                compressed_bytes: Some(160_000),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        };
        let values: Vec<Option<ByteArray>> = (0..2_000)
            .map(|value| Some(ByteArray::from(vec![(value % 16) as u8; 16])))
            .collect();
        let sample = ColumnSample::for_test(&column, &values, false);

        assert!(detect_missing(&sample).is_some());
        let measurement = measure_missing(&sample)
            .expect("measure")
            .expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.5,
            "16 distinct values in 2,000 rows must dictionary-encode, got {}",
            measurement.ratio_low
        );
    }
}

//! `dictionary-unique`: a dictionary page on nearly-unique values, priced
//! against the same writer with the dictionary off. `dictionary-missing`: the
//! same values written without a dictionary, priced with one on.

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::decode::unique_fraction;
use super::price::{PhysicalShape, Settings};
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

/// The finding this column earns when its own chunk carries a dictionary page
/// and the values are nearly unique.
pub(super) fn detect_unique(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.byte_leaf()
        || !sample.chunk_has_dictionary
        || unique_fraction(sample.cells) < UNIQUE_FRACTION
    {
        return None;
    }
    let leaf = sample.leaf;
    Some(Finding {
        message: format!(
            "a dictionary on nearly-unique values at {:.2} B/value",
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
    if !sample.byte_leaf() || sample.chunk_has_dictionary {
        return None;
    }
    let fraction = unique_fraction(sample.cells);
    if fraction >= REPEATED_FRACTION {
        return None;
    }
    let leaf = sample.leaf;
    Some(Finding {
        message: format!(
            "only {:.1}% of sampled values are distinct at {:.2} B/value",
            fraction * 100.0,
            sample.text_bpv
        ),
        remedy: missing_remedy(sample.writer_kind(), leaf),
        remedies: missing_remedies(leaf),
        measure: measure_missing,
    })
}

/// Price the dictionary arm against the same values with the dictionary off,
/// both written by the same writer at the file's codec and level.
fn measure_unique(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let baseline = sheet(sample, sample.chunk_has_dictionary);
    let candidate = sheet(sample, false);
    control::compare(sample, &baseline, &candidate)
}

/// Price the same values without a dictionary against the same values with
/// one, both written by the same writer at the file's codec and level.
fn measure_missing(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let baseline = sheet(sample, false);
    let candidate = sheet(sample, true);
    control::compare(sample, &baseline, &candidate)
}

/// One arm over the sample's values, with the dictionary on or off.
fn sheet<'a>(sample: &ColumnSample<'a>, dictionary: bool) -> Arm<'a> {
    Arm {
        cells: sample.cells,
        settings: Settings {
            shape: PhysicalShape::ByteArray,
            dictionary,
            dictionary_page_size_limit: if dictionary {
                sample.dictionary_page_size_limit()
            } else {
                None
            },
            encoding: if dictionary {
                None
            } else {
                control::source_encoding(sample.mass)
            },
        },
    }
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

    fn column() -> v1::Column {
        v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            mass: Some(v1::Bytemass {
                codec: Some("ZSTD".into()),
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
        let column = column();
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

    /// Nearly-unique values are not a candidate for a dictionary.
    #[test]
    fn nearly_unique_values_do_not_earn_a_dictionary() {
        let column = column();
        let values: Vec<String> = (0..500).map(|value| format!("value-{value}")).collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);
        assert!(detect_missing(&sample).is_none());
    }
}

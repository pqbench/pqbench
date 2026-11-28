//! `delta-text-encoding`: nearly-unique text written without a dictionary,
//! priced Delta-length byte-array encoded against the source's own encoding.

use parquet::basic::Encoding;

use super::control::{self, Arm, Measurement};
use super::price::{PhysicalShape, Settings};
use super::unique_fraction;
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DELTA_TEXT_ENCODING_RULE: &str = "delta-text-encoding";

/// Text is nearly unique when at least this fraction of the sampled non-null
/// values are distinct within the chunk.
const UNIQUE_FRACTION: f64 = 0.5;

/// The finding this column earns when its chunk has no dictionary, its values
/// are nearly unique, and its data pages are not already Delta encoded.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.byte_array() || sample.chunk_has_dictionary {
        return None;
    }
    if matches!(
        control::source_encoding(sample.mass),
        Some(Encoding::DELTA_LENGTH_BYTE_ARRAY | Encoding::DELTA_BYTE_ARRAY)
    ) {
        return None;
    }
    if unique_fraction(sample.cells) < UNIQUE_FRACTION {
        return None;
    }
    Some(Finding {
        message: format!(
            "nearly-unique text at {:.2} B/value without a dictionary",
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf),
        measure,
    })
}

/// Price Delta-length encoding against the source's own encoding and
/// dictionary setting, both written at the file's codec and level.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let baseline = Arm {
        cells: sample.cells,
        settings: Settings {
            shape: PhysicalShape::ByteArray,
            dictionary: false,
            dictionary_page_size_limit: None,
            encoding: control::source_encoding(sample.mass),
        },
    };
    let candidate = Arm {
        cells: sample.cells,
        settings: Settings {
            shape: PhysicalShape::ByteArray,
            dictionary: false,
            dictionary_page_size_limit: None,
            encoding: Some(Encoding::DELTA_LENGTH_BYTE_ARRAY),
        },
    };
    control::compare(sample, &baseline, &candidate)
}

/// The setting that Delta-encodes one column's text, in the writer's terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    match writer {
        Writer::ParquetMr => "write the file at parquet.writer.version=PARQUET_2_0".to_string(),
        Writer::ArrowCpp => format!(
            "use_dictionary=False with column_encoding={{{leaf}: 'DELTA_LENGTH_BYTE_ARRAY'}}"
        ),
        Writer::ArrowRs => {
            format!("set_column_encoding({leaf:?}, Encoding::DELTA_LENGTH_BYTE_ARRAY)")
        }
        Writer::Photon => {
            "write the file at parquet.writer.version=PARQUET_2_0 (Photon forwarding unverified)"
                .to_string()
        }
        Writer::Other => "write the column Delta-length byte-array encoded \
             (Iceberg: write.parquet.page-version=v2)"
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use parquet::data_type::ByteArray;

    use super::{detect, measure, remedy};
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(codec: &str, physical_data_type: &str) -> v1::Column {
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
        assert!(remedy(Writer::ParquetMr, "id").contains("PARQUET_2_0"));
        assert!(remedy(Writer::ArrowCpp, "id").contains("DELTA_LENGTH_BYTE_ARRAY"));
        assert!(remedy(Writer::ArrowRs, "id").contains("set_column_encoding"));
        assert!(remedy(Writer::Photon, "id").contains("unverified"));
        assert!(remedy(Writer::Other, "id").contains("write.parquet.page-version=v2"));
    }

    /// Nearly-unique text without a dictionary is a candidate.
    #[test]
    fn nearly_unique_text_without_a_dictionary_is_a_candidate() {
        let column = column("UNCOMPRESSED", "BYTE_ARRAY");
        let values: Vec<String> = (0..1_000).map(|value| format!("{value:016x}")).collect();
        let cells = cells(&values);

        let sample = ColumnSample::for_test(&column, &cells, false);
        let finding = detect(&sample).expect("nearly-unique text is a candidate");
        assert!(finding.message.contains("B/value"));
    }

    /// Repeated text, a dictionary, and an already-Delta column are not
    /// candidates.
    #[test]
    fn a_dictionary_and_repeated_values_are_not_candidates() {
        let text = column("UNCOMPRESSED", "BYTE_ARRAY");
        let unique: Vec<String> = (0..1_000).map(|value| format!("{value:016x}")).collect();
        let cells_unique = cells(&unique);

        let with_dictionary = ColumnSample::for_test(&text, &cells_unique, true);
        assert!(detect(&with_dictionary).is_none());

        let repeated: Vec<String> = (0..1_000)
            .map(|value| format!("{:016x}", value % 10))
            .collect();
        let cells_repeated = cells(&repeated);
        let without_dictionary = ColumnSample::for_test(&text, &cells_repeated, false);
        assert!(detect(&without_dictionary).is_none());

        let mut delta = column("UNCOMPRESSED", "BYTE_ARRAY");
        delta.mass = Some(v1::Bytemass {
            codec: Some("UNCOMPRESSED".into()),
            encodings: vec!["DELTA_LENGTH_BYTE_ARRAY".into()],
            ..v1::Bytemass::default()
        });
        let already_delta = ColumnSample::for_test(&delta, &cells_unique, false);
        assert!(detect(&already_delta).is_none());
    }

    /// Delta-length encoding nearly-unique text beats PLAIN's per-value length
    /// prefix.
    #[test]
    fn delta_length_encoding_saves_on_nearly_unique_text() {
        let column = column("UNCOMPRESSED", "BYTE_ARRAY");
        let values: Vec<String> = (0..2_000).map(|value| format!("{value:08x}")).collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);

        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.1,
            "Delta lengths cost about a byte where PLAIN costs four, got {}",
            measurement.ratio_low
        );
    }
}

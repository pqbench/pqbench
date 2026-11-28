//! `delta-length-byte-array`: variable-length text written `PLAIN`, priced as
//! `DELTA_LENGTH_BYTE_ARRAY`.
//!
//! `PLAIN` stores a four-byte length before every `BYTE_ARRAY` value, whatever
//! its length; `DELTA_LENGTH_BYTE_ARRAY` stores the lengths as a run-encoded,
//! bit-packed sequence and the values' bytes after it. The saving is about three
//! bytes a value, so it pays on text whose values have no shared prefix — that
//! case belongs to `delta-byte-array`, which also codes the prefix. The rule
//! reads the sample's own values: a column is a candidate only when its values
//! vary in length and neighbours share little, and it never reads a column that
//! already uses a Delta encoding or a dictionary (a dictionary page is the
//! writer's own answer to repeated text).

use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::Settings;
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DELTA_LENGTH_RULE: &str = "delta-length-byte-array";

/// The fewest sampled non-null values a column decision rests on.
const MINIMUM_VALUES: usize = 16;

/// The share of a value's bytes that may repeat the previous value's prefix.
/// Above it, `DELTA_BYTE_ARRAY` codes those bytes and this rule is not the
/// rewrite to recommend.
const SHARED_PREFIX_FRACTION: f64 = 0.25;

/// The finding this column earns when its leaf is variable-length `BYTE_ARRAY`
/// text written `PLAIN` with no shared prefixes.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.byte_array()
        || sample.chunk_has_dictionary
        || control::source_encoding(sample.mass).is_some()
        || !sample
            .column
            .max_repetition_level
            .is_none_or(|level| level == 0)
    {
        return None;
    }
    let (variable_length, prefix_fraction) = length_shape(sample)?;
    if !variable_length || prefix_fraction >= SHARED_PREFIX_FRACTION {
        return None;
    }
    Some(Finding {
        message: format!(
            "variable-length text at {:.2} B/value with a PLAIN length prefix \
             ({:.0}% of bytes repeat a neighbour's prefix)",
            sample.text_bpv,
            prefix_fraction * 100.0
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf),
        remedies: remedies(sample.leaf),
        measure,
    })
}

/// Whether the sample's values vary in length, and the share of their bytes
/// that repeat the previous value's prefix, when there are enough of them.
fn length_shape(sample: &ColumnSample<'_>) -> Option<(bool, f64)> {
    let mut values = 0usize;
    let mut shortest = usize::MAX;
    let mut longest = 0usize;
    let mut total = 0usize;
    let mut shared = 0usize;
    let mut previous: Option<&[u8]> = None;
    for bytes in sample.cells().iter().flatten() {
        let value = bytes.data();
        values += 1;
        shortest = shortest.min(value.len());
        longest = longest.max(value.len());
        total += value.len();
        if let Some(previous) = previous {
            shared += common_prefix(previous, value);
        }
        previous = Some(value);
    }
    if values < MINIMUM_VALUES {
        return None;
    }
    let prefix_fraction = shared as f64 / total.max(1) as f64;
    Some((shortest != longest, prefix_fraction))
}

/// The bytes `left` and `right` share from their start.
fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter().zip(right).take_while(|(a, b)| a == b).count()
}

/// Price `DELTA_LENGTH_BYTE_ARRAY` against the source's `PLAIN` encoding, both
/// written by the same writer at the file's codec and level.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let Some(values) = sample.values() else {
        return Ok(None);
    };
    let baseline = Arm {
        values,
        settings: Settings {
            dictionary: false,
            dictionary_page_size_limit: None,
            data_page_size_limit: None,
            // The rule only detects a chunk with no Delta encoding recorded, so
            // the source's data pages are PLAIN.
            encoding: None,
            version: WriterVersion::PARQUET_1_0,
        },
    };
    let candidate = Arm {
        values,
        settings: Settings {
            encoding: Some(Encoding::DELTA_LENGTH_BYTE_ARRAY),
            ..baseline.settings
        },
    };
    control::compare(sample, &baseline, &candidate)
}

/// The settings that store one column's text Delta-length encoded, one per
/// engine.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new("spark", "parquet.writer.version=PARQUET_2_0"),
        v1::Remedy::new(
            "pyarrow",
            format!(
                "use_dictionary=False with column_encoding={{{}: 'DELTA_LENGTH_BYTE_ARRAY'}}",
                super::py_string(leaf)
            ),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!("set_column_encoding({leaf:?}, Encoding::DELTA_LENGTH_BYTE_ARRAY)"),
        ),
        v1::Remedy::new("delta", "parquet.writer.version=PARQUET_2_0"),
        v1::Remedy::new("iceberg", "write.parquet.page-version=v2"),
        v1::Remedy::new("duckdb", "parquet_version='V2'"),
    ]
}

/// The setting that Delta-length encodes one column's text, in the detected
/// writer's terms.
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
            "write the column Delta-length byte-array encoded ({})",
            setting("iceberg")
        ),
    }
}

#[cfg(test)]
mod tests {
    use parquet::basic::Encoding;
    use parquet::data_type::ByteArray;

    use super::{common_prefix, detect, measure, remedy};
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(codec: &str, encodings: &[Encoding]) -> v1::Column {
        v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            max_repetition_level: Some(0),
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                logical_type: Some("STRING".into()),
                encodings: encodings.iter().map(ToString::to_string).collect(),
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

    /// Deterministic mixed-case text of length `len`: no shared prefixes, and
    /// nothing a codec can recover on its own.
    fn random_text(value: u64, len: usize) -> String {
        let mut state = value.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let mut text = String::with_capacity(len);
        while text.len() < len {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let byte = b'a' + (state >> 33) as u8 % 26;
            text.push(byte as char);
        }
        text
    }

    /// Variable-length strings with no shared prefixes are a candidate, and the
    /// control confirms the length prefix's bytes are what it saves.
    #[test]
    fn random_variable_length_text_earns_delta_length() {
        let column = column("UNCOMPRESSED", &[]);
        let values: Vec<String> = (0..2_000)
            .map(|value| random_text(value, 6 + (value as usize % 11)))
            .collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);

        let finding = detect(&sample).expect("variable-length PLAIN text is a candidate");
        assert!(finding.message.contains("length prefix"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.15,
            "three of eleven bytes are the length prefix, got {}",
            measurement.ratio_low
        );
    }

    /// A column already Delta-encoded, or dictionary-encoded, is not a
    /// candidate; nor is a fixed-width one, where the length prefix costs the
    /// same bytes as the run-encoded lengths.
    #[test]
    fn already_encoded_and_fixed_width_text_is_skipped() {
        let values: Vec<String> = (0..2_000)
            .map(|value| random_text(value, 6 + (value as usize % 11)))
            .collect();
        let variable = cells(&values);

        for encodings in [
            &[Encoding::DELTA_LENGTH_BYTE_ARRAY][..],
            &[Encoding::PLAIN, Encoding::DELTA_BYTE_ARRAY][..],
        ] {
            let encoded = column("UNCOMPRESSED", encodings);
            assert!(
                detect(&ColumnSample::for_test(&encoded, &variable, false)).is_none(),
                "{encodings:?}"
            );
        }

        let dictionary = column("UNCOMPRESSED", &[Encoding::PLAIN]);
        assert!(detect(&ColumnSample::for_test(&dictionary, &variable, true)).is_none());

        let fixed: Vec<String> = (0..2_000).map(|value| random_text(value, 12)).collect();
        let fixed_width = cells(&fixed);
        assert!(detect(&ColumnSample::for_test(&dictionary, &fixed_width, false)).is_none());
    }

    /// Text whose neighbours share a long prefix belongs to `DELTA_BYTE_ARRAY`,
    /// not to this rule.
    #[test]
    fn shared_prefixes_belong_to_the_prefix_encoding() {
        let column = column("UNCOMPRESSED", &[]);
        let values: Vec<String> = (0..2_000)
            .map(|value| format!("org.apache.parquet.column.values.factory.{value:08}"))
            .collect();
        let prefixed = cells(&values);
        assert!(detect(&ColumnSample::for_test(&column, &prefixed, false)).is_none());

        assert_eq!(common_prefix(b"prefix-a", b"prefix-b"), 7);
        assert_eq!(common_prefix(b"a", b"b"), 0);
    }

    /// Too few values is no sample.
    #[test]
    fn a_short_sample_is_not_a_candidate() {
        let column = column("UNCOMPRESSED", &[]);
        let values: Vec<String> = (0..4).map(|value| random_text(value, 9)).collect();
        let cells = cells(&values);
        assert!(detect(&ColumnSample::for_test(&column, &cells, false)).is_none());
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "id").contains("PARQUET_2_0"));
        assert!(remedy(Writer::ArrowCpp, "id").contains("'DELTA_LENGTH_BYTE_ARRAY'"));
        assert!(remedy(Writer::ArrowCpp, "request.ext").contains("'request.ext'"));
        assert!(remedy(Writer::ArrowRs, "id").contains("set_column_encoding"));
        assert!(remedy(Writer::Photon, "id").contains("unverified"));
        assert!(remedy(Writer::Other, "id").contains("page-version=v2"));
    }
}

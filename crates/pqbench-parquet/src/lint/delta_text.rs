//! `delta-text-encoding`: nearly-unique text written without a dictionary,
//! priced against the source's own encoding with both Delta encodings and the
//! smaller one kept.

use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::Settings;
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DELTA_TEXT_ENCODING_RULE: &str = "delta-text-encoding";

/// Text is nearly unique when at least this fraction of the sampled non-null
/// values are distinct within the chunk.
const UNIQUE_FRACTION: f64 = 0.5;

/// The two Delta encodings a text column is priced as. `DELTA_BYTE_ARRAY` also
/// codes the shared prefix each value has with its predecessor, which pays on
/// sorted or dotted text; `DELTA_LENGTH_BYTE_ARRAY` stores each value's length
/// alone, which pays when neighbours share nothing.
const CANDIDATES: [Encoding; 2] = [
    Encoding::DELTA_LENGTH_BYTE_ARRAY,
    Encoding::DELTA_BYTE_ARRAY,
];

/// The finding this column earns when its chunk has no dictionary, its values
/// are nearly unique, and its data pages are not already `DELTA_BYTE_ARRAY`
/// encoded (the encoding that already captures shared prefixes).
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.byte_array() || sample.chunk_has_dictionary {
        return None;
    }
    if control::records(sample.mass, Encoding::DELTA_BYTE_ARRAY) {
        return None;
    }
    if sample.distinct_fraction() < UNIQUE_FRACTION {
        return None;
    }
    let leaf = sample.leaf;
    Some(Finding {
        message: format!(
            "nearly-unique text at {:.2} B/value without a dictionary",
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), leaf),
        remedies: remedies(leaf),
        measure,
    })
}

/// Price both Delta encodings against the source's own encoding and dictionary
/// setting, both written at the file's codec and level, and keep the smaller.
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
            encoding: control::source_encoding(sample.mass),
            version: WriterVersion::PARQUET_1_0,
        },
    };
    let candidates: Vec<Arm<'_>> = CANDIDATES
        .into_iter()
        .map(|encoding| Arm {
            values,
            settings: Settings {
                dictionary: false,
                dictionary_page_size_limit: None,
                data_page_size_limit: None,
                encoding: Some(encoding),
                version: WriterVersion::PARQUET_1_0,
            },
        })
        .collect();
    control::measure_arms(sample, |compression| {
        let baseline_bytes = super::price::price(baseline.values, compression, baseline.settings)?;
        let mut candidate_bytes = u64::MAX;
        for candidate in &candidates {
            let bytes = super::price::price(candidate.values, compression, candidate.settings)?;
            candidate_bytes = candidate_bytes.min(bytes);
        }
        Ok(Some((baseline_bytes, candidate_bytes)))
    })
}

/// The settings that Delta-encode one column's text, one per engine.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new("spark", "parquet.writer.version=PARQUET_2_0"),
        v1::Remedy::new(
            "pyarrow",
            format!(
                "use_dictionary=False with column_encoding={{{}: 'DELTA_BYTE_ARRAY'}} \
                 (or 'DELTA_LENGTH_BYTE_ARRAY')",
                super::py_string(leaf)
            ),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!(
                "set_column_encoding({leaf:?}, Encoding::DELTA_BYTE_ARRAY) (or \
                 Encoding::DELTA_LENGTH_BYTE_ARRAY)"
            ),
        ),
        v1::Remedy::new("delta", "parquet.writer.version=PARQUET_2_0"),
        v1::Remedy::new("iceberg", "write.parquet.page-version=v2"),
        v1::Remedy::new("duckdb", "parquet_version='V2'"),
    ]
}

/// The setting that Delta-encodes one column's text, in the detected writer's
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
            "write the column Delta byte-array encoded ({})",
            setting("iceberg")
        ),
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

    /// The pyarrow setting is a Python dict literal, so every column key is a
    /// quoted string whatever its spelling.
    #[test]
    fn the_pyarrow_remedy_quotes_exotic_column_keys() {
        for leaf in ["request.ext", "site.publisher:ext", "request.@timestamp"] {
            let setting = remedy(Writer::ArrowCpp, leaf);
            assert!(
                setting.contains(&format!(
                    "column_encoding={{'{}': 'DELTA_BYTE_ARRAY'}}",
                    leaf
                )),
                "{setting}"
            );
        }
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "id").contains("PARQUET_2_0"));
        assert!(remedy(Writer::ArrowCpp, "id").contains("DELTA_BYTE_ARRAY"));
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

    /// Repeated text, a dictionary, and a `DELTA_BYTE_ARRAY` column are not
    /// candidates; a `DELTA_LENGTH_BYTE_ARRAY` column still is, because the
    /// shared-prefix encoding may be smaller.
    #[test]
    fn only_a_shared_prefix_encoded_column_is_skipped() {
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
        assert!(detect(&ColumnSample::for_test(&delta, &cells_unique, false)).is_some());

        let mut shared_prefix = column("UNCOMPRESSED", "BYTE_ARRAY");
        shared_prefix.mass = Some(v1::Bytemass {
            codec: Some("UNCOMPRESSED".into()),
            encodings: vec!["DELTA_BYTE_ARRAY".into()],
            ..v1::Bytemass::default()
        });
        assert!(detect(&ColumnSample::for_test(
            &shared_prefix,
            &cells_unique,
            false
        ))
        .is_none());
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

    /// Text whose neighbours share a long prefix is smaller as
    /// `DELTA_BYTE_ARRAY`, and the rule keeps that arm rather than the
    /// Delta-length one.
    #[test]
    fn the_shared_prefix_encoding_wins_on_sorted_text() {
        use parquet::basic::{Compression, Encoding};
        use parquet::file::properties::WriterVersion;

        use crate::lint::price::{price, Settings};

        let column = column("UNCOMPRESSED", "BYTE_ARRAY");
        let prefix = "org.apache.parquet.column.values.factory.";
        let values: Vec<String> = (0..2_000)
            .map(|value| format!("{prefix}{value:08}"))
            .collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);
        let decoded = sample.values().expect("values");
        let arm = |encoding: Option<Encoding>| Settings {
            dictionary: false,
            dictionary_page_size_limit: None,
            data_page_size_limit: None,
            encoding,
            version: WriterVersion::PARQUET_1_0,
        };
        let bytes = |encoding: Option<Encoding>| {
            price(decoded, Compression::UNCOMPRESSED, arm(encoding)).expect("price")
        };
        let baseline = bytes(None) as f64;
        let saving = |encoding: Option<Encoding>| (baseline - bytes(encoding) as f64) / baseline;
        let length = saving(Some(Encoding::DELTA_LENGTH_BYTE_ARRAY));
        let shared_prefix = saving(Some(Encoding::DELTA_BYTE_ARRAY));
        assert!(
            shared_prefix > length,
            "a shared prefix must make DELTA_BYTE_ARRAY smaller: {shared_prefix} vs {length}"
        );

        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            (measurement.ratio_low - shared_prefix).abs() < 1e-9,
            "the smaller arm must be kept: {} vs {shared_prefix}",
            measurement.ratio_low
        );
    }
}

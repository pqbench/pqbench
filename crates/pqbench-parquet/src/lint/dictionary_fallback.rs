//! `dictionary-fallback`: a chunk that wrote a dictionary page and then fell
//! back to `PLAIN` data pages, priced three ways at the file's own codec.
//!
//! A dictionary that outgrows its writer's page size limit stops paying: the
//! chunk keeps the dictionary it had built and stores every later value as
//! `PLAIN`, so it pays the dictionary and the full width of most values. Three
//! arms decide which way out is smaller — the current cap reproduced, a cap the
//! values never reach, and the dictionary off — and the smaller one is the
//! saving the rule reports.

use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Measurement};
use super::price::{price, Settings, Values};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DICTIONARY_FALLBACK_RULE: &str = "dictionary-fallback";

/// The finding this column earns when its chunk has a dictionary page and fell
/// back to `PLAIN` data pages.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.chunk_has_dictionary || !sample.fell_back() {
        return None;
    }
    let dictionary_page_size = sample
        .chunk_dictionary_page_size
        .filter(|bytes| *bytes > 0)?;
    sample.values()?;
    Some(Finding {
        message: format!(
            "a {dictionary_page_size}-byte dictionary page fell back to PLAIN data pages \
             at {:.2} B/value",
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf, dictionary_page_size),
        remedies: remedies(sample.leaf, dictionary_page_size),
        measure,
    })
}

/// Price the three arms — the current cap, a cap the values never reach, and no
/// dictionary — and return the best of the two ways out against the current
/// behavior, all written at the file's codec and levels.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let Some(values) = sample.values() else {
        return Ok(None);
    };
    let Some(cap) = sample.chunk_dictionary_page_size.filter(|bytes| *bytes > 0) else {
        return Ok(None);
    };
    // The source's own fallback encoding: a v2 writer falls back to a Delta
    // encoding, a v1 writer to PLAIN, and the footer's encoding set says which.
    let fallback_encoding = control::source_encoding(sample.mass);
    // A `FIXED_LEN_BYTE_ARRAY` dictionary exists only under the 2.0 page version,
    // so all three arms write v2 pages for such a leaf; otherwise a 1.0-version
    // arm would silently drop the dictionary and compare PLAIN against PLAIN.
    let (version, off_encoding) = match values {
        Values::FixedLen(..) => (
            WriterVersion::PARQUET_2_0,
            fallback_encoding.or(Some(Encoding::PLAIN)),
        ),
        _ => (WriterVersion::PARQUET_1_0, fallback_encoding),
    };
    let current = Settings {
        dictionary: true,
        dictionary_page_size_limit: Some(cap),
        data_page_size_limit: None,
        encoding: None,
        version,
    };
    let raised = Settings {
        dictionary: true,
        dictionary_page_size_limit: Some(usize::MAX),
        data_page_size_limit: None,
        encoding: None,
        version,
    };
    let off = Settings {
        dictionary: false,
        dictionary_page_size_limit: None,
        data_page_size_limit: None,
        encoding: off_encoding,
        version,
    };
    control::measure_arms(sample, |compression| {
        let current_bytes = price(values, compression, current)?;
        let raised_bytes = price(values, compression, raised)?;
        let off_bytes = price(values, compression, off)?;
        Ok(Some((current_bytes, raised_bytes.min(off_bytes))))
    })
}

/// The settings that avoid a dictionary fallback, one per engine: raise the
/// dictionary page size limit past the point the chunk fell back, or turn the
/// column's dictionary off. Both are priced, and the smaller wins.
fn remedies(leaf: &str, cap: usize) -> Vec<v1::Remedy> {
    let raise = format!("(the fallback hit at {cap} bytes)");
    vec![
        v1::Remedy::new(
            "spark",
            format!(
                "raise spark.hadoop.parquet.dictionary.page.size {raise}, or \
                 spark.hadoop.parquet.enable.dictionary#{leaf}=false"
            ),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!(
                "raise dictionary_pagesize_limit {raise}, or use_dictionary=[<other columns>] \
                 (leave {} out)",
                super::py_string(leaf)
            ),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!(
                "raise set_dictionary_page_size_limit {raise}, or \
                 set_column_dictionary_enabled({leaf:?}, false)"
            ),
        ),
        v1::Remedy::new("delta", "raise spark.hadoop.parquet.dictionary.page.size"),
        v1::Remedy::new(
            "iceberg",
            format!(
                "raise write.parquet.dict-size-bytes {raise}, or \
                 write.parquet.dict-encoding-enabled.column.{leaf}=false"
            ),
        ),
        v1::Remedy::new("duckdb", format!("raise dictionary_size_limit {raise}")),
    ]
}

/// The setting that ends a dictionary fallback, in the detected writer's terms.
fn remedy(writer: Writer, leaf: &str, cap: usize) -> String {
    let remedies = remedies(leaf, cap);
    let setting = |engine: &str| super::setting(&remedies, engine);
    let lead = "raise the dictionary page size limit or disable the column's dictionary";
    match writer {
        Writer::ParquetMr => format!("{lead}: {}", setting("spark")),
        Writer::Photon => format!(
            "{lead}: {} (Photon forwarding unverified)",
            setting("delta")
        ),
        Writer::ArrowCpp => format!("{lead}: {}", setting("pyarrow")),
        Writer::ArrowRs => format!("{lead}: {}", setting("parquet-rs")),
        Writer::Other => format!("{lead}: {}", setting("iceberg")),
    }
}

#[cfg(test)]
mod tests {
    use parquet::data_type::ByteArray;

    use super::{detect, measure, remedy};
    use crate::lint::writer::Writer;
    use crate::lint::{cells, ColumnSample};
    use crate::v1;

    fn column(physical_data_type: &str, codec: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some(physical_data_type.into()),
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                compressed_bytes: Some(400_000),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "id", 4_096).contains("parquet.dictionary.page.size"));
        assert!(remedy(Writer::ParquetMr, "id", 4_096).contains("4096 bytes"));
        assert!(remedy(Writer::ArrowCpp, "id", 4_096).contains("dictionary_pagesize_limit"));
        assert!(remedy(Writer::ArrowRs, "id", 4_096).contains("set_dictionary_page_size_limit"));
        assert!(remedy(Writer::Photon, "id", 4_096).contains("unverified"));
        assert!(remedy(Writer::Other, "id", 4_096).contains("dict-size-bytes"));
    }

    /// A chunk without a dictionary page, and one that never fell back, are not
    /// candidates.
    #[test]
    fn only_a_fallen_back_chunk_is_a_candidate() {
        let column = column("BYTE_ARRAY", "UNCOMPRESSED");
        let values: Vec<String> = (0..2_000)
            .map(|value| format!("value-{value:06}"))
            .collect();
        let cells = cells(&values);

        let without_dictionary = ColumnSample::for_test(&column, &cells, false);
        assert!(detect(&without_dictionary).is_none());

        let dictionary = ColumnSample::for_test(&column, &cells, true);
        assert!(detect(&dictionary).is_none());

        let fell_back = dictionary.with_dictionary_page(4_096);
        let finding = detect(&fell_back).expect("a fallen-back chunk is a candidate");
        assert!(finding.message.contains("fell back to PLAIN"));
        assert!(finding.message.contains("4096-byte dictionary page"));
    }

    /// Nearly-unique values that overflowed their dictionary are smaller with
    /// the dictionary off: the chunk already stores most values as PLAIN, so the
    /// win is the dictionary page the fallback left behind, not a change of
    /// encoding.
    #[test]
    fn nearly_unique_values_are_smaller_without_the_dictionary() {
        let column = column("BYTE_ARRAY", "UNCOMPRESSED");
        let values: Vec<String> = (0..4_000).map(|value| format!("{value:032x}")).collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, true).with_dictionary_page(4_096);

        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.0,
            "dropping the dictionary page must be the smaller arm, got {}",
            measurement.ratio_low
        );
    }

    /// Repeated values that overflowed a small dictionary are smaller with the
    /// limit raised, and the rule keeps that arm.
    #[test]
    fn repeated_values_are_smaller_with_the_limit_raised() {
        let column = column("BYTE_ARRAY", "UNCOMPRESSED");
        let values: Vec<String> = (0..2_000)
            .map(|value| format!("region-{:02}", value % 20))
            .collect();
        let cells = cells(&values);
        // 20 values fit in a few hundred bytes, so a 16-byte cap forced the
        // writer to fall back to PLAIN.
        let sample = ColumnSample::for_test(&column, &cells, true).with_dictionary_page(16);

        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.5,
            "a dictionary on 20 distinct values must save most of PLAIN's bytes, got {}",
            measurement.ratio_low
        );
    }

    /// An integer leaf falls back too, and is priced as integers.
    #[test]
    fn an_integer_chunk_is_priced_as_integers() {
        let column = column("INT64", "UNCOMPRESSED");
        let values: Vec<Option<i64>> = (0..4_000)
            .map(|value| Some(value * 2_654_435_761))
            .collect();
        let sample =
            ColumnSample::for_test_int64(&column, &values, true).with_dictionary_page(1_024);

        let finding = detect(&sample).expect("a fallen-back chunk is a candidate");
        assert!(finding.message.contains("fell back to PLAIN"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(measurement.ratio_low > 0.0, "{}", measurement.ratio_low);
    }

    /// A `FIXED_LEN_BYTE_ARRAY` chunk that fell back is priced under the 2.0
    /// page version, the only version whose dictionary exists for that leaf: a
    /// 1.0-version arm would drop it and compare PLAIN against PLAIN.
    #[test]
    fn a_fixed_length_chunk_is_priced_under_the_2_0_version() {
        let column = column("FIXED_LEN_BYTE_ARRAY", "UNCOMPRESSED");
        let values: Vec<Option<ByteArray>> = (0..4_000)
            .map(|value| Some(ByteArray::from(vec![(value % 250) as u8; 16])))
            .collect();
        let sample = ColumnSample::for_test(&column, &values, true).with_dictionary_page(1_024);

        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.5,
            "a 250-value dictionary of 16-byte values must save most of PLAIN, got {}",
            measurement.ratio_low
        );
    }
}

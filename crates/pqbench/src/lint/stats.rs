//! `stats-truncated`: footer `min_value` and `max_value` bounds longer than the
//! 64-byte truncation length, priced by arithmetic on the footer's own bytes.
//!
//! A Parquet footer repeats a chunk's `min_value` and `max_value` so a reader
//! can prune without opening a page. A writer that truncates its statistics at
//! 64 bytes bounds one chunk's bounds; a writer that does not stores the widest
//! value it saw, and a file of long strings carries them in every row group.
//! The bytes are the footer's own, so the saving needs no re-encode: it is the
//! sum of the bytes past the truncation length on both bounds.

use crate::v1;

use super::control::Measurement;
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const STATS_TRUNCATED_RULE: &str = "stats-truncated";

/// The smallest footer win worth a suggestion: a bound a few bytes past the
/// truncation length is not worth rewriting a file for.
const MIN_SAVING_BYTES: u64 = 64;

/// The finding this column earns when its chunk records `min_value` and
/// `max_value` bytes past the truncation length.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !matches!(sample.physical(), "BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY") {
        return None;
    }
    let excess = sample.stats_excess_bytes;
    if excess < MIN_SAVING_BYTES || sample.chunk_compressed_bytes == 0 {
        return None;
    }
    let ratio = excess as f64 / sample.chunk_compressed_bytes as f64;
    Some(Finding {
        message: format!(
            "footer min/max hold {excess} bytes above the 64-byte truncation length, \
             {ratio:.2}% of the sample chunk's {} bytes",
            sample.chunk_compressed_bytes
        ),
        remedy: remedy(sample.writer_kind()),
        remedies: remedies(),
        measure,
    })
}

/// The saving, from the footer's own recorded bounds: the bytes past the
/// truncation length against the sample chunk's compressed bytes.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let excess = sample.stats_excess_bytes;
    let chunk_bytes = sample.chunk_compressed_bytes;
    if excess == 0 || chunk_bytes == 0 {
        return Ok(None);
    }
    let ratio = excess as f64 / chunk_bytes as f64;
    Ok(Some(Measurement {
        ratio_low: ratio,
        ratio_high: ratio,
        levels: "the footer's recorded bounds".to_string(),
    }))
}

/// The settings that truncate a column's recorded statistics, one per engine.
///
/// Arrow C++ has no truncation length: pyarrow writes the full `min_value` and
/// `max_value`, and its only related switch drops the statistics entirely,
/// which costs the pruning they exist for. That engine's remedy says so rather
/// than naming a setting that does not exist.
fn remedies() -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            "spark.hadoop.parquet.statistics.truncate.length=64",
        ),
        v1::Remedy::new(
            "pyarrow",
            "no truncation length in Arrow C++: stats are written in full; \
             write_statistics=False drops them (and the pruning)",
        ),
        v1::Remedy::new("parquet-rs", "set_statistics_truncate_length(Some(64))"),
        v1::Remedy::new(
            "delta",
            "spark.hadoop.parquet.statistics.truncate.length=64",
        ),
        v1::Remedy::new(
            "iceberg",
            "no truncation length in Iceberg: write.parquet.stats-enabled.column.<column>=false \
             drops them (and the pruning)",
        ),
    ]
}

/// The setting that truncates a column's statistics, in the detected writer's
/// terms.
fn remedy(writer: Writer) -> String {
    let remedies = remedies();
    let setting = |engine: &str| super::setting(&remedies, engine);
    let lead = "truncate this column's footer statistics";
    match writer {
        Writer::ParquetMr => format!("{lead}: {}", setting("spark")),
        Writer::Photon => format!(
            "{lead}: {} (Photon forwarding unverified)",
            setting("spark")
        ),
        Writer::ArrowCpp => format!("{lead}: no setting exists ({})", setting("pyarrow")),
        Writer::ArrowRs => format!("{lead}: {}", setting("parquet-rs")),
        Writer::Other => format!("{lead}: {}", setting("iceberg")),
    }
}

#[cfg(test)]
mod tests {
    use super::{detect, measure, remedy};
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(physical_data_type: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some(physical_data_type.into()),
            mass: Some(v1::Bytemass {
                codec: Some("ZSTD".into()),
                compressed_bytes: Some(100_000),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    fn cells(values: &[String]) -> Vec<Option<parquet::data_type::ByteArray>> {
        values
            .iter()
            .map(|value| {
                Some(parquet::data_type::ByteArray::from(
                    value.clone().into_bytes(),
                ))
            })
            .collect()
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr).contains("statistics.truncate.length=64"));
        assert!(remedy(Writer::ArrowRs).contains("set_statistics_truncate_length"));
        assert!(remedy(Writer::ArrowCpp).contains("no setting exists"));
        assert!(remedy(Writer::ArrowCpp).contains("no truncation length"));
        assert!(remedy(Writer::Photon).contains("unverified"));
        assert!(remedy(Writer::Other).contains("stats-enabled.column"));
    }

    /// A footer whose bounds stay within the truncation length is no
    /// candidate, and one past it is, with the exact ratio.
    #[test]
    fn only_bounds_past_the_truncation_length_are_priced() {
        let column = column("BYTE_ARRAY");
        let values: Vec<String> = (0..100).map(|value| format!("value-{value}")).collect();
        let cells = cells(&values);

        let within = ColumnSample::for_test(&column, &cells, false);
        assert!(detect(&within).is_none());

        let bloated = ColumnSample::for_test(&column, &cells, false).with_stats_excess(1_000);
        let finding = detect(&bloated).expect("untruncated bounds are a candidate");
        assert!(finding.message.contains("1000 bytes above"));
        assert!(finding.message.contains("0.01%"), "{}", finding.message);

        let measurement = measure(&bloated).expect("measure").expect("exact");
        assert_eq!(measurement.ratio_low, measurement.ratio_high);
        assert!((measurement.ratio_low - 0.01).abs() < 1e-12);

        // A bound a few bytes past the length is below the reporting floor.
        let trivial = ColumnSample::for_test(&column, &cells, false).with_stats_excess(10);
        assert!(detect(&trivial).is_none());
    }

    /// A numeric leaf's bounds are its fixed physical width: statistics cannot
    /// grow on one.
    #[test]
    fn a_numeric_leaf_is_not_a_candidate() {
        let column = column("INT64");
        let values: Vec<Option<i64>> = (0..100).map(Some).collect();
        let sample = ColumnSample::for_test_int64(&column, &values, false).with_stats_excess(1_000);
        assert!(detect(&sample).is_none());
    }
}

//! The report shape shared by the `lz` and `compression` sweeps.
//!
//! Both sweeps bench a set of codec×level configs over a byte source and report
//! one row per config; the row and its ordering are identical, so the types and
//! the sort live here once. Speeds are carried as [`crate::stats::Estimate`]
//! so error bars survive and per-chunk measurements compose to file-level rows.

use serde::Serialize;

use crate::codecs::Codec;
use crate::stats::Estimate;

/// One codec×level row in a [`Report`].
#[derive(Serialize)]
pub struct ReportRow {
    pub codec: Codec,
    pub level: u8,
    pub compress_estimate: Estimate,
    pub decompress_estimate: Estimate,
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
    /// compressed/uncompressed; 1.0 means no compression.
    pub ratio: f64,
}

impl ReportRow {
    /// Build a row, deriving the ratio from the sizes.
    pub fn new(
        codec: Codec,
        level: u8,
        compress_estimate: Estimate,
        decompress_estimate: Estimate,
        compressed_bytes: usize,
        uncompressed_bytes: usize,
    ) -> ReportRow {
        ReportRow {
            codec,
            level,
            compress_estimate,
            decompress_estimate,
            compressed_bytes,
            uncompressed_bytes,
            ratio: ratio(compressed_bytes, uncompressed_bytes),
        }
    }
}

/// One column's per-codec×level breakdown (from `compression --per-column`).
#[derive(Serialize)]
pub struct ColumnRow {
    pub codec: Codec,
    pub level: u8,
    pub column: String,
    pub compress_estimate: Estimate,
    pub decompress_estimate: Estimate,
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
    /// compressed/uncompressed; 1.0 means no compression.
    pub ratio: f64,
}

impl ColumnRow {
    /// Build a per-column row, deriving the ratio from the sizes.
    pub fn new(
        codec: Codec,
        level: u8,
        column: String,
        compress_estimate: Estimate,
        decompress_estimate: Estimate,
        compressed_bytes: usize,
        uncompressed_bytes: usize,
    ) -> ColumnRow {
        ColumnRow {
            codec,
            level,
            column,
            compress_estimate,
            decompress_estimate,
            compressed_bytes,
            uncompressed_bytes,
            ratio: ratio(compressed_bytes, uncompressed_bytes),
        }
    }
}

/// `compressed/uncompressed`; 1.0 for empty input (nothing to compress).
fn ratio(compressed_bytes: usize, uncompressed_bytes: usize) -> f64 {
    if uncompressed_bytes == 0 {
        return 1.0;
    }
    compressed_bytes as f64 / uncompressed_bytes as f64
}

/// A sweep's result: file-level rows ordered by compress speed, plus the
/// per-column breakdown (empty unless requested).
#[derive(Serialize)]
pub struct Report {
    pub rows: Vec<ReportRow>,
    pub columns: Vec<ColumnRow>,
}

/// Sort the rows by compress speed (fastest first) and wrap them in a [`Report`].
pub fn into_report(mut rows: Vec<ReportRow>, columns: Vec<ColumnRow>) -> Report {
    rows.sort_by(|a, b| {
        b.compress_estimate
            .megabytes_per_second(b.uncompressed_bytes as u64)
            .partial_cmp(
                &a.compress_estimate
                    .megabytes_per_second(a.uncompressed_bytes as u64),
            )
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Report { rows, columns }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::Codec;
    use crate::stats::Estimate;

    #[test]
    fn ratio_is_finite_for_zero_uncompressed() {
        let zero = Estimate::zero();
        let row = ReportRow::new(Codec::Snappy, 1, zero, zero, 1, 0);
        assert!(row.ratio.is_finite(), "ratio was {}", row.ratio);
        let col = ColumnRow::new(Codec::Snappy, 1, "a".to_string(), zero, zero, 9, 0);
        assert!(col.ratio.is_finite(), "ratio was {}", col.ratio);
    }
}

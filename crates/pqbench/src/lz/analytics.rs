//! Analytics: reduce raw per-pass samples (warmup/mode) into
//! [`crate::stats::Estimate`]s and compose them into file-level report rows.

use crate::report;
use crate::stats;

use super::raw::RawRow;

/// Aggregate raw measurements into a report sorted by compress speed.
pub fn aggregate(raw: &[RawRow], cfg: &stats::Config) -> report::Report {
    let rows = raw
        .iter()
        .map(|r| {
            report::ReportRow::new(
                r.codec,
                r.level,
                stats::measure(&r.compress_durations, cfg),
                stats::measure(&r.decompress_durations, cfg),
                r.compressed_bytes,
                r.uncompressed_bytes,
            )
        })
        .collect();
    report::into_report(rows, Vec::new())
}

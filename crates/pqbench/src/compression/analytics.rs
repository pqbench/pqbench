//! Analytics: reduce raw per-pass samples (warmup/mode) into
//! [`crate::stats::Estimate`]s and compose them into file-level and
//! per-column report rows.

use crate::report;
use crate::stats;

use super::raw::{ChunkResult, RawRow};

/// Reduce one chunk's samples into a measurement.
fn chunk_measure(chunk: &ChunkResult, cfg: &stats::Config) -> (stats::Estimate, stats::Estimate) {
    (
        stats::measure(&chunk.compress_durations, cfg),
        stats::measure(&chunk.decompress_durations, cfg),
    )
}

fn chunk_bytes(chunk: &ChunkResult) -> (usize, usize) {
    (
        chunk.pages.iter().map(|p| p.compressed_bytes).sum(),
        chunk.pages.iter().map(|p| p.uncompressed_bytes).sum(),
    )
}

/// Fold a row's chunk measurements into file-level ones (monoid combine).
fn file_measure(row: &RawRow, cfg: &stats::Config) -> (stats::Estimate, stats::Estimate) {
    row.chunks.iter().fold(
        (stats::Estimate::zero(), stats::Estimate::zero()),
        |(c, d), chunk| {
            let (cc, cd) = chunk_measure(chunk, cfg);
            (c.combine(cc), d.combine(cd))
        },
    )
}

/// Fold a row's chunk byte counts into file-level ones.
fn file_bytes(row: &RawRow) -> (usize, usize) {
    row.chunks.iter().fold((0, 0), |(c, u), chunk| {
        let (cc, cu) = chunk_bytes(chunk);
        (c + cc, u + cu)
    })
}

/// Aggregate raw measurements into a report sorted by compress speed. When
/// `per_column` is set, the report also carries one row per column chunk.
pub fn aggregate(raw: &[RawRow], cfg: &stats::Config, per_column: bool) -> report::Report {
    let mut rows = Vec::with_capacity(raw.len());
    let mut columns = Vec::new();
    for r in raw {
        let (compress, decompress) = file_measure(r, cfg);
        let (compressed_bytes, uncompressed_bytes) = file_bytes(r);
        rows.push(report::ReportRow::new(
            r.codec,
            r.level,
            compress,
            decompress,
            compressed_bytes,
            uncompressed_bytes,
        ));
        if per_column {
            for chunk in &r.chunks {
                let (cc, cd) = chunk_measure(chunk, cfg);
                let (compressed_bytes, uncompressed_bytes) = chunk_bytes(chunk);
                columns.push(report::ColumnRow::new(
                    r.codec,
                    r.level,
                    chunk.column.clone(),
                    cc,
                    cd,
                    compressed_bytes,
                    uncompressed_bytes,
                ));
            }
        }
    }
    report::into_report(rows, columns)
}

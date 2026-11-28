//! Presentation shared by the `lz` and `compression` sweeps: each produces a
//! [`report::Report`] and renders the same file-level rows table.

use std::fmt::Write;

use crate::report;

/// Append a report's file-level rows to `out` as a fixed-width table.
pub(crate) fn render_rows(out: &mut String, report: &report::Report) {
    let _ = writeln!(
        out,
        "{:<8} {:>5} {:>13} {:>13} {:>10} {:>7}",
        "codec", "level", "compress", "decompress", "size", "ratio%"
    );
    for r in &report.rows {
        let bytes = r.uncompressed_bytes as u64;
        let compress = r.compress_estimate.megabytes_per_second(bytes);
        let compress_se = r.compress_estimate.megabytes_per_second_se(bytes);
        let decompress = r.decompress_estimate.megabytes_per_second(bytes);
        let decompress_se = r.decompress_estimate.megabytes_per_second_se(bytes);
        let _ = writeln!(
            out,
            "{:<8} {:>5} {:>8.0} ± {:>3.0} {:>8.0} ± {:>3.0} {:>10} {:>7.2}",
            r.codec,
            r.level,
            compress,
            compress_se,
            decompress,
            decompress_se,
            r.compressed_bytes,
            r.ratio * 100.0,
        );
    }
}

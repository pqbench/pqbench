//! Presentation: render a compression [`report::Report`] to a text table.

use std::fmt::Write;

use crate::bench::Error;
use crate::report;
use crate::text;

/// Render a report's file-level rows, plus a per-column section when
/// `per_column` is set.
pub fn render_text(report: &report::Report, per_column: bool) -> Result<String, Error> {
    let mut out = String::new();
    text::render_rows(&mut out, report);
    if per_column {
        for row in &report.rows {
            let _ = writeln!(
                out,
                "\n-- per-column {}@{level} --",
                row.codec,
                level = row.level
            );
            let _ = writeln!(
                out,
                "{:<24} {:>13} {:>13} {:>10} {:>7}",
                "column", "compress", "decompress", "size", "ratio%"
            );
            for c in report
                .columns
                .iter()
                .filter(|c| c.codec == row.codec && c.level == row.level)
            {
                let _ = writeln!(
                    out,
                    "{:<24} {:>8.0} ± {:>3.0} {:>8.0} ± {:>3.0} {:>10} {:>7.2}",
                    c.column,
                    c.compress_estimate
                        .megabytes_per_second(c.uncompressed_bytes as u64),
                    c.compress_estimate
                        .megabytes_per_second_se(c.uncompressed_bytes as u64),
                    c.decompress_estimate
                        .megabytes_per_second(c.uncompressed_bytes as u64),
                    c.decompress_estimate
                        .megabytes_per_second_se(c.uncompressed_bytes as u64),
                    c.compressed_bytes,
                    c.ratio * 100.0,
                );
            }
        }
    }
    Ok(out)
}

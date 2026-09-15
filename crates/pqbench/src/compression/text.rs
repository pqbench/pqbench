//! Presentation: render a [`report::Report`] to the terminal.

use crate::report;
use crate::text;

/// Render a report's file-level rows, plus a per-column section when
/// `per_column` is set.
pub fn render(report: &report::Report, per_column: bool) {
    text::render_rows(report);
    if per_column {
        for row in &report.rows {
            println!(
                "\n-- per-column {}@{level} --",
                row.codec,
                level = row.level
            );
            println!(
                "{:<24} {:>13} {:>13} {:>10} {:>7}",
                "column", "compress", "decompress", "size", "ratio%"
            );
            for c in report
                .columns
                .iter()
                .filter(|c| c.codec == row.codec && c.level == row.level)
            {
                println!(
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
}

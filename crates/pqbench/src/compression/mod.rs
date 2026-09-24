//! `compression`: lzbench-style codec sweep over encoded parquet pages.
//!
//! The command is one function: [`compression`] takes a
//! [`CompressionRequest`] and returns the measured table. Rendering is a fold
//! of that table: [`render_text`] prints the stats table (optionally with the
//! per-column breakdown), [`render_json`] serializes it.
//!
//! Split by layer: `raw` (benchmarking), `analytics` (aggregation), `text`
//! (presentation). Raw sweeps each column chunk independently and records raw
//! per-pass samples; analytics reduces them (warmup/mode) into
//! [`crate::stats::Estimate`]s and composes them per column and per file via
//! the measurement monoid.

mod analytics;
mod api;
mod json;
mod raw;
mod text;

pub use analytics::aggregate;
pub use api::{compression, CompressionRequest};
pub use json::render_json;
pub use raw::{bench_file, ChunkResult, PageResult, RawRow};
pub use text::render_text;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::Codec;
    use crate::parquet_helpers::{ColumnChunk, Page, ParquetFile};
    use crate::stats::{Config, Mode};

    fn fake_file() -> ParquetFile {
        let mk = |s: &str, n: usize| Page {
            payload: s.as_bytes().repeat(n),
            value_count: n as u32,
            dictionary: false,
        };
        ParquetFile {
            chunks: vec![
                ColumnChunk {
                    column: "text".to_string(),
                    pages: vec![
                        mk("the quick brown fox ", 4096),
                        mk("jumps over the lazy dog ", 4096),
                    ],
                },
                ColumnChunk {
                    column: "nums".to_string(),
                    pages: vec![mk("1234567890", 4096)],
                },
            ],
        }
    }

    #[test]
    fn bench_file_and_aggregate_agree_with_compression() {
        let file = fake_file();
        let configs = [(Codec::Snappy, 1), (Codec::Zstd, 3)];
        let cfg = Config {
            warmup_iterations: 1,
            mode: Mode::Fastest,
        };
        let raw = bench_file(&file, &configs, 3).unwrap();

        assert_eq!(raw.len(), 2);
        for r in &raw {
            assert_eq!(r.chunks.len(), 2);
            let columns: Vec<&str> = r.chunks.iter().map(|c| c.column.as_str()).collect();
            assert_eq!(columns, ["text", "nums"]);
            let page_counts: Vec<usize> = r.chunks.iter().map(|c| c.pages.len()).collect();
            assert_eq!(page_counts, [2, 1]);
            for chunk in &r.chunks {
                for p in &chunk.pages {
                    assert!(p.compressed_bytes < p.uncompressed_bytes);
                    assert_eq!(p.compress_durations.len(), 3);
                    assert_eq!(p.decompress_durations.len(), 3);
                }
                assert_eq!(chunk.compress_durations.len(), 3);
                assert_eq!(chunk.decompress_durations.len(), 3);
            }
        }

        let report = aggregate(&raw, &cfg, true);
        assert_eq!(report.rows.len(), 2);
        assert_eq!(report.columns.len(), 4); // 2 codecs × 2 columns
        for row in &report.rows {
            assert!(row.ratio > 0.0 && row.ratio < 1.0);
            assert_eq!(
                row.uncompressed_bytes,
                file.chunks
                    .iter()
                    .flat_map(|c| &c.pages)
                    .map(|p| p.payload.len())
                    .sum::<usize>()
            );
        }
        for col in &report.columns {
            assert!(
                col.compress_estimate
                    .megabytes_per_second(col.uncompressed_bytes as u64)
                    > 0.0
            );
        }
    }
}

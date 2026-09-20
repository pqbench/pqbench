//! `lz`: lzbench-equivalent compression benchmark over raw file bytes.
//!
//! The command is one function: [`lz`] takes an [`LzRequest`] and returns the
//! measured table. Rendering is a fold of that table: [`render_text`] prints
//! the stats table, [`render_json`] serializes it.
//!
//! Split by layer: `raw` (`bench_file`) loads the file once and records raw
//! per-pass times per config; `analytics` (`aggregate`) reduces the samples
//! (warmup/mode) into a shared [`crate::report::ReportRow`] and orders the rows
//! by compress speed.

mod analytics;
mod api;
mod json;
mod raw;
mod text;

pub use analytics::aggregate;
pub use api::{lz, LzRequest};
pub use json::render_json;
pub use raw::{bench_file, RawRow};
pub use text::render_text;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::Codec;
    use crate::stats::{Config, Mode};

    #[test]
    fn lz_runs_the_sweep_and_renders_both_formats() {
        let dir = std::env::temp_dir();
        let path = dir.join("pqbench_lz_command.bin");
        std::fs::write(&path, b"the quick brown fox ".repeat(4096)).unwrap();

        let request = LzRequest {
            file: path.clone(),
            codec_specs: vec!["zstd@1".into()],
            samples: 2,
            warmup_iterations: 1,
            mode: Mode::Fastest,
        };
        let report = lz(&request).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].codec, Codec::Zstd);
        assert!(render_text(&report).unwrap().contains("zstd"));

        let json: serde_json::Value = serde_json::from_str(&render_json(&report).unwrap()).unwrap();
        assert_eq!(json["rows"][0]["level"], 1);
    }

    #[test]
    fn bench_file_and_aggregate_agree_with_compression() {
        let dir = std::env::temp_dir();
        let path = dir.join("pqbench_lz_test.bin");
        std::fs::write(
            &path,
            b"the quick brown fox jumps over the lazy dog ".repeat(8192),
        )
        .unwrap();
        let configs = [(Codec::Snappy, 1), (Codec::Zstd, 3)];
        let raw = bench_file(&path, &configs, 3).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(raw.len(), 2);
        for r in &raw {
            assert_eq!(
                r.uncompressed_bytes,
                b"the quick brown fox jumps over the lazy dog "
                    .repeat(8192)
                    .len()
            );
            assert!(r.compressed_bytes < r.uncompressed_bytes);
            assert_eq!(r.compress_durations.len(), 3);
            assert_eq!(r.decompress_durations.len(), 3);
        }

        let cfg = Config {
            warmup_iterations: 1,
            mode: Mode::Fastest,
        };
        let report = aggregate(&raw, &cfg);
        assert_eq!(report.rows.len(), 2);
        for row in &report.rows {
            assert!(row.ratio > 0.0 && row.ratio < 1.0);
            assert!(row.compress_estimate.n > 0);
            assert!(
                row.compress_estimate
                    .megabytes_per_second(row.uncompressed_bytes as u64)
                    > 0.0
            );
        }
    }

    #[test]
    fn zero_passes_yields_zero_estimate_no_panic() {
        let dir = std::env::temp_dir();
        let path = dir.join("pqbench_lz_zero.bin");
        std::fs::write(&path, b"the quick brown fox ".repeat(4096)).unwrap();
        let raw = bench_file(&path, &[(Codec::Zstd, 3)], 0).unwrap();
        std::fs::remove_file(&path).unwrap();

        let cfg = Config {
            warmup_iterations: 0,
            mode: Mode::Fastest,
        };
        let report = aggregate(&raw, &cfg);
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].compress_estimate.n, 0);
        assert_eq!(
            report.rows[0]
                .compress_estimate
                .megabytes_per_second(1_000_000),
            0.0
        );
        assert!(report.rows[0].ratio.is_finite());
    }
}

//! Blackbox end-to-end tests of the public `compression` command.
//!
//! `small_reddit_none.parquet` is a 3000-row NONE-compressed subset of the
//! MIT-licensed reddit_dataset_90 (goldentraversy07/reddit_dataset_90);
//! `small_snappy.parquet` is the same shape, compressed.

use std::path::PathBuf;

use pqbench::codecs::Codec;
use pqbench::compression::{compression, render_json, render_text, CompressionRequest};
use pqbench::stats::Mode;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn request(file: PathBuf, per_column: bool) -> CompressionRequest {
    CompressionRequest {
        file,
        codec_specs: vec!["zstd@1".into()],
        samples: 1,
        warmup_iterations: 0,
        mode: Mode::Fastest,
        per_column,
    }
}

#[test]
fn sweeps_encoded_pages_and_renders_a_text_table() {
    let report = compression(&request(fixture("small_reddit_none.parquet"), false)).unwrap();

    assert_eq!(report.rows.len(), 1);
    let row = &report.rows[0];
    assert_eq!(row.codec, Codec::Zstd);
    assert_eq!(row.level, 1);
    assert!(row.compressed_bytes < row.uncompressed_bytes);
    assert!(row.ratio > 0.0 && row.ratio < 1.0);
    assert!(report.columns.is_empty());

    let text = render_text(&report, false).unwrap();
    assert!(text.contains("zstd"));
    assert!(!text.contains("-- per-column"));
}

#[test]
fn per_column_adds_one_row_per_column_chunk() {
    let report = compression(&request(fixture("small_reddit_none.parquet"), true)).unwrap();

    assert!(!report.columns.is_empty());
    assert!(report
        .columns
        .iter()
        .all(|column| column.codec == Codec::Zstd && column.level == 1));
    let chunk_bytes: usize = report
        .columns
        .iter()
        .map(|column| column.uncompressed_bytes)
        .sum();
    assert_eq!(chunk_bytes, report.rows[0].uncompressed_bytes);

    let text = render_text(&report, true).unwrap();
    assert!(text.contains("-- per-column"));
    assert!(text.contains("column"));
    for column in &report.columns {
        assert!(text.contains(&column.column), "missing {}", column.column);
    }
}

#[test]
fn renders_the_report_as_composable_json() {
    let report = compression(&request(fixture("small_reddit_none.parquet"), true)).unwrap();
    let json: serde_json::Value = serde_json::from_str(&render_json(&report).unwrap()).unwrap();

    assert_eq!(json["rows"][0]["codec"], "zstd");
    assert!(json["columns"].is_array());
    assert!(!json["columns"].as_array().unwrap().is_empty());
    assert!(json["columns"][0]["column"].is_string());
}

#[test]
fn empty_specs_sweep_every_wired_codec() {
    let request = CompressionRequest {
        codec_specs: vec![],
        ..request(fixture("small_reddit_none.parquet"), false)
    };
    let report = compression(&request).unwrap();

    assert_eq!(report.rows.len(), Codec::all().count());
}

#[test]
fn rejects_compressed_input_unknown_codecs_and_missing_files() {
    let compressed = compression(&request(fixture("small_snappy.parquet"), false)).unwrap_err();
    assert!(compressed.to_string().contains("NONE"), "{compressed}");

    let unknown = CompressionRequest {
        codec_specs: vec!["nope".into()],
        ..request(fixture("small_reddit_none.parquet"), false)
    };
    let error = compression(&unknown).unwrap_err().to_string();
    assert!(error.contains("unknown codec"), "{error}");

    assert!(compression(&request(fixture("does-not-exist.parquet"), false)).is_err());
}

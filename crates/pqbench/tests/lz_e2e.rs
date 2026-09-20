//! Blackbox end-to-end tests of the public `lz` command.
//!
//! `small_reddit_none.parquet` is a 3000-row NONE-compressed subset of the
//! MIT-licensed reddit_dataset_90 (goldentraversy07/reddit_dataset_90).

use std::path::PathBuf;

use pqbench::codecs::Codec;
use pqbench::lz::{lz, render_json, render_text, LzRequest};
use pqbench::stats::Mode;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn request(file: PathBuf) -> LzRequest {
    LzRequest {
        file,
        codec_specs: vec!["snappy@1".into()],
        samples: 1,
        warmup_iterations: 0,
        mode: Mode::Fastest,
    }
}

#[test]
fn sweeps_raw_file_bytes_and_renders_a_text_table() {
    let file = fixture("small_reddit_none.parquet");
    let report = lz(&request(file.clone())).unwrap();

    assert_eq!(report.rows.len(), 1);
    let row = &report.rows[0];
    assert_eq!(row.codec, Codec::Snappy);
    assert_eq!(row.level, 1);
    assert_eq!(
        row.uncompressed_bytes as u64,
        std::fs::metadata(&file).unwrap().len()
    );
    assert!(row.compressed_bytes < row.uncompressed_bytes);
    assert!(row.ratio > 0.0 && row.ratio < 1.0);

    let text = render_text(&report);
    assert!(text.contains("codec"));
    assert!(text.contains("ratio%"));
    assert!(text.contains("snappy"));
}

#[test]
fn renders_the_report_as_composable_json() {
    let report = lz(&request(fixture("small_reddit_none.parquet"))).unwrap();
    let json: serde_json::Value = serde_json::from_str(&render_json(&report).unwrap()).unwrap();

    assert_eq!(json["rows"][0]["codec"], "snappy");
    assert_eq!(json["rows"][0]["level"], 1);
    assert!(json["rows"][0]["compressed_bytes"].is_number());
    assert!(json["rows"][0]["ratio"].is_number());
    // lz never reports per-column rows, so the field is omitted.
    assert!(json.get("columns").is_none());
}

#[test]
fn empty_specs_sweep_every_wired_codec() {
    let request = LzRequest {
        codec_specs: vec![],
        ..request(fixture("small_snappy.parquet"))
    };
    let report = lz(&request).unwrap();

    assert_eq!(report.rows.len(), Codec::all().count());
    let reported: Vec<Codec> = report.rows.iter().map(|row| row.codec).collect();
    for codec in Codec::all() {
        assert!(reported.contains(&codec), "missing {codec}");
    }
}

#[test]
fn explicit_levels_are_carried_into_the_report() {
    let request = LzRequest {
        codec_specs: vec!["zstd@3".into()],
        ..request(fixture("small_snappy.parquet"))
    };
    let report = lz(&request).unwrap();

    assert_eq!(report.rows.len(), 1);
    assert_eq!(report.rows[0].codec, Codec::Zstd);
    assert_eq!(report.rows[0].level, 3);
}

#[test]
fn rejects_unknown_codecs_and_missing_files() {
    let unknown = LzRequest {
        codec_specs: vec!["nope".into()],
        ..request(fixture("small_reddit_none.parquet"))
    };
    let error = lz(&unknown).unwrap_err().to_string();
    assert!(error.contains("unknown codec"), "{error}");

    let missing = lz(&request(fixture("does-not-exist.parquet")));
    assert!(missing.is_err());
}

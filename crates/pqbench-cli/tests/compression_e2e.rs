use std::process::Command;

// `small_reddit_none.parquet` is a 3000-row NONE-compressed subset of the
// MIT-licensed reddit_dataset_90 (goldentraversy07/reddit_dataset_90).

/// End-to-end: `pqbench compression` streams one NDJSON row per codec.
#[test]
fn compression_sweeps_codecs() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    );
    let out = Command::new(exe)
        .args([
            "compression",
            file,
            "--per-column",
            "--samples",
            "1",
            "--warmup-iterations",
            "0",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let records: Vec<serde_json::Value> = out
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect();
    assert_eq!(records[0]["kind"], "pqbench.compression");
    assert_eq!(records[0]["event"], "begin");
    let codecs: Vec<_> = records
        .iter()
        .filter(|record| record["kind"] == "pqbench.compression-row")
        .map(|record| record["codec"].as_str().unwrap().to_string())
        .collect();
    for codec in ["gzip", "lz4", "snappy", "zstd"] {
        assert!(
            codecs.iter().any(|name| name == codec),
            "missing codec {codec}"
        );
    }
    let columns: Vec<_> = records
        .iter()
        .filter(|record| record["kind"] == "pqbench.compression-column")
        .map(|record| record["column"].as_str().unwrap().to_string())
        .collect();
    for column in ["text", "label", "url_encoded"] {
        assert!(
            columns.iter().any(|name| name == column),
            "missing column {column}"
        );
    }
    assert_eq!(records.last().unwrap()["event"], "end");
}

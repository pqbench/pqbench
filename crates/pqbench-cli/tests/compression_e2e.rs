use std::process::Command;

// `small_reddit_none.parquet` is a 3000-row NONE-compressed subset of the
// MIT-licensed reddit_dataset_90 (goldentraversy07/reddit_dataset_90).

/// End-to-end: run `pqbench compression` on a NONE-compressed parquet and verify
/// it reports every wired codec and the per-column breakdown.
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
    let stdout = String::from_utf8(out.stdout).unwrap();

    assert!(stdout.contains("codec"));
    assert!(stdout.contains("ratio%"));
    for codec in ["gzip", "lz4", "snappy", "zstd"] {
        assert!(stdout.contains(codec), "missing codec {codec}");
    }
    assert!(
        stdout.contains("-- per-column"),
        "missing per-column breakdown"
    );
    assert!(stdout.contains("column"));
    for column in ["text", "label", "url_encoded"] {
        assert!(stdout.contains(column), "missing column {column}");
    }
}

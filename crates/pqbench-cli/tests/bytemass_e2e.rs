use std::process::Command;

// `small_reddit_none.parquet` is a 3000-row NONE-compressed subset of the
// MIT-licensed reddit_dataset_90 (goldentraversy07/reddit_dataset_90).

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

/// End-to-end: a pipe streams one NDJSON row per column.
#[test]
fn bytemass_streams_column_rows() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let out = Command::new(exe)
        .args(["bytemass", parquet_fixture()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let records = ndjson_records(&out.stdout);
    assert_eq!(records[0]["kind"], "pqbench.bytemass");
    assert_eq!(records[0]["event"], "begin");
    let columns: Vec<_> = records
        .iter()
        .filter(|record| record["kind"] == "pqbench.bytemass-row")
        .map(|record| record["column"].as_str().unwrap().to_string())
        .collect();
    for column in [
        "url_encoded",
        "text",
        "username_encoded",
        "label",
        "communityName",
        "dataType",
        "datetime",
    ] {
        assert!(
            columns.iter().any(|name| name == column),
            "missing {column}"
        );
    }
    let end = records.last().unwrap();
    assert_eq!(end["event"], "end");
    assert_eq!(end["file_count"], 1);
    assert_eq!(end["num_rows"], 3000);
}

/// End-to-end: `--json` is the same NDJSON stream.
#[test]
fn bytemass_json_is_the_stream() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let out = Command::new(exe)
        .args(["bytemass", parquet_fixture(), "--json"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let records = ndjson_records(&out.stdout);
    assert_eq!(records[0]["event"], "begin");
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.bytemass-row"));
    assert!(!out.stdout.windows(10).any(|w| w == b"\"children\""));
}

/// End-to-end: `--d3` prints a self-contained treemap page.
#[test]
fn bytemass_d3_page_end_to_end() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let out = Command::new(exe)
        .args(["bytemass", parquet_fixture(), "--d3"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();

    assert!(stdout.starts_with("<!DOCTYPE html>"));
    assert!(stdout.contains("<title>small_reddit_none.parquet</title>"));
    assert!(stdout.contains("d3-hierarchy@3"));
    assert!(stdout.contains("bytes per row"));
}

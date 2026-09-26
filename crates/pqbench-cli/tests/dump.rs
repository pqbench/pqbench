use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::json;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

fn pipe(args: &[&str], stdin: &str) -> std::process::Output {
    let mut child = pqbench()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn dump_copies_a_table_document_to_a_directory() {
    let directory = tempfile::tempdir().unwrap();
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let document = json!({
        "kind": "pqbench.table",
        "version": 1,
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": [],
        "log": [],
        "files": [{"path": "small_reddit_none.parquet", "uri": parquet_fixture(), "size_bytes": size}]
    });

    let output = pipe(
        &["dump", directory.path().to_str().unwrap()],
        &document.to_string(),
    );

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let copied = directory.path().join("small_reddit_none.parquet");
    assert_eq!(std::fs::metadata(&copied).unwrap().len(), size);
}

#[test]
fn dump_rejects_a_table_ref_document() {
    let directory = tempfile::tempdir().unwrap();
    let document = json!({"kind": "pqbench.table-ref", "version": 1, "uri": "/tmp/table"});

    let output = pipe(
        &["dump", directory.path().to_str().unwrap()],
        &document.to_string(),
    );

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("pqbench table"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

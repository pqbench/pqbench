use std::io::Write;
mod common;

use std::process::{Command, Stdio};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

fn pipe(args: &[&str], stdin: &[u8]) -> std::process::Output {
    let mut child = pqbench()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn viz_collects_a_bytemass_stream_into_html() {
    let measured = common::measured(parquet_fixture());
    assert!(
        measured.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&measured.stderr)
    );

    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    let output = pipe(
        &["viz", "--output", prefix.to_str().unwrap()],
        &measured.stdout,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("d3-hierarchy@3"));
    assert!(html.contains("bpr"));
}

#[test]
fn viz_stores_proxied_file_stats() {
    let blob = pqbench::v1::name::blob_name("/tmp/part-0.parquet");
    let column = pqbench::v1::name::bytemass(&blob, "id");
    let file = serde_json::json!({
        "name": blob,
        "size_bytes": 40,
        "storage_class": "STANDARD",
        "row_group_count": 1,
        "column_count": 1,
    });
    let row = serde_json::json!({
        "name": column,
        "compressed_bytes": 8,
        "uncompressed_bytes": 16,
        "codec": "ZSTD",
        "encodings": ["PLAIN"],
        "value_count": 10,
        "null_count": 0,
    });
    let document = format!("{file}\n{row}\n");
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    let output = pipe(
        &["viz", "--output", prefix.to_str().unwrap()],
        document.as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.contains("STANDARD"), "{html}");
    assert!(html.contains("part-0.parquet"), "{html}");
}

#[test]
fn viz_rejects_a_table_file() {
    let document = r#"{"name":"catalogs/local/schemas/local/tables/t/files/p","blob":"blobs/file/%2Ftmp%2Ft"}"#;
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    let output = pipe(
        &["viz", "-o", prefix.to_str().unwrap()],
        document.as_bytes(),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("pqbench bytemass"), "{stderr}");
}

#[test]
fn viz_requires_output() {
    let measured = common::measured(parquet_fixture());
    assert!(measured.status.success());
    let output = pipe(&["viz"], &measured.stdout);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--output"), "{stderr}");
}

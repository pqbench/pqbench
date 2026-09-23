use std::io::Write;
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
fn viz_collects_a_bytemass_stream_into_sqlite_and_html() {
    let measured = pqbench()
        .args(["bytemass", parquet_fixture()])
        .output()
        .unwrap();
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

    let sqlite = std::fs::read(prefix.with_extension("sqlite")).unwrap();
    assert!(sqlite.starts_with(b"SQLite format 3"));

    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("sql.js"));
    assert!(html.contains("d3-hierarchy@3"));
    assert!(html.contains("bytes per row"));
}

#[test]
fn viz_rejects_a_table_document() {
    let document = r#"{"kind":"pqbench.table","version":1,"format":"delta","uri":"/tmp/t","snapshot_version":0,"partition_columns":[],"log":[],"files":[]}"#;
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
    let measured = pqbench()
        .args(["bytemass", parquet_fixture()])
        .output()
        .unwrap();
    assert!(measured.status.success());
    let output = pipe(&["viz"], &measured.stdout);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--output"), "{stderr}");
}

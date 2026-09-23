use serde_json::json;
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

fn assert_parquet(bytes: &[u8]) {
    assert!(bytes.starts_with(b"PAR1"), "missing parquet magic");
    assert!(bytes.ends_with(b"PAR1"), "missing parquet footer magic");
}

#[test]
fn dump_writes_parquet_from_a_file() {
    let directory = tempfile::tempdir().unwrap();
    let output_path = directory.path().join("sample.parquet");
    let output = pqbench()
        .args([
            "dump",
            parquet_fixture(),
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(&output_path).unwrap();
    assert_parquet(&bytes);

    let again = directory.path().join("again.parquet");
    let reread = pqbench()
        .args([
            "dump",
            output_path.to_str().unwrap(),
            "--row-groups",
            "first:1",
            "--output",
            again.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        reread.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&reread.stderr)
    );
    assert_parquet(&std::fs::read(&again).unwrap());
}

#[test]
fn dump_writes_parquet_from_a_table_document() {
    let directory = tempfile::tempdir().unwrap();
    let output_path = directory.path().join("sample.parquet");
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let document = json!({
        "kind": "pqbench.table",
        "version": 1,
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": [],
        "log": [],
        "files": [{"path": "small_reddit_none.parquet", "uri": parquet_fixture(), "size": size}]
    });
    let output = pipe(
        &["dump", "--output", output_path.to_str().unwrap()],
        &document.to_string(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_parquet(&std::fs::read(&output_path).unwrap());
}

#[test]
fn dump_prunes_partitions_and_samples_files() {
    let directory = tempfile::tempdir().unwrap();
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let file = |path: &str| {
        let uri = directory.path().join(path.replace('/', "-"));
        std::fs::copy(parquet_fixture(), &uri).unwrap();
        json!({
            "path": path,
            "uri": uri,
            "size": size
        })
    };
    let document = json!({
        "kind": "pqbench.table",
        "version": 1,
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": ["year"],
        "log": [],
        "files": [
            file("year=2023/part-0.parquet"),
            file("year=2024/part-0.parquet"),
            file("year=2024/part-1.parquet"),
            file("year=2025/part-0.parquet")
        ]
    });
    let output_path = directory.path().join("sample.parquet");
    let output = pipe(
        &[
            "dump",
            "--include",
            "year=2024/**",
            "--sample",
            "first:1",
            "--output",
            output_path.to_str().unwrap(),
        ],
        &document.to_string(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(&output_path).unwrap();
    assert_parquet(&bytes);
    let fixture_len = std::fs::metadata(parquet_fixture()).unwrap().len();
    assert!(
        (bytes.len() as u64) < fixture_len.saturating_mul(2),
        "sampled dump should keep one file, got {} bytes vs fixture {fixture_len}",
        bytes.len()
    );
}

#[test]
fn dump_writes_parquet_to_output() {
    let directory = tempfile::tempdir().unwrap();
    let output_path = directory.path().join("sample.parquet");
    let output = pqbench()
        .args([
            "dump",
            parquet_fixture(),
            "--row-groups",
            "first:1",
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_parquet(&std::fs::read(&output_path).unwrap());
}

#[test]
fn dump_rejects_unknown_row_groups() {
    let directory = tempfile::tempdir().unwrap();
    let output_path = directory.path().join("sample.parquet");
    let output = pqbench()
        .args([
            "dump",
            parquet_fixture(),
            "--row-groups",
            "every:2",
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("first:N"), "{stderr}");
}

#[test]
fn dump_rejects_a_lake_that_has_not_been_loaded() {
    let document = json!({
        "kind": "pqbench.lake",
        "version": 1,
        "tables": [{"name": "events", "uri": "/tmp/events"}]
    });
    let output = pipe(&["dump"], &document.to_string());
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("pqbench table"), "{stderr}");
}

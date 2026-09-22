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

#[test]
fn dump_writes_csv_from_a_parquet_file() {
    let output = pqbench()
        .args(["dump", "--csv", parquet_fixture()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let header = stdout.lines().next().unwrap();
    assert!(header.starts_with("_path,"));
    assert!(header.contains("url_encoded"));
    assert!(stdout.contains('\n'));
}

#[test]
fn dump_writes_ndjson_from_a_table_document() {
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
    let output = pipe(&["dump", "--json"], &document.to_string());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let first: serde_json::Value = serde_json::from_str(stdout.lines().next().unwrap()).unwrap();
    assert_eq!(first["_path"], "small_reddit_none.parquet");
    assert!(first.get("url_encoded").is_some());
    assert_eq!(stdout.lines().count(), 3000);
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
    let output = pipe(
        &[
            "dump",
            "--json",
            "--include",
            "year=2024/**",
            "--sample",
            "first:1",
        ],
        &document.to_string(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.lines().count(), 3000);
    let paths: std::collections::BTreeSet<_> = stdout
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["_path"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(
        paths.iter().map(String::as_str).collect::<Vec<_>>(),
        ["year=2024/part-0.parquet"]
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
    let bytes = std::fs::read(&output_path).unwrap();
    assert!(bytes.starts_with(b"PAR1"));
    assert!(bytes.ends_with(b"PAR1"));

    let csv = pqbench()
        .args(["dump", "--csv", output_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(csv.status.success());
    let stdout = String::from_utf8(csv.stdout).unwrap();
    assert!(stdout.starts_with("_path,"));
    assert!(stdout.contains("url_encoded"));
}

#[test]
fn dump_rejects_unknown_row_groups() {
    let output = pqbench()
        .args([
            "dump",
            parquet_fixture(),
            "--row-groups",
            "every:2",
            "--csv",
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

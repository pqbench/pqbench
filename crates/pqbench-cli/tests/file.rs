#![cfg(feature = "delta")]

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::json;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
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

fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

struct DeltaFixture {
    // Held only to keep the temporary directory alive for the test's duration.
    // aipnaming: allow(aip-140/underscores)
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
}

/// A Delta table whose single commit records `commitInfo.timestamp` (ms).
fn dated_delta_fixture(timestamp: i64) -> DeltaFixture {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_path_buf();
    std::fs::create_dir(root.join("_delta_log")).unwrap();
    let data = root.join("data.parquet");
    std::fs::copy(parquet_fixture(), &data).unwrap();
    let size = std::fs::metadata(&data).unwrap().len();
    let commit = json!([
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {
            "id": "11111111-1111-1111-1111-111111111111",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }},
        {"add": {
            "path": "data.parquet",
            "partitionValues": {},
            "size": size,
            "modificationTime": 0,
            "dataChange": true,
            "stats": "{\"numRecords\":3000,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":1},\"nullCount\":{\"id\":0},\"tightBounds\":true}"
        }},
        {"commitInfo": {"timestamp": timestamp, "operation": "WRITE"}}
    ]);
    let text = commit
        .as_array()
        .unwrap()
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(root.join("_delta_log/00000000000000000000.json"), text).unwrap();
    DeltaFixture {
        _directory: directory,
        path: root,
    }
}

/// `list partitions | list files` names the files the window's commits added.
#[test]
fn file_ls_lists_the_files_a_window_added() {
    let fixture = dated_delta_fixture(1_700_000_000_000);
    let listed = pipe(
        &[
            "list",
            "partitions",
            fixture.path.to_str().unwrap(),
            "--format",
            "json",
        ],
        "",
    );
    assert!(
        listed.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let files = pipe(
        &["list", "files", "--format", "json"],
        &String::from_utf8_lossy(&listed.stdout),
    );
    assert!(
        files.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&files.stderr)
    );
    let records = ndjson_records(&files.stdout);
    assert_eq!(records.len(), 1);
    let name = records[0]["name"].as_str().unwrap();
    assert!(
        name.starts_with("catalogs/local/schemas/local/tables/"),
        "{name}"
    );
    assert!(name.ends_with("/files/data.parquet"), "{name}");
    let blob = records[0]["blob"].as_str().unwrap();
    assert!(blob.starts_with("blobs/file/"), "{blob}");
    assert!(blob.ends_with("data.parquet"), "{blob}");
}

/// The summary counts partitions and files apart: one partition's stream yields
/// many files, and a table run shows both counts.
#[test]
fn file_ls_counts_partitions_and_files_apart() {
    let fixture = dated_delta_fixture(1_700_000_000_000);
    let listed = pipe(
        &[
            "list",
            "partitions",
            fixture.path.to_str().unwrap(),
            "--format",
            "json",
        ],
        "",
    );
    assert!(listed.status.success());
    let files = pipe(
        &["list", "files", "--format", "table"],
        &String::from_utf8_lossy(&listed.stdout),
    );
    assert!(
        files.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&files.stderr)
    );
    let out = String::from_utf8_lossy(&files.stdout);
    assert!(out.contains("partitions: 1"), "{out}");
    assert!(out.contains("files: 1"), "{out}");
}

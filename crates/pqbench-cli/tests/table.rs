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
fn bytemass_reads_a_table_document_from_stdin() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let document = json!({
        "kind": "pqbench.table",
        "version": 1,
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": [],
        "log": [{"version": 0, "actions": [{"add": {"path": "small_reddit_none.parquet"}}]}],
        "files": [{"path": "small_reddit_none.parquet", "uri": parquet_fixture(), "size": size}]
    });
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("bytemass: small_reddit_none.parquet"));
    assert!(stdout.contains("url_encoded"));
}

#[cfg(feature = "delta")]
#[test]
fn table_detects_delta_and_pipes_the_log_to_bytemass() {
    let fixture = delta_fixture();
    let table = pqbench()
        .args(["table", fixture.path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        table.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&table.stderr)
    );
    let info: serde_json::Value = serde_json::from_slice(&table.stdout).unwrap();
    assert_eq!(info["kind"], "pqbench.table");
    assert_eq!(info["format"], "delta");
    assert_eq!(info["snapshot_version"], 0);
    assert_eq!(info["files"].as_array().unwrap().len(), 1);

    let measured = pipe(
        &["bytemass", "--json"],
        std::str::from_utf8(&table.stdout).unwrap(),
    );
    assert!(
        measured.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&measured.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&measured.stdout).unwrap();
    assert_eq!(report["file_count"], 1);
    assert_eq!(report["num_rows"], 3000);
}

#[test]
fn bytemass_reads_an_iceberg_table_document_from_stdin() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let document = json!({
        "kind": "pqbench.table",
        "version": 1,
        "format": "iceberg",
        "uri": "/tmp/table",
        "snapshot_version": 1,
        "partition_columns": [],
        "log": [{"version": 1, "actions": [{"snapshot": {"snapshot-id": 1}}]}],
        "files": [{"path": "data/small_reddit_none.parquet", "uri": parquet_fixture(), "size": size}]
    });
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("bytemass: small_reddit_none.parquet"));
}

#[test]
fn table_rejects_an_unrecognized_directory() {
    let directory = tempfile::tempdir().unwrap();
    let output = pqbench()
        .args(["table", directory.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unrecognized table format"), "{stderr}");
}

#[cfg(feature = "delta")]
struct DeltaFixture {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
}

#[cfg(feature = "delta")]
fn delta_fixture() -> DeltaFixture {
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
            "dataChange": true
        }}
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

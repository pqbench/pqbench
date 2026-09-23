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
        "log": [{"version": 0, "actions": [{"kind": "add", "path": "small_reddit_none.parquet"}]}],
        "files": [{"path": "small_reddit_none.parquet", "uri": parquet_fixture(), "size_bytes": size}]
    });
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("pqbench.bytemass-row"));
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
    let records = ndjson_records(&table.stdout);
    assert_eq!(records[0]["kind"], "pqbench.table");
    assert_eq!(records[0]["event"], "begin");
    assert_eq!(records[0]["id"], fixture.path.to_str().unwrap());
    assert_eq!(records[0]["format"], "delta");
    assert_eq!(records[0]["snapshot_version"], 0);
    assert_eq!(
        records
            .iter()
            .filter(|record| record["kind"] == "pqbench.table-file")
            .count(),
        1
    );
    assert_eq!(records.last().unwrap()["event"], "end");

    let measured = pipe(
        &["bytemass", "--json"],
        std::str::from_utf8(&table.stdout).unwrap(),
    );
    assert!(
        measured.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&measured.stderr)
    );
    let records = ndjson_records(&measured.stdout);
    let end = records
        .iter()
        .find(|record| record["event"] == "end")
        .expect("bytemass end");
    assert_eq!(end["file_count"], 1);
    assert_eq!(end["row_count"], 3000);
}

#[cfg(not(feature = "delta"))]
#[test]
fn table_names_the_delta_feature_when_it_is_off() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("_delta_log")).unwrap();
    let output = pqbench()
        .args(["table", directory.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("`delta` feature") || stderr.contains("delta"),
        "{stderr}"
    );
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
        "log": [{"version": 1, "actions": [{"kind": "snapshot"}]}],
        "files": [{"path": "data/small_reddit_none.parquet", "uri": parquet_fixture(), "size": size}]
    });
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("pqbench.bytemass-row"));
    assert!(stdout.contains("url_encoded"));
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

#[test]
fn bytemass_rejects_a_non_aws_env_key() {
    let document = json!({
        "kind": "pqbench.remote-source",
        "version": 1,
        "inputs": [parquet_fixture()],
        "env": {"NOT_AWS": "x"}
    });
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("AWS_*"), "{stderr}");
}

#[test]
fn bytemass_rejects_a_size_mismatch_on_a_table_document() {
    let document = json!({
        "kind": "pqbench.table",
        "version": 1,
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": [],
        "log": [],
        "files": [{"path": "small_reddit_none.parquet", "uri": parquet_fixture(), "size_bytes": 1}]
    });
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("size differs from log"), "{stderr}");
}

#[test]
fn table_rewrites_a_table_document_as_ndjson() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let document = json!({
        "kind": "pqbench.table",
        "version": 1,
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": [],
        "log": [{"version": 0, "actions": [{"kind": "add", "path": "small_reddit_none.parquet"}]}],
        "files": [{"path": "small_reddit_none.parquet", "uri": parquet_fixture(), "size_bytes": size}]
    });
    let output = pipe(&["table"], &document.to_string());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records[0]["event"], "begin");
    assert_eq!(records[0]["id"], "/tmp/table");
    assert_eq!(records[1]["kind"], "pqbench.table-log");
    assert_eq!(records[2]["kind"], "pqbench.table-file");
    assert_eq!(records[2]["id"], "/tmp/table");
    assert_eq!(records[3]["event"], "end");
}

#[test]
fn table_writes_a_zstd_stream_to_output() {
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
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("table.ndjson.zst");
    let output = pipe(
        &["table", "-o", path.to_str().unwrap()],
        &document.to_string(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records[0]["event"], "begin");
    assert_eq!(records.last().unwrap()["event"], "end");
    let magic = std::fs::read(&path).unwrap();
    assert_eq!(&magic[..4], [0x28, 0xB5, 0x2F, 0xFD]);
    let measured = pqbench()
        .args(["bytemass", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        measured.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&measured.stderr)
    );
    let stdout = String::from_utf8(measured.stdout).unwrap();
    assert!(stdout.contains("pqbench.bytemass-row"));
}

#[test]
fn bytemass_reads_a_table_stream_from_stdin() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let begin = json!({
        "kind": "pqbench.table",
        "version": 1,
        "event": "begin",
        "id": "t1",
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": []
    });
    let file = json!({
        "kind": "pqbench.table-file",
        "id": "t1",
        "path": "small_reddit_none.parquet",
        "uri": parquet_fixture(),
        "size_bytes": size
    });
    let end = json!({"kind": "pqbench.table", "event": "end", "id": "t1"});
    let document = format!("{begin}\n{file}\n{end}\n");
    let output = pipe(&["bytemass"], &document);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("pqbench.bytemass-row"));
    assert!(stdout.contains("url_encoded"));
}

#[test]
fn bytemass_rejects_a_truncated_table_stream() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let begin = json!({
        "kind": "pqbench.table",
        "version": 1,
        "event": "begin",
        "id": "t1",
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": []
    });
    let file = json!({
        "kind": "pqbench.table-file",
        "id": "t1",
        "path": "small_reddit_none.parquet",
        "uri": parquet_fixture(),
        "size_bytes": size
    });
    let document = format!("{begin}\n{file}\n");
    let output = pipe(&["bytemass"], &document);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ended without end"), "{stderr}");
}

#[test]
fn bytemass_rejects_a_size_mismatch_on_a_table_stream() {
    let begin = json!({
        "kind": "pqbench.table",
        "version": 1,
        "event": "begin",
        "id": "t1",
        "format": "delta",
        "uri": "/tmp/table",
        "snapshot_version": 0,
        "partition_columns": []
    });
    let file = json!({
        "kind": "pqbench.table-file",
        "id": "t1",
        "path": "small_reddit_none.parquet",
        "uri": parquet_fixture(),
        "size_bytes": 1
    });
    let end = json!({"kind": "pqbench.table", "event": "end", "id": "t1"});
    let output = pipe(&["bytemass"], &format!("{begin}\n{file}\n{end}\n"));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("size differs from log"), "{stderr}");
}

#[test]
fn bytemass_measures_mixed_table_ids() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let begin_a = json!({
        "kind": "pqbench.table",
        "version": 1,
        "event": "begin",
        "id": "a",
        "format": "delta",
        "uri": "/tmp/a",
        "snapshot_version": 0,
        "partition_columns": []
    });
    let begin_b = json!({
        "kind": "pqbench.table",
        "version": 1,
        "event": "begin",
        "id": "b",
        "format": "delta",
        "uri": "/tmp/b",
        "snapshot_version": 0,
        "partition_columns": []
    });
    let file = json!({
        "kind": "pqbench.table-file",
        "path": "small_reddit_none.parquet",
        "uri": parquet_fixture(),
        "size_bytes": size
    });
    let mut file_b = file.clone();
    file_b["id"] = json!("b");
    let mut file_a = file;
    file_a["id"] = json!("a");
    let document = format!(
        "{begin_a}\n{begin_b}\n{file_b}\n{file_a}\n{}\n{}\n",
        json!({"kind": "pqbench.table", "event": "end", "id": "b"}),
        json!({"kind": "pqbench.table", "event": "end", "id": "a"}),
    );
    let output = pipe(&["bytemass"], &document);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    let ids: Vec<_> = records
        .iter()
        .filter(|record| record["kind"] == "pqbench.bytemass-row")
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"a".to_string()));
    assert!(ids.contains(&"b".to_string()));
}

fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

#[cfg(feature = "delta")]
struct DeltaFixture {
    // Held only to keep the temporary directory alive for the test's duration.
    // aipnaming: allow(aip-140/underscores)
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

#[cfg(feature = "delta")]
#[test]
fn table_exits_cleanly_when_stdout_is_closed() {
    let fixture = delta_fixture();
    let mut child = pqbench()
        .args(["table", fixture.path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "delta")]
#[test]
fn table_reports_a_failed_snapshot_without_dependency_panics() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    std::fs::create_dir(root.join("_delta_log")).unwrap();
    let commit = json!([
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {
            "id": "11111111-1111-1111-1111-111111111111",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": ["part"],
            "configuration": {},
            "createdTime": 0
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

    let output = pqbench()
        .args(["table", root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Partition column"), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
}

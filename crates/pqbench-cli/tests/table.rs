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
        "files": [{"path": "data/small_reddit_none.parquet", "uri": parquet_fixture(), "size_bytes": size}]
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
            "dataChange": true,
            "stats": "{\"numRecords\":3000,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":1},\"nullCount\":{\"id\":0},\"tightBounds\":true}"
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

/// A Delta table whose single commit records `commitInfo.timestamp` (ms).
#[cfg(feature = "delta")]
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

/// `table ls` dates each commit by its `commitInfo.timestamp` and groups it
/// into an epoch-aligned, half-open UTC window.
#[cfg(feature = "delta")]
#[test]
fn table_ls_groups_commits_into_natural_windows() {
    let timestamp = 1_700_000_000_000i64;
    let fixture = dated_delta_fixture(timestamp);
    let output = pipe(
        &[
            "table",
            "ls",
            fixture.path.to_str().unwrap(),
            "--format",
            "json",
        ],
        "",
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    let partition = &records[0];
    assert_eq!(partition["kind"], "pqbench.partition");
    assert_eq!(partition["definition"]["kind"], "natural");
    let day = 86_400_000i64;
    let first = timestamp.div_euclid(day) * day;
    assert_eq!(partition["definition"]["first_time"], first);
    assert_eq!(partition["definition"]["last_time"], first + day);
    assert_eq!(partition["commits"][0]["version"], 0);
    assert_eq!(partition["commits"][0]["commit_time"], timestamp);
}

/// `--every` sets the window width; the boundary stays epoch-aligned.
#[cfg(feature = "delta")]
#[test]
fn table_ls_honors_the_window_width() {
    let timestamp = 1_700_000_000_000i64;
    let fixture = dated_delta_fixture(timestamp);
    let output = pipe(
        &[
            "table",
            "ls",
            fixture.path.to_str().unwrap(),
            "--every",
            "1h",
            "--format",
            "json",
        ],
        "",
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    let hour = 3_600_000i64;
    let first = timestamp.div_euclid(hour) * hour;
    assert_eq!(records[0]["definition"]["first_time"], first);
    assert_eq!(records[0]["definition"]["last_time"], first + hour);
}

/// A commit the log does not date is omitted: a window never claims a commit
/// it cannot place.
#[cfg(feature = "delta")]
#[test]
fn table_ls_skips_commits_without_a_time() {
    let fixture = delta_fixture();
    let output = pipe(
        &[
            "table",
            "ls",
            fixture.path.to_str().unwrap(),
            "--format",
            "json",
        ],
        "",
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(ndjson_records(&output.stdout).is_empty());
}

/// A malformed `--every` fails loudly before any table is read.
#[cfg(feature = "delta")]
#[test]
fn table_ls_rejects_a_bad_window() {
    let fixture = dated_delta_fixture(0);
    let output = pipe(
        &[
            "table",
            "ls",
            fixture.path.to_str().unwrap(),
            "--every",
            "1x",
        ],
        "",
    );
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--every unit"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `table ls | partition ls` names the files the window's commits added.
#[cfg(feature = "delta")]
#[test]
fn partition_ls_lists_the_files_a_window_added() {
    let fixture = dated_delta_fixture(1_700_000_000_000);
    let listed = pipe(
        &[
            "table",
            "ls",
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
        &["partition", "ls", "--format", "json"],
        &String::from_utf8_lossy(&listed.stdout),
    );
    assert!(
        files.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&files.stderr)
    );
    let records = ndjson_records(&files.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["kind"], "pqbench.table-file");
    assert_eq!(records[0]["path"], "data.parquet");
    assert!(
        records[0]["uri"]
            .as_str()
            .unwrap()
            .ends_with("/data.parquet"),
        "{}",
        records[0]["uri"]
    );
}

#[cfg(feature = "delta")]
#[test]
fn table_ls_exits_cleanly_when_stdout_is_closed() {
    let fixture = dated_delta_fixture(1_700_000_000_000);
    let mut child = pqbench()
        .args(["table", "ls", fixture.path.to_str().unwrap()])
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

/// A version 2 table-ref from a catalog walk must carry a storage path; one
/// without it fails loudly and names the stage that fills it.
#[test]
fn table_ls_rejects_a_ref_without_a_storage_path() {
    let input =
        r#"{"kind":"pqbench.table-ref","version":2,"id":"c.s.t","uri":"https://example/table"}"#;
    let output = pipe(&["table", "ls"], input);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("storage path"), "{stderr}");
}

use serde_json::json;
use std::io::{Read, Write};
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
    pipe_env(args, stdin.as_bytes(), &[])
}

/// pqbench with extra environment: the walk's context can come from `PQB_*`.
fn pipe_env(args: &[&str], stdin: &[u8], env: &[(&str, &str)]) -> std::process::Output {
    let mut child = pqbench()
        .args(args)
        .envs(env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
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

/// One endpoint that answers by request-line substring: the first matching
/// route wins, so put the more specific needle first. No match is a 404.
/// Bodies are owned so a fixture path can be embedded.
fn routes(routes: Vec<(&'static str, u16, String)>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().take(16) {
            let mut stream = stream.unwrap();
            let mut buffer = [0u8; 2048];
            let read = stream.read(&mut buffer).unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]);
            let (status, body) = routes
                .iter()
                .find(|(needle, _, _)| request.contains(needle))
                .map(|(_, status, body)| (*status, body.as_str()))
                .unwrap_or((404, "{}"));
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.write_all(body.as_bytes());
        }
    });
    address
}

/// A minimal Delta table on disk: one commit with a protocol and metadata and
/// no active files. The `add` action names a file that does not exist, so a
/// read that materializes files would fail.
#[cfg(feature = "delta")]
fn delta_table(dir: &std::path::Path) {
    let log = dir.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let schema = json!({
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
            {"name": "label", "type": "string", "nullable": true, "metadata": {}}
        ]
    });
    let commit = format!(
        "{}\n{}\n{}\n",
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "a1b2c3d4-0000-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": [],
            "configuration": {"delta.appendOnly": "false"},
            "createdTime": 1_700_000_000_000i64
        }}),
        json!({"add": {
            "path": "part-does-not-exist.parquet",
            "size": 123,
            "partitionValues": {},
            "dataChange": true
        }})
    );
    std::fs::write(log.join("00000000000000000000.json"), commit).unwrap();
}

/// A Unity `/tables/{full_name}` record for a local table.
#[cfg(feature = "delta")]
fn unity_record(location: &std::path::Path) -> String {
    json!({
        "name": "trips",
        "catalog_name": "dbx_samples",
        "schema_name": "nyctaxi",
        "data_source_format": "DELTA",
        "storage_location": location.to_string_lossy(),
        "columns": [
            {"name": "id", "type_name": "LONG", "type_text": "long", "nullable": true},
            {"name": "label", "type_name": "STRING", "type_text": "string", "nullable": true}
        ],
        "properties": {"owner": "data"}
    })
    .to_string()
}

/// An Iceberg REST `loadTable` record with the metadata JSON inline. The
/// `metadata-location` names a path that does not exist, so a command that
/// reads storage would fail.
fn loaded_table(location: &str) -> String {
    json!({
        "metadata-location": format!("{location}/metadata/00001.metadata.json"),
        "metadata": {
            "format-version": 2,
            "location": location,
            "current-snapshot-id": 3268038499157964613i64,
            "current-schema-id": 0,
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": false, "type": "long"},
                {"id": 2, "name": "label", "required": true, "type": "string"}
            ]}],
            "properties": {"owner": "data"}
        }
    })
    .to_string()
}

/// `table info` reads one table's record from Unity: the Delta log without
/// files, merged with the catalog's declared columns and properties.
#[cfg(feature = "delta")]
#[test]
fn table_info_reads_a_unity_table() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let address = routes(vec![("/tables/", 200, unity_record(dir.path()))]);
    let output = pipe_env(
        &[
            "table",
            "info",
            "dbx_samples.nyctaxi.trips",
            "--format",
            "json",
        ],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["kind"], "pqbench.table-ref");
    assert_eq!(record["version"], 2);
    assert_eq!(record["id"], "dbx_samples.nyctaxi.trips");
    assert_eq!(record["format"], "delta");
    assert_eq!(record["snapshot_version"], 0);
    assert_eq!(
        record["storage_path"],
        dir.path().to_string_lossy().as_ref()
    );
    let columns: Vec<&str> = record["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| column["name"].as_str().unwrap())
        .collect();
    assert_eq!(columns, ["id", "label"]);
    assert_eq!(record["delta_properties"]["owner"], "data");
    assert_eq!(record["delta_properties"]["delta.appendOnly"], "false");
    assert!(record.get("iceberg_properties").is_none(), "{record:?}");
}

#[cfg(feature = "delta")]
#[test]
fn table_info_prints_the_record_as_a_table() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let address = routes(vec![("/tables/", 200, unity_record(dir.path()))]);
    let output = pipe_env(
        &[
            "table",
            "info",
            "dbx_samples.nyctaxi.trips",
            "--format",
            "table",
        ],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples.nyctaxi.trips"), "{stdout}");
    assert!(stdout.contains("delta"), "{stdout}");
    assert!(stdout.contains("tables: 1"), "{stdout}");
}

/// `table info` reads the Iceberg REST `loadTable` metadata inline, so no
/// storage read runs.
#[test]
fn table_info_reads_an_iceberg_rest_table() {
    let address = routes(vec![(
        "/namespaces/nyctaxi/tables/trips",
        200,
        loaded_table("s3://bucket/events"),
    )]);
    let output = pipe_env(
        &[
            "table",
            "info",
            "dbx_samples.nyctaxi.trips",
            "--format",
            "json",
        ],
        b"",
        &[
            ("PQB_ENDPOINT", address.as_str()),
            ("PQB_TABLE_FORMAT", "iceberg"),
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["kind"], "pqbench.table-ref");
    assert_eq!(record["version"], 2);
    assert_eq!(record["id"], "dbx_samples.nyctaxi.trips");
    assert_eq!(record["format"], "iceberg");
    assert_eq!(record["snapshot_version"], 3268038499157964613i64);
    assert_eq!(record["storage_path"], "s3://bucket/events");
    let columns: Vec<&str> = record["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| column["name"].as_str().unwrap())
        .collect();
    assert_eq!(columns, ["id", "label"]);
    assert_eq!(record["columns"][1]["nullable"], false);
    assert_eq!(record["iceberg_properties"]["owner"], "data");
    assert!(record.get("delta_properties").is_none(), "{record:?}");
}

/// An Iceberg namespace with dots is one path segment, escaped as `%1F`.
#[test]
fn table_info_reads_an_iceberg_namespace_with_dots() {
    let address = routes(vec![(
        "/namespaces/ns%1Fone/tables/trips",
        200,
        loaded_table("s3://bucket/events"),
    )]);
    let output = pipe_env(
        &[
            "table",
            "info",
            "dbx_samples.ns.one.trips",
            "--format",
            "json",
        ],
        b"",
        &[
            ("PQB_ENDPOINT", address.as_str()),
            ("PQB_TABLE_FORMAT", "iceberg"),
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records[0]["id"], "dbx_samples.ns.one.trips");
}

/// `schema ls | table info` chains: the refs on stdin name each table.
#[cfg(feature = "delta")]
#[test]
fn table_info_reads_a_table_ref_stream() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let record = unity_record(dir.path());
    let address = routes(vec![
        ("/tables/dbx_samples.nyctaxi.zones", 200, record.clone()),
        ("/tables/dbx_samples.nyctaxi.trips", 200, record),
    ]);
    let refs = concat!(
        r#"{"kind":"pqbench.table-ref","version":2,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips"}"#,
        "\n",
        r#"{"kind":"pqbench.table-ref","version":2,"id":"dbx_samples.nyctaxi.zones","uri":"http://x/tables/dbx_samples.nyctaxi.zones"}"#,
        "\n",
    );
    let output = pipe_env(
        &["table", "info", "--format", "json"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    let mut names: Vec<&str> = records
        .iter()
        .map(|record| record["id"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["dbx_samples.nyctaxi.trips", "dbx_samples.nyctaxi.zones"]
    );
}

#[test]
fn table_info_rejects_v1_refs() {
    let refs = r#"{"kind":"pqbench.table-ref","version":1,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips"}"#;
    let output = pipe_env(
        &["table", "info"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("version 2"), "{stderr}");
}

#[test]
fn table_info_takes_a_name_or_a_stream_not_both() {
    let refs = r#"{"kind":"pqbench.table-ref","version":1,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips"}"#;
    let output = pipe_env(
        &["table", "info", "dbx_samples.nyctaxi.trips"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both"), "{stderr}");
}

#[test]
fn table_info_needs_a_dotted_name() {
    let output = pipe_env(
        &["table", "info", "trips"],
        b"",
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CATALOG.SCHEMA.TABLE"), "{stderr}");
}

#[test]
fn table_info_rejects_an_empty_document() {
    let output = pipe(&["table", "info", "dbx_samples.nyctaxi.trips"], "");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lake-source"), "{stderr}");
}

#[test]
fn table_info_reports_an_unknown_table() {
    let address = routes(vec![]);
    let output = pipe_env(
        &["table", "info", "dbx_samples.nyctaxi.nope"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

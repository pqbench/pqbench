//! Blackbox tests for `pqbench tablev2 info`: one table's record from both
//! catalog dialects, refs on stdin, and the metadata-only read. The Unity
//! path reads the Delta log without files; the Iceberg path uses the
//! `loadTable` metadata inline and never touches storage.

use std::io::{Read, Write};
use std::process::{Command, Stdio};

use serde_json::{json, Value};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn pipe(args: &[&str], stdin: &[u8]) -> std::process::Output {
    pipe_env(args, stdin, &[])
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

fn ndjson(stdout: &[u8]) -> Vec<Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
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

#[cfg(feature = "delta")]
#[test]
fn tablev2_info_reads_a_unity_table() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let address = routes(vec![("/tables/", 200, unity_record(dir.path()))]);
    let output = pipe_env(
        &[
            "tablev2",
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
    let records = ndjson(&output.stdout);
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
fn tablev2_info_prints_the_record_as_a_table() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let address = routes(vec![("/tables/", 200, unity_record(dir.path()))]);
    let output = pipe_env(
        &[
            "tablev2",
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

#[test]
fn tablev2_info_reads_an_iceberg_rest_table() {
    let address = routes(vec![(
        "/namespaces/nyctaxi/tables/trips",
        200,
        loaded_table("s3://bucket/events"),
    )]);
    let output = pipe_env(
        &[
            "tablev2",
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
    let records = ndjson(&output.stdout);
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

#[test]
fn tablev2_info_reads_an_iceberg_namespace_with_dots() {
    let address = routes(vec![(
        "/namespaces/ns%1Fone/tables/trips",
        200,
        loaded_table("s3://bucket/events"),
    )]);
    let output = pipe_env(
        &[
            "tablev2",
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
    let records = ndjson(&output.stdout);
    assert_eq!(records[0]["id"], "dbx_samples.ns.one.trips");
}

#[cfg(feature = "delta")]
#[test]
fn tablev2_info_reads_a_table_ref_stream() {
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
        &["tablev2", "info", "--format", "json"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
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
fn tablev2_info_rejects_v1_refs() {
    let refs = r#"{"kind":"pqbench.table-ref","version":1,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips"}"#;
    let output = pipe_env(
        &["tablev2", "info"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("version 2"), "{stderr}");
}

#[test]
fn tablev2_info_takes_a_name_or_a_stream_not_both() {
    let refs = r#"{"kind":"pqbench.table-ref","version":1,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips"}"#;
    let output = pipe_env(
        &["tablev2", "info", "dbx_samples.nyctaxi.trips"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both"), "{stderr}");
}

#[test]
fn tablev2_info_needs_a_dotted_name() {
    let output = pipe_env(
        &["tablev2", "info", "trips"],
        b"",
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CATALOG.SCHEMA.TABLE"), "{stderr}");
}

#[test]
fn tablev2_info_rejects_an_empty_document() {
    let output = pipe(&["tablev2", "info", "dbx_samples.nyctaxi.trips"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lake-source"), "{stderr}");
}

#[test]
fn tablev2_info_reports_an_unknown_table() {
    let address = routes(vec![]);
    let output = pipe_env(
        &["tablev2", "info", "dbx_samples.nyctaxi.nope"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

#[cfg(feature = "delta")]
#[test]
fn tablev2_info_names_the_location_when_the_metadata_cannot_be_read() {
    let address = routes(vec![(
        "/tables/dbx_samples.nyctaxi.trips",
        200,
        unity_record(std::path::Path::new("/nonexistent/table")),
    )]);
    let output = pipe_env(
        &["tablev2", "info", "dbx_samples.nyctaxi.trips"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("dbx_samples.nyctaxi.trips"), "{stderr}");
    assert!(stderr.contains("/nonexistent/table"), "{stderr}");
}

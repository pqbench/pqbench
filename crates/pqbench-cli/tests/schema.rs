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

/// One endpoint that answers every GET with `status` and `body`.
fn server(status: u16, body: &'static str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().take(16) {
            let mut stream = stream.unwrap();
            let mut buffer = [0u8; 2048];
            let _ = stream.read(&mut buffer);
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

/// One endpoint that answers by request-line substring: the first matching
/// route wins, so put the more specific needle first. No match is a 404.
fn routes(routes: &'static [(&'static str, u16, &'static str)]) -> String {
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
                .map(|(_, status, body)| (*status, *body))
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

fn source(endpoint: &str) -> Vec<u8> {
    json!({"kind": "pqbench.lake-source", "version": 1, "endpoint": endpoint})
        .to_string()
        .into_bytes()
}

const SCHEMA: &str = r#"{"name":"nyctaxi","catalog_name":"dbx_samples","comment":"taxi data","storage_location":"s3://bucket/nyctaxi","properties":{"owner":"data"}}"#;

const TABLES: &str = r#"{"tables":[{"name":"trips","full_name":"dbx_samples.nyctaxi.trips","data_source_format":"DELTA","storage_location":"s3://bucket/trips"},{"name":"recent","table_type":"VIEW"}],"next_page_token":null}"#;
const TABLES_PAGE: &str = r#"{"tables":[{"name":"trips","full_name":"dbx_samples.nyctaxi.trips","data_source_format":"DELTA","storage_location":"s3://bucket/trips"}],"next_page_token":"more"}"#;
const TABLES_LAST: &str = r#"{"tables":[{"name":"zones","full_name":"dbx_samples.nyctaxi.zones","data_source_format":"DELTA","storage_location":"s3://bucket/zones"}]}"#;

const ICEBERG_CONFIG: &str = r#"{"defaults":{"snapshot-loading-mode":"refs"},"overrides":{"prefix":"catalogs/dbx_samples"}}"#;
const ICEBERG_NAMESPACE: &str =
    r#"{"namespace":["nyctaxi"],"properties":{"location":"s3://bucket/nyctaxi","owner":"data"}}"#;
const ICEBERG_TABLES: &str =
    r#"{"identifiers":[{"namespace":["nyctaxi"],"name":"trips"}],"next-page-token":null}"#;
const ICEBERG_LOADED: &str = r#"{"metadata-location":"s3://bucket/trips/metadata/00000.json","metadata":{"location":"s3://bucket/trips"}}"#;

/// The parent level's stream: one `pqbench.schema` ref per schema.
const SCHEMA_REFS: &str = concat!(
    r#"{"kind":"pqbench.schema","version":1,"catalog":"dbx_samples","name":"nyctaxi"}"#,
    "\n",
    r#"{"kind":"pqbench.schema","version":1,"catalog":"dbx_samples","name":"bakehouse"}"#,
    "\n",
);

#[test]
fn schema_info_prints_the_record_as_a_table() {
    let address = server(200, SCHEMA);
    let output = pipe(
        &["schema", "info", "dbx_samples.nyctaxi", "--format", "table"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("nyctaxi"), "{stdout}");
    assert!(stdout.contains("taxi data"), "{stdout}");
    assert!(stdout.contains("s3://bucket/nyctaxi"), "{stdout}");
    assert!(stdout.contains("schemas: 1"), "{stdout}");
}

#[test]
fn schema_info_streams_the_record_as_ndjson() {
    let address = server(200, SCHEMA);
    let output = pipe(
        &["schema", "info", "dbx_samples.nyctaxi", "--format", "json"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["kind"], "pqbench.schema");
    assert_eq!(records[0]["version"], 1);
    assert_eq!(records[0]["catalog"], "dbx_samples");
    assert_eq!(records[0]["name"], "nyctaxi");
    assert_eq!(records[0]["comment"], "taxi data");
    assert_eq!(records[0]["location"], "s3://bucket/nyctaxi");
    assert_eq!(records[0]["properties"]["owner"], "data");
}

#[test]
fn schema_info_reads_the_iceberg_rest_namespace() {
    let address = routes(&[
        ("/v1/config", 200, ICEBERG_CONFIG),
        ("/namespaces/", 200, ICEBERG_NAMESPACE),
    ]);
    let output = pipe(
        &["schema", "info", "dbx_samples.nyctaxi", "--format", "json"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], "nyctaxi");
    assert_eq!(records[0]["location"], "s3://bucket/nyctaxi");
    assert_eq!(records[0]["properties"]["owner"], "data");
}

#[test]
fn schema_info_reads_a_schema_stream() {
    let address = server(200, SCHEMA);
    let output = pipe_env(
        &["schema", "info", "--format", "json"],
        SCHEMA_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["catalog"], "dbx_samples");
    assert_eq!(records[0]["name"], "nyctaxi");
}

#[test]
fn schema_info_takes_a_schema_or_a_stream_not_both() {
    let address = server(200, SCHEMA);
    let output = pipe_env(
        &["schema", "info", "dbx_samples.nyctaxi"],
        SCHEMA_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both"), "{stderr}");
}

#[test]
fn schema_info_needs_a_dotted_name() {
    let address = server(200, SCHEMA);
    let output = pipe_env(
        &["schema", "info", "nyctaxi"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CATALOG.SCHEMA"), "{stderr}");
}

#[test]
fn schema_info_rejects_an_empty_document() {
    let output = pipe(&["schema", "info", "dbx_samples.nyctaxi"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lake-source"), "{stderr}");
}

#[test]
fn schema_ls_prints_the_tables_as_a_table() {
    let address = routes(&[("/tables", 200, TABLES)]);
    let output = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "table"],
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
    assert!(stdout.contains("s3://bucket/trips"), "{stdout}");
    assert!(stdout.contains("DELTA"), "{stdout}");
    assert!(stdout.contains("tables: 1"), "{stdout}");
}

#[test]
fn schema_ls_streams_table_refs() {
    let address = routes(&[("/tables", 200, TABLES)]);
    let output = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
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
    assert_eq!(records[0]["kind"], "pqbench.table-ref");
    assert_eq!(records[0]["version"], 1);
    assert_eq!(records[0]["id"], "dbx_samples.nyctaxi.trips");
    assert_eq!(records[0]["uri"], "s3://bucket/trips");
    assert_eq!(records[0]["format"], "DELTA");
}

#[test]
fn schema_ls_lists_iceberg_rest_tables() {
    let address = routes(&[
        ("/v1/config", 200, ICEBERG_CONFIG),
        ("/namespaces/nyctaxi/tables/trips", 200, ICEBERG_LOADED),
        ("/namespaces/nyctaxi/tables", 200, ICEBERG_TABLES),
    ]);
    let output = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
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
    assert_eq!(records[0]["kind"], "pqbench.table-ref");
    assert_eq!(records[0]["id"], "dbx_samples.nyctaxi.trips");
    assert_eq!(records[0]["uri"], "s3://bucket/trips/metadata/00000.json");
    assert_eq!(records[0]["format"], "ICEBERG");
}

#[test]
fn schema_ls_follows_unity_page_tokens() {
    let address = routes(&[
        ("page_token=", 200, TABLES_LAST),
        ("/tables", 200, TABLES_PAGE),
    ]);
    let output = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["dbx_samples.nyctaxi.trips", "dbx_samples.nyctaxi.zones"]
    );
}

#[test]
fn schema_ls_reads_a_schema_stream() {
    let address = routes(&[("/tables", 200, TABLES)]);
    let output = pipe_env(
        &["schema", "ls", "--format", "json"],
        SCHEMA_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["kind"], "pqbench.table-ref");
}

#[test]
fn schema_ls_reports_an_unknown_schema() {
    let address = server(404, r#"{"message":"Schema 'nope' does not exist."}"#);
    let output = pipe_env(
        &["schema", "ls", "dbx_samples.nope"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

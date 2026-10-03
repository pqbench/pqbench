use std::io::{Read, Write};
use std::process::{Command, Stdio};

use serde_json::{json, Value};

/// Decode an lz4 frame (`-o` output) into its NDJSON bytes.
fn decode_lz4(bytes: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::new();
    lz4::Decoder::new(std::io::Cursor::new(bytes))
        .unwrap()
        .read_to_end(&mut decoded)
        .unwrap();
    decoded
}

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
        for stream in listener.incoming().take(8) {
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
        for stream in listener.incoming().take(8) {
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

const CATALOG: &str = r#"{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG","comment":"sample catalog","owner":"owner@example.com"}"#;

const SYSTEM_CATALOG: &str =
    r#"{"name":"system","catalog_type":"SYSTEM_CATALOG","comment":"system catalog"}"#;

const SCHEMAS: &str =
    r#"{"schemas":[{"name":"nyctaxi"},{"name":"bakehouse"}],"next_page_token":null}"#;
const SCHEMAS_PAGE: &str = r#"{"schemas":[{"name":"nyctaxi"}],"next_page_token":"more"}"#;
const SCHEMAS_LAST: &str = r#"{"schemas":[{"name":"bakehouse"}]}"#;
const ICEBERG_NAMESPACES: &str =
    r#"{"namespaces":[["nyctaxi"],["bakehouse"]],"next-page-token":null}"#;
const ICEBERG_PAGE: &str = r#"{"namespaces":[["nyctaxi"]],"next-page-token":"more"}"#;
const ICEBERG_LAST: &str = r#"{"namespaces":[["bakehouse"]],"next-page-token":null}"#;

/// The parent level's stream: one `pqbench.catalog` ref per catalog.
const CATALOG_REFS: &str = concat!(
    r#"{"kind":"pqbench.catalog","version":1,"name":"dbx_samples"}"#,
    "\n",
    r#"{"kind":"pqbench.catalog","version":1,"name":"system"}"#,
    "\n",
);

fn source(endpoint: &str) -> Vec<u8> {
    json!({"kind": "pqbench.lake-source", "version": 1, "endpoint": endpoint})
        .to_string()
        .into_bytes()
}

#[test]
fn catalog_info_prints_the_record_as_a_table() {
    let address = server(200, CATALOG);
    let output = pipe(
        &["catalog", "info", "dbx_samples", "--format", "table"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples"), "{stdout}");
    assert!(stdout.contains("MANAGED_CATALOG"), "{stdout}");
    assert!(stdout.contains("catalogs: 1"), "{stdout}");
}

#[test]
fn catalog_info_streams_the_record_as_ndjson() {
    let address = server(200, CATALOG);
    let output = pipe(
        &["catalog", "info", "dbx_samples", "--format", "json"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["kind"], "pqbench.catalog");
    assert_eq!(records[0]["version"], 1);
    assert_eq!(records[0]["name"], "dbx_samples");
    assert_eq!(records[0]["catalog_type"], "MANAGED_CATALOG");
    assert_eq!(records[0]["comment"], "sample catalog");
    assert_eq!(records[0]["owner"], "owner@example.com");
}

#[test]
fn catalog_info_exports_the_ndjson_stream() {
    let address = server(200, CATALOG);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("catalog.ndjson.zst");
    let output = pipe(
        &[
            "catalog",
            "info",
            "dbx_samples",
            "--format",
            "table",
            "-o",
            file.to_str().unwrap(),
        ],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(&file).unwrap();
    let decoded = decode_lz4(&bytes);
    let records = ndjson(&decoded);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["kind"], "pqbench.catalog");
    assert_eq!(records[0]["name"], "dbx_samples");
}

#[test]
fn catalog_info_reports_an_unknown_catalog() {
    let address = server(404, r#"{"message":"Catalog 'nope' does not exist."}"#);
    let output = pipe(&["catalog", "info", "nope"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

#[test]
fn catalog_info_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["catalog", "info", "dbx_samples"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
fn catalog_info_rejects_an_empty_document() {
    let output = pipe(&["catalog", "info", "dbx_samples"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lake-source"), "{stderr}");
}

#[test]
fn catalog_ls_prints_the_schemas_as_a_table() {
    let address = server(200, SCHEMAS);
    let output = pipe(
        &["catalog", "ls", "dbx_samples", "--format", "table"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples"), "{stdout}");
    assert!(stdout.contains("bakehouse"), "{stdout}");
    assert!(stdout.contains("schemas: 2"), "{stdout}");
}

#[test]
fn catalog_ls_streams_the_schemas_as_ndjson() {
    let address = server(200, SCHEMAS);
    let output = pipe(
        &["catalog", "ls", "dbx_samples", "--format", "json"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["bakehouse", "nyctaxi"]);
    for record in &records {
        assert_eq!(record["kind"], "pqbench.schema");
        assert_eq!(record["version"], 1);
        assert_eq!(record["catalog"], "dbx_samples");
    }
}

#[test]
fn catalog_ls_follows_unity_page_tokens() {
    let address = routes(&[
        ("page_token=", 200, SCHEMAS_LAST),
        ("/schemas", 200, SCHEMAS_PAGE),
    ]);
    let output = pipe(
        &["catalog", "ls", "dbx_samples", "--format", "json"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["bakehouse", "nyctaxi"]);
}

#[test]
fn catalog_ls_lists_iceberg_rest_namespaces() {
    let address = routes(&[("/namespaces", 200, ICEBERG_NAMESPACES)]);
    let output = pipe_env(
        &["catalog", "ls", "dbx_samples", "--format", "json"],
        &source(&address),
        &[("PQB_TABLE_FORMAT", "iceberg")],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["bakehouse", "nyctaxi"]);
    for record in &records {
        assert_eq!(record["kind"], "pqbench.schema");
        assert_eq!(record["catalog"], "dbx_samples");
    }
}

#[test]
fn catalog_ls_follows_iceberg_page_tokens() {
    let address = routes(&[
        ("pageToken=", 200, ICEBERG_LAST),
        ("/namespaces", 200, ICEBERG_PAGE),
    ]);
    let output = pipe_env(
        &["catalog", "ls", "dbx_samples", "--format", "json"],
        &source(&address),
        &[("PQB_TABLE_FORMAT", "iceberg")],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["bakehouse", "nyctaxi"]);
}

#[test]
fn catalog_ls_exports_the_ndjson_stream() {
    let address = server(200, SCHEMAS);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("schemas.ndjson.zst");
    let output = pipe(
        &[
            "catalog",
            "ls",
            "dbx_samples",
            "--format",
            "table",
            "-o",
            file.to_str().unwrap(),
        ],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(&file).unwrap();
    let decoded = decode_lz4(&bytes);
    let records = ndjson(&decoded);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["kind"], "pqbench.schema");
    assert_eq!(records[1]["name"], "nyctaxi");
}

#[test]
fn catalog_ls_reports_an_unknown_catalog() {
    let address = server(404, r#"{"message":"Catalog 'nope' does not exist."}"#);
    let output = pipe(&["catalog", "ls", "nope"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

#[test]
fn catalog_ls_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["catalog", "ls", "dbx_samples"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
fn catalog_ls_rejects_an_empty_document() {
    let output = pipe(&["catalog", "ls", "dbx_samples"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lake-source"), "{stderr}");
}

#[test]
fn catalog_info_enriches_a_catalog_stream() {
    let address = routes(&[
        ("/catalogs/dbx_samples", 200, CATALOG),
        ("/catalogs/system", 200, SYSTEM_CATALOG),
    ]);
    let output = pipe_env(
        &["catalog", "info", "--format", "json"],
        CATALOG_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["kind"], "pqbench.catalog");
    assert_eq!(records[0]["name"], "dbx_samples");
    assert_eq!(records[0]["comment"], "sample catalog");
    assert_eq!(records[1]["name"], "system");
}

#[test]
fn catalog_ls_lists_a_catalog_stream() {
    let address = server(200, SCHEMAS);
    let output = pipe_env(
        &["catalog", "ls", "--format", "json"],
        CATALOG_REFS.as_bytes(),
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
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["bakehouse", "nyctaxi", "bakehouse", "nyctaxi"]);
    assert_eq!(records[0]["catalog"], "dbx_samples");
    assert_eq!(records[2]["catalog"], "system");
}

#[test]
fn catalog_info_prefers_the_document_over_the_environment() {
    let address = server(200, CATALOG);
    let output = pipe_env(
        &["catalog", "info", "dbx_samples", "--format", "json"],
        &source(&address),
        &[("PQB_ENDPOINT", "http://127.0.0.1:9")],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], "dbx_samples");
}

#[test]
fn catalog_info_takes_a_catalog_or_a_stream_not_both() {
    let address = server(200, CATALOG);
    let output = pipe_env(
        &["catalog", "info", "dbx_samples"],
        CATALOG_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both"), "{stderr}");
}

#[test]
fn catalog_info_reports_an_empty_stream() {
    let address = server(200, CATALOG);
    let output = pipe_env(
        &["catalog", "info", "--format", "table"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("catalogs: 0"), "{stdout}");
}

#[test]
fn catalog_info_rejects_a_bad_fan_out() {
    let output = pipe(
        &["catalog", "info", "dbx_samples", "--fan-out", "lots"],
        b"",
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("fan-out"), "{stderr}");
}

use std::io::{Read, Write};
use std::process::{Command, Stdio};

use serde_json::{json, Value};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn pipe(args: &[&str], stdin: &[u8]) -> std::process::Output {
    let mut child = pqbench()
        .args(args)
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

const SUMMARY: &str = r#"{"name":"metastore_aws_us_east_2","metastore_id":"29dded30","cloud":"aws","region":"us-east-2"}"#;

fn source(endpoint: &str) -> Vec<u8> {
    json!({"kind": "pqbench.lake-source", "version": 1, "endpoint": endpoint})
        .to_string()
        .into_bytes()
}

#[test]
fn metastore_info_prints_the_record_as_a_table() {
    let address = server(200, SUMMARY);
    let output = pipe(
        &["metastore", "info", "--format", "table"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("metastore_aws_us_east_2"), "{stdout}");
    assert!(stdout.contains("us-east-2"), "{stdout}");
    assert!(stdout.contains("metastores: 1"), "{stdout}");
}

#[test]
fn metastore_info_streams_the_record_as_ndjson() {
    let address = server(200, SUMMARY);
    let output = pipe(
        &["metastore", "info", "--format", "json"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["kind"], "pqbench.metastore");
    assert_eq!(records[0]["version"], 1);
    assert_eq!(records[0]["name"], "metastore_aws_us_east_2");
    assert_eq!(records[0]["id"], "29dded30");
    assert_eq!(records[0]["cloud"], "aws");
    assert_eq!(records[0]["region"], "us-east-2");
}

#[test]
fn metastore_info_exports_the_ndjson_stream() {
    let address = server(200, SUMMARY);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("metastore.ndjson.zst");
    let output = pipe(
        &[
            "metastore",
            "info",
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
    let decoded = zstd::decode_all(&bytes[..]).unwrap();
    let records = ndjson(&decoded);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["kind"], "pqbench.metastore");
}

#[test]
fn metastore_info_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["metastore", "info"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
fn metastore_info_rejects_a_document_that_is_not_a_lake_source() {
    let document = json!({
        "kind": "pqbench.lake",
        "version": 1,
        "tables": [{"name": "events", "uri": "/tmp/events"}]
    });
    let output = pipe(&["metastore", "info"], document.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lake-source"), "{stderr}");
}

#[test]
fn metastore_info_rejects_an_empty_document() {
    let output = pipe(&["metastore", "info"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lake-source"), "{stderr}");
}

/// One endpoint that answers one page per request, in order.
fn server_pages(bodies: &'static [&'static str]) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for (stream, body) in listener.incoming().zip(bodies) {
            let mut stream = stream.unwrap();
            let mut buffer = [0u8; 2048];
            let _ = stream.read(&mut buffer);
            let response = format!(
                "HTTP/1.1 200 X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.write_all(body.as_bytes());
        }
    });
    address
}

const CATALOGS: &str = r#"{"catalogs":[{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG"},{"name":"system","catalog_type":"SYSTEM_CATALOG"}]}"#;

const PAGES: [&str; 2] = [
    r#"{"catalogs":[{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG"}],"next_page_token":"page-2"}"#,
    r#"{"catalogs":[{"name":"samples","catalog_type":"SYSTEM_CATALOG"},{"name":"workspace","catalog_type":"MANAGED_CATALOG"}]}"#,
];

#[test]
fn metastore_ls_prints_the_catalogs_as_a_table() {
    let address = server(200, CATALOGS);
    let output = pipe(&["metastore", "ls", "--format", "table"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples"), "{stdout}");
    assert!(stdout.contains("MANAGED_CATALOG"), "{stdout}");
    assert!(stdout.contains("catalogs: 2"), "{stdout}");
}

#[test]
fn metastore_ls_streams_one_catalog_per_line() {
    let address = server(200, CATALOGS);
    let output = pipe(&["metastore", "ls", "--format", "json"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["kind"], "pqbench.catalog");
    assert_eq!(records[0]["version"], 1);
    assert_eq!(records[0]["name"], "dbx_samples");
    assert_eq!(records[0]["catalog_type"], "MANAGED_CATALOG");
    assert_eq!(records[1]["name"], "system");
}

#[test]
fn metastore_ls_follows_the_next_page_token() {
    let address = server_pages(&PAGES);
    let output = pipe(&["metastore", "ls", "--format", "json"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let names: Vec<&str> = records
        .iter()
        .filter_map(|record| record["name"].as_str())
        .collect();
    assert_eq!(names, ["dbx_samples", "samples", "workspace"]);
}

#[test]
fn metastore_ls_reports_an_empty_catalog_list() {
    let address = server(200, r#"{"catalogs":[]}"#);
    let output = pipe(&["metastore", "ls", "--format", "table"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("catalogs: 0"), "{stdout}");
}

#[test]
fn metastore_ls_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["metastore", "ls"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

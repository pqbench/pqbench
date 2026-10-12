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

fn pipe(args: &[&str], source: &[(&str, &str)]) -> std::process::Output {
    pipe_env(args, b"", source)
}

/// pqbench with stdin, no extra environment.
fn pipe_stdin(args: &[&str], stdin: &[u8]) -> std::process::Output {
    pipe_env(args, stdin, &[])
}

/// pqbench with extra environment: the walk's context comes from `PQB_*`.
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

const CATALOG: &str = r#"{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG","comment":"sample catalog","owner":"owner@example.com"}"#;

const SYSTEM_CATALOG: &str =
    r#"{"name":"system","catalog_type":"SYSTEM_CATALOG","comment":"system catalog"}"#;

/// The parent level's stream: one `Catalog` resource per catalog.
const CATALOG_REFS: &str = concat!(
    r#"{"name":"catalogs/dbx_samples"}"#,
    "\n",
    r#"{"name":"catalogs/system"}"#,
    "\n",
);

const CATALOGS: &str = r#"{"catalogs":[{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG"},{"name":"system","catalog_type":"SYSTEM_CATALOG"}]}"#;

const PAGES: [&str; 2] = [
    r#"{"catalogs":[{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG"}],"next_page_token":"page-2"}"#,
    r#"{"catalogs":[{"name":"samples","catalog_type":"SYSTEM_CATALOG"},{"name":"workspace","catalog_type":"MANAGED_CATALOG"}]}"#,
];

/// The walk's config: `PQB_ENDPOINT` names the endpoint.
fn source(endpoint: &str) -> [(&str, &str); 1] {
    [("PQB_ENDPOINT", endpoint)]
}

#[test]
fn catalog_get_prints_the_record_as_a_table() {
    let address = server(200, CATALOG);
    let output = pipe(
        &["get", "catalog", "dbx_samples", "--format", "table"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples"), "{stdout}");
    assert!(stdout.contains("CATALOG_TYPE_MANAGED"), "{stdout}");
    assert!(stdout.contains("catalogs: 1"), "{stdout}");
}

#[test]
fn catalog_get_streams_the_record_as_ndjson() {
    let address = server(200, CATALOG);
    let output = pipe(
        &["get", "catalog", "dbx_samples", "--format", "json"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], "catalogs/dbx_samples");
    assert_eq!(records[0]["catalog_type"], "CATALOG_TYPE_MANAGED");
    assert_eq!(records[0]["comment"], "sample catalog");
    assert_eq!(records[0]["owner"], "owner@example.com");
    assert!(records[0].get("kind").is_none(), "{}", records[0]);
}

#[test]
fn catalog_get_exports_the_ndjson_stream() {
    let address = server(200, CATALOG);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("catalog.ndjson.zst");
    let output = pipe(
        &[
            "get",
            "catalog",
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
    assert_eq!(records[0]["name"], "catalogs/dbx_samples");
}

#[test]
fn catalog_get_reports_an_unknown_catalog() {
    let address = server(404, r#"{"message":"Catalog 'nope' does not exist."}"#);
    let output = pipe(&["get", "catalog", "nope"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

#[test]
fn catalog_get_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["get", "catalog", "dbx_samples"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
fn catalog_get_rejects_an_empty_document() {
    let output = pipe_stdin(&["get", "catalog", "dbx_samples"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PQB_ENDPOINT"), "{stderr}");
}

#[test]
fn catalog_get_enriches_a_catalog_stream() {
    let address = routes(&[
        ("/catalogs/dbx_samples", 200, CATALOG),
        ("/catalogs/system", 200, SYSTEM_CATALOG),
    ]);
    let output = pipe_env(
        &["get", "catalog", "--format", "json"],
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
    let dbx_samples = records
        .iter()
        .find(|record| record["name"] == "catalogs/dbx_samples")
        .unwrap();
    assert_eq!(dbx_samples["comment"], "sample catalog");
    assert!(records
        .iter()
        .any(|record| record["name"] == "catalogs/system"));
}

#[test]
fn catalog_get_takes_a_catalog_or_a_stream_not_both() {
    let address = server(200, CATALOG);
    let output = pipe_env(
        &["get", "catalog", "dbx_samples"],
        CATALOG_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both"), "{stderr}");
}

#[test]
fn catalog_get_reports_an_empty_stream() {
    let address = server(200, CATALOG);
    let output = pipe_env(
        &["get", "catalog", "--format", "table"],
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
fn catalog_get_rejects_a_bad_fan_out() {
    let output = pipe_stdin(&["get", "catalog", "dbx_samples", "--fan-out", "lots"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("fan-out"), "{stderr}");
}

#[test]
fn catalog_ls_prints_the_catalogs_as_a_table() {
    let address = server(200, CATALOGS);
    let output = pipe(
        &["list", "catalogs", "--format", "table"],
        &source(&address),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples"), "{stdout}");
    assert!(stdout.contains("CATALOG_TYPE_MANAGED"), "{stdout}");
    assert!(stdout.contains("catalogs: 2"), "{stdout}");
}

#[test]
fn catalog_ls_streams_one_catalog_per_line() {
    let address = server(200, CATALOGS);
    let output = pipe(&["list", "catalogs", "--format", "json"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], "catalogs/dbx_samples");
    assert_eq!(records[0]["catalog_type"], "CATALOG_TYPE_MANAGED");
    assert_eq!(records[1]["name"], "catalogs/system");
    assert!(records[0].get("kind").is_none(), "{}", records[0]);
}

#[test]
fn catalog_ls_follows_the_next_page_token() {
    let address = server_pages(&PAGES);
    let output = pipe(&["list", "catalogs", "--format", "json"], &source(&address));
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
    assert_eq!(
        names,
        [
            "catalogs/dbx_samples",
            "catalogs/samples",
            "catalogs/workspace"
        ]
    );
}

#[test]
fn catalog_ls_reports_an_empty_catalog_list() {
    let address = server(200, r#"{"catalogs":[]}"#);
    let output = pipe(
        &["list", "catalogs", "--format", "table"],
        &source(&address),
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
fn catalog_ls_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["list", "catalogs"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
fn catalog_ls_reads_the_endpoint_from_the_environment() {
    let address = server(200, CATALOGS);
    let output = pipe_env(
        &["list", "catalogs", "--format", "json"],
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
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["catalogs/dbx_samples", "catalogs/system"]);
}

#[test]
fn catalog_ls_reads_a_metastore_stream() {
    let address = server(200, CATALOGS);
    let record = json!({"name": "metastores/unity"});
    let output = pipe_env(
        &["list", "catalogs", "--format", "json"],
        record.to_string().as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(ndjson(&output.stdout).len(), 2);
}

#[test]
fn catalog_ls_rejects_a_non_metastore_stream() {
    let address = server(200, CATALOGS);
    let record = json!({"name": "catalogs/dbx_samples"});
    let output = pipe_env(
        &["list", "catalogs", "--format", "json"],
        record.to_string().as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("metastore"), "{stderr}");
}

use std::io::{Read, Write};
use std::process::{Command, Stdio};

use serde_json::Value;

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

/// Render a command's stdout with `pqbench format`.
fn table(bytes: &[u8]) -> String {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .arg("format")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(bytes).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
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
/// route wins, so put the more specific needle first. No match is a 404; a `0`
/// status closes the connection with no answer, which the caller sees as a
/// transport failure.
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
            if status == 0 {
                drop(stream);
                continue;
            }
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

/// The walk's config: `PQB_ENDPOINT` names the endpoint.
fn source(endpoint: &str) -> [(&str, &str); 1] {
    [("PQB_ENDPOINT", endpoint)]
}

const SCHEMA: &str = r#"{"name":"nyctaxi","catalog_name":"dbx_samples","comment":"taxi data","storage_location":"s3://bucket/nyctaxi","properties":{"owner":"data"}}"#;

const ICEBERG_NAMESPACE: &str =
    r#"{"namespace":["nyctaxi"],"properties":{"location":"s3://bucket/nyctaxi","owner":"data"}}"#;

/// The parent level's stream: one `Schema` resource per schema.
const SCHEMA_REFS: &str = concat!(
    r#"{"name":"catalogs/dbx_samples/schemas/nyctaxi"}"#,
    "\n",
    r#"{"name":"catalogs/dbx_samples/schemas/bakehouse"}"#,
    "\n",
);

const SCHEMAS: &str =
    r#"{"schemas":[{"name":"nyctaxi"},{"name":"bakehouse"}],"next_page_token":null}"#;
const SCHEMAS_PAGE: &str = r#"{"schemas":[{"name":"nyctaxi"}],"next_page_token":"more"}"#;
const SCHEMAS_LAST: &str = r#"{"schemas":[{"name":"bakehouse"}]}"#;
const ICEBERG_NAMESPACES: &str =
    r#"{"namespaces":[["nyctaxi"],["bakehouse"]],"next-page-token":null}"#;
const ICEBERG_PAGE: &str = r#"{"namespaces":[["nyctaxi"]],"next-page-token":"more"}"#;
const ICEBERG_LAST: &str = r#"{"namespaces":[["bakehouse"]],"next-page-token":null}"#;

/// The parent level's stream: one `Catalog` resource per catalog.
const CATALOG_REFS: &str = concat!(
    r#"{"name":"catalogs/dbx_samples"}"#,
    "\n",
    r#"{"name":"catalogs/system"}"#,
    "\n",
);

/// The same stream with the unreachable catalog first, so a drop cannot be
/// confused with the stream ending.
const CATALOG_REFS_BAD_FIRST: &str = concat!(
    r#"{"name":"catalogs/system"}"#,
    "\n",
    r#"{"name":"catalogs/dbx_samples"}"#,
    "\n",
);

#[test]
fn schema_get_prints_the_record_as_a_table() {
    let address = server(200, SCHEMA);
    let output = pipe(&["get", "schema", "dbx_samples.nyctaxi"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = table(&output.stdout);
    assert!(stdout.contains("nyctaxi"), "{stdout}");
    assert!(stdout.contains("taxi data"), "{stdout}");
    assert!(stdout.contains("s3://bucket/nyctaxi"), "{stdout}");
}

#[test]
fn schema_get_streams_the_record_as_ndjson() {
    let address = server(200, SCHEMA);
    let output = pipe(&["get", "schema", "dbx_samples.nyctaxi"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], "catalogs/dbx_samples/schemas/nyctaxi");
    assert_eq!(records[0]["comment"], "taxi data");
    assert_eq!(records[0]["uri"], "s3://bucket/nyctaxi");
    assert_eq!(records[0]["properties"]["owner"], "data");
    assert!(records[0].get("kind").is_none(), "{}", records[0]);
}

#[test]
fn schema_get_reads_the_iceberg_rest_namespace() {
    let address = routes(&[("/namespaces/", 200, ICEBERG_NAMESPACE)]);
    let output = pipe_env(
        &["get", "schema", "dbx_samples.nyctaxi"],
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
    assert_eq!(records[0]["name"], "catalogs/dbx_samples/schemas/nyctaxi");
    assert_eq!(records[0]["uri"], "s3://bucket/nyctaxi");
    assert_eq!(records[0]["properties"]["owner"], "data");
}

#[test]
fn schema_get_reads_a_schema_stream() {
    let address = server(200, SCHEMA);
    let output = pipe_env(
        &["get", "schema"],
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
    // The canned endpoint answers `nyctaxi` for either ref.
    assert!(records
        .iter()
        .all(|record| record["name"] == "catalogs/dbx_samples/schemas/nyctaxi"));
}

#[test]
fn schema_get_takes_a_schema_or_a_stream_not_both() {
    let address = server(200, SCHEMA);
    let output = pipe_env(
        &["get", "schema", "dbx_samples.nyctaxi"],
        SCHEMA_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both"), "{stderr}");
}

#[test]
fn schema_get_needs_a_dotted_name() {
    let address = server(200, SCHEMA);
    let output = pipe_env(
        &["get", "schema", "nyctaxi"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CATALOG.SCHEMA"), "{stderr}");
}

#[test]
fn schema_get_rejects_an_empty_document() {
    let output = pipe_stdin(&["get", "schema", "dbx_samples.nyctaxi"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PQB_ENDPOINT"), "{stderr}");
}

#[test]
fn schema_ls_prints_the_schemas_as_a_table() {
    let address = server(200, SCHEMAS);
    let output = pipe(&["list", "schemas", "dbx_samples"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = table(&output.stdout);
    assert!(stdout.contains("dbx_samples"), "{stdout}");
    assert!(stdout.contains("bakehouse"), "{stdout}");
}

#[test]
fn schema_ls_streams_the_schemas_as_ndjson() {
    let address = server(200, SCHEMAS);
    let output = pipe(&["list", "schemas", "dbx_samples"], &source(&address));
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
    assert_eq!(
        names,
        [
            "catalogs/dbx_samples/schemas/bakehouse",
            "catalogs/dbx_samples/schemas/nyctaxi"
        ]
    );
    for record in &records {
        assert!(record.get("kind").is_none(), "{record}");
    }
}

#[test]
fn schema_ls_follows_unity_page_tokens() {
    let address = routes(&[
        ("page_token=", 200, SCHEMAS_LAST),
        ("/schemas", 200, SCHEMAS_PAGE),
    ]);
    let output = pipe(&["list", "schemas", "dbx_samples"], &source(&address));
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
    assert_eq!(
        names,
        [
            "catalogs/dbx_samples/schemas/bakehouse",
            "catalogs/dbx_samples/schemas/nyctaxi"
        ]
    );
}

#[test]
fn schema_ls_lists_iceberg_rest_namespaces() {
    let address = routes(&[("/namespaces", 200, ICEBERG_NAMESPACES)]);
    let output = pipe_env(
        &["list", "schemas", "dbx_samples"],
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
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "catalogs/dbx_samples/schemas/bakehouse",
            "catalogs/dbx_samples/schemas/nyctaxi"
        ]
    );
    for record in &records {
        assert!(record.get("kind").is_none(), "{record}");
    }
}

#[test]
fn schema_ls_follows_iceberg_page_tokens() {
    let address = routes(&[
        ("pageToken=", 200, ICEBERG_LAST),
        ("/namespaces", 200, ICEBERG_PAGE),
    ]);
    let output = pipe_env(
        &["list", "schemas", "dbx_samples"],
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
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "catalogs/dbx_samples/schemas/bakehouse",
            "catalogs/dbx_samples/schemas/nyctaxi"
        ]
    );
}

#[test]
fn schema_ls_reports_an_unknown_catalog() {
    let address = server(404, r#"{"message":"Catalog 'nope' does not exist."}"#);
    let output = pipe(&["list", "schemas", "nope"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

#[test]
fn schema_ls_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["list", "schemas", "dbx_samples"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
fn schema_ls_rejects_an_empty_document() {
    let output = pipe_stdin(&["list", "schemas", "dbx_samples"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PQB_ENDPOINT"), "{stderr}");
}

#[test]
fn schema_ls_lists_a_catalog_stream() {
    let address = server(200, SCHEMAS);
    let output = pipe_env(
        &["list", "schemas"],
        CATALOG_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 4);
    for record in &records {
        assert!(record.get("kind").is_none(), "{record}");
    }
    let schemas = |catalog: &str| -> Vec<&str> {
        let prefix = format!("catalogs/{catalog}/schemas/");
        records
            .iter()
            .filter_map(|record| record["name"].as_str())
            .filter_map(|name| name.strip_prefix(&prefix))
            .collect()
    };
    assert_eq!(schemas("dbx_samples"), ["bakehouse", "nyctaxi"]);
    assert_eq!(schemas("system"), ["bakehouse", "nyctaxi"]);
}

#[test]
fn schema_ls_drops_a_catalog_that_never_answers() {
    // The walk shares one endpoint: one catalog's request gets no answer (a
    // timeout or a dropped connection) while the rest answer. The level reports
    // it, finishes the stream, and then exits non-zero — one unreachable ref
    // does not truncate the walk.
    let address = routes(&[
        ("catalog_name=dbx_samples", 200, SCHEMAS),
        ("catalog_name=system", 0, ""),
    ]);
    let output = pipe_env(
        &["list", "schemas"],
        CATALOG_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        !output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2, "{records:?}");
    assert!(records.iter().all(|record| record["name"]
        .as_str()
        .unwrap()
        .starts_with("catalogs/dbx_samples/")));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("catalogs/system"), "{stderr}");
}

#[test]
fn schema_ls_drops_a_catalog_that_answers_an_error() {
    // The drop is type-agnostic: a 500 is treated like a timeout, named on
    // standard error, and the rest of the walk carries on. The run exits
    // non-zero after the stream finishes.
    let address = routes(&[
        ("catalog_name=dbx_samples", 200, SCHEMAS),
        ("catalog_name=system", 500, r#"{"message":"boom"}"#),
    ]);
    let output = pipe_env(
        &["list", "schemas"],
        CATALOG_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        !output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2, "{records:?}");
    assert!(records.iter().all(|record| record["name"]
        .as_str()
        .unwrap()
        .starts_with("catalogs/dbx_samples/")));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("catalogs/system"), "{stderr}");
    assert!(stderr.contains("500"), "{stderr}");
}

#[test]
fn schema_ls_keeps_reading_after_a_drop() {
    // The failed ref is first and one request is in flight, so the good
    // catalog's schemas are read only after the drop — not already buffered
    // when the stream ends.
    let address = routes(&[
        ("catalog_name=dbx_samples", 200, SCHEMAS),
        ("catalog_name=system", 0, ""),
    ]);
    let output = pipe_env(
        &["list", "schemas", "--fan-out", "1"],
        CATALOG_REFS_BAD_FIRST.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        !output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2, "{records:?}");
    assert!(records.iter().all(|record| record["name"]
        .as_str()
        .unwrap()
        .starts_with("catalogs/dbx_samples/")));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("catalogs/system"), "{stderr}");
}

#[test]
fn schema_ls_counts_every_dropped_catalog() {
    let refs = concat!(
        r#"{"name":"catalogs/system"}"#,
        "\n",
        r#"{"name":"catalogs/system"}"#,
        "\n",
    );
    let address = routes(&[("catalog_name=system", 0, "")]);
    let output = pipe_env(
        &["list", "schemas", "--fan-out", "1"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.matches("error: catalogs/system").count(),
        2,
        "{stderr}"
    );
}

#[test]
fn schema_ls_finishes_the_table_before_a_drop() {
    let address = routes(&[
        ("catalog_name=dbx_samples", 200, SCHEMAS),
        ("catalog_name=system", 0, ""),
    ]);
    let output = pipe_env(
        &["list", "schemas", "--fan-out", "1"],
        CATALOG_REFS_BAD_FIRST.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        !output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples"), "{stdout}");
}

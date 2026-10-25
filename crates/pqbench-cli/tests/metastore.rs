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

/// The walk's config: `PQB_ENDPOINT` names the endpoint.
fn source(endpoint: &str) -> [(&str, &str); 1] {
    [("PQB_ENDPOINT", endpoint)]
}

#[test]
fn metastore_get_prints_the_record_as_a_table() {
    let address = server(200, SUMMARY);
    let output = pipe(&["get", "metastore"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = table(&output.stdout);
    assert!(stdout.contains("metastores/unity"), "{stdout}");
    assert!(stdout.contains("us-east-2"), "{stdout}");
}

#[test]
fn metastore_get_streams_the_record_as_ndjson() {
    let address = server(200, SUMMARY);
    let output = pipe(&["get", "metastore"], &source(&address));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], "metastores/unity");
    assert_eq!(records[0]["uid"], "29dded30");
    assert_eq!(records[0]["display_name"], "metastore_aws_us_east_2");
    assert_eq!(records[0]["cloud_provider"], "aws");
    assert_eq!(records[0]["region"], "us-east-2");
    assert!(records[0].get("kind").is_none(), "{}", records[0]);
}

#[test]
fn metastore_get_reports_an_unauthorized_endpoint() {
    let address = server(401, r#"{"message":"Unauthorized"}"#);
    let output = pipe(&["get", "metastore"], &source(&address));
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
fn metastore_get_needs_an_endpoint() {
    let output = pipe_stdin(&["get", "metastore"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PQB_ENDPOINT"), "{stderr}");
}

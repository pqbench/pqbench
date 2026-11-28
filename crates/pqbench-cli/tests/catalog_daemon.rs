//! The catalog levels through the daemon: records arrive unchanged, and the
//! daemon paces the Unity requests they fan out to.

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

fn pqbench() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pqbench"));
    command.env_remove("PQB_PROFILE");
    command.env("PQB_CONFIG_FILE", "/dev/null");
    command
}

/// One request the fake endpoint answered: its path, the bearer it carried, and
/// when it arrived.
#[derive(Clone)]
struct Hit {
    path: String,
    authorization: Option<String>,
    request_time: Instant,
}

/// The `Authorization` header a request carried, as the fake endpoint read it.
fn bearer(request: &str) -> Option<String> {
    request.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then(|| value.trim().to_string())
    })
}

/// A fake Unity endpoint. Every request is recorded, and the path selects the
/// page: catalogs, then schemas, then tables.
fn server() -> (String, Arc<Mutex<Vec<Hit>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&hits);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]);
            let line = request.lines().next().unwrap_or_default();
            let path = line.split_whitespace().nth(1).unwrap_or_default();
            recorded.lock().unwrap().push(Hit {
                path: path.to_string(),
                authorization: bearer(&request),
                request_time: Instant::now(),
            });
            let body = if path.contains("/catalogs") {
                r#"{"catalogs":[{"name":"c1"},{"name":"c2"}]}"#
            } else if path.contains("/schemas") {
                r#"{"schemas":[{"name":"s1"},{"name":"s2"}]}"#
            } else if path.contains("/tables") {
                r#"{"tables":[{"name":"t1","full_name":"c1.s1.t1","storage_location":"s3://b/t1"},{"name":"t2","full_name":"c1.s1.t2","storage_location":"s3://b/t2"}]}"#
            } else {
                "{}"
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (address, hits)
}

fn ndjson(stdout: &[u8]) -> Vec<Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

fn pipe(args: &[&str], stdin: &[u8], env: &[(&str, &str)]) -> std::process::Output {
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

fn start(socket: &str, endpoint: &str) {
    let status = pqbench()
        .args(["daemon", "start", "--idle-seconds", "10"])
        .env("PQB_DAEMON_SOCK", socket)
        .env("PQB_ENDPOINT", endpoint)
        .env("PQB_TABLE_FORMAT", "unity")
        .env("PQB_CONFIG_FILE", "/dev/null")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .unwrap();
    assert!(status.success(), "the daemon did not start");
    for _ in 0..250 {
        let status = Command::new(env!("CARGO_BIN_EXE_pqbench"))
            .args(["daemon", "status"])
            .env("PQB_CONFIG_FILE", "/dev/null")
            .env("PQB_DAEMON_SOCK", socket)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        if status.success() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("the daemon did not answer");
}

fn stop(socket: &str) {
    let _ = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(["daemon", "stop"])
        .env("PQB_CONFIG_FILE", "/dev/null")
        .env("PQB_DAEMON_SOCK", socket)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// The whole listing pipeline writes the same records with and without the
/// daemon, and the daemon spaces its Unity requests at 15/s.
#[test]
fn the_catalog_levels_go_through_the_daemon() {
    let (endpoint, hits) = server();
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("daemon.sock");
    let socket = socket.to_str().unwrap();
    start(socket, &endpoint);
    let env = [
        ("PQB_DAEMON_SOCK", socket),
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TABLE_FORMAT", "unity"),
        ("PQB_TOKEN", "token"),
    ];

    let catalogs = pipe(&["list", "catalogs"], b"", &env);
    assert!(
        catalogs.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&catalogs.stderr)
    );
    let catalogs = ndjson(&catalogs.stdout);
    assert_eq!(catalogs.len(), 2);

    let schemas = pipe(&["list", "schemas"], &catalogs_output(&catalogs), &env);
    assert!(
        schemas.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&schemas.stderr)
    );
    let schemas = ndjson(&schemas.stdout);
    assert_eq!(schemas.len(), 4);
    assert!(schemas
        .iter()
        .any(|schema| schema["name"] == "catalogs/c1/schemas/s1"));

    let tables = pipe(&["list", "tables"], &catalogs_output(&schemas), &env);
    assert!(
        tables.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&tables.stderr)
    );
    let tables = ndjson(&tables.stdout);
    assert_eq!(tables.len(), 8);
    assert!(tables
        .iter()
        .any(|table| table["name"] == "catalogs/c1/schemas/s1/tables/t1"));

    let hits = hits.lock().unwrap().clone();
    assert_eq!(hits.len(), 1 + 2 + 4, "one request per ref");
    assert_eq!(
        hits.iter()
            .filter(|hit| hit.path.contains("/catalogs"))
            .count(),
        1
    );
    assert_eq!(
        hits.iter()
            .filter(|hit| hit.path.contains("/schemas"))
            .count(),
        2
    );
    assert_eq!(
        hits.iter()
            .filter(|hit| hit.path.contains("/tables"))
            .count(),
        4
    );
    let mut gaps: Vec<Duration> = hits
        .windows(2)
        .map(|pair| pair[1].request_time - pair[0].request_time)
        .collect();
    gaps.sort();
    assert!(
        gaps[0] >= Duration::from_millis(55),
        "requests were not paced at 15/s: {gaps:?}"
    );

    stop(socket);
}

/// A catalog command makes no catalog call of its own: with no daemon on the
/// socket it fails, names the socket, says how to start one, and calls nothing.
#[test]
fn a_catalog_level_without_a_daemon_says_how_to_start_one() {
    let (endpoint, hits) = server();
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("absent.sock");
    let env = [
        ("PQB_DAEMON_SOCK", socket.to_str().unwrap()),
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TABLE_FORMAT", "unity"),
    ];
    let catalogs = pipe(&["list", "catalogs"], b"", &env);
    let stderr = String::from_utf8_lossy(&catalogs.stderr);
    assert!(!catalogs.status.success());
    assert!(stderr.contains("pqbench daemon start"), "stderr: {stderr}");
    assert!(stderr.contains("absent.sock"), "stderr: {stderr}");
    assert!(catalogs.stdout.is_empty());
    assert!(hits.lock().unwrap().is_empty());
}

/// A fake workspace serving the OAuth token route and the Unity catalog
/// listing route. Every request is recorded with the bearer it carried.
fn oauth_server() -> (String, Arc<Mutex<Vec<Hit>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&hits);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buffer = [0u8; 8192];
            let read = stream.read(&mut buffer).unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            let path = request
                .lines()
                .next()
                .unwrap_or_default()
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();
            recorded.lock().unwrap().push(Hit {
                path: path.clone(),
                authorization: bearer(&request),
                request_time: Instant::now(),
            });
            let body = if path.ends_with("/oidc/v1/token") {
                r#"{"access_token":"minted","expires_in":3600}"#
            } else {
                r#"{"catalogs":[{"name":"c1"}]}"#
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (address, hits)
}

/// A daemon started for a service principal mints its own bearer: the child
/// process resolves the client credentials `daemon start` passed it, and the
/// catalog request it sends carries the minted token. Neither the secret nor
/// the token reaches the command's output.
#[test]
fn the_daemon_mints_a_service_principals_bearer() {
    let (endpoint, hits) = oauth_server();
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("daemon.sock");
    let socket = socket.to_str().unwrap();
    let env = [
        ("PQB_DAEMON_SOCK", socket),
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TABLE_FORMAT", "unity"),
        ("PQB_CLIENT_ID", "0123-4567"),
        ("PQB_CLIENT_SECRET", "oauth-secret-value"),
    ];
    let status = pqbench()
        .args(["daemon", "start", "--idle-seconds", "10"])
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&status.stderr);
    assert!(status.status.success(), "stderr: {stderr}");
    assert!(!stderr.contains("oauth-secret-value"), "stderr: {stderr}");

    let catalogs = pipe(&["list", "catalogs"], b"", &env);
    let stderr = String::from_utf8_lossy(&catalogs.stderr);
    assert!(catalogs.status.success(), "stderr: {stderr}");
    assert!(!stderr.contains("oauth-secret-value"), "stderr: {stderr}");
    assert_eq!(ndjson(&catalogs.stdout).len(), 1);
    assert!(!String::from_utf8_lossy(&catalogs.stdout).contains("minted"));

    let hits = hits.lock().unwrap().clone();
    assert!(
        hits.iter().any(|hit| hit.path.ends_with("/oidc/v1/token")),
        "the daemon minted no token"
    );
    let listing = hits
        .iter()
        .find(|hit| hit.path.contains("/catalogs"))
        .expect("the daemon listed the catalogs");
    assert_eq!(listing.authorization.as_deref(), Some("Bearer minted"));

    stop(socket);
}

fn catalogs_output(records: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for record in records {
        out.extend_from_slice(serde_json::to_string(record).unwrap().as_bytes());
        out.push(b'\n');
    }
    out
}

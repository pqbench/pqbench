//! `pqbench setup`: the shell exports, and the `--check` preflight.
//!
//! A fake workspace answers the token route and the first listing level, so the
//! preflight's three checks are exercised without a workspace: a service
//! principal's bearer is minted, the endpoint is read under it, and neither the
//! client secret nor the token ever reaches the output.

use std::io::{Read, Write};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};

/// One request the fake endpoint answered: its path, and the bearer it carried.
#[derive(Clone)]
struct Received {
    path: String,
    authorization: Option<String>,
}

/// What the fake endpoint answers.
#[derive(Clone, Copy)]
enum Answer {
    /// Mint a bearer living `expires_seconds` seconds; every other path is `200`.
    Mint { expires_seconds: u64 },
    /// Refuse the token request with `401` and this error document.
    Refuse(&'static str),
    /// Mint, then refuse the listing route with `403`.
    Forbid,
}

/// A fake workspace: the token route and the Unity catalog listing route.
/// Every request is recorded.
fn workspace(answer: Answer) -> (String, Arc<Mutex<Vec<Received>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let reads = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&reads);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buffer = [0u8; 8192];
            let read = stream.read(&mut buffer).unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            let mut lines = request.lines();
            let path = lines
                .next()
                .unwrap_or_default()
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();
            let authorization = lines.find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("authorization")
                    .then(|| value.trim().to_string())
            });
            recorded.lock().unwrap().push(Received {
                path: path.clone(),
                authorization,
            });
            let (status, body) = if path.ends_with("/oidc/v1/token") {
                match answer {
                    Answer::Mint { expires_seconds } => (
                        200,
                        format!(r#"{{"access_token":"minted","expires_in":{expires_seconds}}}"#),
                    ),
                    Answer::Refuse(body) => (401, body.to_string()),
                    Answer::Forbid => (
                        200,
                        r#"{"access_token":"minted","expires_in":3600}"#.to_string(),
                    ),
                }
            } else if matches!(answer, Answer::Forbid) {
                (403, r#"{"error":"forbidden"}"#.to_string())
            } else {
                (200, r#"{"catalogs":[{"name":"c1"}]}"#.to_string())
            };
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (address, reads)
}

/// `pqbench` with no profile and no config file, so only the test's own
/// variables resolve it.
fn pqbench() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pqbench"));
    command.env_remove("PQB_PROFILE");
    command.env("PQB_CONFIG_FILE", "/dev/null");
    command
}

/// Run one command with the test's environment and no stdin.
fn run(args: &[&str], env: &[(&str, &str)]) -> Output {
    pqbench()
        .args(args)
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap()
}

/// The service principal a test authenticates as; the secret is the value no
/// output may carry.
const CLIENT_ID: &str = "0123-4567";
const CLIENT_SECRET: &str = "oauth-secret-value";

/// `setup` and `setup --check` name this identity's checks the same way.
fn text(output: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// A configured service principal passes the preflight: the configuration
/// resolves, the token is minted, and the endpoint answers the first listing
/// level under that minted bearer.
#[test]
fn a_service_principal_passes_the_preflight() {
    let (endpoint, reads) = workspace(Answer::Mint {
        expires_seconds: 3600,
    });
    let output = run(
        &["setup", "--check"],
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_CLIENT_ID", CLIENT_ID),
            ("PQB_CLIENT_SECRET", CLIENT_SECRET),
        ],
    );
    let (stdout, stderr) = text(&output);
    assert!(
        output.status.success(),
        "stdout: {stdout}\nstderr: {stderr}"
    );
    assert!(stdout.contains("ok"), "{stdout}");
    assert!(stdout.contains("config"), "{stdout}");
    assert!(stdout.contains("auth"), "{stdout}");
    assert!(stdout.contains("catalog"), "{stdout}");
    assert!(stdout.contains("expires in 3600 s"), "{stdout}");
    assert!(!stdout.contains("fail"), "{stdout}");

    // The secret is never printed, and the listing read carried the minted
    // bearer.
    assert!(!stdout.contains(CLIENT_SECRET), "{stdout}");
    assert!(!stderr.contains(CLIENT_SECRET), "{stderr}");
    let reads = reads.lock().unwrap().clone();
    let listing = reads
        .iter()
        .find(|read| read.path.contains("/catalogs"))
        .expect("the preflight read the listing route");
    assert_eq!(listing.authorization.as_deref(), Some("Bearer minted"));
}

/// A refused token fails the preflight: the auth line names the answer, the
/// catalog line still runs, and the exit status is non-zero.
#[test]
fn a_refused_token_fails_the_preflight() {
    let (endpoint, _) = workspace(Answer::Refuse(
        r#"{"error":"invalid_client","error_description":"no such client"}"#,
    ));
    let output = run(
        &["setup", "--check"],
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_CLIENT_ID", CLIENT_ID),
            ("PQB_CLIENT_SECRET", CLIENT_SECRET),
        ],
    );
    let (stdout, stderr) = text(&output);
    assert!(!output.status.success(), "stdout: {stdout}");
    assert!(stdout.contains("fail"), "{stdout}");
    assert!(stdout.contains("auth"), "{stdout}");
    assert!(stdout.contains("HTTP 401"), "{stdout}");
    assert!(stdout.contains("invalid_client"), "{stdout}");
    assert!(stdout.contains("catalog"), "{stdout}");
    assert!(!stdout.contains(CLIENT_SECRET), "{stdout}");
    assert!(!stderr.contains(CLIENT_SECRET), "{stderr}");
}

/// A reachable but refusing endpoint fails only the catalog check.
#[test]
fn a_refused_listing_fails_the_preflight() {
    let (endpoint, _) = workspace(Answer::Forbid);
    let output = run(
        &["setup", "--check"],
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_CLIENT_ID", CLIENT_ID),
            ("PQB_CLIENT_SECRET", CLIENT_SECRET),
        ],
    );
    let (stdout, _) = text(&output);
    assert!(!output.status.success(), "stdout: {stdout}");
    assert!(stdout.contains("answered 403"), "{stdout}");
    assert_eq!(
        stdout.matches("fail").count(),
        1,
        "only the catalog check should fail: {stdout}"
    );
}

/// No credentials is two failed checks, and the message says what to set.
#[test]
fn no_credential_fails_the_preflight() {
    let output = run(
        &["setup", "--check"],
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    let (stdout, stderr) = text(&output);
    assert!(!output.status.success());
    assert!(stdout.contains("no credential"), "{stdout}");
    assert!(stdout.contains("PQB_CLIENT_ID"), "{stdout}");
    assert!(stderr.contains("setup check(s) failed"), "{stderr}");
}

/// `setup` exports a bearer minted for the run from the client credentials, and
/// never the secret itself.
#[test]
fn setup_exports_a_minted_bearer() {
    let (endpoint, reads) = workspace(Answer::Mint {
        expires_seconds: 3600,
    });
    let output = run(
        &["setup"],
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_CLIENT_ID", CLIENT_ID),
            ("PQB_CLIENT_SECRET", CLIENT_SECRET),
        ],
    );
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "stderr: {stderr}");
    assert!(stdout.contains("export PQB_TOKEN='minted'"), "{stdout}");
    assert!(stdout.contains("export PQB_ENDPOINT="), "{stdout}");
    assert!(!stdout.contains(CLIENT_SECRET), "{stdout}");
    assert!(!stderr.contains(CLIENT_SECRET), "{stderr}");
    assert_eq!(reads.lock().unwrap().len(), 1, "one mint, no listing read");
}

/// A static token wins over configured client credentials: nothing is minted.
#[test]
fn a_static_token_is_not_minted() {
    let (endpoint, reads) = workspace(Answer::Mint {
        expires_seconds: 3600,
    });
    let output = run(
        &["setup"],
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", "dapi-static"),
            ("PQB_CLIENT_ID", CLIENT_ID),
            ("PQB_CLIENT_SECRET", CLIENT_SECRET),
        ],
    );
    let (stdout, stderr) = text(&output);
    assert!(output.status.success(), "stderr: {stderr}");
    assert!(
        stdout.contains("export PQB_TOKEN='dapi-static'"),
        "{stdout}"
    );
    assert!(reads.lock().unwrap().is_empty(), "a token was minted");
}

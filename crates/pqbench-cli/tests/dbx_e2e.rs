//! Live Databricks metastore e2e: mint an OAuth M2M bearer (or take
//! `DBX_TOKEN`), then read the live metastore record with `metastore info`.
//!
//! The workspace URL comes from `DBX_HOST` (a repository secret in CI); the
//! service principal's credentials come from `DBX_SAMPLES_SP_CLIENT_ID` /
//! `DBX_SAMPLES_SP_CLIENT_SECRET`, or a ready `DBX_TOKEN` is used instead.
//!
//! Auth flows covered: a short-lived minted token used as a standard bearer,
//! a missing token, and an invalid token (both rejected with 401).
//!
//! Ignored by default so `make test` stays offline; run with `make dbx-e2e`
//! (or `cargo test -p pqbench-cli --test dbx_e2e -- --ignored`). When
//! `DBX_HOST` or the credentials are not set, each test skips, so
//! `--include-ignored` legs without secrets stay green.
//!
//! Setup: `docs/auth.md`.

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn pipe(args: &[&str], stdin: &[u8]) -> std::process::Output {
    // The credentials are the test's, not pqbench's: the token travels on the
    // document, so the child gets a scrubbed environment.
    let mut child = pqbench()
        .args(args)
        .env_remove("DBX_TOKEN")
        .env_remove("DBX_HOST")
        .env_remove("DBX_SAMPLES_SP_CLIENT_ID")
        .env_remove("DBX_SAMPLES_SP_CLIENT_SECRET")
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

/// The live workspace URL, from `DBX_HOST` (a CI secret or the local shell).
fn dbx_host() -> Option<String> {
    std::env::var("DBX_HOST")
        .ok()
        .filter(|host| !host.is_empty())
}

fn unity_endpoint(host: &str) -> String {
    format!("{}/api/2.1/unity-catalog", host.trim_end_matches('/'))
}

/// A `pqbench.lake-source` naming the live endpoint.
fn source(endpoint: &str, token: Option<&str>) -> Value {
    let mut document = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": endpoint,
    });
    if let Some(token) = token {
        document["token"] = json!(token);
    }
    document
}

/// The service principal's short-lived OAuth M2M bearer, minted once per test
/// process. `None` when the SP credentials are not configured.
fn minted_token(host: &str) -> Option<String> {
    static TOKEN: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    TOKEN
        .get_or_init(|| {
            let client_id = std::env::var("DBX_SAMPLES_SP_CLIENT_ID").ok()?;
            let client_secret = std::env::var("DBX_SAMPLES_SP_CLIENT_SECRET").ok()?;
            mint_token(host, &client_id, &client_secret)
        })
        .clone()
}

/// A ready bearer from the environment: a PAT or a token minted elsewhere.
fn ready_token() -> Option<String> {
    std::env::var("DBX_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
}

/// A token for a test that only needs to be authenticated.
fn any_token(host: &str) -> Option<String> {
    ready_token().or_else(|| minted_token(host))
}

/// `POST /oidc/v1/token` with HTTP Basic `client_id:client_secret`.
fn mint_token(host: &str, client_id: &str, client_secret: &str) -> Option<String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .ok()?;
    let response = client
        .post(format!("{host}/oidc/v1/token"))
        .basic_auth(client_id, Some(client_secret))
        .form(&[("grant_type", "client_credentials"), ("scope", "all-apis")])
        .send()
        .ok()?;
    if !response.status().is_success() {
        eprintln!("OAuth M2M mint failed: HTTP {}", response.status());
        return None;
    }
    response.json::<Value>().ok()?["access_token"]
        .as_str()
        .map(str::to_owned)
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_info_reads_the_live_metastore() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe(
        &["metastore", "info", "--format", "json"],
        source(&unity_endpoint(&host), Some(&token))
            .to_string()
            .as_bytes(),
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
    assert!(
        !records[0]["name"].as_str().unwrap_or_default().is_empty(),
        "{records:?}"
    );
    assert!(
        !records[0]["cloud"].as_str().unwrap_or_default().is_empty(),
        "{records:?}"
    );
    assert!(
        !records[0]["region"].as_str().unwrap_or_default().is_empty(),
        "{records:?}"
    );
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_ls_lists_the_live_catalogs() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let document = source(&unity_endpoint(&host), Some(&token)).to_string();
    let output = pipe(
        &["metastore", "ls", "--format", "json"],
        document.as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    for record in &records {
        assert_eq!(record["kind"], "pqbench.catalog");
        assert_eq!(record["version"], 1);
    }
    let names: Vec<&str> = records
        .iter()
        .filter_map(|record| record["name"].as_str())
        .collect();
    assert_eq!(names, ["dbx_samples", "samples", "system", "workspace"]);

    let output = pipe(
        &["metastore", "ls", "--format", "table"],
        document.as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dbx_samples"), "{stdout}");
    assert!(stdout.contains("catalogs: 4"), "{stdout}");

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("catalogs.ndjson.zst");
    let output = pipe(
        &[
            "metastore",
            "ls",
            "--format",
            "json",
            "-o",
            file.to_str().unwrap(),
        ],
        document.as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(&file).unwrap();
    let decoded = zstd::decode_all(&bytes[..]).unwrap();
    assert_eq!(ndjson(&decoded).len(), 4);
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_info_rejects_a_missing_token() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let output = pipe(
        &["metastore", "info"],
        source(&unity_endpoint(&host), None).to_string().as_bytes(),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_info_rejects_an_invalid_token() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let output = pipe(
        &["metastore", "info"],
        source(&unity_endpoint(&host), Some("not-a-real-token"))
            .to_string()
            .as_bytes(),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

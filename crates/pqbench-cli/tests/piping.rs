//! The metadata walk as a pipe: `list catalogs | get catalog | list schemas`,
//! context from `PQB_ENDPOINT`, refs on stdin.

use std::io::{Read, Write};
use std::process::{Command, Stdio};

use serde_json::Value;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

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
fn routes(routes: &'static [(&'static str, u16, &'static str)]) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().take(64) {
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

const CATALOGS: &str = r#"{"catalogs":[{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG"},{"name":"system","catalog_type":"SYSTEM_CATALOG"}]}"#;
const DBX_SAMPLES: &str = r#"{"name":"dbx_samples","catalog_type":"MANAGED_CATALOG","comment":"sample catalog","owner":"owner@example.com"}"#;
const SYSTEM: &str = r#"{"name":"system","catalog_type":"SYSTEM_CATALOG"}"#;
const SCHEMAS: &str = r#"{"schemas":[{"name":"nyctaxi"}],"next_page_token":null}"#;
const SCHEMA: &str =
    r#"{"name":"nyctaxi","comment":"taxi data","storage_location":"s3://bucket/nyctaxi"}"#;
const TABLES: &str = r#"{"tables":[{"name":"trips","full_name":"dbx_samples.nyctaxi.trips","data_source_format":"DELTA","storage_location":"s3://bucket/trips"}],"next_page_token":null}"#;

#[test]
fn list_catalogs_pipes_into_get_catalog_and_list_schemas() {
    let address = routes(&[
        ("/schemas?", 200, SCHEMAS),
        ("/schemas/", 200, SCHEMA),
        ("/tables", 200, TABLES),
        ("/catalogs/dbx_samples", 200, DBX_SAMPLES),
        ("/catalogs/system", 200, SYSTEM),
        ("/catalogs?", 200, CATALOGS),
    ]);
    let env = [("PQB_ENDPOINT", address.as_str())];

    let catalogs = pipe_env(&["list", "catalogs"], b"", &env);
    assert!(
        catalogs.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&catalogs.stderr)
    );
    let refs = catalogs.stdout;
    assert_eq!(ndjson(&refs).len(), 2);

    let enriched = pipe_env(&["get", "catalog"], &refs, &env);
    assert!(
        enriched.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&enriched.stderr)
    );
    let records = ndjson(&enriched.stdout);
    assert_eq!(records.len(), 2);
    let dbx_samples = records
        .iter()
        .find(|record| record["name"] == "catalogs/dbx_samples")
        .unwrap();
    assert_eq!(dbx_samples["comment"], "sample catalog");
    assert!(records
        .iter()
        .any(|record| record["name"] == "catalogs/system"));

    let schemas = pipe_env(&["list", "schemas"], &refs, &env);
    assert!(
        schemas.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&schemas.stderr)
    );
    let records = ndjson(&schemas.stdout);
    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .any(|record| record["name"] == "catalogs/dbx_samples/schemas/nyctaxi"));
    assert!(records
        .iter()
        .any(|record| record["name"] == "catalogs/system/schemas/nyctaxi"));

    let enriched = pipe_env(&["get", "schema"], &schemas.stdout, &env);
    assert!(
        enriched.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&enriched.stderr)
    );
    let records = ndjson(&enriched.stdout);
    assert_eq!(records.len(), 2);
    assert!(records.iter().any(
        |record| record["name"] == "catalogs/dbx_samples/schemas/nyctaxi"
            && record["comment"] == "taxi data"
    ));
    assert!(records
        .iter()
        .any(|record| record["name"] == "catalogs/system/schemas/nyctaxi"));

    let tables = pipe_env(&["list", "tables"], &schemas.stdout, &env);
    assert!(
        tables.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&tables.stderr)
    );
    let records = ndjson(&tables.stdout);
    assert!(records.iter().any(|record| record["name"]
        == "catalogs/dbx_samples/schemas/nyctaxi/tables/trips"
        && record["uri"] == "s3://bucket/trips"));
    assert!(records
        .iter()
        .any(|record| record["name"] == "catalogs/system/schemas/nyctaxi/tables/trips"));
}

#[test]
fn list_files_rejects_a_malformed_partition_record() {
    // A bad name is the record's contract: it is named and dropped, and the
    // level still exits non-zero.
    let output = pipe_env(
        &["list", "files"],
        br#"{"name":"nope","uri":"file:///tmp"}"#,
        &[],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("a partition record needs a"), "{stderr}");
    assert!(!stderr.contains("could not be read"), "{stderr}");
}

#[test]
fn bytemass_rejects_a_remote_source_that_matches_nothing() {
    // Expanding the source is a contract step; only each file's read is
    // dropped.
    let source = concat!(
        r#"{"version":1,"inputs":[""#,
        env!("CARGO_MANIFEST_DIR"),
        r#"/tests/fixtures/no-such-*.parquet"],"env":{}}"#,
    );
    let output = pipe_env(&["bytemass"], source.as_bytes(), &[]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("mask matched no files"), "{stderr}");
    assert!(!stderr.contains("could not be read"), "{stderr}");
}

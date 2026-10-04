//! Live Databricks metastore e2e: mint an OAuth M2M bearer (or take
//! `DBX_TOKEN`), read the live metastore record with `metastore info`, list
//! its catalogs with `metastore ls`, read one with `catalog info`, list its
//! schemas with `catalog ls` (Unity REST and Iceberg REST), and walk them
//! into a job directory of NDJSON files (the issue-#58 shape, one command per
//! iteration).
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

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

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

/// pqbench with a scrubbed environment and piped stdin: the credentials are
/// the test's, not the child's — the token travels on the document.
fn scrubbed() -> Command {
    let mut command = pqbench();
    command
        .env_remove("DBX_TOKEN")
        .env_remove("DBX_HOST")
        .env_remove("DBX_SAMPLES_SP_CLIENT_ID")
        .env_remove("DBX_SAMPLES_SP_CLIENT_SECRET")
        .stdin(Stdio::piped());
    command
}

fn pipe(args: &[&str], stdin: &[u8]) -> std::process::Output {
    pipe_env(args, stdin, &[])
}

/// pqbench with the walk's context in the environment, stdin piped: the
/// `PQB_ENDPOINT` / `PQB_TOKEN` half of decision 0004.
fn pipe_env(args: &[&str], stdin: &[u8], env: &[(&str, &str)]) -> std::process::Output {
    let mut child = scrubbed()
        .args(args)
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

/// One walk step: run pqbench with `stdin` and write stdout to `path` — the
/// job tree's NDJSON file for that step.
fn pipe_output(args: &[&str], stdin: &[u8], path: &std::path::Path) -> std::process::Output {
    let file = std::fs::File::create(path).unwrap();
    let mut child = scrubbed()
        .args(args)
        .stdout(Stdio::from(file))
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

/// The NDJSON records in one job-tree file.
fn read_ndjson(path: &std::path::Path) -> Vec<Value> {
    ndjson(&std::fs::read(path).unwrap())
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
    assert_eq!(
        names,
        [
            "dbx_samples",
            "pqbench_ext",
            "samples",
            "system",
            "workspace"
        ]
    );

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
    assert!(stdout.contains("catalogs: 5"), "{stdout}");

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
    let decoded = decode_lz4(&bytes);
    assert_eq!(ndjson(&decoded).len(), 5);
}

/// The issue-#58 walk, for the commands that exist: a job directory holds
/// each step's NDJSON, and the walk prints the result from the tree. Auth is
/// the bearer the whole walk runs under; the missing/invalid-token tests
/// cover its failures. `schema ls` and below extend this tree next.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_walk_writes_the_job_tree_and_prints_the_result() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let document = source(&unity_endpoint(&host), Some(&token)).to_string();

    let job = tempfile::tempdir().unwrap();
    let metastore = job.path().join("metastore");
    std::fs::create_dir_all(&metastore).unwrap();

    let metastore_info = metastore.join("info.jsonl");
    let output = pipe_output(
        &["metastore", "info", "--format", "json"],
        document.as_bytes(),
        &metastore_info,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let catalogs = metastore.join("catalogs.jsonl");
    let output = pipe_output(
        &["metastore", "ls", "--format", "json"],
        document.as_bytes(),
        &catalogs,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let records = read_ndjson(&metastore_info);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["kind"], "pqbench.metastore");
    println!("metastore: {}", records[0]["name"].as_str().unwrap_or("-"));

    let records = read_ndjson(&catalogs);
    let names: Vec<String> = records
        .iter()
        .map(|record| {
            assert_eq!(record["kind"], "pqbench.catalog");
            record["name"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(
        names,
        [
            "dbx_samples",
            "pqbench_ext",
            "samples",
            "system",
            "workspace"
        ]
    );

    // catalog → each catalog's record and schemas, one directory per catalog
    for name in &names {
        let dir = job.path().join("catalog").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let catalog_info = dir.join("info.jsonl");
        let output = pipe_output(
            &["catalog", "info", name, "--format", "json"],
            document.as_bytes(),
            &catalog_info,
        );
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let records = read_ndjson(&catalog_info);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["kind"], "pqbench.catalog");
        assert_eq!(records[0]["name"], name.as_str());
        println!(
            "catalog: {} ({})",
            name,
            records[0]["catalog_type"].as_str().unwrap_or("-")
        );

        let catalog_schemas = dir.join("schemas.jsonl");
        let output = pipe_output(
            &["catalog", "ls", name, "--format", "json"],
            document.as_bytes(),
            &catalog_schemas,
        );
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let records = read_ndjson(&catalog_schemas);
        assert!(!records.is_empty(), "{name} listed no schemas");
        for record in &records {
            assert_eq!(record["kind"], "pqbench.schema");
            assert_eq!(record["version"], 1);
            assert_eq!(record["catalog"], name.as_str());
        }
        println!("catalog {name}: {} schema(s)", records.len());

        // schema → each schema's tables, one directory per catalog.schema
        for record in &records {
            let schema = record["name"].as_str().unwrap();
            let fqn = format!("{name}.{schema}");
            let dir = job.path().join("schema").join(&fqn);
            std::fs::create_dir_all(&dir).unwrap();
            let tables = dir.join("tables.jsonl");
            let output = pipe_output(
                &["schema", "ls", &fqn, "--format", "json"],
                document.as_bytes(),
                &tables,
            );
            assert!(
                output.status.success(),
                "stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let records = read_ndjson(&tables);
            for record in &records {
                assert_eq!(record["kind"], "pqbench.table-ref");
                assert_eq!(record["version"], 2);
            }
            println!("schema {fqn}: {} table(s)", records.len());
        }
    }
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn catalog_info_reads_the_live_catalog() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe(
        &["catalog", "info", "dbx_samples", "--format", "json"],
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
    assert_eq!(records[0]["kind"], "pqbench.catalog");
    assert_eq!(records[0]["version"], 1);
    assert_eq!(records[0]["name"], "dbx_samples");
    assert_eq!(records[0]["catalog_type"], "MANAGED_CATALOG");
}

/// The schemas `dbx_samples` answers with, from either dialect.
const DBX_SAMPLES_SCHEMAS: [&str; 5] = [
    "bakehouse",
    "information_schema",
    "nyctaxi",
    "tpch_sf1",
    "wanderbricks",
];

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn catalog_ls_lists_the_live_schemas() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe(
        &["catalog", "ls", "dbx_samples", "--format", "json"],
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
    let names: Vec<&str> = records
        .iter()
        .map(|record| {
            assert_eq!(record["kind"], "pqbench.schema");
            assert_eq!(record["catalog"], "dbx_samples");
            record["name"].as_str().unwrap()
        })
        .collect();
    assert_eq!(names, DBX_SAMPLES_SCHEMAS);
}

/// `catalog ls` and `schema ls` on the external-location fixture: three
/// schemas and thirty external Delta tables in `pqbench_ext`, all registered
/// on customer S3 through the `pqbench_uc_e2e` external location.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn catalog_ls_lists_the_external_fixture() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
    ];

    let output = pipe_env(
        &["catalog", "ls", "pqbench_ext", "--format", "json"],
        b"",
        &env,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let schemas: Vec<String> = ndjson(&output.stdout)
        .iter()
        .map(|record| record["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        schemas,
        ["events", "information_schema", "reference", "sales"]
    );

    let mut tables = 0;
    for schema in ["events", "sales", "reference"] {
        let fqn = format!("pqbench_ext.{schema}");
        let output = pipe_env(&["schema", "ls", &fqn, "--format", "json"], b"", &env);
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let ids: Vec<String> = ndjson(&output.stdout)
            .iter()
            .map(|record| record["id"].as_str().unwrap().to_string())
            .collect();
        let expected: Vec<String> = (1..=10)
            .map(|i| format!("pqbench_ext.{schema}.t{i:02}"))
            .collect();
        assert_eq!(ids, expected);
        tables += ids.len();
    }
    assert_eq!(tables, 30);
}

/// `schema info` reads `dbx_samples.nyctaxi` from both dialects.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn schema_info_reads_the_live_schema() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    for endpoint in [
        unity_endpoint(&host),
        format!(
            "{}/iceberg-rest/v1/catalogs/dbx_samples",
            unity_endpoint(&host)
        ),
    ] {
        let iceberg = endpoint.contains("/iceberg-rest");
        let document = source(&endpoint, Some(&token)).to_string();
        let output = if iceberg {
            pipe_env(
                &["schema", "info", "dbx_samples.nyctaxi", "--format", "json"],
                document.as_bytes(),
                &[("PQB_TABLE_FORMAT", "iceberg")],
            )
        } else {
            pipe(
                &["schema", "info", "dbx_samples.nyctaxi", "--format", "json"],
                document.as_bytes(),
            )
        };
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
    }
}

/// `schema ls` lists `dbx_samples.nyctaxi` from both dialects: Unity fills
/// the storage path in the listing, Iceberg REST leaves it to `table info`.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn schema_ls_lists_the_live_tables() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
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
    assert!(!records.is_empty());
    for record in &records {
        assert_eq!(record["kind"], "pqbench.table-ref");
        assert_eq!(record["version"], 2);
        assert!(
            record["uri"].as_str().unwrap().contains("/tables/"),
            "{record:?}"
        );
        assert!(
            record["storage_path"].as_str().is_some(),
            "Unity fills the storage path: {record:?}"
        );
    }
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["id"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"dbx_samples.nyctaxi.trips"), "{names:?}");

    let endpoint = format!(
        "{}/iceberg-rest/v1/catalogs/dbx_samples",
        unity_endpoint(&host)
    );
    let output = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
        source(&endpoint, Some(&token)).to_string().as_bytes(),
        &[("PQB_TABLE_FORMAT", "iceberg")],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let trips = records
        .iter()
        .find(|record| record["id"] == "dbx_samples.nyctaxi.trips")
        .unwrap_or_else(|| panic!("no trips table: {records:?}"));
    assert_eq!(trips["version"], 2);
    assert!(trips["storage_path"].is_null(), "{trips:?}");
    assert!(
        trips["uri"].as_str().unwrap().ends_with("/tables/trips"),
        "{trips:?}"
    );

    // The new tree enriches its own v2 refs: `tablev2 info` reads the
    // `loadTable` metadata inline, so no storage read runs.
    let info = pipe_env(
        &["tablev2", "info", "--format", "json"],
        &output.stdout,
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", token.as_str()),
            ("PQB_TABLE_FORMAT", "iceberg"),
        ],
    );
    assert!(
        info.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&info.stderr)
    );
    let records = ndjson(&info.stdout);
    let trips = records
        .iter()
        .find(|record| record["id"] == "dbx_samples.nyctaxi.trips")
        .unwrap_or_else(|| panic!("no trips table: {records:?}"));
    assert_eq!(trips["kind"], "pqbench.table-ref");
    assert_eq!(trips["version"], 2);
    assert_eq!(trips["format"], "iceberg");
    assert!(!trips["columns"].as_array().unwrap().is_empty());
    assert!(trips["snapshot_version"].as_u64().unwrap() > 0);
    assert!(
        !trips["storage_path"].as_str().unwrap().is_empty(),
        "{trips:?}"
    );
}

/// `tablev2 info` reads the live Iceberg REST table: the `loadTable` response
/// carries the metadata inline, so the record is complete without any storage
/// read (the default-storage Delta path cannot read its log).
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn tablev2_info_reads_the_live_iceberg_table() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = format!(
        "{}/iceberg-rest/v1/catalogs/dbx_samples",
        unity_endpoint(&host)
    );
    let output = pipe_env(
        &[
            "tablev2",
            "info",
            "dbx_samples.nyctaxi.trips",
            "--format",
            "json",
        ],
        source(&endpoint, Some(&token)).to_string().as_bytes(),
        &[("PQB_TABLE_FORMAT", "iceberg")],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["kind"], "pqbench.table-ref");
    assert_eq!(record["version"], 2);
    assert_eq!(record["id"], "dbx_samples.nyctaxi.trips");
    assert_eq!(record["format"], "iceberg");
    assert!(record["snapshot_version"].as_u64().unwrap() > 0);
    assert!(record["columns"].as_array().unwrap().len() >= 6);
    assert!(record["iceberg_properties"].is_object(), "{record:?}");
}

/// `tablev2 info` on the live Unity catalog: the table read runs with the env
/// it is given — here none — so the `without_files()` Delta log read of the
/// managed default-storage table fails outside compute and the error names the
/// table and its location.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn tablev2_info_names_the_location_when_the_metadata_cannot_be_read() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let output = pipe(
        &["tablev2", "info", "dbx_samples.nyctaxi.trips"],
        source(&endpoint, Some(&token)).to_string().as_bytes(),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("dbx_samples.nyctaxi.trips"), "{stderr}");
    assert!(stderr.contains("s3://"), "{stderr}");
}

/// `credentials check` on the live Unity catalog: the UniForm Delta fixture
/// (`pqbench_delta_test`) lists no direct-external-engine capability in its
/// manifest, so the check drops it with the reason on stderr; the managed
/// Iceberg tables carry the capability and pass through, including from a
/// mixed schema.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn credentials_check_gates_the_live_tables() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
    ];
    let reference =
        |id: &str| json!({"kind": "pqbench.table-ref", "version": 2, "id": id}).to_string();
    let unsupported = pipe_env(
        &["credentials", "check", "--format", "json"],
        format!("{}\n", reference("dbx_samples.nyctaxi.pqbench_delta_test")).as_bytes(),
        &env,
    );
    assert!(!unsupported.status.success());
    let stderr = String::from_utf8_lossy(&unsupported.stderr);
    assert!(
        stderr.contains("dbx_samples.nyctaxi.pqbench_delta_test"),
        "{stderr}"
    );
    assert!(
        stderr.contains("no direct external engine read support"),
        "{stderr}"
    );
    let eligible = pipe_env(
        &["credentials", "check", "--format", "json"],
        format!("{}\n", reference("dbx_samples.nyctaxi.trips")).as_bytes(),
        &env,
    );
    assert!(
        eligible.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&eligible.stderr)
    );
    let records = ndjson(&eligible.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], "dbx_samples.nyctaxi.trips");

    // A mixed schema drops the ineligible table and keeps the eligible ones:
    // the filter the walk composes on.
    let refs = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
        b"",
        &env,
    );
    assert!(
        refs.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&refs.stderr)
    );
    let mixed = pipe_env(
        &["credentials", "check", "--format", "json"],
        &refs.stdout,
        &env,
    );
    assert!(!mixed.status.success());
    let mixed_stderr = String::from_utf8_lossy(&mixed.stderr);
    assert!(
        mixed_stderr.contains("dbx_samples.nyctaxi.pqbench_delta_test"),
        "{mixed_stderr}"
    );
    assert!(
        mixed_stderr.contains("no direct external engine read support"),
        "{mixed_stderr}"
    );
    let mixed_records = ndjson(&mixed.stdout);
    let passed: Vec<&str> = mixed_records
        .iter()
        .map(|record| record["id"].as_str().unwrap())
        .collect();
    assert_eq!(passed.len(), 2, "{passed:?}");
    assert!(passed.contains(&"dbx_samples.nyctaxi.trips"), "{passed:?}");
    assert!(
        passed.contains(&"dbx_samples.nyctaxi.pqbench_iceberg_test"),
        "{passed:?}"
    );
}

/// The whole credentials stage on the external fixture: every table in the
/// three schemas is eligible, so `schema ls | credentials check | credentials
/// get` passes each ref through with a vended lease on `env`.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn credentials_get_vends_the_external_fixture() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
    ];

    let mut tables = 0;
    for schema in ["events", "sales", "reference"] {
        let fqn = format!("pqbench_ext.{schema}");
        let listed = pipe_env(&["schema", "ls", &fqn, "--format", "json"], b"", &env);
        assert!(
            listed.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&listed.stderr)
        );
        let checked = pipe_env(
            &["credentials", "check", "--format", "json"],
            &listed.stdout,
            &env,
        );
        assert!(
            checked.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&checked.stderr)
        );
        let vended = pipe_env(
            &["credentials", "get", "--format", "json"],
            &checked.stdout,
            &env,
        );
        assert!(
            vended.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&vended.stderr)
        );
        for record in ndjson(&vended.stdout) {
            assert!(
                record["env"]["AWS_ACCESS_KEY_ID"].is_string(),
                "no vended lease: {record}"
            );
            tables += 1;
        }
    }
    assert_eq!(tables, 30);
}

/// `tablev2 info` on a table in customer storage: `credentials get`
/// materializes the vended lease on the ref, the test puts the ref's keys in
/// the process environment with the bucket's region, and the Delta log read
/// runs under them. The fixture lives in `us-east-2`; set `DBX_AWS_TABLE` /
/// `DBX_AWS_REGION` to read another one.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn tablev2_info_reads_the_external_aws_table() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let table = std::env::var("DBX_AWS_TABLE")
        .ok()
        .filter(|table| !table.is_empty())
        .unwrap_or_else(|| "pqbench_ext.events.t01".to_string());
    let region = std::env::var("DBX_AWS_REGION")
        .ok()
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "us-east-2".to_string());
    let endpoint = unity_endpoint(&host);
    let reference = json!({"kind": "pqbench.table-ref", "version": 2, "id": table}).to_string();
    let vended = pipe_env(
        &["credentials", "get", "--format", "json"],
        format!("{reference}\n").as_bytes(),
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", token.as_str()),
        ],
    );
    assert!(
        vended.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&vended.stderr)
    );
    let vended = ndjson(&vended.stdout);
    assert_eq!(vended.len(), 1);
    let lease = &vended[0]["env"];
    assert!(
        lease["AWS_ACCESS_KEY_ID"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "no vended lease: {}",
        vended[0]
    );
    let key = lease["AWS_ACCESS_KEY_ID"].as_str().unwrap();
    let secret = lease["AWS_SECRET_ACCESS_KEY"].as_str().unwrap();
    let session = lease["AWS_SESSION_TOKEN"].as_str().unwrap();
    let output = pipe_env(
        &["tablev2", "info", "--format", "json"],
        format!("{reference}\n").as_bytes(),
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", token.as_str()),
            ("AWS_REGION", region.as_str()),
            ("AWS_ACCESS_KEY_ID", key),
            ("AWS_SECRET_ACCESS_KEY", secret),
            ("AWS_SESSION_TOKEN", session),
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["id"], table.as_str());
    assert_eq!(record["format"], "delta");
    assert!(
        record["storage_path"]
            .as_str()
            .unwrap_or_default()
            .starts_with("s3://"),
        "{record:?}"
    );
    assert!(
        !record["columns"].as_array().unwrap().is_empty(),
        "{record:?}"
    );
}

/// `credentials get` on the live Unity catalog: the managed Iceberg tables
/// (`trips`, `pqbench_iceberg_test`) carry direct-external-engine support in
/// their capability manifest, so Unity vends temporary read credentials onto
/// the ref's `env`; the UniForm Delta table (`pqbench_delta_test`) lists no
/// such capability and passes through with no `env`.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn credentials_get_vends_the_live_tables() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
    ];
    let refs = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
        b"",
        &env,
    );
    assert!(
        refs.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&refs.stderr)
    );
    let output = pipe_env(
        &["credentials", "get", "--format", "json"],
        &refs.stdout,
        &env,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    for id in [
        "dbx_samples.nyctaxi.trips",
        "dbx_samples.nyctaxi.pqbench_iceberg_test",
    ] {
        let vended = records
            .iter()
            .find(|record| record["id"] == id)
            .unwrap_or_else(|| panic!("no {id}: {records:?}"));
        assert_eq!(vended["kind"], "pqbench.table-ref");
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            assert!(
                vended["env"][key]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "{id} {key}: {vended:?}"
            );
        }
    }
    let passed = records
        .iter()
        .find(|record| record["id"] == "dbx_samples.nyctaxi.pqbench_delta_test")
        .unwrap_or_else(|| panic!("no pqbench_delta_test: {records:?}"));
    assert!(passed["env"].is_null(), "{passed:?}");
}

/// `credentials get` with `PQB_TABLE_FORMAT=iceberg` on the live catalog: the
/// `loadCredentials` route returns the catalog's credentials for the managed
/// Iceberg tables onto the ref's `env`; the UniForm Delta table cannot mint
/// there, falls back to the delegated `loadTable`, and passes through with no
/// `env`.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn credentials_get_iceberg_vends_the_live_tables() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = format!(
        "{}/iceberg-rest/v1/catalogs/dbx_samples",
        unity_endpoint(&host)
    );
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
        ("PQB_TABLE_FORMAT", "iceberg"),
    ];
    let refs = pipe_env(
        &["schema", "ls", "dbx_samples.nyctaxi", "--format", "json"],
        b"",
        &env,
    );
    assert!(
        refs.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&refs.stderr)
    );
    let output = pipe_env(
        &["credentials", "get", "--format", "json"],
        &refs.stdout,
        &env,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    for id in [
        "dbx_samples.nyctaxi.trips",
        "dbx_samples.nyctaxi.pqbench_iceberg_test",
    ] {
        let vended = records
            .iter()
            .find(|record| record["id"] == id)
            .unwrap_or_else(|| panic!("no {id}: {records:?}"));
        assert_eq!(vended["kind"], "pqbench.table-ref");
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            assert!(
                vended["env"][key]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "{id} {key}: {vended:?}"
            );
        }
    }
    let passed = records
        .iter()
        .find(|record| record["id"] == "dbx_samples.nyctaxi.pqbench_delta_test")
        .unwrap_or_else(|| panic!("no pqbench_delta_test: {records:?}"));
    assert!(passed["env"].is_null(), "{passed:?}");
}

/// The walk as a pipe, context in `PQB_ENDPOINT` / `PQB_TOKEN`:
/// `metastore ls | catalog info | catalog ls` (decision 0004).
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_ls_pipes_into_catalog_info_and_ls() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
    ];

    let catalogs = pipe_env(&["metastore", "ls", "--format", "json"], b"", &env);
    assert!(
        catalogs.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&catalogs.stderr)
    );
    let refs = catalogs.stdout;
    let names: Vec<String> = ndjson(&refs)
        .iter()
        .map(|record| record["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        [
            "dbx_samples",
            "pqbench_ext",
            "samples",
            "system",
            "workspace"
        ]
    );

    let info = pipe_env(&["catalog", "info", "--format", "json"], &refs, &env);
    assert!(
        info.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&info.stderr)
    );
    let records = ndjson(&info.stdout);
    assert_eq!(records.len(), 5);
    for record in &records {
        assert_eq!(record["kind"], "pqbench.catalog");
        assert!(names.contains(&record["name"].as_str().unwrap().to_string()));
    }

    let schemas = pipe_env(&["catalog", "ls", "--format", "json"], &refs, &env);
    assert!(
        schemas.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&schemas.stderr)
    );
    let records = ndjson(&schemas.stdout);
    assert!(!records.is_empty());
    for record in &records {
        assert_eq!(record["kind"], "pqbench.schema");
        assert!(names.contains(&record["catalog"].as_str().unwrap().to_string()));
    }
    let dbx_samples: Vec<&str> = records
        .iter()
        .filter(|record| record["catalog"] == "dbx_samples")
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(dbx_samples, DBX_SAMPLES_SCHEMAS);
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn catalog_ls_lists_the_live_iceberg_rest_namespaces() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = format!(
        "{}/iceberg-rest/v1/catalogs/dbx_samples",
        unity_endpoint(&host)
    );
    let output = pipe_env(
        &["catalog", "ls", "dbx_samples", "--format", "json"],
        source(&endpoint, Some(&token)).to_string().as_bytes(),
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
        .map(|record| {
            assert_eq!(record["kind"], "pqbench.schema");
            assert_eq!(record["catalog"], "dbx_samples");
            record["name"].as_str().unwrap()
        })
        .collect();
    assert_eq!(names, DBX_SAMPLES_SCHEMAS);
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

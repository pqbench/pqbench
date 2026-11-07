//! Live Databricks metastore e2e: mint an OAuth M2M bearer (or take
//! `DBX_TOKEN`), read the live metastore record with `get metastore`, list
//! its catalogs with `list catalogs`, read one with `get catalog`, list its
//! schemas with `list schemas` (Unity REST and Iceberg REST), and walk them
//! into a job directory of NDJSON files (the issue-#58 shape, one command per
//! iteration).
//!
//! The external fixture (`pqbench_ext`, external Delta on customer S3 — see
//! `scripts/dbx-ext/`) covers the rest of the walk:
//! `external_fixture_walks_to_a_treemap` runs the full pipeline. The
//! whole-catalog walk lives in `scripts/dbx-walk.sh` (`make dbx-walk`).
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
//! Setup: `docs/guides/auth.md`.

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

/// pqbench with a scrubbed environment and piped stdin: the credentials are
/// the test's, not the parent shell's.
fn scrubbed() -> Command {
    let mut command = pqbench();
    for name in [
        "DBX_TOKEN",
        "DBX_HOST",
        "DBX_SAMPLES_SP_CLIENT_ID",
        "DBX_SAMPLES_SP_CLIENT_SECRET",
        "PQB_ENDPOINT",
        "PQB_TOKEN",
        "PQB_PROFILE",
        "PQB_CONFIG_FILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_ENDPOINT",
        "AWS_ENDPOINT_URL",
        "AWS_REGION",
        "AWS_ALLOW_HTTP",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
    ] {
        command.env_remove(name);
    }
    // Do not attach to a developer's daemon at the derived socket path.
    let socket = std::env::temp_dir().join(format!("pqbench-e2e-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket);
    command.env("PQB_DAEMON_SOCK", socket);
    command.stdin(Stdio::piped());
    command
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

/// One walk step with extra environment.
fn pipe_output_env(
    args: &[&str],
    stdin: &[u8],
    path: &std::path::Path,
    env: &[(&str, &str)],
) -> std::process::Output {
    let file = std::fs::File::create(path).unwrap();
    let mut child = scrubbed()
        .args(args)
        .envs(env.iter().copied())
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

/// The resource type a record names: the collection segment of its `name`
/// (the segment before the id).
fn resource(record: &Value) -> &str {
    let name = record["name"].as_str().unwrap_or_default();
    let segments: Vec<&str> = name.split('/').collect();
    segments
        .get(segments.len().saturating_sub(2))
        .copied()
        .unwrap_or("")
}

/// A record's bare id: the last segment of its `name`.
fn id(record: &Value) -> &str {
    record["name"]
        .as_str()
        .unwrap_or_default()
        .rsplit('/')
        .next()
        .unwrap_or_default()
}

/// The dotted `catalog.schema[.table]` a record's `name` names.
fn fqn(record: &Value) -> String {
    let name = record["name"].as_str().unwrap_or_default();
    let segments: Vec<&str> = name.split('/').collect();
    segments
        .iter()
        .skip(1)
        .step_by(2)
        .copied()
        .collect::<Vec<_>>()
        .join(".")
}

/// A catalog resource document from a short catalog name.
fn catalog_ref(name: &str) -> String {
    json!({"name": format!("catalogs/{name}")}).to_string()
}

/// A schema resource document from a dotted `catalog.schema` name.
fn schema_ref(name: &str) -> String {
    let parts: Vec<&str> = name.split('.').collect();
    json!({"name": format!("catalogs/{}/schemas/{}", parts[0], parts[1])}).to_string()
}

/// A table resource document from a dotted `catalog.schema.table` name.
fn table_ref(name: &str) -> String {
    let parts: Vec<&str> = name.split('.').collect();
    json!({"name": format!(
        "catalogs/{}/schemas/{}/tables/{}",
        parts[0], parts[1], parts[2]
    )})
    .to_string()
}

fn unity_endpoint(host: &str) -> String {
    format!("{}/api/2.1/unity-catalog", host.trim_end_matches('/'))
}

/// The walk's config: `PQB_ENDPOINT` / `PQB_TOKEN` for the live endpoint.
fn source<'a>(endpoint: &'a str, token: Option<&'a str>) -> Vec<(&'a str, &'a str)> {
    let mut env = vec![("PQB_ENDPOINT", endpoint)];
    if let Some(token) = token {
        env.push(("PQB_TOKEN", token));
    }
    env
}

/// pqbench against the live endpoint, with empty stdin.
fn pipe_live(args: &[&str], endpoint: &str, token: Option<&str>) -> std::process::Output {
    pipe_env(args, b"", &source(endpoint, token))
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
fn metastore_get_reads_the_live_metastore() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe_live(&["get", "metastore"], &unity_endpoint(&host), Some(&token));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(resource(&records[0]), "metastores");
    assert!(
        !records[0]["name"].as_str().unwrap_or_default().is_empty(),
        "{records:?}"
    );
    assert!(
        !records[0]["cloud_provider"]
            .as_str()
            .unwrap_or_default()
            .is_empty(),
        "{records:?}"
    );
    assert!(
        !records[0]["region"].as_str().unwrap_or_default().is_empty(),
        "{records:?}"
    );
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn catalog_ls_lists_the_live_catalogs() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe_live(&["list", "catalogs"], &unity_endpoint(&host), Some(&token));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    for record in &records {
        assert_eq!(resource(record), "catalogs");
    }
    let names: Vec<&str> = records.iter().map(id).collect();
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

    let output = pipe_live(&["list", "catalogs"], &unity_endpoint(&host), Some(&token));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(ndjson(&output.stdout).len(), 5);
}

/// The issue-#58 walk, for the commands that exist: a job directory holds
/// each step's NDJSON, and the walk prints the result from the tree. Auth is
/// the bearer the whole walk runs under; the missing/invalid-token tests
/// cover its failures. `list tables` and below extend this tree next.
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
    let endpoint = unity_endpoint(&host);
    let config = source(&endpoint, Some(&token));

    let job = tempfile::tempdir().unwrap();
    let metastore = job.path().join("metastore");
    std::fs::create_dir_all(&metastore).unwrap();

    let metastore_info = metastore.join("info.jsonl");
    let output = pipe_output_env(&["get", "metastore"], b"", &metastore_info, &config);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let catalogs = metastore.join("catalogs.jsonl");
    let output = pipe_output_env(&["list", "catalogs"], b"", &catalogs, &config);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let records = read_ndjson(&metastore_info);
    assert_eq!(records.len(), 1);
    assert_eq!(resource(&records[0]), "metastores");
    println!("metastore: {}", records[0]["name"].as_str().unwrap_or("-"));

    let records = read_ndjson(&catalogs);
    let names: Vec<String> = records
        .iter()
        .map(|record| {
            assert_eq!(resource(record), "catalogs");
            id(record).to_string()
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
        let output = pipe_output_env(
            &["get", "catalog"],
            catalog_ref(name).as_bytes(),
            &catalog_info,
            &config,
        );
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let records = read_ndjson(&catalog_info);
        assert_eq!(records.len(), 1);
        assert_eq!(resource(&records[0]), "catalogs");
        assert_eq!(records[0]["name"], format!("catalogs/{name}"));
        println!(
            "catalog: {} ({})",
            name,
            records[0]["catalog_type"].as_str().unwrap_or("-")
        );

        let catalog_schemas = dir.join("schemas.jsonl");
        let output = pipe_output_env(
            &["list", "schemas"],
            catalog_ref(name).as_bytes(),
            &catalog_schemas,
            &config,
        );
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let records = read_ndjson(&catalog_schemas);
        assert!(!records.is_empty(), "{name} listed no schemas");
        for record in &records {
            assert_eq!(resource(record), "schemas");
            assert_eq!(fqn(record), format!("{name}.{}", id(record)));
        }
        println!("catalog {name}: {} schema(s)", records.len());

        // schema → each schema's tables, one directory per catalog.schema
        for record in &records {
            let schema = id(record);
            let fqn = format!("{name}.{schema}");
            let dir = job.path().join("schema").join(&fqn);
            std::fs::create_dir_all(&dir).unwrap();
            let tables = dir.join("tables.jsonl");
            let output = pipe_output_env(
                &["list", "tables"],
                schema_ref(&fqn).as_bytes(),
                &tables,
                &config,
            );
            assert!(
                output.status.success(),
                "stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let records = read_ndjson(&tables);
            for record in &records {
                assert_eq!(resource(record), "tables");
            }
            println!("schema {fqn}: {} table(s)", records.len());
        }
    }
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn catalog_get_reads_the_live_catalog() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe_env(
        &["get", "catalog"],
        catalog_ref("dbx_samples").as_bytes(),
        &source(&unity_endpoint(&host), Some(&token)),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 1);
    assert_eq!(resource(&records[0]), "catalogs");
    assert_eq!(records[0]["name"], "catalogs/dbx_samples");
    assert_eq!(records[0]["catalog_type"], "CATALOG_TYPE_MANAGED");
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
fn schema_ls_lists_the_live_schemas() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe_env(
        &["list", "schemas"],
        catalog_ref("dbx_samples").as_bytes(),
        &source(&unity_endpoint(&host), Some(&token)),
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
            assert_eq!(resource(record), "schemas");
            assert_eq!(fqn(record), format!("dbx_samples.{}", id(record)));
            id(record)
        })
        .collect();
    assert_eq!(names, DBX_SAMPLES_SCHEMAS);
}

/// The `pqbench_ext` schemas the e2e service principal can see, sorted: the
/// synthetic `events`/`sales`/`reference` plus the real sample data copied
/// server-side from `dbx_samples` (see `scripts/dbx-ext/`).
const EXTERNAL_FIXTURE_SCHEMAS: [&str; 12] = [
    "accuweather",
    "bakehouse",
    "clickbench",
    "events",
    "healthverity",
    "information_schema",
    "nyctaxi",
    "reference",
    "sales",
    "tpcds_sf1",
    "tpch_sf1",
    "tpch_sf10",
];

/// The table ids `list tables` lists for one `pqbench_ext` schema.
fn external_schema_ids(env: &[(&str, &str)], schema: &str) -> Vec<String> {
    let qualified = format!("pqbench_ext.{schema}");
    let output = pipe_env(&["list", "tables"], schema_ref(&qualified).as_bytes(), env);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    ndjson(&output.stdout)
        .iter()
        .map(|record| {
            assert_eq!(resource(record), "tables");
            fqn(record)
        })
        .collect()
}

/// `list schemas` and `list tables` on the external-location fixture: the
/// synthetic `events`/`sales`/`reference` (ten two-column tables each) and the
/// real sample data copied from `dbx_samples`, all external Delta on customer
/// S3 through the `pqbench_uc_e2e` external location.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn schema_ls_lists_the_external_fixture() {
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
        &["list", "schemas"],
        catalog_ref("pqbench_ext").as_bytes(),
        &env,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let schemas: Vec<String> = ndjson(&output.stdout)
        .iter()
        .map(|record| id(record).to_string())
        .collect();
    assert_eq!(schemas, EXTERNAL_FIXTURE_SCHEMAS);

    // The synthetic tables name t01..t10.
    let mut synthetic = 0;
    for schema in ["events", "sales", "reference"] {
        let ids = external_schema_ids(&env, schema);
        let expected: Vec<String> = (1..=10)
            .map(|i| format!("pqbench_ext.{schema}.t{i:02}"))
            .collect();
        assert_eq!(ids, expected);
        synthetic += ids.len();
    }
    assert_eq!(synthetic, 30);

    // The copied sample tables. The counts pin the copy set; the singletons
    // pin the names the walk tests read.
    for (schema, count) in [
        ("bakehouse", 6),
        ("accuweather", 12),
        ("healthverity", 1),
        ("tpcds_sf1", 24),
        ("tpch_sf1", 8),
        ("tpch_sf10", 8),
        ("nyctaxi", 1),
        ("clickbench", 1),
    ] {
        let ids = external_schema_ids(&env, schema);
        assert_eq!(ids.len(), count, "{schema}: {ids:?}");
    }
    assert_eq!(
        external_schema_ids(&env, "nyctaxi"),
        ["pqbench_ext.nyctaxi.trips"]
    );
    assert_eq!(
        external_schema_ids(&env, "clickbench"),
        ["pqbench_ext.clickbench.hits"]
    );
    assert_eq!(
        external_schema_ids(&env, "healthverity"),
        ["pqbench_ext.healthverity.claims_sample_synthetic"]
    );
}

/// `get schema` reads `dbx_samples.nyctaxi` from both dialects.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn schema_get_reads_the_live_schema() {
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
        let output = if iceberg {
            pipe_env(
                &["get", "schema"],
                schema_ref("dbx_samples.nyctaxi").as_bytes(),
                &[
                    ("PQB_ENDPOINT", endpoint.as_str()),
                    ("PQB_TOKEN", token.as_str()),
                    ("PQB_TABLE_FORMAT", "iceberg"),
                ],
            )
        } else {
            pipe_env(
                &["get", "schema"],
                schema_ref("dbx_samples.nyctaxi").as_bytes(),
                &source(&endpoint, Some(&token)),
            )
        };
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let records = ndjson(&output.stdout);
        assert_eq!(records.len(), 1);
        assert_eq!(resource(&records[0]), "schemas");
        assert_eq!(records[0]["name"], "catalogs/dbx_samples/schemas/nyctaxi");
    }
}

/// `list tables` lists `dbx_samples.nyctaxi` from both dialects: Unity fills
/// the storage path in the listing, Iceberg REST leaves it to `get table`.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn table_ls_lists_the_live_tables() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let output = pipe_env(
        &["list", "tables"],
        schema_ref("dbx_samples.nyctaxi").as_bytes(),
        &source(&unity_endpoint(&host), Some(&token)),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert!(!records.is_empty());
    for record in &records {
        assert_eq!(resource(record), "tables");
        assert!(
            record["name"].as_str().unwrap().contains("/tables/"),
            "{record:?}"
        );
        assert!(
            record["uri"].as_str().is_some(),
            "Unity fills the storage URI: {record:?}"
        );
    }
    let names: Vec<String> = records.iter().map(fqn).collect();
    assert!(
        names.iter().any(|name| name == "dbx_samples.nyctaxi.trips"),
        "{names:?}"
    );

    let endpoint = format!(
        "{}/iceberg-rest/v1/catalogs/dbx_samples",
        unity_endpoint(&host)
    );
    let output = pipe_env(
        &["list", "tables"],
        schema_ref("dbx_samples.nyctaxi").as_bytes(),
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", token.as_str()),
            ("PQB_TABLE_FORMAT", "iceberg"),
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let trips = records
        .iter()
        .find(|record| fqn(record) == "dbx_samples.nyctaxi.trips")
        .unwrap_or_else(|| panic!("no trips table: {records:?}"));
    assert_eq!(
        trips["name"],
        "catalogs/dbx_samples/schemas/nyctaxi/tables/trips"
    );
    assert!(trips["uri"].is_null(), "{trips:?}");

    // The new tree enriches its own v2 refs: `get table` reads the
    // `loadTable` metadata inline, so no storage read runs.
    let info = pipe_env(
        &["get", "table"],
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
        .find(|record| fqn(record) == "dbx_samples.nyctaxi.trips")
        .unwrap_or_else(|| panic!("no trips table: {records:?}"));
    assert_eq!(resource(trips), "tables");
    assert_eq!(trips["format"], "TABLE_FORMAT_ICEBERG");
    assert!(!trips["declared_columns"].as_array().unwrap().is_empty());
    assert!(trips["snapshot_version"].as_u64().unwrap() > 0);
    assert!(!trips["uri"].as_str().unwrap().is_empty(), "{trips:?}");
}

/// `get table` reads the live Iceberg REST table: the `loadTable` response
/// carries the metadata inline, so the record is complete without any storage
/// read (the default-storage Delta path cannot read its log).
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn table_get_reads_the_live_iceberg_table() {
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
        &["get", "table"],
        table_ref("dbx_samples.nyctaxi.trips").as_bytes(),
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", token.as_str()),
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
    let record = &records[0];
    assert_eq!(resource(record), "tables");
    assert_eq!(fqn(record), "dbx_samples.nyctaxi.trips");
    assert_eq!(record["format"], "TABLE_FORMAT_ICEBERG");
    assert!(record["snapshot_version"].as_u64().unwrap() > 0);
    assert!(record["declared_columns"].as_array().unwrap().len() >= 6);
    assert!(record["properties"].is_object(), "{record:?}");
}

/// `get table` on the live Unity catalog: the table read runs with the env
/// it is given — here none — so the `without_files()` Delta log read of the
/// managed default-storage table fails outside compute and the error names the
/// table and its location.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn table_get_names_the_location_when_the_metadata_cannot_be_read() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let output = pipe_env(
        &["get", "table"],
        table_ref("dbx_samples.nyctaxi.trips").as_bytes(),
        &source(&endpoint, Some(&token)),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("dbx_samples.nyctaxi.trips"), "{stderr}");
    assert!(stderr.contains("s3://"), "{stderr}");
}

/// `get table` on the live Unity catalog: the UniForm Delta fixture
/// (`pqbench_delta_test`) is on default storage, so it gets no lease and its
/// storage read fails with the table named.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn get_table_refuses_default_storage() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let region = std::env::var("DBX_AWS_REGION")
        .ok()
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "us-east-2".to_string());
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
        ("AWS_REGION", region.as_str()),
    ];
    let unsupported = pipe_env(
        &["get", "table"],
        format!("{}\n", table_ref("dbx_samples.nyctaxi.pqbench_delta_test")).as_bytes(),
        &env,
    );
    assert!(!unsupported.status.success());
    let stderr = String::from_utf8_lossy(&unsupported.stderr);
    assert!(
        stderr.contains("catalogs/dbx_samples/schemas/nyctaxi/tables/pqbench_delta_test"),
        "{stderr}"
    );
}

/// `get table` on the external fixture: every table in the synthetic schemas
/// and in a copied schema is eligible, so each read resolves its own lease and
/// the emitted records carry no credentials.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn get_table_reads_the_external_fixture() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let endpoint = unity_endpoint(&host);
    let region = std::env::var("DBX_AWS_REGION")
        .ok()
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "us-east-2".to_string());
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
        ("AWS_REGION", region.as_str()),
    ];

    let mut tables = 0;
    for schema in ["events", "sales", "reference", "tpch_sf1", "clickbench"] {
        let fqn = format!("pqbench_ext.{schema}");
        let listed = pipe_env(&["list", "tables"], schema_ref(&fqn).as_bytes(), &env);
        assert!(
            listed.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&listed.stderr)
        );
        let read = pipe_env(&["get", "table"], &listed.stdout, &env);
        assert!(
            read.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&read.stderr)
        );
        for record in ndjson(&read.stdout) {
            assert!(
                record.get("credentials").is_none(),
                "the lease rode on the record: {record}"
            );
            tables += 1;
        }
    }
    assert_eq!(tables, 39);
}

/// Run one walk stage and time it; used by the perf target.
fn timed_stage(
    env: &[(&str, &str)],
    args: &[&str],
    input: &[u8],
) -> (Vec<u8>, std::time::Duration) {
    let started = std::time::Instant::now();
    let output = pipe_env(args, input, env);
    let elapsed = started.elapsed();
    assert!(
        output.status.success(),
        "{args:?} stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (output.stdout, elapsed)
}

/// The whole issue-#58 walk on a copied schema, all the way to a treemap:
/// `list tables` → `get table` →
/// `list files` → `list columns` → `summarize columns` → `format --html`, each
/// stage fed the
/// previous stage's NDJSON. Set
/// `DBX_WALK_SCHEMA` to walk another schema (default `pqbench_ext.tpch_sf1`).
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn external_fixture_walks_to_a_treemap() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let Some(token) = any_token(&host) else {
        eprintln!("skipping: DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set");
        return;
    };
    let region = std::env::var("DBX_AWS_REGION")
        .ok()
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "us-east-2".to_string());
    let schema = std::env::var("DBX_WALK_SCHEMA")
        .ok()
        .filter(|schema| !schema.is_empty())
        .unwrap_or_else(|| "pqbench_ext.tpch_sf1".to_string());
    let endpoint = unity_endpoint(&host);
    let env = [
        ("PQB_ENDPOINT", endpoint.as_str()),
        ("PQB_TOKEN", token.as_str()),
        ("AWS_REGION", region.as_str()),
    ];

    let (listed, _) = timed_stage(&env, &["list", "tables"], schema_ref(&schema).as_bytes());
    let (info, _) = timed_stage(&env, &["get", "table"], &listed);
    let (files, _) = timed_stage(&env, &["list", "files"], &info);
    let (measured, _) = timed_stage(&env, &["list", "columns"], &files);

    // The walk narrows: tables → files → columns.
    let tables = ndjson(&info).len();
    let file_count = ndjson(&files).len();
    let records = ndjson(&measured);
    assert!(tables > 0, "{schema}: no tables");
    assert!(
        file_count >= tables,
        "{schema}: {file_count} files < {tables} tables"
    );
    assert!(
        records.iter().any(|record| record["name"]
            .as_str()
            .is_some_and(|name| pqbench::v1::name::parse_blob(name).is_some())),
        "{schema}: no Blob record"
    );
    assert!(
        records.iter().any(|record| record["name"]
            .as_str()
            .is_some_and(|name| pqbench::v1::Column::parse_name(name).is_some())),
        "{schema}: no Column record"
    );

    let (summarized, _) = timed_stage(&env, &["summarize", "columns"], &measured);
    let rows = ndjson(&summarized);
    assert!(!rows.is_empty(), "{schema}: no ColumnMass row");
    assert!(
        rows.iter().all(|row| row["table"]
            .as_str()
            .is_some_and(|table| pqbench::v1::Table::parse_name(table).is_some())),
        "{schema}: a ColumnMass row names no table"
    );
    assert!(
        rows.iter().any(|row| row["compressed_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)),
        "{schema}: no measured bytes"
    );

    let rendered = pipe_env(&["format", "--html"], &summarized, &env);
    assert!(
        rendered.status.success(),
        "format --html stderr: {}",
        String::from_utf8_lossy(&rendered.stderr)
    );
    let html = String::from_utf8(rendered.stdout).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>"), "{schema}: no page");
    assert!(html.contains("d3-hierarchy@3"), "{schema}: no treemap");
    let schema_id = schema.rsplit('.').next().unwrap_or_default();
    assert!(
        html.contains(&format!("\"display_name\":\"{schema_id}\"")),
        "{schema}: the page names no schema"
    );

    println!(
        "walk {schema}: {tables} tables, {file_count} files, {} column records, {} rows",
        records.len(),
        rows.len()
    );
}

/// `get table` on a table in customer storage: the stage resolves the table's
/// vended lease itself, reads the Delta log under it, and emits a record with
/// no credentials. The fixture lives in `us-east-2`; set `DBX_AWS_TABLE` /
/// `DBX_AWS_REGION` to read another one.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn table_get_reads_the_external_aws_table() {
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
    let reference = table_ref(&table);
    let output = pipe_env(
        &["get", "table"],
        format!("{reference}\n").as_bytes(),
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", token.as_str()),
            ("AWS_REGION", region.as_str()),
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
    assert_eq!(fqn(record), table);
    assert_eq!(record["format"], "TABLE_FORMAT_DELTA");
    assert!(
        record["uri"]
            .as_str()
            .unwrap_or_default()
            .starts_with("s3://"),
        "{record:?}"
    );
    assert!(
        !record["declared_columns"].as_array().unwrap().is_empty(),
        "{record:?}"
    );
    assert!(
        record.get("credentials").is_none(),
        "the lease rode on the record: {record:?}"
    );
}

/// `get table` with `PQB_TABLE_FORMAT=iceberg` on the live catalog: loadTable
/// returns the metadata inline, and the emitted records carry no credentials.
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn get_table_reads_the_live_iceberg_tables() {
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
    for table in [
        "dbx_samples.nyctaxi.trips",
        "dbx_samples.nyctaxi.pqbench_iceberg_test",
    ] {
        let output = pipe_env(
            &["get", "table"],
            format!("{}\n", table_ref(table)).as_bytes(),
            &env,
        );
        assert!(
            output.status.success(),
            "{table} stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let records = ndjson(&output.stdout);
        assert_eq!(records.len(), 1, "{table}");
        assert!(
            records[0].get("credentials").is_none(),
            "{table}: the lease rode on the record: {}",
            records[0]
        );
    }
}

/// The walk as a pipe, context in `PQB_ENDPOINT` / `PQB_TOKEN`:
/// `list catalogs | get catalog | list schemas` (decision 0004).
#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn catalog_ls_pipes_into_catalog_get_and_ls() {
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

    let catalogs = pipe_env(&["list", "catalogs"], b"", &env);
    assert!(
        catalogs.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&catalogs.stderr)
    );
    let refs = catalogs.stdout;
    let names: Vec<String> = ndjson(&refs)
        .iter()
        .map(|record| id(record).to_string())
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

    let info = pipe_env(&["get", "catalog"], &refs, &env);
    assert!(
        info.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&info.stderr)
    );
    let records = ndjson(&info.stdout);
    assert_eq!(records.len(), 5);
    for record in &records {
        assert_eq!(resource(record), "catalogs");
        assert!(names.contains(&id(record).to_string()));
    }

    let schemas = pipe_env(&["list", "schemas"], &refs, &env);
    assert!(
        schemas.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&schemas.stderr)
    );
    let records = ndjson(&schemas.stdout);
    assert!(!records.is_empty());
    for record in &records {
        assert_eq!(resource(record), "schemas");
        let catalog = fqn(record);
        assert!(names.contains(&catalog.split('.').next().unwrap().to_string()));
    }
    let dbx_samples: Vec<&str> = records
        .iter()
        .filter(|record| fqn(record).starts_with("dbx_samples."))
        .map(id)
        .collect();
    assert_eq!(dbx_samples, DBX_SAMPLES_SCHEMAS);
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn schema_ls_lists_the_live_iceberg_rest_namespaces() {
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
        &["list", "schemas"],
        catalog_ref("dbx_samples").as_bytes(),
        &[
            ("PQB_ENDPOINT", endpoint.as_str()),
            ("PQB_TOKEN", token.as_str()),
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
        .map(|record| {
            assert_eq!(resource(record), "schemas");
            assert_eq!(fqn(record), format!("dbx_samples.{}", id(record)));
            id(record)
        })
        .collect();
    assert_eq!(names, DBX_SAMPLES_SCHEMAS);
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_get_rejects_a_missing_token() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let output = pipe_live(&["get", "metastore"], &unity_endpoint(&host), None);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

#[test]
#[ignore = "network: reads the live Databricks endpoint"]
fn metastore_get_rejects_an_invalid_token() {
    let Some(host) = dbx_host() else {
        eprintln!("skipping: DBX_HOST is not set");
        return;
    };
    let output = pipe_live(
        &["get", "metastore"],
        &unity_endpoint(&host),
        Some("not-a-real-token"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("401"), "{stderr}");
}

use serde_json::{json, Value};
#[cfg(feature = "unity")]
use std::io::Read;
use std::io::Write;
use std::process::{Command, Stdio};
#[cfg(feature = "unity")]
use std::sync::{Arc, Mutex};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

/// One Unity Catalog list server. `expect_bearer` is the Databricks token the
/// client must send; `None` is Unity OSS, which sends no Authorization header.
#[cfg(feature = "unity")]
struct Catalog {
    address: String,
    seen: Arc<Mutex<Vec<String>>>,
    #[allow(dead_code)]
    thread: std::thread::JoinHandle<()>,
}

#[cfg(feature = "unity")]
impl Catalog {
    fn spawn(expect_bearer: Option<&'static str>, tables: Vec<Value>) -> Self {
        Self::spawn_catalogs(expect_bearer, vec!["main"], tables)
    }

    fn spawn_catalogs(
        expect_bearer: Option<&'static str>,
        catalogs: Vec<&'static str>,
        tables: Vec<Value>,
    ) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_thread = seen.clone();
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming().take(64) {
                let mut stream = stream.unwrap();
                let mut buffer = [0u8; 4096];
                let n = stream.read(&mut buffer).unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..n]);
                seen_thread
                    .lock()
                    .unwrap()
                    .push(request.lines().next().unwrap_or("").to_string());
                // Header names are case-insensitive; reqwest sends them lowercased.
                let authorization = request
                    .lines()
                    .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                    .and_then(|line| line.split_once(':').map(|(_, value)| value.trim()));
                // A request must carry exactly the credential the endpoint
                // expects: a bearer token for Databricks, and none for Unity
                // OSS, where any Authorization header is rejected.
                let expected = expect_bearer.map(|token| format!("Bearer {token}"));
                if authorization != expected.as_deref() {
                    let body = b"unauthorized";
                    let response = format!(
                        "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.write_all(body);
                    continue;
                }
                let body = catalog_body(&request, &catalogs, &tables);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(body.as_bytes());
            }
        });
        Self {
            address,
            seen,
            thread,
        }
    }

    fn requested(&self, needle: &str) -> bool {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.contains(needle))
    }
}

#[cfg(feature = "unity")]
fn catalog_body(request: &str, catalogs: &[&str], tables: &[Value]) -> String {
    let path = request.split_whitespace().nth(1).unwrap_or("");
    // 200 without `defaults` is Unity; Iceberg REST answers with a defaults object.
    if path.contains("/v1/config") {
        return "{}".into();
    }
    if path.contains("/catalogs") {
        let items: Vec<Value> = catalogs.iter().map(|name| json!({"name": name})).collect();
        return json!({"catalogs": items}).to_string();
    }
    if path.contains("/schemas") {
        if path.contains("schema_name=empty") || path.contains("catalog_name=samples") {
            return "{}".into();
        }
        return r#"{"schemas":[{"name":"default"}]}"#.into();
    }
    // Databricks list tables: a page may be empty and still carry next_page_token.
    // https://docs.databricks.com/api/workspace/tables/list
    if path.contains("page_token=more") {
        return json!({"tables": tables}).to_string();
    }
    if path.contains("/tables") && path.contains("catalog_name=samples") {
        return "{}".into();
    }
    json!({"tables": [], "next_page_token": "more"}).to_string()
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

fn table_refs(records: &[Value]) -> Vec<Value> {
    records
        .iter()
        .filter(|record| record["kind"] == "pqbench.table-ref")
        .cloned()
        .collect()
}

#[test]
fn lake_lists_delta_and_iceberg_tables_as_ndjson_on_a_pipe() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("sales/events/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("orders/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("catalog/reviews/metadata")).unwrap();
    std::fs::write(
        root.path()
            .join("catalog/reviews/metadata/v1.metadata.json"),
        "{}",
    )
    .unwrap();
    let output = pqbench().arg("lake").arg(root.path()).output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records[0]["event"], "begin");
    let refs = table_refs(&records);
    assert_eq!(refs[0]["id"], "catalog/reviews");
    assert_eq!(refs[1]["id"], "orders");
    assert_eq!(refs[2]["id"], "sales/events");
    assert_eq!(records.last().unwrap()["event"], "end");
    assert_eq!(records.last().unwrap()["table_count"], 3);
}

#[test]
fn lake_include_and_exclude_filter_directory_names() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("sales/events/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("sales/tmp/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("orders/_delta_log")).unwrap();
    let output = pqbench()
        .args([
            "lake",
            root.path().to_str().unwrap(),
            "--include",
            "sales/*",
            "--exclude",
            "sales/tmp",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ids: Vec<_> = table_refs(&ndjson(&output.stdout))
        .into_iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, ["sales/events"]);
}

#[cfg(unix)]
#[test]
fn lake_include_does_not_walk_a_pruned_prefix() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("sales/events/_delta_log")).unwrap();
    let orders = root.path().join("orders");
    std::fs::create_dir_all(orders.join("_delta_log")).unwrap();
    std::fs::set_permissions(&orders, std::fs::Permissions::from_mode(0o000)).unwrap();
    // Permission bits are advisory to root; without enforcement there is no
    // observable difference between pruning a prefix and filtering its result.
    if std::fs::read_dir(&orders).is_ok() {
        std::fs::set_permissions(&orders, std::fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }

    let unfiltered = pqbench().arg("lake").arg(root.path()).output().unwrap();
    let filtered = pqbench()
        .args(["lake", root.path().to_str().unwrap(), "--include", "sales"])
        .output()
        .unwrap();
    std::fs::set_permissions(&orders, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(
        !unfiltered.status.success(),
        "an unreadable prefix must fail an unpruned walk"
    );
    assert!(
        filtered.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&filtered.stderr)
    );
    let ids: Vec<_> = table_refs(&ndjson(&filtered.stdout))
        .into_iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, ["sales/events"]);
}

#[test]
fn lake_file_uri_matches_a_bare_path() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("orders/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("reviews/data/nested/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("reviews/metadata")).unwrap();
    std::fs::write(root.path().join("reviews/metadata/v1.metadata.json"), "{}").unwrap();
    let path = pqbench().arg("lake").arg(root.path()).output().unwrap();
    assert!(
        path.status.success(),
        "{}",
        String::from_utf8_lossy(&path.stderr)
    );
    let uri = format!("file://{}", root.path().display());
    let file = pqbench().arg("lake").arg(&uri).output().unwrap();
    assert!(
        file.status.success(),
        "{}",
        String::from_utf8_lossy(&file.stderr)
    );
    let from_path: Vec<_> = table_refs(&ndjson(&path.stdout))
        .into_iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    let from_file: Vec<_> = table_refs(&ndjson(&file.stdout))
        .into_iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(from_path, ["orders", "reviews"]);
    assert_eq!(from_path, from_file);
}

#[test]
fn lake_max_depth_does_not_walk_past_the_bound() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("orders/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("sales/events/_delta_log")).unwrap();
    let output = pqbench()
        .args(["lake", root.path().to_str().unwrap(), "--max-depth", "1"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ids: Vec<_> = table_refs(&ndjson(&output.stdout))
        .into_iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, ["orders"]);
}

#[cfg(not(feature = "aws"))]
#[test]
fn lake_names_the_missing_aws_feature_for_s3() {
    let output = pqbench()
        .args(["lake", "s3://bucket/warehouse"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("aws"), "{stderr}");
}

#[cfg(feature = "unity")]
#[test]
fn unity_oss_lists_delta_tables_without_a_token() {
    let table = json!({
        "name": "events",
        "catalog_name": "main",
        "schema_name": "default",
        "full_name": "main.default.events",
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "storage_location": "s3://lakehouse/unity/events"
    });
    let catalog = Catalog::spawn(
        None,
        vec![
            table,
            json!({
                "name": "view",
                "table_type": "VIEW",
                "data_source_format": null
            }),
            json!({
                "name": "files",
                "data_source_format": "PARQUET",
                "storage_location": "s3://lakehouse/files"
            }),
        ],
    );
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address,
        "env": {"AWS_REGION": "us-east-1"}
    });
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unity.json");
    std::fs::write(&path, source.to_string()).unwrap();
    let output = pqbench().arg("lake").arg(&path).output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let refs = table_refs(&ndjson(&output.stdout));
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0]["id"], "main.default.events");
    assert_eq!(refs[0]["uri"], "s3://lakehouse/unity/events");
    assert_eq!(refs[0]["env"]["AWS_REGION"], "us-east-1");
}

#[cfg(feature = "unity")]
#[test]
fn databricks_list_follows_an_empty_page_token() {
    let catalog = Catalog::spawn(
        Some("dapi-example"),
        vec![json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "default",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/events",
            "full_name": "main.default.events",
            "table_id": "11111111-1111-1111-1111-111111111111",
            "columns": []
        })],
    );
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address,
        "token": "dapi-example"
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains("dapi-example"));
    let refs = table_refs(&ndjson(stdout.as_bytes()));
    assert_eq!(refs[0]["id"], "main.default.events");
    assert_eq!(refs[0]["uri"], "s3://bucket/events");
}

#[cfg(feature = "unity")]
#[test]
fn unity_skips_an_empty_schema_page() {
    let catalog = Catalog::spawn_catalogs(
        None,
        vec!["main", "samples"],
        vec![json!({
            "name": "events",
            "full_name": "main.default.events",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/events"
        })],
    );
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let refs = table_refs(&ndjson(&output.stdout));
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0]["id"], "main.default.events");
}

#[cfg(feature = "unity")]
#[test]
fn include_and_exclude_match_table_fqn() {
    let catalog = Catalog::spawn(
        None,
        vec![
            json!({
                "name": "events",
                "full_name": "main.default.events",
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": "s3://bucket/events"
            }),
            json!({
                "name": "tmp",
                "full_name": "main.default.tmp",
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": "s3://bucket/tmp"
            }),
        ],
    );
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address
    });
    let output = pipe(
        &[
            "lake",
            "--include",
            "main.default.*",
            "--exclude",
            "main.default.tmp",
        ],
        source.to_string().as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ids: Vec<_> = table_refs(&ndjson(&output.stdout))
        .into_iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, ["main.default.events"]);
}

#[cfg(feature = "unity")]
#[test]
fn include_table_fqn_skips_other_catalogs() {
    let catalog = Catalog::spawn_catalogs(
        None,
        vec!["main", "system"],
        vec![json!({
            "name": "events",
            "full_name": "main.default.events",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/events"
        })],
    );
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address
    });
    let output = pipe(
        &["lake", "--include", "main.default.events"],
        source.to_string().as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!catalog.requested("/catalogs"));
    assert!(!catalog.requested("catalog_name=system"));
    let ids: Vec<_> = table_refs(&ndjson(&output.stdout))
        .into_iter()
        .map(|record| record["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, ["main.default.events"]);
}

#[cfg(feature = "unity")]
#[test]
fn include_prefix_skips_other_catalogs() {
    let catalog = Catalog::spawn_catalogs(
        None,
        vec!["main", "system"],
        vec![json!({
            "name": "events",
            "full_name": "main.default.events",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/events"
        })],
    );
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address
    });
    let output = pipe(
        &["lake", "--include", "main"],
        source.to_string().as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!catalog.requested("/catalogs"));
    assert!(!catalog.requested("catalog_name=system"));
    assert!(catalog.requested("catalog_name=main"));
    let refs = table_refs(&ndjson(&output.stdout));
    assert_eq!(refs.len(), 1);
}

#[cfg(feature = "unity")]
#[test]
fn iceberg_rest_lists_tables_from_metadata_location() {
    let catalog = IcebergCatalog::spawn();
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address,
        "env": {"AWS_REGION": "us-east-1"}
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let refs = table_refs(&ndjson(&output.stdout));
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0]["id"], "demo.events");
    assert_eq!(
        refs[0]["uri"],
        "s3://lakehouse/iceberg/demo/events/metadata/v1.metadata.json"
    );
    assert_eq!(refs[0]["env"]["AWS_REGION"], "us-east-1");
    assert!(refs[0].get("info").is_none());
}

#[cfg(feature = "unity")]
#[test]
fn iceberg_rest_rejects_a_table_without_metadata_location() {
    let catalog = IcebergCatalog::spawn_load("{}");
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("metadata-location") || stderr.contains("expected document"),
        "{stderr}"
    );
}

#[cfg(feature = "unity")]
#[test]
fn iceberg_rest_rejects_a_namespace_page_without_namespaces() {
    let catalog = IcebergCatalog::spawn_namespaces("{}");
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("expected document") || stderr.contains("namespaces"),
        "{stderr}"
    );
}

#[cfg(feature = "unity")]
#[test]
fn iceberg_rest_rejects_an_identifier_without_a_name() {
    let catalog = IcebergCatalog::spawn_identifiers(r#"{"identifiers":[{}]}"#);
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": catalog.address
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("expected document") || stderr.contains("name"),
        "{stderr}"
    );
}

#[cfg(feature = "unity")]
#[test]
fn lake_source_rejects_a_catalog_that_does_not_answer() {
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": "http://127.0.0.1:1"
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("catalog request failed") || stderr.contains("catalog"),
        "{stderr}"
    );
}

/// One Iceberg REST list server: `/v1/config` reports the defaults object that
/// selects this dialect, then namespaces, tables, and `loadTable`.
#[cfg(feature = "unity")]
struct IcebergCatalog {
    address: String,
    // Held only to keep the mock catalog thread alive for the test's duration.
    #[allow(dead_code)]
    thread: std::thread::JoinHandle<()>,
}

#[cfg(feature = "unity")]
impl IcebergCatalog {
    fn spawn() -> Self {
        Self::serve(None, None, None)
    }

    fn spawn_namespaces(namespaces: &'static str) -> Self {
        Self::serve(Some(namespaces), None, None)
    }

    fn spawn_identifiers(identifiers: &'static str) -> Self {
        Self::serve(None, Some(identifiers), None)
    }

    fn spawn_load(loaded: &'static str) -> Self {
        Self::serve(None, None, Some(loaded))
    }

    fn serve(
        namespaces: Option<&'static str>,
        identifiers: Option<&'static str>,
        loaded: Option<&'static str>,
    ) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming().take(8) {
                let mut stream = stream.unwrap();
                let mut buffer = [0u8; 4096];
                let n = stream.read(&mut buffer).unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..n]);
                let path = request.split_whitespace().nth(1).unwrap_or("");
                let body = if path.starts_with("/v1/config") {
                    json!({"defaults": {}, "overrides": {}}).to_string()
                } else if path.starts_with("/v1/namespaces/demo/tables/events") {
                    loaded.map(str::to_string).unwrap_or_else(|| {
                        json!({
                            "metadata-location":
                                "s3://lakehouse/iceberg/demo/events/metadata/v1.metadata.json"
                        })
                        .to_string()
                    })
                } else if path.starts_with("/v1/namespaces/demo/tables") {
                    identifiers.map(str::to_string).unwrap_or_else(|| {
                        json!({"identifiers":[{"namespace":["demo"],"name":"events"}]}).to_string()
                    })
                } else if path.starts_with("/v1/namespaces") {
                    namespaces
                        .map(str::to_string)
                        .unwrap_or_else(|| json!({"namespaces":[["demo"]]}).to_string())
                } else {
                    json!({"error": path}).to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(body.as_bytes());
            }
        });
        Self { address, thread }
    }
}

#[test]
fn bytemass_rejects_a_lake_that_has_not_been_loaded() {
    let document = json!({
        "kind": "pqbench.lake",
        "version": 1,
        "tables": [{"name": "events", "uri": "/tmp/events"}]
    });
    let output = pipe(&["bytemass"], document.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("pqbench table"), "{stderr}");
}

#[cfg(feature = "delta")]
#[test]
fn lake_table_bytemass_measures_each_table() {
    let root = tempfile::tempdir().unwrap();
    let table = root.path().join("events");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    let parquet = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    );
    let data = table.join("data.parquet");
    std::fs::copy(parquet, &data).unwrap();
    let size = std::fs::metadata(&data).unwrap().len();
    let commit = format!(
        "{}\n{}\n{}\n",
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "11111111-1111-1111-1111-111111111111",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }}),
        json!({"add": {
            "path": "data.parquet",
            "partitionValues": {},
            "size": size,
            "modificationTime": 0,
            "dataChange": true
        }})
    );
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), commit).unwrap();

    let listed = pqbench().arg("lake").arg(root.path()).output().unwrap();
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let loaded = pipe(&["table"], &listed.stdout);
    assert!(
        loaded.status.success(),
        "{}",
        String::from_utf8_lossy(&loaded.stderr)
    );
    let measured = pipe(&["bytemass", "--json"], &loaded.stdout);
    assert!(
        measured.status.success(),
        "{}",
        String::from_utf8_lossy(&measured.stderr)
    );
    let records = ndjson(&measured.stdout);
    let end = records
        .iter()
        .find(|record| record["event"] == "end")
        .unwrap();
    assert_eq!(end["row_count"], 3000);
    assert_eq!(end["file_count"], 1);
}

#[test]
fn lake_source_rejects_a_non_aws_env_key() {
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": "http://127.0.0.1:9",
        "env": {"NOT_AWS": "x"}
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("AWS_*"), "{stderr}");
}

#[cfg(feature = "unity")]
#[test]
fn lake_rejects_a_catalog_with_no_delta_tables() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let _thread = std::thread::spawn(move || {
        // Probe is GET /v1/config (200 without defaults → Unity), then catalogs.
        for stream in listener.incoming().take(2) {
            let mut stream = stream.unwrap();
            let mut buffer = [0u8; 1024];
            let _ = stream.read(&mut buffer);
            let body = b"{}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.write_all(body);
        }
    });
    let source = json!({
        "kind": "pqbench.lake-source",
        "version": 1,
        "endpoint": address
    });
    let output = pipe(&["lake"], source.to_string().as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no Delta tables"), "{stderr}");
}

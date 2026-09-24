use serde_json::{json, Value};
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

/// One Unity Catalog list server. `expect_bearer` is the Databricks token the
/// client must send; `None` is Unity OSS, which sends no Authorization header.
struct Catalog {
    address: String,
    seen: Arc<Mutex<Vec<String>>>,
    _thread: std::thread::JoinHandle<()>,
}

impl Catalog {
    fn spawn(expect_bearer: Option<&'static str>, tables: Vec<Value>) -> Self {
        Self::spawn_with_catalogs(expect_bearer, vec!["main"], tables)
    }

    fn spawn_with_catalogs(
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
                if expect_bearer.is_some()
                    && !request
                        .contains(&format!("Authorization: Bearer {}", expect_bearer.unwrap()))
                {
                    let body = b"unauthorized";
                    let response = format!(
                        "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.write_all(body);
                    continue;
                }
                if expect_bearer.is_none()
                    && request.to_ascii_lowercase().contains("authorization:")
                {
                    panic!("Unity OSS request must not send an Authorization header");
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
            _thread: thread,
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

fn catalog_body(request: &str, catalogs: &[&str], tables: &[Value]) -> String {
    let path = request.split_whitespace().nth(1).unwrap_or("");
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
fn lake_lists_delta_tables_as_ndjson_on_a_pipe() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("sales/events/_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("orders/_delta_log")).unwrap();
    let output = pqbench().arg("lake").arg(root.path()).output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records[0]["event"], "begin");
    let refs = table_refs(&records);
    assert_eq!(refs[0]["id"], "orders");
    assert_eq!(refs[1]["id"], "sales/events");
    assert_eq!(records.last().unwrap()["event"], "end");
    assert_eq!(records.last().unwrap()["table_count"], 2);
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

#[test]
fn unity_skips_an_empty_schema_page() {
    let catalog = Catalog::spawn_with_catalogs(
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

#[test]
fn include_table_fqn_skips_other_catalogs() {
    let catalog = Catalog::spawn_with_catalogs(
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

#[test]
fn include_prefix_skips_other_catalogs() {
    let catalog = Catalog::spawn_with_catalogs(
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
    assert_eq!(end["num_rows"], 3000);
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

#[test]
fn lake_rejects_a_catalog_with_no_delta_tables() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let _thread = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
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

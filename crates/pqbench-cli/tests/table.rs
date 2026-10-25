use serde_json::json;
use std::io::{Read, Write};
use std::process::{Command, Stdio};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

fn pipe(args: &[&str], stdin: &str) -> std::process::Output {
    pipe_env(args, stdin.as_bytes(), &[])
}

/// pqbench with extra environment: the walk's context can come from `PQB_*`.
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

/// A table-file document: the file's resource name, the blob it reads, and
/// the size the log claims (omitted when the log carries none).
fn table_file(table: &str, uri: &str, size: Option<u64>) -> serde_json::Value {
    let path = uri.rsplit('/').next().unwrap();
    let mut file = json!({
        "name": pqbench::v1::name::table_file_name(table, path),
        "blob": pqbench::v1::name::blob_name(uri),
    });
    if let Some(size) = size {
        file["size_bytes"] = json!(size);
    }
    file
}

fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

fn ndjson(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
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
/// route wins, so put the more specific needle first. No match is a 404.
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

/// One endpoint that answers by request-line substring, with owned bodies so a
/// fixture path can be embedded. The first matching route wins; no match is a
/// 404.
fn routes_owned(routes: Vec<(&'static str, u16, String)>) -> String {
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
                .map(|(_, status, body)| (*status, body.as_str()))
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

/// Like `routes`, but each request sleeps `delay` and the server records the
/// highest number of requests in flight — a command that awaits one request
/// at a time never sees two.
fn overlapping_routes(
    routes: &'static [(&'static str, u16, &'static str)],
    delay: std::time::Duration,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (requests_in_flight, requests_peak) = (in_flight.clone(), peak.clone());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let (in_flight, peak) = (requests_in_flight.clone(), requests_peak.clone());
            std::thread::spawn(move || {
                let mut stream = stream.unwrap();
                let mut buffer = [0u8; 2048];
                let read = stream.read(&mut buffer).unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..read]);
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                std::thread::sleep(delay);
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
                in_flight.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    (address, peak)
}

/// The table the fixture's files belong to, as the walk would name them.
const LOCAL_TABLE: &str = "catalogs/local/schemas/local/tables/t";

const TABLES: &str = r#"{"tables":[{"name":"trips","full_name":"dbx_samples.nyctaxi.trips","data_source_format":"DELTA","storage_location":"s3://bucket/trips"},{"name":"recent","table_type":"VIEW"}],"next_page_token":null}"#;
const TABLES_PAGE: &str = r#"{"tables":[{"name":"trips","full_name":"dbx_samples.nyctaxi.trips","data_source_format":"DELTA","storage_location":"s3://bucket/trips"}],"next_page_token":"more"}"#;
const TABLES_LAST: &str = r#"{"tables":[{"name":"zones","full_name":"dbx_samples.nyctaxi.zones","data_source_format":"DELTA","storage_location":"s3://bucket/zones"}]}"#;
const ICEBERG_TABLES: &str =
    r#"{"identifiers":[{"namespace":["nyctaxi"],"name":"trips"}],"next-page-token":null}"#;

/// The parent level's stream: one `Schema` resource per schema.
const SCHEMA_REFS: &str = concat!(
    r#"{"name":"catalogs/dbx_samples/schemas/nyctaxi"}"#,
    "\n",
    r#"{"name":"catalogs/dbx_samples/schemas/bakehouse"}"#,
    "\n",
);

#[test]
fn bytemass_reads_a_table_file_from_stdin() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let document = table_file(LOCAL_TABLE, parquet_fixture(), Some(size));
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("blobs/"), "{stdout}");
    assert!(stdout.contains("columns/url_encoded/bytemass"), "{stdout}");
}

#[test]
fn bytemass_rejects_a_non_aws_env_key() {
    let document = json!({
        "version": 1,
        "inputs": [parquet_fixture()],
        "env": {"NOT_AWS": "x"}
    });
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("AWS_*"), "{stderr}");
}

#[test]
fn bytemass_rejects_a_size_mismatch_on_a_table_file() {
    let document = table_file(LOCAL_TABLE, parquet_fixture(), Some(1));
    let output = pipe(&["bytemass"], &document.to_string());
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("size differs from log"), "{stderr}");
}

#[test]
fn bytemass_measures_mixed_table_ids() {
    let a = "catalogs/local/schemas/local/tables/a";
    let b = "catalogs/local/schemas/local/tables/b";
    let document = format!(
        "{}\n{}\n",
        table_file(b, parquet_fixture(), None),
        table_file(a, parquet_fixture(), None)
    );
    let output = pipe(&["bytemass"], &document);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    let blobs = records
        .iter()
        .filter(|record| {
            record["name"]
                .as_str()
                .is_some_and(|name| pqbench::v1::name::parse_blob(name).is_some())
        })
        .count();
    let columns = records
        .iter()
        .filter(|record| {
            record["name"]
                .as_str()
                .is_some_and(|name| pqbench::v1::name::parse_bytemass(name).is_some())
        })
        .count();
    // One Blob and seven Bytemass records per table file, for both ids.
    assert_eq!(blobs, 2, "{records:?}");
    assert_eq!(columns, 14, "{records:?}");
}

/// A minimal Delta table on disk: one commit with a protocol and metadata and
/// no active files. The `add` action names a file that does not exist, so a
/// read that materializes files would fail.
#[cfg(feature = "delta")]
fn delta_table(dir: &std::path::Path) {
    write_delta_table(
        dir,
        json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": true, "metadata": {}},
                {"name": "label", "type": "string", "nullable": true, "metadata": {}}
            ]
        }),
    );
}

/// A minimal Delta table whose log declares `schema` and names a data file that
/// does not exist, so a read that materializes files would fail.
#[cfg(feature = "delta")]
fn write_delta_table(dir: &std::path::Path, schema: serde_json::Value) {
    let log = dir.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let commit = format!(
        "{}\n{}\n{}\n",
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "a1b2c3d4-0000-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": [],
            "configuration": {"delta.appendOnly": "false"},
            "createdTime": 1_700_000_000_000i64
        }}),
        json!({"add": {
            "path": "part-does-not-exist.parquet",
            "size": 123,
            "partitionValues": {},
            "dataChange": true
        }})
    );
    std::fs::write(log.join("00000000000000000000.json"), commit).unwrap();
}

/// The columns a Unity table for the Delta fixture declares.
#[cfg(feature = "delta")]
fn unity_columns() -> serde_json::Value {
    json!([
        {"name": "id", "type_name": "LONG", "type_text": "long", "nullable": true},
        {"name": "label", "type_name": "STRING", "type_text": "string", "nullable": true}
    ])
}

/// A Unity `/tables/{full_name}` record for a local table.
#[cfg(feature = "delta")]
fn unity_record(location: &std::path::Path) -> String {
    unity_table_record(location, unity_columns())
}

/// A Unity `/tables/{full_name}` record for a local table, with `columns`.
#[cfg(feature = "delta")]
fn unity_table_record(location: &std::path::Path, columns: serde_json::Value) -> String {
    json!({
        "name": "trips",
        "catalog_name": "dbx_samples",
        "schema_name": "nyctaxi",
        "data_source_format": "DELTA",
        "storage_location": location.to_string_lossy(),
        "columns": columns,
        "properties": {"owner": "data"}
    })
    .to_string()
}

/// Run `get table` for the local Unity table at `dir`, whose catalog declares
/// `columns`, and return the one record it emits.
#[cfg(feature = "delta")]
fn read_unity_table(dir: &std::path::Path, columns: serde_json::Value) -> serde_json::Value {
    let address = routes_owned(vec![("/tables/", 200, unity_table_record(dir, columns))]);
    let output = pipe_env(
        &[
            "get",
            "table",
            "dbx_samples.nyctaxi.trips",
            "--format",
            "json",
        ],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    records.remove(0)
}

/// An Iceberg REST `loadTable` record with the metadata JSON inline. The
/// `metadata-location` names a path that does not exist, so a command that
/// reads storage would fail.
fn loaded_table(location: &str) -> String {
    json!({
        "metadata-location": format!("{location}/metadata/00001.metadata.json"),
        "metadata": {
            "format-version": 2,
            "location": location,
            "current-snapshot-id": 3268038499157964613i64,
            "current-schema-id": 0,
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": false, "type": "long"},
                {"id": 2, "name": "label", "required": true, "type": "string"}
            ]}],
            "properties": {"owner": "data"}
        }
    })
    .to_string()
}

/// `get table` reads one table's record from Unity: the Delta log without
/// files, merged with the catalog's declared columns and properties.
#[cfg(feature = "delta")]
#[test]
fn table_get_reads_a_unity_table() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let record = read_unity_table(dir.path(), unity_columns());
    assert_eq!(
        record["name"],
        "catalogs/dbx_samples/schemas/nyctaxi/tables/trips"
    );
    assert_eq!(record["format"], "TABLE_FORMAT_DELTA");
    assert_eq!(record["snapshot_version"], 0);
    assert_eq!(record["uri"], dir.path().to_string_lossy().as_ref());
    let columns: Vec<&str> = record["declared_columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| column["column"].as_str().unwrap())
        .collect();
    assert_eq!(columns, ["id", "label"]);
    assert_eq!(record["properties"]["owner"], "data");
    assert_eq!(record["properties"]["delta.appendOnly"], "false");
    assert!(record.get("iceberg_properties").is_none(), "{record:?}");
}

/// Spark writes a bare `NULL` as a `void` column. Delta leaves the type
/// undefined; `get table` still resolves the snapshot and reports the catalog's
/// columns instead of failing the whole table.
#[cfg(feature = "delta")]
#[test]
fn table_get_reads_a_void_column() {
    let dir = tempfile::tempdir().unwrap();
    write_delta_table(
        dir.path(),
        json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": true, "metadata": {}},
                {"name": "note", "type": "void", "nullable": true, "metadata": {}}
            ]
        }),
    );
    let record = read_unity_table(
        dir.path(),
        json!([
            {"name": "id", "type_name": "LONG", "type_text": "long", "nullable": true},
            {"name": "note", "type_name": "VOID", "type_text": "void", "nullable": true}
        ]),
    );
    assert_eq!(record["snapshot_version"], 0);
    let columns: Vec<(&str, &str)> = record["declared_columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| {
            (
                column["column"].as_str().unwrap(),
                column["data_type"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(columns, [("id", "long"), ("note", "void")]);
}

#[cfg(feature = "delta")]
#[test]
fn table_get_prints_the_record_as_a_table() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let address = routes_owned(vec![("/tables/", 200, unity_record(dir.path()))]);
    let output = pipe_env(
        &[
            "get",
            "table",
            "dbx_samples.nyctaxi.trips",
            "--format",
            "table",
        ],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("catalogs/dbx_samples/schemas/nyctaxi/tables/trips"),
        "{stdout}"
    );
    assert!(stdout.contains("delta"), "{stdout}");
    assert!(stdout.contains("tables: 1"), "{stdout}");
}

/// `get table` reads the Iceberg REST `loadTable` metadata inline, so no
/// storage read runs.
#[test]
fn table_get_reads_an_iceberg_rest_table() {
    let address = routes_owned(vec![(
        "/namespaces/nyctaxi/tables/trips",
        200,
        loaded_table("s3://bucket/events"),
    )]);
    let output = pipe_env(
        &[
            "get",
            "table",
            "dbx_samples.nyctaxi.trips",
            "--format",
            "json",
        ],
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
    let records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(
        record["name"],
        "catalogs/dbx_samples/schemas/nyctaxi/tables/trips"
    );
    assert_eq!(record["format"], "TABLE_FORMAT_ICEBERG");
    assert_eq!(record["snapshot_version"], 3268038499157964613i64);
    assert_eq!(
        record["uri"],
        "s3://bucket/events/metadata/00001.metadata.json"
    );
    let columns: Vec<&str> = record["declared_columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| column["column"].as_str().unwrap())
        .collect();
    assert_eq!(columns, ["id", "label"]);
    assert_eq!(record["declared_columns"][1]["nullable"], false);
    assert_eq!(record["properties"]["owner"], "data");
    assert!(record.get("delta_properties").is_none(), "{record:?}");
}

/// An Iceberg namespace with dots is one path segment, escaped as `%1F`.
#[test]
fn table_get_reads_an_iceberg_namespace_with_dots() {
    let address = routes_owned(vec![(
        "/namespaces/ns%1Fone/tables/trips",
        200,
        loaded_table("s3://bucket/events"),
    )]);
    let output = pipe_env(
        &[
            "get",
            "table",
            "dbx_samples.ns.one.trips",
            "--format",
            "json",
        ],
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
    let records = ndjson_records(&output.stdout);
    assert_eq!(
        records[0]["name"],
        "catalogs/dbx_samples/schemas/ns.one/tables/trips"
    );
}

/// `list schemas | get table` chains: the refs on stdin name each table.
#[cfg(feature = "delta")]
#[test]
fn table_get_reads_a_table_ref_stream() {
    let dir = tempfile::tempdir().unwrap();
    delta_table(dir.path());
    let record = unity_record(dir.path());
    let address = routes_owned(vec![
        ("/tables/dbx_samples.nyctaxi.zones", 200, record.clone()),
        ("/tables/dbx_samples.nyctaxi.trips", 200, record),
    ]);
    let refs = concat!(
        r#"{"name":"catalogs/dbx_samples/schemas/nyctaxi/tables/trips"}"#,
        "\n",
        r#"{"name":"catalogs/dbx_samples/schemas/nyctaxi/tables/zones"}"#,
        "\n",
    );
    let output = pipe_env(
        &["get", "table", "--format", "json"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    let mut names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "catalogs/dbx_samples/schemas/nyctaxi/tables/trips",
            "catalogs/dbx_samples/schemas/nyctaxi/tables/zones"
        ]
    );
}

#[test]
fn table_get_rejects_a_record_without_a_table_name() {
    let refs = r#"{"kind":"pqbench.table-ref","version":1,"id":"dbx_samples.nyctaxi.trips"}"#;
    let output = pipe_env(
        &["get", "table"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("table resources"), "{stderr}");
}

#[test]
fn table_get_takes_a_name_or_a_stream_not_both() {
    let refs = r#"{"kind":"pqbench.table-ref","version":1,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips"}"#;
    let output = pipe_env(
        &["get", "table", "dbx_samples.nyctaxi.trips"],
        refs.as_bytes(),
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both"), "{stderr}");
}

#[test]
fn table_get_needs_a_dotted_name() {
    let output = pipe_env(
        &["get", "table", "trips"],
        b"",
        &[("PQB_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CATALOG.SCHEMA.TABLE"), "{stderr}");
}

#[test]
fn table_get_needs_an_endpoint() {
    let output = pipe(&["get", "table", "dbx_samples.nyctaxi.trips"], "");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PQB_ENDPOINT"), "{stderr}");
}

#[test]
fn table_get_reports_an_unknown_table() {
    let address = routes_owned(vec![]);
    let output = pipe_env(
        &["get", "table", "dbx_samples.nyctaxi.nope"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

#[test]
fn table_ls_prints_the_tables_as_a_table() {
    let address = routes(&[("/tables", 200, TABLES)]);
    let output = pipe_env(
        &["list", "tables", "dbx_samples.nyctaxi", "--format", "table"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("catalogs/dbx_samples/schemas/nyctaxi/tables/trips"),
        "{stdout}"
    );
    assert!(stdout.contains("s3://bucket/trips"), "{stdout}");
    assert!(
        stdout.contains("catalogs/dbx_samples/schemas/nyctaxi/tables/recent"),
        "{stdout}"
    );
    assert!(stdout.contains("tables: 2"), "{stdout}");
}

#[test]
fn table_ls_streams_table_refs() {
    let address = routes(&[("/tables", 200, TABLES)]);
    let output = pipe_env(
        &["list", "tables", "dbx_samples.nyctaxi", "--format", "json"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2);
    // Sorted by name: the view (`recent`) precedes the table (`trips`).
    assert_eq!(
        records[0]["name"],
        "catalogs/dbx_samples/schemas/nyctaxi/tables/recent"
    );
    assert!(records[0].get("uri").is_none(), "{}", records[0]);
    assert_eq!(
        records[1]["name"],
        "catalogs/dbx_samples/schemas/nyctaxi/tables/trips"
    );
    assert_eq!(records[1]["uri"], "s3://bucket/trips");
}

#[test]
fn table_ls_lists_iceberg_rest_tables() {
    let address = routes(&[("/namespaces/nyctaxi/tables", 200, ICEBERG_TABLES)]);
    let output = pipe_env(
        &["list", "tables", "dbx_samples.nyctaxi", "--format", "json"],
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
    assert_eq!(
        records[0]["name"],
        "catalogs/dbx_samples/schemas/nyctaxi/tables/trips"
    );
    assert!(records[0].get("uri").is_none(), "{}", records[0]);
}

#[test]
fn table_ls_follows_unity_page_tokens() {
    let address = routes(&[
        ("page_token=", 200, TABLES_LAST),
        ("/tables", 200, TABLES_PAGE),
    ]);
    let output = pipe_env(
        &["list", "tables", "dbx_samples.nyctaxi", "--format", "json"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
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
            "catalogs/dbx_samples/schemas/nyctaxi/tables/trips",
            "catalogs/dbx_samples/schemas/nyctaxi/tables/zones"
        ]
    );
}

#[test]
fn table_ls_reads_a_schema_stream() {
    let address = routes(&[("/tables", 200, TABLES)]);
    let output = pipe_env(
        &["list", "tables", "--format", "json"],
        SCHEMA_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 4);
    assert!(records[0].get("kind").is_none(), "{}", records[0]);
}

#[test]
fn table_ls_multiplexes_its_refs() {
    let (address, peak) = overlapping_routes(
        &[("/tables", 200, TABLES)],
        std::time::Duration::from_millis(50),
    );
    let output = pipe_env(
        &["list", "tables", "--format", "json"],
        SCHEMA_REFS.as_bytes(),
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(ndjson(&output.stdout).len(), 4);
    assert!(
        peak.load(std::sync::atomic::Ordering::SeqCst) >= 2,
        "the two refs' requests did not overlap"
    );
}

#[test]
fn table_ls_reports_an_unknown_schema() {
    let address = server(404, r#"{"message":"Schema 'nope' does not exist."}"#);
    let output = pipe_env(
        &["list", "tables", "dbx_samples.nope"],
        b"",
        &[("PQB_ENDPOINT", address.as_str())],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("404"), "{stderr}");
}

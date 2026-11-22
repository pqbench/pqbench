#![cfg(feature = "delta")]

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::{json, Value};

fn list_partitions(stdin: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(["list", "partitions"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn table_record(name: &str, root: &Path) -> String {
    format!(
        "{}\n",
        json!({
            "name": format!("catalogs/local/schemas/local/tables/{name}"),
            "uri": root.to_str().unwrap(),
            "partition_columns": ["day"],
        })
    )
}

fn records(stdout: &[u8]) -> Vec<Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

/// A table partitioned by `day`; the files are named, never read.
fn header() -> Vec<Value> {
    vec![
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "22222222-2222-2222-2222-222222222222",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": ["day"],
            "configuration": {},
            "createdTime": 0
        }}),
    ]
}

fn add(path: &str, day: Option<&str>, size: u64, time: i64, rows: u64) -> Value {
    json!({"add": {
        "path": path,
        "partitionValues": {"day": day},
        "size": size,
        "modificationTime": time,
        "dataChange": true,
        "stats": format!("{{\"numRecords\":{rows}}}")
    }})
}

fn remove(path: &str, day: &str, size: u64) -> Value {
    json!({"remove": {
        "path": path,
        "partitionValues": {"day": day},
        "size": size,
        "deletionTimestamp": 1_700_000_100_000i64,
        "dataChange": true
    }})
}

fn commit(root: &Path, version: u64, mut actions: Vec<Value>) {
    actions.push(json!({"commitInfo": {"timestamp": 1_700_000_000_000i64, "operation": "WRITE"}}));
    let text = actions
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(root.join(format!("_delta_log/{version:020}.json")), text).unwrap();
}

/// Three partitions, one of them the null partition.
fn fixture() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("_delta_log")).unwrap();
    let mut actions = header();
    actions.extend([
        add("day=1/a.parquet", Some("1"), 100, 2_000, 10),
        add("day=1/b.parquet", Some("1"), 300, 1_000, 30),
        add("day=1/c.parquet", Some("1"), 200, 3_000, 20),
        add("day=2/d.parquet", Some("2"), 50, 4_000, 5),
        add(
            "day=__HIVE_DEFAULT_PARTITION__/e.parquet",
            None,
            7,
            5_000,
            1,
        ),
    ]);
    commit(directory.path(), 0, actions);
    directory
}

fn partitions_keyed(records: &[Value]) -> std::collections::BTreeMap<String, Value> {
    records
        .iter()
        .map(|record| {
            let name = record["name"].as_str().unwrap();
            let id = name.rsplit_once("/partitions/").unwrap().1.to_owned();
            (id, record.clone())
        })
        .collect()
}

/// One record per partition, sorted by name, with the partition's totals and
/// the oldest, newest and sample file.
#[test]
fn list_partitions_folds_the_files_of_each_partition() {
    let table = fixture();
    let output = list_partitions(&table_record("events", table.path()));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = records(&output.stdout);
    let names: Vec<&str> = records
        .iter()
        .map(|record| record["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "catalogs/local/schemas/local/tables/events/partitions/day%3D1",
            "catalogs/local/schemas/local/tables/events/partitions/day%3D2",
            "catalogs/local/schemas/local/tables/events/partitions/day%3D__HIVE_DEFAULT_PARTITION__",
        ]
    );
    let day = &records[0];
    assert_eq!(day["partition_values"], json!({"day": "1"}));
    assert_eq!(day["file_count"], 3);
    assert_eq!(day["size_bytes"], 600);
    assert_eq!(day["record_count"], 60);
    assert_eq!(day["smallest_file_size_bytes"], 100);
    assert_eq!(day["largest_file_size_bytes"], 300);
    assert_eq!(day["first_file"]["path"], "day=1/b.parquet");
    assert_eq!(day["first_file"]["modification_time"], 1_000);
    assert_eq!(day["last_file"]["path"], "day=1/c.parquet");
    assert_eq!(
        day["last_file"]["name"],
        "catalogs/local/schemas/local/tables/events/files/day%3D1%2Fc.parquet"
    );
    assert!(day["last_file"].get("partition_values").is_none());
    assert!(day["last_file"].get("stats").is_none());
    let sample = day["sample_file"]["path"].as_str().unwrap();
    assert!(sample.starts_with("day=1/"), "{sample}");
    assert_eq!(records[2]["partition_values"], json!({"day": null}));
    assert_eq!(day["removed_file_count"], 0);
    assert_eq!(day["removed_size_bytes"], 0);
}

/// A removed file counts until a vacuum, in its partition; a path removed and
/// added again (a restore, or a deletion-vector update) is live, not removed; a partition
/// whose files were all removed is still listed, with no files.
#[test]
fn list_partitions_counts_the_removed_files_no_active_file_holds() {
    let table = fixture();
    commit(
        table.path(),
        1,
        vec![
            remove("day=1/b.parquet", "1", 300),
            remove("day=1/a.parquet", "1", 100),
            remove("day=2/d.parquet", "2", 50),
        ],
    );
    commit(
        table.path(),
        2,
        vec![add("day=1/a.parquet", Some("1"), 100, 6_000, 10)],
    );
    let output = list_partitions(&table_record("events", table.path()));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let partitions = partitions_keyed(&records(&output.stdout));
    assert_eq!(partitions["day%3D1"]["file_count"], 2);
    assert_eq!(partitions["day%3D1"]["removed_file_count"], 1);
    assert_eq!(partitions["day%3D1"]["removed_size_bytes"], 300);
    assert_eq!(partitions["day%3D2"]["file_count"], 0);
    assert_eq!(partitions["day%3D2"]["size_bytes"], 0);
    assert_eq!(partitions["day%3D2"]["removed_file_count"], 1);
    assert_eq!(partitions["day%3D2"]["removed_size_bytes"], 50);
    assert!(partitions["day%3D2"].get("sample_file").is_none());
}

/// The etag is the same on a rerun and changes only for a partition a commit
/// touched; a remove alone changes it.
#[test]
fn list_partitions_etag_changes_only_with_the_partitions_files() {
    let table = fixture();
    let stdin = table_record("events", table.path());
    let first = partitions_keyed(&records(&list_partitions(&stdin).stdout));
    let again = partitions_keyed(&records(&list_partitions(&stdin).stdout));
    assert_eq!(first, again);
    commit(table.path(), 1, vec![remove("day=1/b.parquet", "1", 300)]);
    let after = partitions_keyed(&records(&list_partitions(&stdin).stdout));
    assert_ne!(after["day%3D1"]["etag"], first["day%3D1"]["etag"]);
    assert_eq!(after["day%3D1"]["file_count"], 2);
    assert_eq!(after["day%3D1"]["first_file"]["path"], "day=1/a.parquet");
    assert_eq!(after["day%3D2"], first["day%3D2"]);
}

/// A table that cannot be read is reported and writes nothing; the others on
/// the stream still do.
#[test]
fn list_partitions_keeps_the_tables_that_load() {
    let table = fixture();
    let missing = table.path().join("missing");
    let stdin = format!(
        "{}{}",
        table_record("gone", &missing),
        table_record("events", table.path())
    );
    let output = list_partitions(&stdin);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("tables/gone"), "stderr: {stderr}");
    let records = records(&output.stdout);
    assert_eq!(records.len(), 3);
    assert!(records
        .iter()
        .all(|record| record["name"].as_str().unwrap().contains("tables/events/")));
}

/// Names follow the table's partition column order, so a record that does not
/// name the columns (a `list tables` ref) is refused rather than named another
/// way.
#[test]
fn list_partitions_needs_the_partition_columns() {
    let table = fixture();
    let stdin = format!(
        "{}\n",
        json!({
            "name": "catalogs/local/schemas/local/tables/events",
            "uri": table.path().to_str().unwrap(),
        })
    );
    let output = list_partitions(&stdin);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("get table"), "stderr: {stderr}");
    assert!(output.stdout.is_empty());
}

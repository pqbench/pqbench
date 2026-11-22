#![cfg(feature = "delta")]

//! The daemon's master through its jobs: `job scan` lists tables into it,
//! `job list` reads its rows, `job purge` drops what the scan no longer finds.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};

use serde_json::{json, Value};

/// A daemon serving the master over `store`, on a private socket.
struct Master {
    child: Child,
    socket: PathBuf,
    store: PathBuf,
}

impl Master {
    fn start(directory: &Path) -> Self {
        let socket = directory.join("d.sock");
        let store = directory.join("store");
        // A killed daemon leaves its socket behind.
        let _ = std::fs::remove_file(&socket);
        let mut child = pqbench()
            .args(["daemon", "serve", "--master", "--store"])
            .arg(&store)
            .arg("--socket")
            .arg(&socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        for _ in 0..500 {
            if socket.exists() || child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(socket.exists(), "the master did not start");
        Master {
            child,
            socket,
            store,
        }
    }

    fn run(&self, args: &[&str], stdin: &str) -> Output {
        let mut child = pqbench()
            .args(args)
            .env("PQB_DAEMON_SOCK", &self.socket)
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
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{args:?} stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn scan(&self, tables: &str, complete: bool) {
        let mut args = vec!["job", "scan"];
        if complete {
            args.push("--complete");
        }
        self.run(&args, tables);
    }

    /// The rows, by partition id and etag.
    fn rows(&self) -> Vec<Value> {
        let output = self.run(&["job", "list"], "");
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    }

    fn stop(mut self) {
        let _ = pqbench()
            .args(["daemon", "stop"])
            .env("PQB_DAEMON_SOCK", &self.socket)
            .status();
        let _ = self.child.wait();
    }
}

impl Drop for Master {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pqbench() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pqbench"));
    command
        .env("PQB_CONFIG_FILE", "/dev/null")
        .env("PQB_ENDPOINT", "http://127.0.0.1:1")
        .env_remove("PQB_PROFILE");
    command
}

fn commit(root: &Path, version: u64, actions: Vec<Value>) {
    let mut lines: Vec<String> = actions.iter().map(Value::to_string).collect();
    lines.push(json!({"commitInfo": {"timestamp": 1_700_000_000_000i64}}).to_string());
    std::fs::create_dir_all(root.join("_delta_log")).unwrap();
    std::fs::write(
        root.join(format!("_delta_log/{version:020}.json")),
        lines.join("\n") + "\n",
    )
    .unwrap();
}

fn add(path: &str, day: &str, size: u64) -> Value {
    json!({"add": {
        "path": path,
        "partitionValues": {"day": day},
        "size": size,
        "modificationTime": 1_700_000_000_000i64,
        "dataChange": true
    }})
}

/// A table partitioned by `day` with two partitions.
fn table(root: &Path) {
    commit(
        root,
        0,
        vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({"metaData": {
                "id": "33333333-3333-3333-3333-333333333333",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": ["day"],
                "configuration": {},
                "createdTime": 0
            }}),
            add("day=1/a.parquet", "1", 100),
            add("day=2/b.parquet", "2", 200),
        ],
    );
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

fn id(row: &Value) -> String {
    let name = row["name"].as_str().unwrap();
    name.rsplit_once("/partitions/").unwrap().1.to_string()
}

fn result_file(master: &Master, row: &Value, output: &str) -> PathBuf {
    master
        .store
        .join("results")
        .join(row["name"].as_str().unwrap())
        .join(row["etag"].as_str().unwrap())
        .join(format!("{output}.ndjson"))
}

/// Each partition becomes one PENDING version with its listing stored; a
/// second scan of the unchanged table moves nothing.
#[test]
fn a_scan_adds_each_partition_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    let master = Master::start(directory.path());
    let tables = table_record("events", &root);
    master.scan(&tables, false);
    let first = master.rows();
    assert_eq!(first.len(), 2);
    assert!(
        first.iter().all(|row| row["state"] == "PENDING"),
        "{first:?}"
    );
    let listing =
        std::fs::read_to_string(result_file(&master, &first[0], "list-partitions")).unwrap();
    let record: Value = serde_json::from_str(listing.trim()).unwrap();
    assert_eq!(record["etag"], first[0]["etag"]);
    master.scan(&tables, false);
    assert_eq!(master.rows(), first);
}

/// A changed partition is a new version: the old one is DELETED, the new one
/// PENDING with the partition's history; purge then drops the old version's
/// row and results.
#[test]
fn a_change_is_a_new_version_and_purge_drops_the_old() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    let master = Master::start(directory.path());
    let tables = table_record("events", &root);
    master.scan(&tables, false);
    let before = master.rows();
    commit(&root, 1, vec![add("day=1/c.parquet", "1", 300)]);
    master.scan(&tables, false);
    let rows = master.rows();
    let day1: Vec<&Value> = rows.iter().filter(|row| id(row) == "day%3D1").collect();
    assert_eq!(day1.len(), 2, "{rows:?}");
    let old = day1
        .iter()
        .find(|row| row["etag"] == before.iter().find(|row| id(row) == "day%3D1").unwrap()["etag"])
        .unwrap();
    let new = day1.iter().find(|row| row["etag"] != old["etag"]).unwrap();
    assert_eq!(old["state"], "DELETED");
    assert_eq!(new["state"], "PENDING");
    assert_eq!(new["etag_change_count"], 1);
    assert_eq!(new["first_listed_time"], old["first_listed_time"]);
    let old_listing = result_file(&master, old, "list-partitions");
    assert!(old_listing.exists());
    master.run(&["job", "purge"], "");
    let after = master.rows();
    assert_eq!(after.len(), 2);
    assert!(after.iter().all(|row| row["state"] == "PENDING"));
    assert!(!old_listing.exists());
}

/// A complete walk that no longer names a table deletes its versions; a walk
/// that is not complete leaves them.
#[test]
fn a_complete_walk_deletes_the_tables_it_lacks() {
    let directory = tempfile::tempdir().unwrap();
    let events = directory.path().join("events");
    let orders = directory.path().join("orders");
    table(&events);
    table(&orders);
    let master = Master::start(directory.path());
    master.scan(
        &(table_record("events", &events) + &table_record("orders", &orders)),
        true,
    );
    master.scan(&table_record("events", &events), false);
    assert!(master.rows().iter().all(|row| row["state"] == "PENDING"));
    master.scan(&table_record("events", &events), true);
    let rows = master.rows();
    let deleted: Vec<&Value> = rows
        .iter()
        .filter(|row| row["state"] == "DELETED")
        .collect();
    assert_eq!(deleted.len(), 2);
    assert!(deleted
        .iter()
        .all(|row| row["name"].as_str().unwrap().contains("/tables/orders/")));
}

/// The rows outlive the daemon: a restart over the same store has them all.
#[test]
fn the_rows_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    let master = Master::start(directory.path());
    master.scan(&table_record("events", &root), false);
    let rows = master.rows();
    master.stop();
    let master = Master::start(directory.path());
    assert_eq!(master.rows(), rows);
}

/// A daemon killed mid-life loses nothing it answered: the restart replays the
/// edit log over the image.
#[test]
fn the_rows_survive_a_kill() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    let master = Master::start(directory.path());
    master.scan(&table_record("events", &root), false);
    commit(&root, 1, vec![add("day=1/c.parquet", "1", 300)]);
    master.scan(&table_record("events", &root), false);
    let rows = master.rows();
    drop(master);
    let master = Master::start(directory.path());
    assert_eq!(master.rows(), rows);
}

/// A table whose files are real Parquet (one fixture file per partition), so
/// the measuring steps have footers to read.
fn readable_table(root: &Path, partitions: &[&str]) -> Vec<Value> {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    );
    let size = std::fs::metadata(fixture).unwrap().len();
    let mut actions = vec![
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "44444444-4444-4444-4444-444444444444",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": ["day"],
            "configuration": {},
            "createdTime": 0
        }}),
    ];
    for day in partitions {
        let path = format!("day={day}/part-0.parquet");
        std::fs::create_dir_all(root.join(format!("day={day}"))).unwrap();
        std::fs::copy(fixture, root.join(&path)).unwrap();
        actions.push(add(&path, day, size));
    }
    actions
}

fn run_steps(master: &Master) {
    for job in ["price", "bytemass", "lint"] {
        master.run(&["job", job], "");
    }
}

/// Price, bytemass and lint take each version to UP_TO_DATE, each command's
/// output stored under the version.
#[test]
fn the_steps_take_each_version_up_to_date() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    let master = Master::start(directory.path());
    master.scan(&table_record("events", &root), false);
    run_steps(&master);
    let rows = master.rows();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row["state"], "UP_TO_DATE", "{row}");
        assert!(row["up_to_date_time"].is_u64());
        for output in ["price", "bytemass", "list-suggestions"] {
            assert!(
                result_file(&master, row, output).exists(),
                "{output} for {row}"
            );
        }
        let bytemass = std::fs::read_to_string(result_file(&master, row, "bytemass")).unwrap();
        assert!(!bytemass.trim().is_empty());
    }
}

/// While a changed partition's new version is processed, purge keeps the old
/// version's complete results; once the new one is UP_TO_DATE, the old goes.
#[test]
fn purge_keeps_the_last_complete_version_until_it_is_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    let master = Master::start(directory.path());
    let tables = table_record("events", &root);
    master.scan(&tables, false);
    run_steps(&master);
    let old = master.rows().remove(0);
    let fixture = root.join("day=1/part-0.parquet");
    std::fs::copy(&fixture, root.join("day=1/part-1.parquet")).unwrap();
    let size = std::fs::metadata(&fixture).unwrap().len();
    commit(&root, 1, vec![add("day=1/part-1.parquet", "1", size)]);
    master.scan(&tables, false);
    master.run(&["job", "purge"], "");
    let rows = master.rows();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(result_file(&master, &old, "bytemass").exists());
    run_steps(&master);
    master.run(&["job", "purge"], "");
    let rows = master.rows();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_ne!(rows[0]["etag"], old["etag"]);
    assert_eq!(rows[0]["state"], "UP_TO_DATE");
    assert!(!result_file(&master, &old, "bytemass").exists());
}

/// A version whose command fails goes back with its error counted, and after
/// two failures it is left out of claims, so the job ends.
#[test]
fn a_failing_version_is_left_after_two_failures() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    std::fs::remove_file(root.join("day=1/part-0.parquet")).unwrap();
    let master = Master::start(directory.path());
    master.scan(&table_record("events", &root), false);
    master.run(&["job", "price"], "");
    let output = pqbench()
        .args(["job", "bytemass"])
        .env("PQB_DAEMON_SOCK", &master.socket)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let row = master.rows().remove(0);
    assert_eq!(row["state"], "PRICED");
    assert_eq!(row["failure_count"], 2);
    assert!(row["error"].as_str().is_some_and(|error| !error.is_empty()));
}

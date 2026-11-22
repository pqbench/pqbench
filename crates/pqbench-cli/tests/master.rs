#![cfg(feature = "delta")]

//! The daemon's master: it walks the tables it is given, runs the transitions on
//! each partition version, purges what a walk no longer finds, and serves
//! the versions to `list partition-versions` and one version's results to
//! `get partition-version`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// A daemon serving the master over `store`, on a private socket.
struct Master {
    child: Child,
    socket: PathBuf,
    store: PathBuf,
}

impl Master {
    /// Start the master on `directory`, walking the Table records of
    /// `{directory}/tables.ndjson` when there are some.
    fn start(directory: &Path, flags: &[&str]) -> Self {
        let socket = directory.join("d.sock");
        let store = directory.join("store");
        let tables = directory.join("tables.ndjson");
        // A killed daemon leaves its socket behind.
        let _ = std::fs::remove_file(&socket);
        let mut command = pqbench();
        command
            .args(["daemon", "serve", "--master", "--store"])
            .arg(&store)
            .arg("--socket")
            .arg(&socket);
        if tables.exists() {
            command.arg("--walk-tables").arg(&tables);
        }
        let mut child = command
            .args(flags)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        for _ in 0..500 {
            if socket.exists() || child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
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

    /// The partition versions, in name order.
    fn rows(&self) -> Vec<Value> {
        self.versions(&[])
    }

    /// `list partition-versions ARGS`.
    fn versions(&self, args: &[&str]) -> Vec<Value> {
        let output = self.run(&[&["list", "partition-versions"], args].concat(), "");
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    }

    /// The rows, once `done` holds for them; fails after a while.
    fn wait(&self, done: impl Fn(&[Value]) -> bool) -> Vec<Value> {
        let start = Instant::now();
        loop {
            let rows = self.rows();
            if done(&rows) {
                return rows;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "the master did not get there: {rows:#?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
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
    // Whole or not at all, as a writer commits: a walk may read the log
    // at any moment.
    let temporary = root.join(format!("_delta_log/.{version:020}.json.tmp"));
    std::fs::write(&temporary, lines.join("\n") + "\n").unwrap();
    std::fs::rename(
        &temporary,
        root.join(format!("_delta_log/{version:020}.json")),
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

/// A table partitioned by `day` with two partitions, whose files are not
/// there: enough for a walk, not for a read.
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

/// A table whose files are real Parquet (one fixture file per partition), so
/// the transitions have data to read.
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

/// Write the Table records the master walks.
fn walk(directory: &Path, tables: &[(&str, &Path)]) {
    let lines: String = tables
        .iter()
        .map(|(name, root)| {
            format!(
                "{}\n",
                json!({
                    "name": format!("catalogs/local/schemas/local/tables/{name}"),
                    "uri": root.to_str().unwrap(),
                    "partition_columns": ["day"],
                })
            )
        })
        .collect();
    std::fs::write(directory.join("tables.ndjson"), lines).unwrap();
}

/// A version's partition id.
fn id(row: &Value) -> String {
    let partition = partition(row);
    partition.rsplit_once("/partitions/").unwrap().1.to_string()
}

/// A version's partition name.
fn partition(row: &Value) -> &str {
    let name = row["name"].as_str().unwrap();
    name.rsplit_once("/versions/").unwrap().0
}

/// A version's etag.
fn etag(row: &Value) -> &str {
    let name = row["name"].as_str().unwrap();
    name.rsplit_once("/versions/").unwrap().1
}

fn result_file(master: &Master, row: &Value, output: &str) -> PathBuf {
    master
        .store
        .join("results")
        .join(partition(row))
        .join(etag(row))
        .join(output)
}

/// A walk makes each partition one version with its listing stored; later
/// walks find the same versions and make no new ones.
#[test]
fn a_walk_adds_each_partition_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &["--walk-period-seconds", "0"]);
    let first = master.wait(|rows| rows.len() == 2);
    let listing =
        std::fs::read_to_string(result_file(&master, &first[0], "list-partitions.ndjson")).unwrap();
    let record: Value = serde_json::from_str(listing.trim()).unwrap();
    assert_eq!(record["etag"], etag(&first[0]));
    let later = master.wait(|rows| rows[0]["list_time"] != first[0]["list_time"]);
    let names =
        |rows: &[Value]| -> Vec<Value> { rows.iter().map(|row| row["name"].clone()).collect() };
    assert_eq!(names(&later), names(&first));
    assert_eq!(later[0]["create_time"], first[0]["create_time"]);
}

/// A changed partition is a new version with the partition's history; the
/// old one is deleted, and with no complete result to keep, purged.
#[test]
fn a_change_is_a_new_version_and_the_old_is_purged() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &["--walk-period-seconds", "0"]);
    let before = master.wait(|rows| rows.len() == 2);
    let old = before
        .iter()
        .find(|row| id(row) == "day%3D1")
        .unwrap()
        .clone();
    commit(&root, 1, vec![add("day=1/c.parquet", "1", 300)]);
    let rows = master.wait(|rows| {
        rows.len() == 2
            && rows
                .iter()
                .any(|row| id(row) == "day%3D1" && etag(row) != etag(&old))
    });
    let new = rows.iter().find(|row| id(row) == "day%3D1").unwrap();
    assert_eq!(new["etag_change_count"], 1);
    assert_eq!(new["first_listed_time"], old["first_listed_time"]);
    assert!(!result_file(&master, &old, "list-partitions.ndjson").exists());
}

/// A walk purges the tables it does not list; a table it lists but cannot
/// read keeps its versions, and a walk that cannot list the tables purges
/// nothing.
#[test]
fn a_walk_purges_the_tables_it_does_not_list() {
    let directory = tempfile::tempdir().unwrap();
    let events = directory.path().join("events");
    let orders = directory.path().join("orders");
    table(&events);
    table(&orders);
    walk(
        directory.path(),
        &[("events", &events), ("orders", &orders)],
    );
    let master = Master::start(directory.path(), &["--walk-period-seconds", "0"]);
    master.wait(|rows| rows.len() == 4);

    // `events` is listed but its log is gone: its versions stay.
    std::fs::remove_dir_all(events.join("_delta_log")).unwrap();
    let rewritten = pqbench_time();
    master.wait(|rows| {
        rows.iter()
            .any(|row| row["list_time"].as_u64().unwrap_or(0) > rewritten + 200)
    });
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        master.rows().len(),
        4,
        "an unreadable table lost its versions"
    );

    // `events` is no longer listed: its versions go, `orders` stays.
    walk(directory.path(), &[("orders", &orders)]);
    let rows = master.wait(|rows| rows.len() == 2);
    assert!(rows
        .iter()
        .all(|row| row["name"].as_str().unwrap().contains("/tables/orders/")));

    // No table listing at all: nothing is purged.
    std::fs::remove_file(directory.path().join("tables.ndjson")).unwrap();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(master.rows().len(), 2);
}

/// Milliseconds since the Unix epoch.
fn pqbench_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// `list partition-versions` takes a parent at any level, and refuses a
/// name that is not one.
#[test]
fn list_partition_versions_reads_under_a_parent() {
    let directory = tempfile::tempdir().unwrap();
    let events = directory.path().join("events");
    let orders = directory.path().join("orders");
    table(&events);
    table(&orders);
    walk(
        directory.path(),
        &[("events", &events), ("orders", &orders)],
    );
    let master = Master::start(directory.path(), &[]);
    master.wait(|rows| rows.len() == 4);
    let names = |args: &[&str]| -> Vec<String> {
        master
            .versions(args)
            .iter()
            .map(|row| partition(row).to_string())
            .collect()
    };
    let all = names(&[]);
    assert_eq!(names(&["catalogs/local"]), all);
    let orders = names(&["catalogs/local/schemas/local/tables/orders"]);
    assert_eq!(orders.len(), 2);
    assert!(orders.iter().all(|name| name.contains("/tables/orders/")));
    assert_eq!(names(&[&orders[0]]), [orders[0].clone()]);
    assert_eq!(
        names(&["catalogs/local/schemas/local/tables/events/partitions/day%3D1"]).len(),
        1
    );
    assert!(names(&["catalogs/other"]).is_empty());
    let refused = pqbench()
        .args(["list", "partition-versions", "catalogs/local/tables/orders"])
        .env("PQB_DAEMON_SOCK", &master.socket)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("is not a catalog"));
}

/// The rows outlive the daemon: a restart over the same store has them all.
#[test]
fn the_rows_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &[]);
    let rows =
        master.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));
    master.stop();
    std::fs::remove_file(directory.path().join("tables.ndjson")).unwrap();
    let master = Master::start(directory.path(), &[]);
    assert_eq!(master.rows(), rows);
}

/// The transitions take each version to UP_TO_DATE: a price, the sample file's
/// columns and suggestions, the records naming the table's file and not the
/// copy they were read from, which is gone once the version is done.
#[test]
fn the_transitions_take_each_version_up_to_date() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &[]);
    let rows =
        master.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));
    for row in &rows {
        assert!(row["up_to_date_time"].is_u64());
        assert!(row.get("process_id").is_none(), "{row}");
        for output in ["price.ndjson", "bytemass.ndjson", "list-suggestions.ndjson"] {
            assert!(
                result_file(&master, row, output).exists(),
                "{output} for {row}"
            );
        }
        assert!(!result_file(&master, row, "sample.parquet").exists());
        let bytemass =
            std::fs::read_to_string(result_file(&master, row, "bytemass.ndjson")).unwrap();
        let blob: Value = serde_json::from_str(bytemass.lines().next().unwrap()).unwrap();
        assert!(
            blob["name"].as_str().unwrap().ends_with("part-0.parquet"),
            "{blob}"
        );
    }
    let leftovers: Vec<PathBuf> = walk_files(&master.store.join("results"))
        .into_iter()
        .filter(|path| {
            path.extension().is_some_and(|extension| {
                extension == "tmp" || extension == "err" || extension == "in"
            })
        })
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

fn walk_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk_files(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// On a low network no file is copied and nothing reads its data: bytemass
/// reads the sample file's footer in place, and the version is done.
#[test]
fn a_low_network_reads_only_the_footer() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &["--low-network"]);
    let rows = master.wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE");
    assert!(result_file(&master, &rows[0], "bytemass.ndjson").exists());
    assert!(!result_file(&master, &rows[0], "list-suggestions.ndjson").exists());
    assert!(!result_file(&master, &rows[0], "sample.parquet").exists());
}

fn remove(path: &str, day: &str, size: u64) -> Value {
    json!({"remove": {
        "path": path,
        "partitionValues": {"day": day},
        "size": size,
        "deletionTimestamp": 1_700_000_000_000i64,
        "dataChange": true
    }})
}

/// While a changed partition's new version is in progress, the old version's
/// complete results are kept; once a newer version is UP_TO_DATE, the old
/// goes.
#[test]
fn purge_keeps_the_last_complete_version_until_it_is_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &["--walk-period-seconds", "0"]);
    let old = master
        .wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE")
        .remove(0);

    // A new version whose only file is missing: it cannot be done.
    let fixture = root.join("day=1/part-0.parquet");
    let size = std::fs::metadata(&fixture).unwrap().len();
    commit(
        &root,
        1,
        vec![
            remove("day=1/part-0.parquet", "1", size),
            add("day=1/part-1.parquet", "1", size),
        ],
    );
    let rows = master.wait(|rows| rows.iter().any(|row| row["failure_count"] == 2));
    assert_eq!(rows.len(), 2);
    let kept = rows.iter().find(|row| etag(row) == etag(&old)).unwrap();
    assert_eq!(kept["state"], "DELETED");
    assert!(result_file(&master, &old, "bytemass.ndjson").exists());

    std::fs::copy(&fixture, root.join("day=1/part-2.parquet")).unwrap();
    commit(
        &root,
        2,
        vec![
            remove("day=1/part-1.parquet", "1", size),
            add("day=1/part-2.parquet", "1", size),
        ],
    );
    let rows = master.wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE");
    assert_ne!(etag(&rows[0]), etag(&old));
    assert!(!result_file(&master, &old, "bytemass.ndjson").exists());
}

/// A version whose command fails goes back with its error counted, and after
/// two failures it stays where it is.
#[test]
fn a_failing_version_is_left_after_two_failures() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    std::fs::remove_file(root.join("day=1/part-0.parquet")).unwrap();
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &[]);
    let rows = master.wait(|rows| rows.len() == 1 && rows[0]["failure_count"] == 2);
    assert_eq!(rows[0]["state"], "PRICED");
    assert!(rows[0]["error"]
        .as_str()
        .is_some_and(|error| !error.is_empty()));
}

/// A daemon whose executable is replaced on disk keeps running its own: its
/// commands start from the image it runs, so the versions move on.
#[test]
fn commands_go_on_after_the_executable_is_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    // A second name for the binary, on its filesystem, to delete under the
    // running daemon.
    let binaries = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let binary = binaries.path().join("pqbench");
    std::fs::hard_link(env!("CARGO_BIN_EXE_pqbench"), &binary).unwrap();
    let socket = directory.path().join("d.sock");
    let daemon = Command::new(&binary)
        .env("PQB_CONFIG_FILE", "/dev/null")
        .env("PQB_ENDPOINT", "http://127.0.0.1:1")
        .args(["daemon", "serve", "--master", "--store"])
        .arg(directory.path().join("store"))
        .arg("--socket")
        .arg(&socket)
        .arg("--walk-tables")
        .arg(directory.path().join("tables.ndjson"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    while !socket.exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
    std::fs::remove_file(&binary).unwrap();
    let master = Master {
        child: daemon,
        socket,
        store: directory.path().join("store"),
    };
    let rows =
        master.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));
    assert!(
        rows.iter().all(|row| row.get("error").is_none()),
        "{rows:#?}"
    );
}

/// One version's stored results are readable by name: its price, its column
/// masses and its listing, one record per line. A partition's name reads its
/// newest live version.
#[test]
fn one_version_reads_its_stored_results() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    let actions = readable_table(&root, &["1", "2"]);
    commit(&root, 0, actions);
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &[]);
    let rows =
        master.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));

    let name = rows[0]["name"].as_str().unwrap();
    let read = |name: &str| -> Vec<Value> {
        let output = master.run(&["get", "partition-version", name], "");
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    };
    let results = read(name);
    // A local table's files are not S3 objects, so its price names the etag
    // and prices nothing.
    assert_eq!(
        results[0]["name"].as_str().unwrap(),
        format!("{}/price", partition(&rows[0]))
    );
    assert!(results[0]["partition_etag"].is_string(), "{results:#?}");
    assert!(
        results.iter().any(|r| r.get("rule").is_some()),
        "{results:#?}"
    );
    assert!(
        results.iter().any(|r| r.get("mass").is_some()),
        "{results:#?}"
    );
    assert!(
        results.iter().any(|r| r.get("etag").is_some()),
        "{results:#?}"
    );

    // The partition's name reads the same version's results.
    assert_eq!(read(partition(&rows[0])), results);
}

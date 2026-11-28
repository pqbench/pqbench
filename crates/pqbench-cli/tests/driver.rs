#![cfg(feature = "delta")]

//! The driver's process: it walks the tables it is given, runs the
//! transitions on each partition version, purges what a walk no longer finds,
//! and serves the versions to `list partition-versions` and one version's
//! results to `get partition-version`. The commands read it over
//! `PQB_DRIVER_SOCK`; no catalog daemon runs here, so every table is local.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// The `pqbench-driver` binary beside the test's `pqbench`.
fn driver_binary() -> PathBuf {
    Path::new(env!("CARGO_BIN_EXE_pqbench")).with_file_name("pqbench-driver")
}

/// A driver serving `store` on a private socket.
struct Driver {
    child: Child,
    socket: PathBuf,
    store: PathBuf,
}

impl Driver {
    /// Start the driver on `directory`, walking the Table records of
    /// `{directory}/tables.ndjson` when there are some.
    fn start(directory: &Path, flags: &[&str]) -> Self {
        Driver::configured(directory, flags, Path::new("/dev/null"))
    }

    /// Start the driver reading `config` as its run-control file.
    fn configured(directory: &Path, flags: &[&str], config: &Path) -> Self {
        let socket = directory.join("d.sock");
        let store = directory.join("store");
        let tables = directory.join("tables.ndjson");
        // A killed driver leaves its socket behind.
        let _ = std::fs::remove_file(&socket);
        let mut command = driver();
        command.env("PQB_CONFIG_FILE", config);
        command
            .args(["serve", "--store"])
            .arg(&store)
            .arg("--socket")
            .arg(&socket)
            .arg("--pqbench")
            .arg(env!("CARGO_BIN_EXE_pqbench"));
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
        await_socket(&socket, &mut child);
        Driver {
            child,
            socket,
            store,
        }
    }

    fn run(&self, args: &[&str], stdin: &str) -> Output {
        let mut child = pqbench()
            .args(args)
            .env("PQB_DRIVER_SOCK", &self.socket)
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
                "the driver did not get there: {rows:#?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn stop(mut self) {
        let _ = driver()
            .args(["stop", "--socket"])
            .arg(&self.socket)
            .status();
        let _ = self.child.wait();
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn await_socket(socket: &Path, child: &mut Child) {
    let start = Instant::now();
    while !socket.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("pqbench-driver exited before listening: {status}");
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "pqbench-driver did not start on {}",
            socket.display()
        );
        std::thread::sleep(Duration::from_millis(5));
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

fn driver() -> Command {
    let mut command = Command::new(driver_binary());
    command
        .env("PQB_CONFIG_FILE", "/dev/null")
        .env("PQB_ENDPOINT", "http://127.0.0.1:1")
        .env_remove("PQB_PROFILE")
        .env_remove("PQB_DAEMON_SOCK")
        .env_remove("PQB_DRIVER_SOCK");
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

/// Write the Table records the driver walks.
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

fn result_file(driver: &Driver, row: &Value, output: &str) -> PathBuf {
    driver
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
    let driver = Driver::start(directory.path(), &["--walk-period-seconds", "0"]);
    let first = driver.wait(|rows| rows.len() == 2);
    let listing =
        std::fs::read_to_string(result_file(&driver, &first[0], "list-partitions.ndjson")).unwrap();
    let record: Value = serde_json::from_str(listing.trim()).unwrap();
    assert_eq!(record["etag"], etag(&first[0]));
    let later = driver.wait(|rows| rows[0]["list_time"] != first[0]["list_time"]);
    let names =
        |rows: &[Value]| -> Vec<Value> { rows.iter().map(|row| row["name"].clone()).collect() };
    assert_eq!(names(&later), names(&first));
    assert_eq!(later[0]["create_time"], first[0]["create_time"]);
}

/// A changed partition is a new version with the partition's history; the
/// old one leaves the map and its results go.
#[test]
fn a_change_is_a_new_version_and_the_old_goes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    walk(directory.path(), &[("events", &root)]);
    let driver = Driver::start(directory.path(), &["--walk-period-seconds", "0"]);
    let before = driver.wait(|rows| rows.len() == 2);
    let old = before
        .iter()
        .find(|row| id(row) == "day%3D1")
        .unwrap()
        .clone();
    commit(&root, 1, vec![add("day=1/c.parquet", "1", 300)]);
    let rows = driver.wait(|rows| {
        rows.len() == 2
            && rows
                .iter()
                .any(|row| id(row) == "day%3D1" && etag(row) != etag(&old))
    });
    let new = rows.iter().find(|row| id(row) == "day%3D1").unwrap();
    assert_eq!(new["etag_change_count"], 1);
    assert_eq!(new["first_listed_time"], old["first_listed_time"]);
    assert!(!result_file(&driver, &old, "list-partitions.ndjson").exists());
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
    let driver = Driver::start(directory.path(), &["--walk-period-seconds", "0"]);
    driver.wait(|rows| rows.len() == 4);

    // `events` is listed but its log is gone: its versions stay.
    std::fs::remove_dir_all(events.join("_delta_log")).unwrap();
    let rewritten = pqbench_time();
    // The wait ends only after a walk that could not read the log finished, so
    // a wrong purge would already have happened; no extra sleep is needed.
    driver.wait(|rows| {
        rows.iter()
            .any(|row| row["list_time"].as_u64().unwrap_or(0) > rewritten + 200)
    });
    assert_eq!(
        driver.rows().len(),
        4,
        "an unreadable table lost its versions"
    );

    // `events` is no longer listed: its versions go, `orders` stays.
    walk(directory.path(), &[("orders", &orders)]);
    let rows = driver.wait(|rows| rows.len() == 2);
    assert!(rows
        .iter()
        .all(|row| row["name"].as_str().unwrap().contains("/tables/orders/")));

    // No table listing at all: a walk still runs and purges nothing.
    std::fs::remove_file(directory.path().join("tables.ndjson")).unwrap();
    let before = driver_status(&driver)["last_walk_time"]
        .as_u64()
        .unwrap_or(0);
    let start = Instant::now();
    loop {
        let status = driver_status(&driver);
        if status["last_walk_time"].as_u64().unwrap_or(0) > before {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "{status}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(driver.rows().len(), 2);
}

/// Milliseconds since the Unix epoch.
fn pqbench_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// The driver's `status`, as the JSON object it prints.
fn driver_status(running: &Driver) -> Value {
    let output = driver()
        .args(["status", "--socket"])
        .arg(&running.socket)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "pqbench-driver status: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("the status is one JSON object")
}

/// The driver's `list suggestions`, one record per line.
fn list_suggestions(running: &Driver, args: &[&str]) -> Vec<Value> {
    let output = driver()
        .env("PQB_DRIVER_SOCK", &running.socket)
        .args(["list", "suggestions"])
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "pqbench-driver list suggestions {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
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
    let driver = Driver::start(directory.path(), &[]);
    driver.wait(|rows| rows.len() == 4);
    let names = |args: &[&str]| -> Vec<String> {
        driver
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
        .env("PQB_DRIVER_SOCK", &driver.socket)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("is not a catalog"));
}

/// The rows outlive the driver: a restart over the same store has them all.
#[test]
fn the_rows_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    let driver = Driver::start(directory.path(), &[]);
    let rows =
        driver.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));
    driver.stop();
    std::fs::remove_file(directory.path().join("tables.ndjson")).unwrap();
    let driver = Driver::start(directory.path(), &[]);
    assert_eq!(driver.rows(), rows);
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
    let driver = Driver::start(directory.path(), &[]);
    let rows =
        driver.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));
    for row in &rows {
        for output in [
            "price.ndjson",
            "bytemass.ndjson",
            "list-suggestions.ndjson",
            "suggestions.ndjson",
        ] {
            assert!(
                result_file(&driver, row, output).exists(),
                "{output} for {row}"
            );
        }
        assert_eq!(row["state"], "UP_TO_DATE");
        assert!(!result_file(&driver, row, "sample.parquet").exists());
        let bytemass =
            std::fs::read_to_string(result_file(&driver, row, "bytemass.ndjson")).unwrap();
        let blob: Value = serde_json::from_str(bytemass.lines().next().unwrap()).unwrap();
        assert!(
            blob["name"].as_str().unwrap().ends_with("part-0.parquet"),
            "{blob}"
        );
    }
    let leftovers: Vec<PathBuf> = walk_files(&driver.store.join("results"))
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
    let driver = Driver::start(directory.path(), &["--low-network"]);
    let rows = driver.wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE");
    assert!(result_file(&driver, &rows[0], "bytemass.ndjson").exists());
    assert!(!result_file(&driver, &rows[0], "list-suggestions.ndjson").exists());
    assert!(!result_file(&driver, &rows[0], "sample.parquet").exists());
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

/// A partition's old version goes as soon as a new etag is found, even when
/// the new version cannot finish: the map never holds two versions of a
/// partition.
#[test]
fn an_etag_change_drops_the_old_version_at_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    walk(directory.path(), &[("events", &root)]);
    let driver = Driver::start(directory.path(), &["--walk-period-seconds", "0"]);
    let old = driver
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
    let rows = driver.wait(|rows| rows.len() == 1 && rows[0]["failure_count"] == 2);
    assert_ne!(etag(&rows[0]), etag(&old));
    assert!(!result_file(&driver, &old, "bytemass.ndjson").exists());
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
    let driver = Driver::start(directory.path(), &[]);
    let rows = driver.wait(|rows| rows.len() == 1 && rows[0]["failure_count"] == 2);
    assert_eq!(rows[0]["state"], "PRICED");
    assert!(rows[0]["error"]
        .as_str()
        .is_some_and(|error| !error.is_empty()));
}

/// A driver whose executable is replaced on disk keeps running its own: its
/// commands start from the image it runs, so the versions move on.
#[test]
fn commands_go_on_after_the_executable_is_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    // A second name for the pqbench binary, on its filesystem, to delete
    // under the running driver.
    let binaries = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let binary = binaries.path().join("pqbench");
    std::fs::hard_link(env!("CARGO_BIN_EXE_pqbench"), &binary).unwrap();
    let socket = directory.path().join("d.sock");
    let mut child = driver();
    child
        .args(["serve", "--store"])
        .arg(directory.path().join("store"))
        .arg("--socket")
        .arg(&socket)
        .arg("--pqbench")
        .arg(&binary)
        .arg("--walk-tables")
        .arg(directory.path().join("tables.ndjson"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let mut child = child.spawn().unwrap();
    await_socket(&socket, &mut child);
    std::fs::remove_file(&binary).unwrap();
    let driver = Driver {
        child,
        socket,
        store: directory.path().join("store"),
    };
    let rows =
        driver.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));
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
    let driver = Driver::start(directory.path(), &[]);
    let rows =
        driver.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));

    let name = rows[0]["name"].as_str().unwrap();
    let read = |name: &str| -> Vec<Value> {
        let output = driver.run(&["get", "partition-version", name], "");
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

/// The driver's two settings are the run-control file's, not flags: the store's
/// checkpoint lands under the configured codec's name, and a restart loads it.
#[test]
fn the_run_control_file_sets_the_checkpoint_codec() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    walk(directory.path(), &[("events", &root)]);
    let config = directory.path().join("run-control");
    std::fs::write(
        &config,
        "[DEFAULT]\ncheckpoint-codec = lz4\nstep-timeout = 60\n",
    )
    .unwrap();
    let driver = Driver::configured(directory.path(), &[], &config);
    // The table's files are not there, so both versions fail twice and settle:
    // a version caught mid-transition would go back a state on the restart.
    let rows =
        driver.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["failure_count"] == 2));
    driver.stop();

    let store = directory.path().join("store");
    let checkpoint = store.join("checkpoint.ndjson.lz4");
    assert!(
        checkpoint.is_file(),
        "no LZ4 checkpoint: {:?}",
        walk_files(&store)
    );
    let bytes = std::fs::read(&checkpoint).unwrap();
    assert!(
        bytes.starts_with(&[0x04, 0x22, 0x4d, 0x18]),
        "the checkpoint is not an LZ4 frame"
    );
    for other in ["checkpoint.ndjson", "checkpoint.ndjson.zst"] {
        assert!(!store.join(other).exists(), "{other} was written too");
    }

    // Nothing to walk, so the rows are the checkpoint's: the driver loaded it.
    std::fs::remove_file(directory.path().join("tables.ndjson")).unwrap();
    let driver = Driver::configured(directory.path(), &[], &config);
    assert_eq!(driver.rows(), rows);
}

/// A table whose one partition holds `files` copies of the fixture: enough
/// files, each small, for the `small-files` rule.
fn small_files_table(root: &Path, files: usize) -> Vec<Value> {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    );
    let size = std::fs::metadata(fixture).unwrap().len();
    let mut actions = vec![
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "55555555-5555-5555-5555-555555555555",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": ["day"],
            "configuration": {},
            "createdTime": 0
        }}),
    ];
    for at in 0..files {
        let path = format!("day=1/part-{at}.parquet");
        std::fs::create_dir_all(root.join("day=1")).unwrap();
        std::fs::copy(fixture, root.join(&path)).unwrap();
        actions.push(add(&path, "1", size));
    }
    actions
}

/// `status` reports the map as a whole: the versions by state, the failing
/// count and the last walk, without a flag to choose the output.
#[test]
fn status_reports_the_map() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    let driver = Driver::start(directory.path(), &[]);
    let rows =
        driver.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));

    let start = Instant::now();
    let status = loop {
        let status = driver_status(&driver);
        if status["running_commands"] == 0 {
            break status;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "{status}");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status["total_versions"], rows.len());
    assert_eq!(status["state_counts"]["UP_TO_DATE"], rows.len());
    assert_eq!(status["state_counts"]["PENDING"], 0);
    assert_eq!(status["state_counts"]["FAILED"], 0);
    assert_eq!(status["failing_versions"], 0);
    // Every version takes a run per transition, so the completed runs outnumber
    // the versions; the counters only ever grow, so nothing failed here.
    assert!(
        status["completed_run_count"].as_u64().unwrap_or(0) >= rows.len() as u64,
        "{status}"
    );
    assert_eq!(status["failed_run_count"], 0);
    assert_eq!(status["timed_out_run_count"], 0);
    assert_eq!(status["quarantined_version_count"], 0);
    assert!(
        status["last_walk_time"]
            .as_u64()
            .is_some_and(|time| time > 0),
        "{status}"
    );
    assert!(status.get("last_walk_error").is_none(), "{status}");
}

/// A version's measured suggestions are on disk beside it, and `list
/// suggestions` returns them largest saving first: the log's tombstones (an
/// exact count) above the column rewrites, with `small-files` in the same
/// record set.
#[test]
fn the_driver_persists_and_lists_its_measured_suggestions() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    let removed_file_bytes = 2 * 1024 * 1024;
    commit(&root, 0, small_files_table(&root, 16));
    commit(
        &root,
        1,
        vec![
            remove("day=1/gone-0.parquet", "1", removed_file_bytes),
            remove("day=1/gone-2.parquet", "1", removed_file_bytes),
        ],
    );
    walk(directory.path(), &[("events", &root)]);
    let running = Driver::start(directory.path(), &[]);
    let rows = running.wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE");

    let persisted = std::fs::read_to_string(result_file(&running, &rows[0], "suggestions.ndjson"))
        .expect("the version kept its measured suggestions");
    let records: Vec<Value> = persisted
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(records.iter().any(|record| record["rule"] == "small-files"));
    assert!(!result_file(&running, &rows[0], "sample.parquet").exists());

    let listed = list_suggestions(&running, &[]);
    assert!(!listed.is_empty(), "{listed:#?}");
    let savings: Vec<u64> = listed
        .iter()
        .map(|record| record["savings_bytes"].as_u64().unwrap_or(0))
        .collect();
    assert!(
        savings.windows(2).all(|pair| pair[0] >= pair[1]),
        "not largest saving first: {savings:?}"
    );
    let tombstones = listed
        .iter()
        .find(|record| record["rule"] == "tombstones")
        .expect("a tombstones suggestion");
    assert_eq!(tombstones["verified"], true);
    assert_eq!(
        tombstones["savings_bytes"].as_u64(),
        Some(2 * removed_file_bytes)
    );
    assert_eq!(
        tombstones["name"],
        format!("{}/suggestions/tombstones", partition(&rows[0]))
    );
    let small_files = listed
        .iter()
        .find(|record| record["rule"] == "small-files")
        .expect("a small-files suggestion");
    assert_eq!(small_files["partition"], partition(&rows[0]));
    assert!(small_files["message"]
        .as_str()
        .unwrap()
        .contains("16 files"));

    // A parent narrows the listing; one that is not a name the driver knows
    // is refused.
    assert!(list_suggestions(&running, &["catalogs/other"]).is_empty());
    assert_eq!(
        list_suggestions(&running, &["catalogs/local"]).len(),
        listed.len()
    );
    let refused = driver()
        .env("PQB_DRIVER_SOCK", &running.socket)
        .args(["list", "suggestions", "catalogs/local/tables/events"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("is not a catalog"));
}

/// A host with little network copies no sample, so there is no column
/// suggestion to measure, and the partition rules still land: the log's
/// tombstones are an exact saving from metadata alone.
#[test]
fn a_low_network_still_persists_the_partition_suggestions() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    let removed_file_bytes = 3 * 1024 * 1024;
    commit(&root, 0, readable_table(&root, &["1"]));
    commit(
        &root,
        1,
        vec![remove("day=1/gone.parquet", "1", removed_file_bytes)],
    );
    walk(directory.path(), &[("events", &root)]);
    let driver = Driver::start(directory.path(), &["--low-network"]);
    let rows = driver.wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE");
    assert!(!result_file(&driver, &rows[0], "sample.parquet").exists());
    assert!(!result_file(&driver, &rows[0], "list-suggestions.ndjson").exists());

    let listed = list_suggestions(&driver, &[]);
    assert_eq!(listed.len(), 1, "{listed:#?}");
    assert_eq!(listed[0]["rule"], "tombstones");
    assert_eq!(listed[0]["verified"], true);
    assert_eq!(listed[0]["savings_bytes"], removed_file_bytes);
}

/// A version that fails its last strike is quarantined: `FAILED`, so no
/// transition runs on it again, with the copy of the sample file a crash left
/// swept. The checkpoint holds the state, the count and the error, so a restart
/// does not un-quarantine it.
#[test]
fn the_last_strike_quarantines_a_version_across_restarts() {
    let directory = tempfile::tempdir().unwrap();
    let partition = "catalogs/local/schemas/local/tables/events/partitions/day%3D1";
    let version_dir = directory
        .path()
        .join("store/results")
        .join(partition)
        .join("aa");
    std::fs::create_dir_all(&version_dir).unwrap();
    // The map a previous driver checkpointed: this version has one strike left,
    // and its listing is gone, so no command can read it.
    let row = json!({
        "name": format!("{partition}/versions/aa"),
        "state": "PENDING",
        "create_time": 1,
        "first_listed_time": 1,
        "failure_count": 5,
        "error": "five attempts read nothing",
        "file_count": 1,
        "size_bytes": 10,
    });
    std::fs::write(
        directory.path().join("store/checkpoint.ndjson"),
        row.to_string() + "\n",
    )
    .unwrap();
    // A copy a crash left behind: no transition reads it again.
    std::fs::write(version_dir.join("sample.parquet"), "x").unwrap();
    let config = directory.path().join("run-control");
    std::fs::write(&config, "[DEFAULT]\ncheckpoint-codec = none\n").unwrap();

    let running = Driver::configured(directory.path(), &[], &config);
    let rows = running.wait(|rows| rows.len() == 1 && rows[0]["state"] == "FAILED");
    assert_eq!(rows[0]["failure_count"], 6);
    assert!(
        rows[0]["error"]
            .as_str()
            .is_some_and(|error| error.contains("listing")),
        "{rows:#?}"
    );
    assert!(
        !version_dir.join("sample.parquet").exists(),
        "a quarantined version kept its copy of the sample file"
    );

    let status = driver_status(&running);
    assert_eq!(status["state_counts"]["FAILED"], 1);
    assert_eq!(status["quarantined_version_count"], 1);
    assert_eq!(status["failed_run_count"], 1);
    assert_eq!(status["timed_out_run_count"], 0);
    assert_eq!(status["completed_run_count"], 0);
    assert_eq!(status["failing_versions"], 1);

    // A restart loads the quarantine rather than trying the version again.
    running.stop();
    let running = Driver::configured(directory.path(), &[], &config);
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(1_000) {
        assert_eq!(running.rows(), rows, "the quarantine did not hold");
        std::thread::sleep(Duration::from_millis(100));
    }
    let status = driver_status(&running);
    assert_eq!(status["state_counts"]["FAILED"], 1);
    assert_eq!(status["failed_run_count"], 0);
}

/// The map holds each version's measured suggestions, so a listing reads no
/// results file: with every `suggestions.ndjson` gone the records are unchanged
/// rather than empty.
#[test]
fn the_measured_suggestions_are_served_from_the_map() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, small_files_table(&root, 8));
    walk(directory.path(), &[("events", &root)]);
    let running = Driver::start(directory.path(), &[]);
    running.wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE");
    let listed = list_suggestions(&running, &[]);
    assert!(!listed.is_empty(), "{listed:#?}");

    let mut removed = 0;
    for file in walk_files(&running.store.join("results")) {
        if file
            .file_name()
            .is_some_and(|name| name == "suggestions.ndjson")
        {
            std::fs::remove_file(&file).unwrap();
            removed += 1;
        }
    }
    assert!(removed > 0, "the results held no suggestions to delete");
    assert_eq!(
        list_suggestions(&running, &[]),
        listed,
        "the suggestions were read from disk, not from the map"
    );
}

/// A signal stops the driver cleanly: the runs are aborted, the map is
/// checkpointed, the socket goes away, and the process exits 0. `SIGINT` and
/// `SIGTERM` are the two a supervisor sends.
#[test]
fn a_signal_stops_the_driver_cleanly() {
    // The table and its walk are the slow part, and both signals exercise the
    // same shutdown path, so the setup is shared: the second driver resumes
    // from the checkpoint the first one left.
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    for (index, signal) in [libc::SIGTERM, libc::SIGINT].into_iter().enumerate() {
        // A store per signal, so the checkpoint really is the one this run
        // wrote.
        let store = directory.path().join(format!("store-{index}"));
        let socket = directory.path().join(format!("signal-{index}.sock"));
        let mut child = driver()
            .args(["serve", "--store"])
            .arg(&store)
            .arg("--socket")
            .arg(&socket)
            .arg("--pqbench")
            .arg(env!("CARGO_BIN_EXE_pqbench"))
            .arg("--walk-tables")
            .arg(directory.path().join("tables.ndjson"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        await_socket(&socket, &mut child);
        // SAFETY: the child is this test's own process, and `kill` takes its
        // pid and a signal number.
        assert_eq!(
            unsafe { libc::kill(child.id() as libc::pid_t, signal) },
            0,
            "cannot signal the driver"
        );
        let start = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "the driver ignored signal {signal}"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(status.success(), "signal {signal} exited {status}");
        assert!(!socket.exists(), "the socket outlived signal {signal}");
        assert!(
            ["checkpoint.ndjson.zst", "checkpoint.ndjson"]
                .iter()
                .any(|name| store.join(name).is_file()),
            "no checkpoint after signal {signal}"
        );
    }
}

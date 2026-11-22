#![cfg(feature = "delta")]

//! The daemon's master: it walks the tables it is given, runs the steps on
//! each partition version, purges what a walk no longer finds, and serves
//! the versions to `list partition-versions` and the web app.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const STEPS: &[&str] = &[
    "--step", "price", "--step", "download", "--step", "bytemass", "--step", "lint",
];

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
/// the steps have data to read.
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

fn states(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["state"].as_str().unwrap())
        .collect()
}

/// A walk makes each partition one PENDING version with its listing stored;
/// later walks find the same versions and move nothing.
#[test]
fn a_walk_adds_each_partition_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    table(&root);
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &["--walk-period-seconds", "0"]);
    let first = master.wait(|rows| rows.len() == 2);
    assert_eq!(states(&first), ["PENDING", "PENDING"]);
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
    assert_eq!(new["state"], "PENDING");
    assert_eq!(new["etag_change_count"], 1);
    assert_eq!(new["first_listed_time"], old["first_listed_time"]);
    assert!(!result_file(&master, &old, "list-partitions.ndjson").exists());
}

/// A walk that read every table deletes the tables it did not list; a walk
/// with a failed table deletes no other table.
#[test]
fn only_a_complete_walk_deletes_the_tables_it_lacks() {
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

    walk(
        directory.path(),
        &[
            ("events", &events),
            ("broken", &directory.path().join("missing")),
        ],
    );
    let rewritten = pqbench_time();
    master.wait(|rows| rows[0]["list_time"].as_u64().unwrap() > rewritten + 200);
    assert_eq!(master.rows().len(), 4, "a failed walk deleted a table");

    walk(directory.path(), &[("events", &events)]);
    let rows = master.wait(|rows| rows.len() == 2);
    assert!(rows
        .iter()
        .all(|row| row["name"].as_str().unwrap().contains("/tables/events/")));
}

/// Milliseconds since the Unix epoch.
fn pqbench_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// `list partition-versions` takes a parent at any level, `-` for any id,
/// and refuses a name that is not one.
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
    let orders = names(&["catalogs/-/schemas/-/tables/orders"]);
    assert_eq!(orders.len(), 2);
    assert!(orders.iter().all(|name| name.contains("/tables/orders/")));
    assert_eq!(names(&[&orders[0]]), [orders[0].clone()]);
    assert_eq!(
        names(&["catalogs/local/schemas/-/tables/-/partitions/day%3D1"]).len(),
        2
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
    table(&root);
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &[]);
    let rows = master.wait(|rows| rows.len() == 2);
    master.stop();
    std::fs::remove_file(directory.path().join("tables.ndjson")).unwrap();
    let master = Master::start(directory.path(), &[]);
    assert_eq!(master.rows(), rows);
}

/// The steps take each version to UP_TO_DATE: a price, a copy of the sample
/// file, its columns and its suggestions, the records naming the table's
/// file and not the copy.
#[test]
fn the_steps_take_each_version_up_to_date() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1", "2"]));
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), STEPS);
    let rows =
        master.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));
    for row in &rows {
        assert!(row["up_to_date_time"].is_u64());
        assert!(row.get("process_id").is_none(), "{row}");
        for output in [
            "price.ndjson",
            "sample.parquet",
            "bytemass.ndjson",
            "list-suggestions.ndjson",
        ] {
            assert!(
                result_file(&master, row, output).exists(),
                "{output} for {row}"
            );
        }
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

/// With only the price step on, versions stop at PRICED and no file is
/// copied: the setting for a host with little network.
#[test]
fn only_the_configured_steps_run() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), &["--step", "price"]);
    let rows = master.wait(|rows| rows.len() == 1 && rows[0]["state"] == "PRICED");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(master.rows()[0]["state"], "PRICED");
    assert!(!result_file(&master, &rows[0], "sample.parquet").exists());
}

/// While a changed partition's new version is processed, the old version's
/// complete results are kept; once the new one is UP_TO_DATE, the old goes.
#[test]
fn purge_keeps_the_last_complete_version_until_it_is_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    commit(&root, 0, readable_table(&root, &["1"]));
    walk(directory.path(), &[("events", &root)]);
    let master = Master::start(directory.path(), STEPS);
    let old = master
        .wait(|rows| rows.len() == 1 && rows[0]["state"] == "UP_TO_DATE")
        .remove(0);
    master.stop();
    let fixture = root.join("day=1/part-0.parquet");
    std::fs::copy(&fixture, root.join("day=1/part-1.parquet")).unwrap();
    let size = std::fs::metadata(&fixture).unwrap().len();
    commit(&root, 1, vec![add("day=1/part-1.parquet", "1", size)]);

    let master = Master::start(directory.path(), &[]);
    let rows = master.wait(|rows| rows.len() == 2);
    let kept = rows.iter().find(|row| etag(row) == etag(&old)).unwrap();
    assert_eq!(kept["state"], "DELETED");
    assert!(result_file(&master, &old, "bytemass.ndjson").exists());
    master.stop();

    let master = Master::start(directory.path(), STEPS);
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
    let master = Master::start(directory.path(), STEPS);
    let rows = master.wait(|rows| rows.len() == 1 && rows[0]["failure_count"] == 2);
    assert_eq!(rows[0]["state"], "PRICED");
    assert!(rows[0]["error"]
        .as_str()
        .is_some_and(|error| !error.is_empty()));
}

/// One HTTP GET to the web app: the status code and the body.
fn http_get(address: &str, path: &str) -> (u16, String) {
    use std::io::Read;
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, body.to_string())
}

/// Once the master has nothing to run, its stats are the fold of its
/// versions, and the web app serves them and the versions by name.
#[test]
fn the_web_app_serves_the_stats_and_the_records() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("events");
    let mut actions = readable_table(&root, &["1", "2"]);
    std::fs::copy(
        root.join("day=2/part-0.parquet"),
        root.join("day=2/part-1.parquet"),
    )
    .unwrap();
    let size = std::fs::metadata(root.join("day=2/part-1.parquet"))
        .unwrap()
        .len();
    actions.push(add("day=2/part-1.parquet", "2", size));
    commit(&root, 0, actions);
    walk(directory.path(), &[("events", &root)]);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let address = format!("127.0.0.1:{port}");
    let flags = [STEPS, &["--web-ui", "--listen", &address]].concat();
    let master = Master::start(directory.path(), &flags);
    master.wait(|rows| rows.len() == 2 && rows.iter().all(|row| row["state"] == "UP_TO_DATE"));

    let versions = master.run(&["list", "partition-versions"], "");
    let expected = master.run(
        &["summarize", "partition-versions"],
        &String::from_utf8(versions.stdout).unwrap(),
    );
    let expected = String::from_utf8(expected.stdout).unwrap();
    let start = Instant::now();
    while std::fs::read_to_string(master.store.join("stats.ndjson")).unwrap_or_default() != expected
    {
        assert!(start.elapsed() < Duration::from_secs(30), "no stats");
        std::thread::sleep(Duration::from_millis(20));
    }

    let (status, page) = http_get(&address, "/");
    assert_eq!(status, 200);
    assert!(page.contains("/v1/"));
    let stats = |path: &str| -> Vec<Value> {
        let (status, body) = http_get(&address, path);
        assert_eq!(status, 200, "{body}");
        serde_json::from_str(&body).unwrap()
    };
    let catalogs = stats("/v1/catalogs");
    assert_eq!(catalogs[0]["name"], "catalogs/local/partitionStats");
    assert_eq!(catalogs[0]["partition_count"], 2);
    assert_eq!(catalogs[0]["size_bytes"], 3 * size);
    let tables = stats("/v1/catalogs/-/schemas/-/tables");
    assert_eq!(
        tables[0]["name"],
        "catalogs/local/schemas/local/tables/events/partitionStats"
    );
    assert_eq!(tables[0]["version_counts"]["UP_TO_DATE"], 2);

    let list = |query: &str| {
        let (status, body) = http_get(
            &address,
            &format!("/v1/catalogs/local/schemas/local/tables/events/partitions?{query}"),
        );
        assert_eq!(status, 200, "{body}");
        serde_json::from_str::<Value>(&body).unwrap()
    };
    let all = list("");
    assert_eq!(all["total_size"], 2);
    let pages = all["partitions"].as_array().unwrap();
    assert!(pages.iter().all(|v| v["list_partitions"]["etag"] == etag(v)
        && v["size_bytes"] == v["list_partitions"]["size_bytes"]));
    assert!(pages.iter().all(|v| v.get("bytemass").is_none()));
    let first = list("page_size=1");
    let token = first["next_page_token"].as_str().unwrap();
    let second = list(&format!("page_size=1&page_token={token}"));
    assert_eq!(first["partitions"][0]["name"], pages[0]["name"]);
    assert_eq!(second["partitions"][0]["name"], pages[1]["name"]);
    assert_eq!(
        list("order_by=last_modification_time+desc")["total_size"],
        2
    );
    assert_eq!(
        http_get(
            &address,
            "/v1/catalogs/local/schemas/local/tables/events/partitions?order_by=etag"
        )
        .0,
        400
    );
    let name = partition(&pages[0]);
    let (status, body) = http_get(&address, &format!("/v1/{name}"));
    assert_eq!(status, 200);
    let one: Vec<Value> = serde_json::from_str(&body).unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0]["state"], "UP_TO_DATE");
    assert!(one[0]["bytemass"]
        .as_array()
        .is_some_and(|lines| !lines.is_empty()));
    assert!(one[0]["list_suggestions"].is_array());
    assert_eq!(http_get(&address, "/v1/catalogs/local").0, 404);
}

/// The web app needs the master.
#[test]
fn the_web_app_requires_the_master() {
    let output = pqbench()
        .args(["daemon", "serve", "--web-ui"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--master"));
}

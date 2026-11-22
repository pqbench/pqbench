//! The master's driver: the worker side. It runs the pqbench commands the
//! master hands out, keeps their files, and tells the master how each ended.
//!
//! One task on the daemon's runtime awaits the running commands, gives the
//! master each finished one, starts the runs the master hands out next, and
//! starts a walk when one is due; it also purges and checkpoints. Only the
//! driver changes the map, and each change takes the write lock for one
//! short update.
//!
//! - **Runs**: one command per version (`get partition-price`, `download`,
//!   `bytemass`, `list suggestions`), one per CPU at once.
//! - **The walk**: `list catalogs | list schemas | list tables | get table |
//!   list partitions`, the whole metastore, or `list partitions` over the
//!   configured Table records instead; its listing makes and deletes
//!   versions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use pqbench::v1::PartitionPrice;
use serde_json::Value;
use tokio::task::JoinSet;

use super::master::{self, Input, Key, Master, Run, State};
use super::process;
use super::store::{Store, SAMPLE};

/// How long the driver sleeps when no command finishes: when a walk falls
/// due, and when a checkpoint does, are checked this often.
const TICK: Duration = Duration::from_secs(1);

/// How often the map is checkpointed while commands run and it changes: the
/// most work a crash can cost. Once nothing runs, a change is checkpointed at
/// once.
const CHECKPOINT_PERIOD: Duration = Duration::from_secs(10);

/// What the driver runs: the operator's settings.
pub(crate) struct Settings {
    /// A file of Table records the walk lists instead of the metastore.
    pub walk_tables: Option<PathBuf>,
    /// How long after a walk the next one is due; `None` walks once, when the
    /// daemon starts.
    pub walk_period: Option<Duration>,
}

/// How a run ended: its price when it priced the version.
type Outcome = Result<Option<PartitionPrice>, String>;

struct Driver {
    master: Arc<RwLock<Master>>,
    store: Store,
    settings: Settings,
    /// The most runs at once: one per CPU.
    commands: usize,
    socket: PathBuf,
    /// The runs in progress, each ending in its version's key and outcome.
    running: JoinSet<(Key, Outcome)>,
    /// The walk, while it runs.
    walk: JoinSet<Result<(), String>>,
    /// When the last walk started, milliseconds since the Unix epoch.
    walk_time: Option<u64>,
    walked: bool,
    checkpoint_time: Instant,
}

/// Start the driver on the daemon's runtime. A pass does short blocking file
/// work (a checkpoint, a listing), which the socket waits out.
pub(crate) fn spawn(
    master: Arc<RwLock<Master>>,
    store: Store,
    settings: Settings,
    socket: PathBuf,
) {
    let walk_time = read(&master).last_list_time();
    let mut driver = Driver {
        master,
        store,
        settings,
        commands: std::thread::available_parallelism().map_or(1, usize::from),
        socket,
        running: JoinSet::new(),
        walk: JoinSet::new(),
        walk_time,
        walked: false,
        checkpoint_time: Instant::now(),
    };
    tokio::spawn(async move {
        loop {
            driver.start_walk();
            driver.start_runs();
            driver.checkpoint();
            tokio::select! {
                Some(done) = driver.running.join_next() => match done {
                    Ok((key, outcome)) => driver.finish(&key, outcome),
                    Err(error) => eprintln!("pqbench daemon: a run's task failed: {error}"),
                },
                Some(done) = driver.walk.join_next() => {
                    let outcome = done.unwrap_or_else(|error| Err(error.to_string()));
                    driver.finish_walk(outcome);
                }
                () = tokio::time::sleep(TICK) => {}
            }
        }
    });
}

fn read(master: &RwLock<Master>) -> RwLockReadGuard<'_, Master> {
    master
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write(master: &RwLock<Master>) -> RwLockWriteGuard<'_, Master> {
    master
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Driver {
    /// Start the runs the master hands out, while there are free slots.
    fn start_runs(&mut self) {
        let free = self.commands.saturating_sub(self.running.len());
        if free == 0 {
            return;
        }
        for run in write(&self.master).start(free) {
            let store = self.store.clone();
            let socket = self.socket.clone();
            self.running.spawn(async move {
                let outcome = work(&store, &run, &socket).await;
                if outcome.is_err() {
                    store.discard(&run.key, run.transition.output);
                }
                (run.key, outcome)
            });
        }
    }

    /// Give the master a finished run. A version that is done drops its
    /// copy of the sample file, and the version it replaces can go.
    fn finish(&mut self, key: &Key, outcome: Outcome) {
        let state = write(&self.master).finish(key, outcome, master::now_ms());
        if state == Some(State::UpToDate) {
            self.store.remove_sample(key);
            self.purge(&key.0);
        }
    }

    /// Purge the partitions under `prefix`; their results go after their
    /// rows.
    fn purge(&mut self, prefix: &str) {
        for key in write(&self.master).purge(prefix) {
            if let Err(error) = self.store.remove_version(&key) {
                eprintln!("pqbench daemon: {error}");
            }
        }
    }

    /// Start a walk when none runs and one is due: the first since the
    /// daemon started, then one each walk period.
    fn start_walk(&mut self) {
        if !self.walk.is_empty() {
            return;
        }
        let now = master::now_ms();
        let due = match (self.walked, self.settings.walk_period, self.walk_time) {
            (false, _, _) => true,
            (true, None, _) => false,
            (true, Some(_), None) => true,
            (true, Some(period), Some(last)) => {
                now.saturating_sub(last) >= period.as_millis() as u64
            }
        };
        if !due {
            return;
        }
        self.walked = true;
        self.walk_time = Some(now);
        let opened = walk_files(&self.store.walk_dir(), self.settings.walk_tables.as_deref());
        let (stages, input, output, error) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                eprintln!("pqbench daemon: cannot start the walk: {error}");
                return;
            }
        };
        let socket = self.socket.clone();
        self.walk
            .spawn(async move { process::pipeline(&stages, input, output, &error, &socket).await });
    }

    /// Apply the walk's listing: each listed table's versions are found or
    /// deleted, and when every command succeeded, the tables the walk did not
    /// list are deleted too.
    fn finish_walk(&mut self, outcome: Result<(), String>) {
        let directory = self.store.walk_dir();
        if let Err(error) = &outcome {
            let text = fs::read_to_string(directory.join("walk.err")).unwrap_or_default();
            eprintln!(
                "pqbench daemon: the walk failed ({error}): {}",
                process::tail(text.trim())
            );
        }
        let now = master::now_ms();
        let mut found: BTreeMap<String, BTreeSet<Key>> = BTreeMap::new();
        let lines = File::open(directory.join("partitions.ndjson"))
            .map(|file| BufReader::new(file).lines().map_while(Result::ok))
            .into_iter()
            .flatten();
        for line in lines.filter(|line| !line.trim().is_empty()) {
            match self.found(&line, now) {
                Ok(key) => {
                    let table = master::parent_table(&key.0).to_string();
                    found.entry(table).or_default().insert(key);
                }
                Err(error) => eprintln!("pqbench daemon: walk: {error}"),
            }
        }
        for (table, keys) in &found {
            write(&self.master).missing(table, keys);
        }
        if outcome.is_ok() {
            let tables: BTreeSet<String> = found.into_keys().collect();
            write(&self.master).retain(&tables);
        }
        self.purge("");
        let _ = fs::remove_dir_all(&directory);
    }

    /// One line of the walk's listing, a Partition record: a new version's
    /// listing is stored before the master has the version.
    fn found(&mut self, line: &str, now: u64) -> Result<Key, String> {
        let listing: Value =
            serde_json::from_str(line).map_err(|error| format!("bad listing line: {error}"))?;
        let (Some(name), Some(etag)) = (listing["name"].as_str(), listing["etag"].as_str()) else {
            return Err("a listing line has no name or etag".into());
        };
        let key = master::key(name, etag)
            .ok_or_else(|| format!("{name} at {etag} is not a partition version"))?;
        if !read(&self.master).contains(&key) {
            self.store.write_listing(&key, line)?;
        }
        write(&self.master).found(&key, &listing, now);
        Ok(key)
    }

    /// Checkpoint the map when it changed, once the period passed or
    /// nothing runs.
    fn checkpoint(&mut self) {
        let idle = self.walk.is_empty() && self.running.is_empty();
        if !idle && self.checkpoint_time.elapsed() < CHECKPOINT_PERIOD {
            return;
        }
        self.checkpoint_time = Instant::now();
        if !write(&self.master).take_changed() {
            return;
        }
        if let Err(error) = self.store.write_checkpoint(read(&self.master).versions()) {
            eprintln!("pqbench daemon: {error}");
        }
    }
}

/// Run one transition on one version: its command, built from the version's
/// listing, and its output landed in the version's directory. A price run
/// returns the price.
async fn work(store: &Store, run: &Run, socket: &Path) -> Outcome {
    let (key, transition) = (&run.key, run.transition);
    let listing = store.read_listing(key)?;
    let input = match transition.input {
        Input::Partition => listing,
        Input::SampleFile => serde_json::from_str::<Value>(&listing)
            .ok()
            .and_then(|record| record.get("sample_file").map(Value::to_string))
            .ok_or_else(|| format!("the listing of {} names no sample file", key.0))?,
    };
    let mut command: Vec<String> = transition
        .command
        .iter()
        .map(|word| (*word).to_string())
        .collect();
    if !transition.stdout {
        let temporary = store.temporary(key, transition.output);
        command.push("--output".into());
        command.push(temporary.to_string_lossy().into_owned());
    }
    if transition.local {
        let sample = store.version_dir(key).join(SAMPLE);
        command.push("--local".into());
        command.push(sample.to_string_lossy().into_owned());
    }
    let stdout = process::run(&command, &(input + "\n"), socket).await?;
    store.land(
        key,
        transition.output,
        transition.stdout.then_some(&stdout[..]),
    )?;
    if transition.running != State::Pricing {
        return Ok(None);
    }
    let first = String::from_utf8_lossy(&stdout);
    let first = first.lines().next().unwrap_or_default();
    serde_json::from_str(first)
        .map(Some)
        .map_err(|error| format!("bad price: {error}"))
}

/// The walk's pipeline and its files in `directory`: the whole metastore,
/// from `list catalogs`, or `list partitions` over the configured Table
/// records instead.
fn walk_files(
    directory: &Path,
    tables: Option<&Path>,
) -> std::io::Result<(Vec<Vec<String>>, File, File, File)> {
    fs::create_dir_all(directory)?;
    let stage = |words: &[&str]| words.iter().map(|word| (*word).to_string()).collect();
    let (stages, input) = match tables {
        Some(tables) => (vec![stage(&["list", "partitions"])], File::open(tables)?),
        None => (
            vec![
                stage(&["list", "catalogs"]),
                stage(&["list", "schemas"]),
                stage(&["list", "tables"]),
                stage(&["get", "table"]),
                stage(&["list", "partitions"]),
            ],
            File::open("/dev/null")?,
        ),
    };
    let output = File::create(directory.join("partitions.ndjson"))?;
    let error = File::create(directory.join("walk.err"))?;
    Ok((stages, input, output, error))
}

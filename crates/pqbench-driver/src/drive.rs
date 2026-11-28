//! The driver's worker side: it runs the pqbench commands the engine hands out,
//! keeps their files, and tells the engine how each ended.
//!
//! One task on the driver's runtime awaits the running commands, gives the
//! engine each finished one, starts the runs the engine hands out next, and
//! starts a walk when one is due; it also moves superseded results to the
//! trash and checkpoints. Only the driver changes the map, and each change
//! takes the write lock for one short update.
//!
//! - **Runs**: one command per version (`get partition-price`, `download`,
//!   `bytemass`, `list suggestions`), one per CPU at once.
//! - **The walk**: `list catalogs | list schemas | list tables | get table |
//!   list partitions`, the whole metastore, or `list partitions` over the
//!   configured Table records instead; its listing makes and deletes
//!   versions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use pqbench::v1::{Partition, PartitionPrice};
use tokio::task::JoinSet;

use super::engine::{self, Engine, Input, Key, Run, State};
use super::process::{self, WorkerExe};
use super::store::{Store, SAMPLE};

/// How long the driver sleeps when no command finishes: when a walk falls
/// due, and when a checkpoint does, are checked this often.
const TICK: Duration = Duration::from_secs(1);

/// How often the map is checkpointed while commands run and it changes: the
/// most work a crash can cost. Once nothing runs, a change is checkpointed at
/// once.
const CHECKPOINT_PERIOD: Duration = Duration::from_secs(5 * 60);

/// What the driver runs: the operator's settings.
pub(crate) struct Settings {
    /// A file of Table records the walk lists instead of the metastore.
    pub walk_tables: Option<PathBuf>,
    /// How long after a walk the next one is due; `None` walks once, when the
    /// engine starts.
    pub walk_period: Option<Duration>,
}

/// How a run ended: its price when it priced the version.
type Outcome = Result<Option<PartitionPrice>, String>;

struct Driver {
    engine: Arc<RwLock<Engine>>,
    store: Store,
    settings: Settings,
    /// The most runs at once: one per CPU.
    commands: usize,
    /// The pqbench image every command runs.
    exe: WorkerExe,
    /// The daemon's socket, when commands talk to one.
    daemon_socket: Option<PathBuf>,
    /// The runs in progress, each ending in its version's key and outcome.
    running: JoinSet<(Key, Outcome)>,
    /// The walk, while it runs.
    walk: JoinSet<Result<(), String>>,
    /// The trash being emptied, if it is.
    emptying: JoinSet<()>,
    /// When the last walk started, milliseconds since the Unix epoch.
    walk_time: Option<u64>,
    walked: bool,
    checkpoint_time: Instant,
}

/// Start the driver on the runtime. A pass does short blocking file
/// work (a checkpoint, a listing), which the socket waits out.
pub(crate) fn spawn(
    engine: Arc<RwLock<Engine>>,
    store: Store,
    settings: Settings,
    exe: WorkerExe,
    daemon_socket: Option<PathBuf>,
) {
    let walk_time = read_engine(&engine).last_list_time();
    let mut driver = Driver {
        engine,
        store,
        settings,
        commands: std::thread::available_parallelism().map_or(1, usize::from),
        exe,
        daemon_socket,
        running: JoinSet::new(),
        walk: JoinSet::new(),
        emptying: JoinSet::new(),
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
                    Err(error) => eprintln!("pqbench-driver: a run's task failed: {error}"),
                },
                Some(done) = driver.walk.join_next() => {
                    let outcome = done.unwrap_or_else(|error| Err(error.to_string()));
                    driver.finish_walk(outcome);
                }
                Some(_) = driver.emptying.join_next() => {}
                () = tokio::time::sleep(TICK) => {}
            }
        }
    });
}

fn read_engine(engine: &RwLock<Engine>) -> RwLockReadGuard<'_, Engine> {
    engine
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_engine(engine: &RwLock<Engine>) -> RwLockWriteGuard<'_, Engine> {
    engine
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Driver {
    /// Start the runs the engine hands out, while there are free slots.
    fn start_runs(&mut self) {
        let free = self.commands.saturating_sub(self.running.len());
        if free == 0 {
            return;
        }
        for run in write_engine(&self.engine).start(free) {
            let store = self.store.clone();
            let exe = self.exe.clone();
            let daemon_socket = self.daemon_socket.clone();
            self.running.spawn(async move {
                let outcome = work(&store, &run, &exe, daemon_socket.as_deref()).await;
                if outcome.is_err() {
                    store.discard(&run.key, run.transition.output);
                }
                (run.key, outcome)
            });
        }
    }

    /// Give the engine a finished run. A version with no transition left to
    /// read it drops its copy of the sample file.
    fn finish(&mut self, key: &Key, outcome: Outcome) {
        let Some(conclusion) = write_engine(&self.engine).finish(key, outcome) else {
            return;
        };
        if !conclusion.sample {
            self.store.remove_sample(key);
        }
    }

    /// Move the results of the versions a walk dropped to the trash, and
    /// empty it off the runtime.
    fn trash(&mut self, keys: Vec<Key>) {
        let mut trashed = false;
        for key in keys {
            match self.store.trash(&key) {
                Ok(()) => trashed = true,
                Err(error) => eprintln!("pqbench-driver: {error}"),
            }
        }
        if !trashed || !self.emptying.is_empty() {
            return;
        }
        let store = self.store.clone();
        self.emptying.spawn_blocking(move || {
            if let Err(error) = store.empty_trash() {
                eprintln!("pqbench-driver: {error}");
            }
        });
    }

    /// Start a walk when none runs and one is due: the first since the
    /// engine started, then one each walk period.
    fn start_walk(&mut self) {
        if !self.walk.is_empty() {
            return;
        }
        let now = engine::now_ms();
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
                eprintln!("pqbench-driver: cannot start the walk: {error}");
                return;
            }
        };
        let exe = self.exe.clone();
        let daemon_socket = self.daemon_socket.clone();
        self.walk.spawn(async move {
            process::pipeline(
                &exe,
                &stages,
                input,
                output,
                &error,
                daemon_socket.as_deref(),
            )
            .await
        });
    }

    /// Apply the walk's listing: each listed table's versions are found, and
    /// when every command succeeded, a version the listing does not name is
    /// deleted. Only a complete listing can say a version is gone, so a failed
    /// walk, or a listing that cannot be read, deletes nothing.
    fn finish_walk(&mut self, outcome: Result<(), String>) {
        let directory = self.store.walk_dir();
        let listing = fs::read_to_string(directory.join("partitions.ndjson"))
            .map_err(|error| format!("cannot read the walk's listing: {error}"));
        if let Err(error) = &outcome {
            let text = fs::read_to_string(directory.join("walk.err")).unwrap_or_default();
            eprintln!(
                "pqbench-driver: the walk failed ({error}): {}",
                process::tail(text.trim())
            );
        }
        let listing = match listing {
            Ok(listing) => listing,
            Err(error) => {
                eprintln!("pqbench-driver: the walk has no listing ({error})");
                let _ = fs::remove_dir_all(&directory);
                return;
            }
        };
        let now = engine::now_ms();
        let mut found: BTreeMap<String, BTreeSet<Key>> = BTreeMap::new();
        let mut dropped = Vec::new();
        for line in listing.lines().filter(|line| !line.trim().is_empty()) {
            match self.found(line, now) {
                Ok((key, superseded)) => {
                    let table = engine::parent_table(&key.0).to_string();
                    found.entry(table).or_default().insert(key);
                    dropped.extend(superseded);
                }
                Err(error) => eprintln!("pqbench-driver: walk: {error}"),
            }
        }
        // A listing that stops early still names real versions; only a
        // complete walk can say that one is gone.
        if outcome.is_ok() {
            for (table, keys) in &found {
                dropped.extend(write_engine(&self.engine).missing(table, keys));
            }
            let tables: BTreeSet<String> = found.into_keys().collect();
            dropped.extend(write_engine(&self.engine).retain(&tables));
        }
        self.trash(dropped);
        let _ = fs::remove_dir_all(&directory);
    }

    /// One line of the walk's listing, a Partition record: a new version's
    /// listing is stored before the engine has the version. Returns the
    /// version and the keys of the partition's versions it supersedes.
    fn found(&mut self, line: &str, now: u64) -> Result<(Key, Vec<Key>), String> {
        let listing: Partition =
            serde_json::from_str(line).map_err(|error| format!("bad listing line: {error}"))?;
        let key = engine::key(&listing.name, &listing.etag).ok_or_else(|| {
            format!(
                "{} at {} is not a partition version",
                listing.name, listing.etag
            )
        })?;
        if read_engine(&self.engine).state(&key).is_none() {
            self.store.write_listing(&key, line)?;
        }
        let superseded = write_engine(&self.engine).found(&key, &listing, now);
        Ok((key, superseded))
    }

    /// Checkpoint the map when it changed, once the period passed or
    /// nothing runs.
    fn checkpoint(&mut self) {
        let idle = self.walk.is_empty() && self.running.is_empty();
        if !idle && self.checkpoint_time.elapsed() < CHECKPOINT_PERIOD {
            return;
        }
        self.checkpoint_time = Instant::now();
        if !read_engine(&self.engine).changed() {
            return;
        }
        if let Err(error) = self
            .store
            .write_checkpoint(read_engine(&self.engine).versions())
        {
            eprintln!("pqbench-driver: {error}");
            return;
        }
        write_engine(&self.engine).checkpointed();
    }
}

/// Run one transition on one version: its command, built from the version's
/// listing, and its output landed in the version's directory. A price run
/// returns the price.
async fn work(store: &Store, run: &Run, exe: &WorkerExe, daemon_socket: Option<&Path>) -> Outcome {
    let (key, transition) = (&run.key, run.transition);
    let listing = store.read_listing(key)?;
    let input = match transition.input {
        Input::Partition => listing,
        Input::SampleFile => {
            let partition: Partition = serde_json::from_str(&listing)
                .map_err(|error| format!("bad listing of {}: {error}", key.0))?;
            let sample = partition
                .sample_file
                .ok_or_else(|| format!("the listing of {} names no sample file", key.0))?;
            serde_json::to_string(&sample)
                .map_err(|error| format!("cannot write the sample file of {}: {error}", key.0))?
        }
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
    let stdout = process::run(exe, &command, &(input + "\n"), daemon_socket).await?;
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

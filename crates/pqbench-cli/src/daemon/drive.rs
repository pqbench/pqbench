//! The master's driver: runs pqbench commands for the master and hands it
//! their results.
//!
//! One task on the daemon's runtime awaits the running commands, gives the
//! master each finished one, starts what the master plans next, and starts a
//! walk when one is due; it also purges and checkpoints. Only the driver
//! changes the map, and each change takes the write lock for one short
//! update.
//!
//! - **Versions**: one command per version (`get partition-price`,
//!   `download`, `bytemass`, `list suggestions`), one per CPU at once.
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

use tokio::task::JoinSet;

use super::master::{self, Key, Master};
use super::process;

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

struct Driver {
    master: Arc<RwLock<Master>>,
    settings: Settings,
    /// The most transition commands running at once: one per CPU.
    commands: usize,
    socket: PathBuf,
    store: PathBuf,
    /// The transition commands running, each ending in its version's key and
    /// outcome.
    running: JoinSet<(Key, Result<Vec<u8>, String>)>,
    /// The walk, while it runs.
    walk: JoinSet<Result<(), String>>,
    /// When the last walk started, milliseconds since the Unix epoch.
    walk_time: Option<u64>,
    walked: bool,
    checkpoint_time: Instant,
}

/// Start the driver on the daemon's runtime. A pass does short blocking file
/// work (a checkpoint, a result), which the socket waits out.
pub(crate) fn spawn(master: Arc<RwLock<Master>>, settings: Settings, socket: PathBuf) {
    let store = read(&master).store().to_path_buf();
    let walk_time = read(&master).last_list_time();
    let mut driver = Driver {
        master,
        settings,
        commands: std::thread::available_parallelism().map_or(1, usize::from),
        socket,
        store,
        running: JoinSet::new(),
        walk: JoinSet::new(),
        walk_time,
        walked: false,
        checkpoint_time: Instant::now(),
    };
    tokio::spawn(async move {
        loop {
            driver.pass();
            tokio::select! {
                Some(done) = driver.running.join_next() => match done {
                    Ok((key, outcome)) => {
                        write(&driver.master).finish(&key, outcome, master::now_ms());
                    }
                    Err(error) => eprintln!("pqbench daemon: a command's task failed: {error}"),
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
    fn pass(&mut self) {
        self.purge();
        self.start_walk();
        self.start_transitions();
        self.checkpoint();
    }

    /// Start what the master plans next, while there are free slots.
    fn start_transitions(&mut self) {
        let free = self.commands.saturating_sub(self.running.len());
        if free == 0 {
            return;
        }
        let runs = read(&self.master).plan(free);
        for run in runs {
            write(&self.master).start(&run);
            let socket = self.socket.clone();
            self.running.spawn(async move {
                let outcome = process::run(&run.command_words, &run.input, &socket).await;
                (run.key, outcome)
            });
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
        let opened = walk_files(&self.store, self.settings.walk_tables.as_deref());
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
        let directory = self.store.join("walk");
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
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            match write(&self.master).found(&line, now) {
                Ok(key) => {
                    let table = key
                        .0
                        .split_once("/partitions/")
                        .map_or("", |(table, _)| table);
                    found.entry(table.to_string()).or_default().insert(key);
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
        let _ = fs::remove_dir_all(directory);
    }

    /// Drop what the walk deleted; the directories go after the lock.
    fn purge(&mut self) {
        let directories = write(&self.master).purge();
        for directory in directories {
            if let Err(error) = master::remove_dir(&directory) {
                eprintln!("pqbench daemon: {error}");
            }
        }
    }

    /// Checkpoint the map when it changed, once the period passed or nothing
    /// runs.
    fn checkpoint(&mut self) {
        let idle = self.walk.is_empty() && self.running.is_empty();
        if !idle && self.checkpoint_time.elapsed() < CHECKPOINT_PERIOD {
            return;
        }
        self.checkpoint_time = Instant::now();
        if !write(&self.master).take_changed() {
            return;
        }
        if let Err(error) = read(&self.master).checkpoint() {
            eprintln!("pqbench daemon: {error}");
        }
    }
}

/// The walk's pipeline and its files: the whole metastore, from `list
/// catalogs`, or `list partitions` over the configured Table records instead.
fn walk_files(
    store: &Path,
    tables: Option<&Path>,
) -> std::io::Result<(Vec<Vec<String>>, File, File, File)> {
    let directory = store.join("walk");
    fs::create_dir_all(&directory)?;
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

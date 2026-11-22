//! The master's driver: runs pqbench commands for the master and hands it
//! their results.
//!
//! One thread makes a pass every [`PASS_PERIOD`]. A pass checks the running
//! commands without waiting on any, gives the master each finished one,
//! starts what the master plans next, and starts a walk when one is due; it
//! also purges, checkpoints, and refreshes the stats. Only the driver changes
//! the map, and each change takes the write lock for one short update.
//!
//! - **Versions**: one command per version (`get partition-price`,
//!   `download`, `bytemass`, `list suggestions`), one per CPU at once, each
//!   killed past the time limit.
//! - **The walk**: `list catalogs | list schemas | list tables | get table |
//!   list partitions`, the whole metastore, or `list partitions` over the
//!   configured Table records instead; its listing makes and deletes
//!   versions.
//! - **Stats**: `summarize partition-versions` over each new checkpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use super::master::{self, Master};
use super::process::{self, Exit};

/// How often the driver makes a pass.
const PASS_PERIOD: Duration = Duration::from_millis(100);

/// How often the map is checkpointed while commands run and it changes: the
/// most work a crash can cost. Once nothing runs, a change is checkpointed at
/// the next pass.
const CHECKPOINT_PERIOD: Duration = Duration::from_secs(10);

/// The most of a failed command's standard error kept as its error.
const ERROR_BYTES: usize = 2000;

/// What the driver runs: the operator's settings.
pub(crate) struct Settings {
    /// A file of Table records the walk lists instead of the metastore.
    pub walk_tables: Option<PathBuf>,
    /// How long after a walk the next one is due; `None` walks once, when the
    /// daemon starts.
    pub walk_period: Option<Duration>,
    /// How long a transition's command may run before it is killed.
    pub time_limit: Option<Duration>,
}

/// A walk in progress: its processes, output and error file.
struct Walk {
    process_ids: Vec<u32>,
    finished: Vec<Result<(), String>>,
    output: PathBuf,
    error: PathBuf,
}

/// The stats run in progress.
struct Stats {
    process_id: u32,
}

struct Driver {
    master: Arc<RwLock<Master>>,
    settings: Settings,
    /// The most transition commands running at once: one per CPU.
    commands: usize,
    socket: PathBuf,
    store: PathBuf,
    walk: Option<Walk>,
    /// When the last walk started, milliseconds since the Unix epoch.
    walk_time: Option<u64>,
    walked: bool,
    stats: Option<Stats>,
    /// The checkpoint the stats are due for, once the running stats finish.
    stats_due: Option<PathBuf>,
    checkpoint_time: Instant,
}

/// Start the driver thread.
pub(crate) fn spawn(master: Arc<RwLock<Master>>, settings: Settings, socket: PathBuf) {
    let store = read(&master).store().to_path_buf();
    let walk_time = read(&master).last_list_time();
    let mut driver = Driver {
        master,
        settings,
        commands: std::thread::available_parallelism().map_or(1, usize::from),
        socket,
        store,
        walk: None,
        walk_time,
        walked: false,
        stats: None,
        stats_due: None,
        checkpoint_time: Instant::now(),
    };
    std::thread::spawn(move || loop {
        driver.pass();
        std::thread::sleep(PASS_PERIOD);
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
        self.finish_transitions();
        self.finish_walk();
        self.purge();
        self.start_walk();
        self.start_transitions();
        self.checkpoint();
        self.finish_stats();
        self.start_stats();
    }

    /// Give the master each transition command that exited, and kill the ones past
    /// the time limit.
    fn finish_transitions(&mut self) {
        let now = master::now_ms();
        let runs = read(&self.master).runs();
        for run in runs {
            let late = self
                .settings
                .time_limit
                .filter(|limit| now.saturating_sub(run.start_time) > limit.as_millis() as u64);
            match process::poll(run.process_id) {
                Exit::Running => {
                    if late.is_some() {
                        process::kill(run.process_id);
                    }
                }
                Exit::Exited(outcome) => {
                    let outcome = outcome.map_err(|status| match late {
                        Some(limit) => format!(
                            "`pqbench {}` ran past the time limit of {} s",
                            run.command.join(" "),
                            limit.as_secs()
                        ),
                        None => command_error(&run, &status),
                    });
                    write(&self.master).finish(&run.key, outcome, now);
                }
            }
        }
    }

    /// Start what the master plans next, while there are free processes.
    fn start_transitions(&mut self) {
        let running = read(&self.master).runs().len();
        let free = self.commands.saturating_sub(running);
        if free == 0 {
            return;
        }
        let runs = read(&self.master).plan(free);
        for run in runs {
            let started = fs::write(&run.input_path, &run.input).and_then(|()| {
                process::start(
                    &run.command_words,
                    &run.input_path,
                    run.stdout.as_deref(),
                    &run.stderr,
                    &self.socket,
                )
            });
            match started {
                Ok(process_id) => write(&self.master).start(&run, process_id, master::now_ms()),
                // Not the version's failure: the host could not start a
                // process. It stays where it is, and the next pass tries
                // again.
                Err(error) => {
                    let _ = fs::remove_file(&run.input_path);
                    eprintln!(
                        "pqbench daemon: cannot start `pqbench {}`: {error}",
                        run.transition.command.join(" ")
                    );
                    return;
                }
            }
        }
    }

    /// Start a walk when none runs and one is due: the first since the
    /// daemon started, then one each walk period.
    fn start_walk(&mut self) {
        if self.walk.is_some() {
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
        match self.walk_processes() {
            Ok(walk) => self.walk = Some(walk),
            Err(error) => eprintln!("pqbench daemon: cannot start the walk: {error}"),
        }
        self.walked = true;
        self.walk_time = Some(now);
    }

    /// The walk's pipeline: the whole metastore, from `list catalogs`, or
    /// `list partitions` over the configured Table records instead.
    fn walk_processes(&self) -> std::io::Result<Walk> {
        let directory = self.store.join("walk");
        fs::create_dir_all(&directory)?;
        let error_path = directory.join("walk.err");
        let error = File::create(&error_path)?;
        let stage = |words: &[&str]| words.iter().map(|word| (*word).to_string()).collect();
        let (stages, input): (Vec<Vec<String>>, File) = match &self.settings.walk_tables {
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
        let output = directory.join("partitions.ndjson");
        let process_ids =
            process::start_pipeline(&stages, input, File::create(&output)?, &error, &self.socket)?;
        Ok(Walk {
            process_ids,
            finished: Vec::new(),
            output,
            error: error_path,
        })
    }

    /// Once every process of the walk exited, apply its listing: each listed
    /// table's versions are found or deleted, and when every process
    /// succeeded, the tables the walk did not list are deleted too.
    fn finish_walk(&mut self) {
        let Some(walk) = &mut self.walk else {
            return;
        };
        walk.process_ids
            .retain(|process_id| match process::poll(*process_id) {
                Exit::Running => true,
                Exit::Exited(outcome) => {
                    walk.finished.push(outcome);
                    false
                }
            });
        if !walk.process_ids.is_empty() {
            return;
        }
        let walk = self.walk.take().expect("a walk runs");
        let complete = walk.finished.iter().all(Result::is_ok);
        if !complete {
            let text = fs::read_to_string(&walk.error).unwrap_or_default();
            eprintln!("pqbench daemon: the walk failed: {}", tail(text.trim()));
        }
        let now = master::now_ms();
        let mut found: BTreeMap<String, BTreeSet<master::Key>> = BTreeMap::new();
        let lines = File::open(&walk.output)
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
        if complete {
            let tables: BTreeSet<String> = found.into_keys().collect();
            write(&self.master).retain(&tables);
        }
        let _ = fs::remove_dir_all(self.store.join("walk"));
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
    /// runs, then refresh the stats from the new checkpoint.
    fn checkpoint(&mut self) {
        let idle = self.walk.is_none() && read(&self.master).runs().is_empty();
        if !idle && self.checkpoint_time.elapsed() < CHECKPOINT_PERIOD {
            return;
        }
        self.checkpoint_time = Instant::now();
        if !write(&self.master).take_changed() {
            return;
        }
        match read(&self.master).checkpoint() {
            Ok(path) => self.stats_due = Some(path),
            Err(error) => eprintln!("pqbench daemon: {error}"),
        }
    }

    /// Start the stats over the newest checkpoint, when none run.
    fn start_stats(&mut self) {
        if self.stats.is_some() {
            return;
        }
        let Some(checkpoint) = self.stats_due.take() else {
            return;
        };
        match self.stats_process(&checkpoint) {
            Ok(stats) => self.stats = Some(stats),
            Err(error) => eprintln!("pqbench daemon: cannot start the stats: {error}"),
        }
    }

    /// `summarize partition-versions` over the checkpoint just written; the
    /// open file keeps that checkpoint even when the next one replaces it.
    fn stats_process(&self, checkpoint: &Path) -> std::io::Result<Stats> {
        let error = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.store.join("stats.err"))?;
        let process_ids = process::start_pipeline(
            &[vec!["summarize".into(), "partition-versions".into()]],
            File::open(checkpoint)?,
            File::create(self.store.join("stats.ndjson.tmp"))?,
            &error,
            &self.socket,
        )?;
        Ok(Stats {
            process_id: process_ids[0],
        })
    }

    /// Rename the stats in once their run succeeded.
    fn finish_stats(&mut self) {
        let Some(stats) = &self.stats else {
            return;
        };
        let Exit::Exited(outcome) = process::poll(stats.process_id) else {
            return;
        };
        self.stats = None;
        let temporary = self.store.join("stats.ndjson.tmp");
        match outcome {
            Ok(()) => {
                if let Err(error) = fs::rename(&temporary, self.store.join("stats.ndjson")) {
                    eprintln!("pqbench daemon: cannot rename the stats: {error}");
                }
            }
            Err(status) => {
                let text = fs::read_to_string(self.store.join("stats.err")).unwrap_or_default();
                eprintln!(
                    "pqbench daemon: the stats failed ({status}): {}",
                    tail(text.trim())
                );
                let _ = fs::remove_file(temporary);
            }
        }
    }
}

/// A failed transition's error: the end of its standard error, else how it ended.
fn command_error(run: &master::Running, status: &str) -> String {
    let text = fs::read_to_string(&run.stderr).unwrap_or_default();
    let text = text.trim();
    match text.is_empty() {
        true => format!("`pqbench {}` {status}", run.command.join(" ")),
        false => tail(text),
    }
}

/// The end of a long error text.
fn tail(text: &str) -> String {
    if text.len() <= ERROR_BYTES {
        return text.to_string();
    }
    let mut start = text.len() - ERROR_BYTES;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &text[start..])
}

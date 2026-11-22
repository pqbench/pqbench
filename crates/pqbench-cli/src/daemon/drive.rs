//! The master's driver: runs pqbench commands for the master and hands it
//! their results.
//!
//! One thread makes a pass every [`PASS_PERIOD`]. A pass checks the running
//! commands without waiting on any, gives the master each finished one,
//! starts what the master plans next, and starts a walk when one is due; it
//! also purges, checkpoints, and refreshes the stats. Only the driver changes
//! the map, and each change takes the write lock for one short update.
//!
//! - **Steps**: one command per version (`get partition-price`, `download`,
//!   `bytemass --local`, `list suggestions --local`), at most
//!   [`Settings::processes`] at once, each killed past the time limit.
//! - **The walk**: `list schemas | list tables | get table | list partitions`
//!   from the configured catalogs and schemas, and `list partitions` over
//!   the configured Table records; its listing makes and deletes versions.
//! - **Stats**: `summarize partition-versions` over each new checkpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use super::master::{self, Master, Step};
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
    /// Catalog and schema names the walk starts from.
    pub walk: Vec<String>,
    /// A file of Table records the walk lists as they are.
    pub walk_tables: Option<PathBuf>,
    /// How long after a walk the next one is due; `None` walks once, when the
    /// daemon starts.
    pub walk_period: Option<Duration>,
    /// The steps that run; the others' versions wait.
    pub steps: Vec<&'static Step>,
    /// The most step commands running at once.
    pub processes: usize,
    /// How long a step command may run before it is killed.
    pub time_limit: Option<Duration>,
}

/// A walk in progress: its processes, outputs and error file.
struct Walk {
    process_ids: Vec<u32>,
    finished: Vec<Result<(), String>>,
    outputs: Vec<PathBuf>,
    error: PathBuf,
}

/// The stats run in progress.
struct Stats {
    process_id: u32,
}

struct Driver {
    master: Arc<RwLock<Master>>,
    settings: Settings,
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
        self.finish_steps();
        self.finish_walk();
        self.purge();
        self.start_walk();
        self.start_steps();
        self.checkpoint();
        self.finish_stats();
        self.start_stats();
    }

    /// Give the master each step command that exited, and kill the ones past
    /// the time limit.
    fn finish_steps(&mut self) {
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
                        None => step_error(&run, &status),
                    });
                    write(&self.master).finish(&run.key, outcome, now);
                }
            }
        }
    }

    /// Start what the master plans next, while there are free processes.
    fn start_steps(&mut self) {
        let running = read(&self.master).runs().len();
        let free = self.settings.processes.saturating_sub(running);
        if free == 0 || self.settings.steps.is_empty() {
            return;
        }
        let runs = read(&self.master).plan(&self.settings.steps, free);
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
                        run.step.command.join(" ")
                    );
                    return;
                }
            }
        }
    }

    /// Start a walk when none runs and one is due: the first since the
    /// daemon started, then one each walk period.
    fn start_walk(&mut self) {
        if self.walk.is_some()
            || (self.settings.walk.is_empty() && self.settings.walk_tables.is_none())
        {
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

    /// The walk's pipelines: one from the catalogs, one from the schemas, one
    /// over the Table records.
    fn walk_processes(&self) -> std::io::Result<Walk> {
        let directory = self.store.join("walk");
        fs::create_dir_all(&directory)?;
        let error_path = directory.join("walk.err");
        let error = File::create(&error_path)?;
        let stage = |words: &[&str]| words.iter().map(|word| (*word).to_string()).collect();
        let catalogs: Vec<&String> = self
            .settings
            .walk
            .iter()
            .filter(|name| name.split('/').count() == 2)
            .collect();
        let schemas: Vec<&String> = self
            .settings
            .walk
            .iter()
            .filter(|name| name.split('/').count() == 4)
            .collect();
        let mut pipelines: Vec<(Vec<Vec<String>>, File)> = Vec::new();
        for (roots, stages, name) in [
            (
                catalogs,
                vec![
                    stage(&["list", "schemas"]),
                    stage(&["list", "tables"]),
                    stage(&["get", "table"]),
                    stage(&["list", "partitions"]),
                ],
                "catalogs",
            ),
            (
                schemas,
                vec![
                    stage(&["list", "tables"]),
                    stage(&["get", "table"]),
                    stage(&["list", "partitions"]),
                ],
                "schemas",
            ),
        ] {
            if roots.is_empty() {
                continue;
            }
            let input = directory.join(format!("{name}.in"));
            let lines: String = roots
                .iter()
                .map(|root| format!("{}\n", serde_json::json!({ "name": root })))
                .collect();
            fs::write(&input, lines)?;
            pipelines.push((stages, File::open(input)?));
        }
        if let Some(tables) = &self.settings.walk_tables {
            pipelines.push((vec![stage(&["list", "partitions"])], File::open(tables)?));
        }
        let mut walk = Walk {
            process_ids: Vec::new(),
            finished: Vec::new(),
            outputs: Vec::new(),
            error: error_path,
        };
        for (at, (stages, input)) in pipelines.into_iter().enumerate() {
            let output = directory.join(format!("{at}.ndjson"));
            let file = File::create(&output)?;
            walk.process_ids.extend(process::start_pipeline(
                &stages,
                input,
                file,
                &error,
                &self.socket,
            )?);
            walk.outputs.push(output);
        }
        Ok(walk)
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
        for output in &walk.outputs {
            let Ok(file) = File::open(output) else {
                continue;
            };
            for line in BufReader::new(file).lines().map_while(Result::ok) {
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

/// A failed step's error: the end of its standard error, else how it ended.
fn step_error(run: &master::Running, status: &str) -> String {
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

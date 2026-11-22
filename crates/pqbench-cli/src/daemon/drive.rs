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
use std::io::{BufRead, BufReader, BufWriter, Write};
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

/// A walk in progress: one command per phase, each phase reading the file the
/// one before it wrote.
///
/// `list catalogs`, `list schemas` and `list tables` name every table, in
/// seconds; then the same Table records go through `get table` and `list
/// partitions` in as many streams as there are CPUs, since one stream would
/// pay each table's catalog calls in turn.
struct Walk {
    phase: Phase,
    process_ids: Vec<u32>,
    /// How the phase's commands ended.
    finished: Vec<Result<(), String>>,
    /// The tables `list tables` named: their versions are kept even when
    /// they cannot be read, and the tables it did not name are purged.
    listed: BTreeSet<String>,
    /// Each stream's listing.
    outputs: Vec<PathBuf>,
    /// Where the phase's error text goes.
    error: PathBuf,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Catalogs,
    Schemas,
    Tables,
    Columns,
    Partitions,
}

impl Walk {
    /// Start `phase`'s commands, reading the files the phase before wrote.
    fn start(&mut self, driver: &Driver, phase: Phase) -> Result<(), String> {
        let directory = driver.store.join("walk");
        self.phase = phase;
        self.process_ids.clear();
        self.finished.clear();
        self.error = directory.join(format!("{}.err", phase.name()));
        File::create(&self.error).map_err(|error| error.to_string())?;
        let (words, inputs, prefix): (Vec<&str>, Vec<PathBuf>, &str) = match phase {
            Phase::Catalogs => (
                vec!["list", "catalogs"],
                vec![PathBuf::from("/dev/null")],
                "catalogs",
            ),
            Phase::Schemas => (
                vec!["list", "schemas"],
                vec![directory.join("catalogs.ndjson")],
                "schemas",
            ),
            Phase::Tables => (
                vec!["list", "tables"],
                vec![directory.join("schemas.ndjson")],
                "tables",
            ),
            Phase::Columns => (vec!["get", "table"], driver.chunks(&directory)?, "columns"),
            Phase::Partitions => (
                vec!["list", "partitions"],
                partition_inputs(&directory)?,
                "partitions",
            ),
        };
        let mut outputs = Vec::new();
        for (at, input) in inputs.iter().enumerate() {
            // One input is one file the next phase reads by name; a stream
            // per CPU is numbered.
            let output = match inputs.len() {
                1 => directory.join(format!("{prefix}.ndjson")),
                _ => directory.join(format!("{prefix}-{at}.ndjson")),
            };
            let process_id = driver.run_command(&words, input, &output, &self.error)?;
            self.process_ids.push(process_id);
            outputs.push(output);
        }
        self.outputs = outputs;
        Ok(())
    }
}

impl Phase {
    /// The phase's name, in the store's files.
    fn name(self) -> &'static str {
        match self {
            Phase::Catalogs => "catalogs",
            Phase::Schemas => "schemas",
            Phase::Tables => "tables",
            Phase::Columns => "columns",
            Phase::Partitions => "partitions",
        }
    }
}

/// The files the Partitions phase reads: the Columns phase's records, or the
/// chunks themselves when the operator's Table records are already complete.
fn partition_inputs(directory: &Path) -> Result<Vec<PathBuf>, String> {
    let mut columns = streams(directory, "columns")?;
    if columns.is_empty() {
        columns = streams(directory, "chunk")?;
    }
    Ok(columns)
}

/// The `{prefix}-{n}.ndjson` files in a directory, in number order.
fn streams(directory: &Path, prefix: &str) -> Result<Vec<PathBuf>, String> {
    let mut paths: Vec<PathBuf> = fs::read_dir(directory)
        .map_err(|error| error.to_string())?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.strip_prefix(prefix)
                        .and_then(|rest| rest.strip_prefix('-'))
                        .and_then(|rest| rest.strip_suffix(".ndjson"))
                        .is_some_and(|number| number.parse::<usize>().is_ok())
                })
        })
        .collect();
    paths.sort_by_key(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.rsplit_once('-'))
            .and_then(|(_, rest)| rest.strip_suffix(".ndjson"))
            .and_then(|number| number.parse::<usize>().ok())
            .unwrap_or(0)
    });
    Ok(paths)
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

/// Start the driver on the daemon's runtime: one thread, like the rest of
/// the daemon. A pass does short blocking file work (a checkpoint, a
/// spawn), which the socket and the web app wait out.
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
    tokio::spawn(async move {
        loop {
            driver.pass();
            tokio::time::sleep(PASS_PERIOD).await;
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
        match self.start_walk_phase() {
            Ok(walk) => self.walk = Some(walk),
            Err(error) => eprintln!("pqbench daemon: cannot start the walk: {error}"),
        }
        self.walked = true;
        self.walk_time = Some(now);
    }

    /// The walk's first phase: `list catalogs` names the catalogs. A
    /// --walk-tables file is its own table listing, so the walk starts at the
    /// columns of those records.
    fn start_walk_phase(&self) -> Result<Walk, String> {
        let directory = self.store.join("walk");
        fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
        let mut walk = Walk {
            phase: Phase::Catalogs,
            process_ids: Vec::new(),
            finished: Vec::new(),
            listed: BTreeSet::new(),
            outputs: Vec::new(),
            error: directory.join("walk.err"),
        };
        if let Some(tables) = &self.settings.walk_tables {
            let output = directory.join("tables.ndjson");
            fs::copy(tables, &output).map_err(|error| error.to_string())?;
            walk.listed = table_names(&output)?;
            // The records are complete, so `list partitions` reads them as
            // they are: no `get table` between.
            for (at, chunk) in self.chunks(&directory)?.into_iter().enumerate() {
                fs::rename(&chunk, directory.join(format!("columns-{at}.ndjson")))
                    .map_err(|error| error.to_string())?;
            }
            walk.start(self, Phase::Partitions)?;
            return Ok(walk);
        }
        walk.start(self, Phase::Catalogs)?;
        Ok(walk)
    }

    /// Run one command on one file.
    fn run_command(
        &self,
        words: &[&str],
        input: &Path,
        output: &Path,
        error: &Path,
    ) -> Result<u32, String> {
        let arguments: Vec<String> = words.iter().map(|word| (*word).to_string()).collect();
        process::start(&arguments, input, Some(output), error, &self.socket)
            .map_err(|error| error.to_string())
    }

    /// The chunk files the Columns and Partitions phases read, one per CPU.
    fn chunks(&self, directory: &Path) -> Result<Vec<PathBuf>, String> {
        let tables = directory.join("tables.ndjson");
        let text = fs::read_to_string(&tables).map_err(|error| error.to_string())?;
        let lines: Vec<&str> = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        if lines.is_empty() {
            return Ok(Vec::new());
        }
        let count = self.commands.min(lines.len());
        let mut files: Vec<BufWriter<File>> = Vec::new();
        let mut paths = Vec::new();
        for at in 0..count {
            let path = directory.join(format!("chunk-{at}.ndjson"));
            files.push(BufWriter::new(
                File::create(&path).map_err(|error| error.to_string())?,
            ));
            paths.push(path);
        }
        for (at, line) in lines.iter().enumerate() {
            writeln!(files[at % count], "{line}").map_err(|error| error.to_string())?;
        }
        for mut file in files {
            file.flush().map_err(|error| error.to_string())?;
        }
        Ok(paths)
    }

    /// Check the walk's phase without waiting on any process: when every
    /// command of the phase exited, start the next, or apply the listing. A
    /// table `get table` drops is left alone: its versions are neither found
    /// nor deleted, though purge still drops the tables the walk no longer
    /// lists.
    fn finish_walk(&mut self) {
        let Some(mut walk) = self.walk.take() else {
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
            self.walk = Some(walk);
            return;
        }
        let complete = walk.finished.iter().all(Result::is_ok);
        if !complete {
            // Each failure is one line in the error file, which the operator
            // can read whole; the pass says how many and where.
            let text = fs::read_to_string(&walk.error).unwrap_or_default();
            let failed = text
                .lines()
                .filter(|line| line.starts_with("error"))
                .count();
            eprintln!(
                "pqbench daemon: the walk's {} phase lost {failed} error(s), in {}",
                walk.phase.name(),
                walk.error.display()
            );
        }
        let next = match walk.phase {
            Phase::Catalogs => Some(Phase::Schemas),
            Phase::Schemas => Some(Phase::Tables),
            Phase::Tables if !complete => {
                // The tables are unknown, so no table's versions may be
                // purged: stop here.
                eprintln!(
                    "pqbench daemon: the walk could not list the tables; no versions were purged"
                );
                None
            }
            Phase::Tables => {
                walk.listed = match table_names(&self.store.join("walk/tables.ndjson")) {
                    Ok(listed) => listed,
                    Err(error) => {
                        eprintln!("pqbench daemon: {error}");
                        BTreeSet::new()
                    }
                };
                Some(Phase::Columns)
            }
            Phase::Columns => Some(Phase::Partitions),
            Phase::Partitions => None,
        };
        match next {
            Some(phase) => match walk.start(self, phase) {
                Ok(()) => self.walk = Some(walk),
                Err(error) => {
                    eprintln!("pqbench daemon: cannot start the walk: {error}");
                    let _ = fs::remove_dir_all(self.store.join("walk"));
                }
            },
            None => {
                if walk.phase == Phase::Partitions {
                    self.apply_walk(walk);
                }
                let _ = fs::remove_dir_all(self.store.join("walk"));
            }
        }
    }

    /// Apply the walk's listing, once the Partitions phase is done: each
    /// table's versions are found or deleted, and a walk that listed the
    /// tables purges the ones it no longer lists.
    fn apply_walk(&mut self, walk: Walk) {
        let complete = !walk.listed.is_empty();
        let now = master::now_ms();
        let mut found: BTreeMap<String, BTreeSet<master::Key>> = BTreeMap::new();
        for output in &walk.outputs {
            let lines = File::open(output)
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
        }
        for (table, keys) in &found {
            write(&self.master).missing(table, keys);
        }
        if complete {
            write(&self.master).retain(&walk.listed);
        }
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

/// The names of the tables a listing names.
fn table_names(path: &Path) -> Result<BTreeSet<String>, String> {
    let mut names = BTreeSet::new();
    let text =
        fs::read_to_string(path).map_err(|error| format!("cannot read the tables: {error}"))?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: serde_json::Value =
            serde_json::from_str(line).map_err(|error| format!("bad table line: {error}"))?;
        if let Some(name) = record["name"].as_str() {
            names.insert(name.to_string());
        }
    }
    Ok(names)
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

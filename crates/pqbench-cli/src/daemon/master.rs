//! The master: the map of partition versions, its whole state, and the
//! transitions that move them.
//!
//! The work is done by plain pqbench commands the daemon runs (`drive`); the
//! master decides what runs next for a version and what a finished run means
//! for it. A walk's listing makes versions and deletes the ones it no longer
//! finds; each [`Transition`] takes a version from the state before it, through the
//! transition's running state, to the state after it; purge drops what a walk
//! deleted. Every change is a short update of the map; no command is waited
//! for inside one.
//!
//! The store, which only the master writes:
//!
//! ```text
//! {store}/checkpoint.ndjson                  the map, one PartitionVersion per line
//! {store}/walk/                              the running walk's input and output
//! {store}/results/{partition}/{etag}/{file}  a version's results
//! ```
//!
//! A transition's command writes into its version's directory under the attempt's
//! id (`{file}.{attempt}.tmp`, `{transition}.{attempt}.err`); a success renames the
//! output in, a failure deletes it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use pqbench::v1::partition_version::State;
use pqbench::v1::PartitionVersion;
use serde_json::Value;

/// A version that failed this many times in one transition is left where it is.
const MAX_FAILURES: u64 = 2;

/// Classes whose objects cannot be read without a restore, and the
/// Intelligent-Tiering access tiers that cannot.
const ARCHIVE_CLASSES: &[&str] = &["GLACIER", "DEEP_ARCHIVE"];
const ARCHIVE_TIERS: &[&str] = &["ARCHIVE_ACCESS", "DEEP_ARCHIVE_ACCESS"];

/// What a transition's command reads on standard input.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Input {
    /// The version's `list partitions` record.
    Partition,
    /// The partition's sample file, a TableFile record.
    SampleFile,
}

/// One transition: the command it runs on a version and the states it moves the
/// version through.
pub(crate) struct Transition {
    pub name: &'static str,
    pub source: State,
    pub running: State,
    pub done: State,
    pub command: &'static [&'static str],
    pub input: Input,
    /// The result file the run leaves in the version's directory.
    pub output: &'static str,
    /// Whether the command writes the result to standard output; otherwise it
    /// is given `--output` and writes the file itself.
    pub stdout: bool,
    /// Whether the command reads the downloaded sample with `--local`.
    pub local: bool,
}

const PRICE: Transition = Transition {
    name: "price",
    source: State::Pending,
    running: State::Pricing,
    done: State::Priced,
    command: &["get", "partition-price"],
    input: Input::Partition,
    output: "price.ndjson",
    stdout: true,
    local: false,
};

/// The transitions, in pipeline order: the sample file is copied, and the
/// commands that read its data read the copy.
const TRANSITIONS: &[Transition] = &[
    PRICE,
    Transition {
        name: "download",
        source: State::Priced,
        running: State::Downloading,
        done: State::Downloaded,
        command: &["download"],
        input: Input::SampleFile,
        output: "sample.parquet",
        stdout: false,
        local: false,
    },
    Transition {
        name: "bytemass",
        source: State::Downloaded,
        running: State::Bytemassing,
        done: State::Bytemassed,
        command: &["bytemass"],
        input: Input::SampleFile,
        output: "bytemass.ndjson",
        stdout: true,
        local: true,
    },
    Transition {
        name: "lint",
        source: State::Bytemassed,
        running: State::Linting,
        done: State::UpToDate,
        command: &["list", "suggestions"],
        input: Input::SampleFile,
        output: "list-suggestions.ndjson",
        stdout: true,
        local: true,
    },
];

/// The transitions on a host with little network: no file is copied;
/// bytemass reads only the sample file's footer, in place, and is the last.
const LOW_NETWORK_TRANSITIONS: &[Transition] = &[
    PRICE,
    Transition {
        name: "bytemass",
        source: State::Priced,
        running: State::Bytemassing,
        done: State::UpToDate,
        command: &["bytemass"],
        input: Input::SampleFile,
        output: "bytemass.ndjson",
        stdout: true,
        local: false,
    },
];

/// The transition of `transitions` a version in `state` is running, if any.
fn running_transition(
    transitions: &'static [Transition],
    state: State,
) -> Option<&'static Transition> {
    transitions
        .iter()
        .find(|transition| transition.running == state)
}

/// A transition running on a version: its process, when it started, and where its
/// errors go.
pub(crate) struct Running {
    pub key: Key,
    pub process_id: u32,
    pub start_time: u64,
    pub command: &'static [&'static str],
    pub stderr: PathBuf,
}

/// One run of a transition the master wants started: the command line, what goes
/// on its standard input, and where its output and error go.
pub(crate) struct Run {
    pub key: Key,
    pub transition: &'static Transition,
    pub attempt_id: String,
    pub command_words: Vec<String>,
    pub input: String,
    pub input_path: PathBuf,
    /// Where standard output goes; `None` discards it.
    pub stdout: Option<PathBuf>,
    pub stderr: PathBuf,
}

pub(crate) type Key = (String, String);

pub(crate) struct Master {
    store: PathBuf,
    /// The transitions the master runs: [`LOW_NETWORK_TRANSITIONS`] on a host
    /// with little network, else [`TRANSITIONS`].
    transitions: &'static [Transition],
    /// The versions by partition and etag; a row's `name` is left empty, the
    /// key holds it, and [`version`] puts it back.
    rows: BTreeMap<Key, PartitionVersion>,
    /// The rows a transition is running on: an index of `rows`, not state.
    running: BTreeSet<Key>,
    /// Whether the rows changed since the last checkpoint.
    changed: bool,
    /// Whether a version may be purgeable since the last purge.
    purge_due: bool,
}

impl Master {
    /// Open the store: load the checkpoint, then put back what a crash left.
    /// The commands a version was running died with the old daemon, so the
    /// version goes back to the state before its transition; files of unfinished
    /// attempts, and result directories no row names, are deleted.
    pub(crate) fn open(store: &Path, low_network: bool) -> Result<Self, String> {
        for directory in ["results", "walk"] {
            fs::create_dir_all(store.join(directory))
                .map_err(|error| format!("cannot create {}: {error}", store.display()))?;
        }
        let transitions = if low_network {
            LOW_NETWORK_TRANSITIONS
        } else {
            TRANSITIONS
        };
        let mut master = Master {
            store: store.to_path_buf(),
            transitions,
            rows: read_checkpoint(&store.join("checkpoint.ndjson"))?,
            running: BTreeSet::new(),
            changed: true,
            purge_due: true,
        };
        for row in master.rows.values_mut() {
            if let Some(transition) = running_transition(transitions, row.state) {
                row.state = transition.source;
            }
            clear_attempt(row);
        }
        let _ = fs::remove_dir_all(store.join("walk"));
        fs::create_dir_all(store.join("walk")).map_err(|error| error.to_string())?;
        master.sweep()?;
        Ok(master)
    }

    /// The directory the master keeps its checkpoint and results in.
    pub(crate) fn store(&self) -> &Path {
        &self.store
    }

    /// The versions under `parent`, by reference, in key order: a catalog,
    /// schema, table or partition name, with `-` for any id (AIP-159), or
    /// every version when it is empty.
    pub(crate) fn list(&self, parent: &str) -> Result<Vec<(&Key, &PartitionVersion)>, String> {
        let wanted: Vec<&str> = parent
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        let collections = ["catalogs", "schemas", "tables", "partitions"];
        if !wanted.len().is_multiple_of(2)
            || wanted.len() > 2 * collections.len()
            || wanted
                .iter()
                .step_by(2)
                .zip(collections)
                .any(|(have, want)| *have != want)
        {
            return Err(format!(
                "{parent} is not a catalog, schema, table or partition name"
            ));
        }
        let literal = wanted.iter().take_while(|segment| **segment != "-").count();
        let prefix = wanted[..literal].join("/");
        let under = |name: &str| {
            name.split('/')
                .zip(&wanted)
                .all(|(have, want)| *want == "-" || have == *want)
        };
        Ok(self
            .rows
            .range((prefix.clone(), String::new())..)
            .take_while(|((name, _), _)| name.starts_with(&prefix))
            .filter(|((name, _), _)| under(name))
            .collect())
    }

    /// The versions under `parent` as the records they are, in key order.
    pub(crate) fn records(&self, parent: &str) -> Result<Vec<PartitionVersion>, String> {
        Ok(self
            .list(parent)?
            .into_iter()
            .map(|(key, row)| version(key, row))
            .collect())
    }

    /// One version's stored results, in the order its transitions wrote
    /// them: its price, each suggestion, each column mass, then the listing.
    /// `name` is a version, or a partition for its newest live version.
    pub(crate) fn results(&self, name: &str) -> Result<Vec<Value>, String> {
        if name.is_empty() {
            return Err("a version or partition name is required".into());
        }
        let partition = match name.split_once("/versions/") {
            Some((partition, _)) => partition.to_string(),
            None => name.to_string(),
        };
        let mut rows = self.records(&partition)?;
        if rows.is_empty() {
            return Err(format!("no such partition: {name}"));
        }
        rows.sort_by_key(|row| std::cmp::Reverse(row.create_time));
        let row = match name.split_once("/versions/") {
            Some((_, etag)) => rows
                .iter()
                .find(|row| row.name.ends_with(&format!("/versions/{etag}")))
                .ok_or_else(|| format!("no such version: {name}"))?,
            None => rows
                .iter()
                .find(|row| row.state != State::Deleted)
                .unwrap_or(&rows[0]),
        };
        let etag = row
            .name
            .rsplit_once("/versions/")
            .map(|(_, etag)| etag.to_string())
            .ok_or_else(|| format!("{} names no version", row.name))?;
        let directory = self.result_dir(&(partition, etag));
        let mut results = Vec::new();
        for (file, all) in [
            ("price.ndjson", false),
            ("list-suggestions.ndjson", true),
            ("bytemass.ndjson", true),
            ("list-partitions.ndjson", false),
        ] {
            let Ok(text) = fs::read_to_string(directory.join(file)) else {
                continue;
            };
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let record: Value = serde_json::from_str(line)
                    .map_err(|error| format!("bad result line in {file}: {error}"))?;
                results.push(record);
                if !all {
                    break;
                }
            }
        }
        if results.is_empty() {
            let state = serde_json::to_value(row.state)
                .ok()
                .and_then(|state| state.as_str().map(str::to_string))
                .unwrap_or_default();
            return Err(format!("{name} has no results yet ({state})"));
        }
        Ok(results)
    }

    /// When the newest walk found anything: the time a walk was last due
    /// from.
    pub(crate) fn last_list_time(&self) -> Option<u64> {
        self.rows.values().filter_map(|row| row.list_time).max()
    }

    /// Apply one line of a walk's listing, a Partition record: a version the
    /// map lacks is a new `PENDING` row with its listing stored (its history
    /// from the partition's newest version, its sizes and newest file's time
    /// from the listing), a `DELETED` one found again is `PENDING`, and every
    /// found version is stamped with the walk's time. Returns its key.
    pub(crate) fn found(&mut self, line: &str, now: u64) -> Result<Key, String> {
        let record: Value =
            serde_json::from_str(line).map_err(|error| format!("bad listing line: {error}"))?;
        let (Some(name), Some(etag)) = (record["name"].as_str(), record["etag"].as_str()) else {
            return Err("a listing line has no name or etag".into());
        };
        if !valid_key(name, etag) {
            return Err(format!("{name} at {etag} is not a partition version"));
        }
        let key = (name.to_string(), etag.to_string());
        match self.rows.get_mut(&key) {
            Some(row) => {
                if row.state == State::Deleted {
                    row.state = State::Pending;
                    row.failure_count = 0;
                    row.error = None;
                }
                row.list_time = Some(now);
            }
            None => {
                self.write_result(&key, "list-partitions.ndjson", line)?;
                let row = PartitionVersion {
                    list_time: Some(now),
                    file_count: record["file_count"].as_u64().unwrap_or(0),
                    size_bytes: record["size_bytes"].as_u64().unwrap_or(0),
                    removed_size_bytes: record["removed_size_bytes"].as_u64(),
                    last_modification_time: record["last_file"]["modification_time"].as_u64(),
                    ..self.new_version(name, now)
                };
                self.rows.insert(key.clone(), row);
            }
        }
        self.changed = true;
        Ok(key)
    }

    /// After a table's listing: its live versions the listing did not name
    /// are `DELETED`.
    pub(crate) fn missing(&mut self, table: &str, found: &BTreeSet<Key>) {
        let prefix = table_prefix(table);
        let gone: Vec<Key> = self
            .rows
            .range((prefix.clone(), String::new())..)
            .take_while(|((name, _), _)| name.starts_with(&prefix))
            .filter(|(key, row)| row.state != State::Deleted && !found.contains(*key))
            .map(|(key, _)| key.clone())
            .collect();
        for key in &gone {
            self.delete_version(key);
        }
    }

    /// After a walk that read every table: the live versions of the tables it
    /// did not list are `DELETED`.
    pub(crate) fn retain(&mut self, tables: &BTreeSet<String>) {
        let gone: Vec<Key> = self
            .rows
            .iter()
            .filter(|((name, _), row)| {
                row.state != State::Deleted && !tables.contains(parent_table(name))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in &gone {
            self.delete_version(key);
        }
    }

    /// The runs to start next: up to `limit` versions waiting for a
    /// transition, the later transitions first, so a version in progress
    /// finishes before a new one starts. A version that failed its
    /// transition twice waits.
    pub(crate) fn plan(&self, limit: usize) -> Vec<Run> {
        let mut runs = Vec::new();
        for transition in self.transitions.iter().rev() {
            for (key, row) in &self.rows {
                if runs.len() >= limit {
                    return runs;
                }
                if row.state != transition.source || row.failure_count >= MAX_FAILURES {
                    continue;
                }
                if let Some(run) = self.run(key, transition) {
                    runs.push(run);
                }
            }
        }
        runs
    }

    /// The run of `transition` on one version: its command line and files.
    fn run(&self, key: &Key, transition: &'static Transition) -> Option<Run> {
        let directory = self.result_dir(key);
        let listing = fs::read_to_string(directory.join("list-partitions.ndjson")).ok()?;
        let input = match transition.input {
            Input::Partition => listing.trim().to_string(),
            Input::SampleFile => {
                let record: Value = serde_json::from_str(listing.trim()).ok()?;
                record.get("sample_file")?.to_string()
            }
        };
        let attempt_id = attempt_id();
        let temporary = directory.join(format!("{}.{attempt_id}.tmp", transition.output));
        let mut command_words: Vec<String> =
            transition.command.iter().map(|arg| (*arg).into()).collect();
        if !transition.stdout {
            command_words.push("--output".into());
            command_words.push(temporary.to_string_lossy().into_owned());
        }
        if transition.local {
            command_words.push("--local".into());
            command_words.push(
                directory
                    .join("sample.parquet")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        Some(Run {
            key: key.clone(),
            transition,
            input: input + "\n",
            input_path: directory.join(format!("{}.{attempt_id}.in", transition.name)),
            stdout: transition.stdout.then_some(temporary),
            stderr: directory.join(format!("{}.{attempt_id}.err", transition.name)),
            command_words,
            attempt_id,
        })
    }

    /// A run started: the version is in the transition's running state, with its
    /// process, attempt and start time.
    pub(crate) fn start(&mut self, run: &Run, process_id: u32, now: u64) {
        let Some(row) = self.rows.get_mut(&run.key) else {
            return;
        };
        row.state = run.transition.running;
        row.process_id = Some(process_id);
        row.attempt_id = Some(run.attempt_id.clone());
        row.start_time = Some(now);
        self.running.insert(run.key.clone());
        self.changed = true;
    }

    /// The versions a transition is running on.
    pub(crate) fn runs(&self) -> Vec<Running> {
        self.running
            .iter()
            .filter_map(|key| {
                let row = self.rows.get(key)?;
                let transition = running_transition(self.transitions, row.state)?;
                let attempt = row.attempt_id.as_deref()?;
                Some(Running {
                    key: key.clone(),
                    process_id: row.process_id?,
                    start_time: row.start_time?,
                    command: transition.command,
                    stderr: self
                        .result_dir(key)
                        .join(format!("{}.{attempt}.err", transition.name)),
                })
            })
            .collect()
    }

    /// A run's process exited: `Ok` when it succeeded, else its error. A
    /// success renames the output in and moves the version on: a price whose
    /// partition has no live file, or whose files need a restore, ends the
    /// version, since there is nothing to read, and a version that is done
    /// drops its copy of the sample file. A failure puts the version back
    /// with the error counted.
    pub(crate) fn finish(&mut self, key: &Key, outcome: Result<(), String>, now: u64) {
        self.running.remove(key);
        let directory = self.result_dir(key);
        let transitions = self.transitions;
        let Some(row) = self.rows.get_mut(key) else {
            return;
        };
        let (Some(transition), Some(attempt)) = (
            running_transition(transitions, row.state),
            row.attempt_id.clone(),
        ) else {
            return;
        };
        let temporary = directory.join(format!("{}.{attempt}.tmp", transition.output));
        let landed = directory.join(transition.output);
        let _ = fs::remove_file(directory.join(format!("{}.{attempt}.in", transition.name)));
        let _ = fs::remove_file(directory.join(format!("{}.{attempt}.err", transition.name)));
        clear_attempt(row);
        let outcome = outcome.and_then(|()| {
            fs::rename(&temporary, &landed).map_err(|error| {
                format!("the {} transition left no output: {error}", transition.name)
            })
        });
        match outcome {
            Ok(()) => {
                let mut next = transition.done;
                if transition.running == State::Pricing {
                    (row.lowest_cost_per_month, row.highest_cost_per_month) = cost_bounds(&landed);
                    if row.file_count == 0 || !readable(&landed) {
                        next = State::UpToDate;
                    }
                }
                row.state = next;
                row.failure_count = 0;
                row.error = None;
                if next == State::UpToDate {
                    row.up_to_date_time = Some(now);
                    self.purge_due = true;
                    // The copy was for the transitions that read data; they are
                    // done, and a sample file can be large.
                    let _ = fs::remove_file(directory.join("sample.parquet"));
                }
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                row.state = transition.source;
                row.failure_count += 1;
                row.error = Some(error);
            }
        }
        self.changed = true;
    }

    /// Drop every `DELETED` version except a kept one, and return the result
    /// directories to delete: a partition's newest version that reached
    /// `UP_TO_DATE` stays while a newer version of it is live and not
    /// `UP_TO_DATE` yet, so its complete results are shown until the new ones
    /// replace them. Nothing is walked unless a version may have become
    /// purgeable since the last purge.
    pub(crate) fn purge(&mut self) -> Vec<PathBuf> {
        if !self.purge_due {
            return Vec::new();
        }
        self.purge_due = false;
        let mut by_name: BTreeMap<&str, Vec<(&Key, &PartitionVersion)>> = BTreeMap::new();
        for (key, row) in &self.rows {
            by_name.entry(key.0.as_str()).or_default().push((key, row));
        }
        let mut purged = Vec::new();
        for versions in by_name.values() {
            let kept = kept_version(versions);
            purged.extend(
                versions
                    .iter()
                    .filter(|(key, row)| row.state == State::Deleted && Some(*key) != kept)
                    .map(|(key, _)| (*key).clone()),
            );
        }
        let directories = purged.iter().map(|key| self.result_dir(key)).collect();
        for key in &purged {
            self.rows.remove(key);
        }
        if !purged.is_empty() {
            self.changed = true;
        }
        directories
    }

    /// Whether the rows changed since the last call.
    pub(crate) fn take_changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }

    /// Write every row to the checkpoint: a temporary file, synced, then
    /// renamed over it. Returns its path.
    pub(crate) fn checkpoint(&self) -> Result<PathBuf, String> {
        let path = self.store.join("checkpoint.ndjson");
        let temporary = path.with_extension("ndjson.tmp");
        let write = || -> std::io::Result<()> {
            let mut file = BufWriter::new(File::create(&temporary)?);
            for (key, row) in &self.rows {
                serde_json::to_writer(&mut file, &version(key, row))?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
            file.get_ref().sync_all()?;
            fs::rename(&temporary, &path)
        };
        write().map_err(|error| format!("cannot write the checkpoint: {error}"))?;
        Ok(path)
    }

    fn new_version(&self, name: &str, now: u64) -> PartitionVersion {
        let newest = self
            .rows
            .range((name.to_string(), String::new())..)
            .take_while(|((other, _), _)| other == name)
            .map(|(_, row)| row)
            .max_by_key(|row| row.create_time);
        PartitionVersion {
            state: State::Pending,
            create_time: now,
            first_listed_time: newest.map_or(now, |row| row.first_listed_time),
            etag_change_count: newest.map_or(0, |row| row.etag_change_count + 1),
            ..PartitionVersion::default()
        }
    }

    /// A version a walk no longer finds: a run on it is left to finish, and
    /// its result is then dropped with the version.
    fn delete_version(&mut self, key: &Key) {
        let transitions = self.transitions;
        let row = self.rows.get_mut(key).expect("the key exists");
        if running_transition(transitions, row.state).is_none() {
            row.state = State::Deleted;
        } else {
            // The run's finish moves it on; the next walk deletes it then.
            return;
        }
        self.purge_due = true;
        self.changed = true;
    }

    fn result_dir(&self, key: &Key) -> PathBuf {
        self.store.join("results").join(&key.0).join(&key.1)
    }

    /// Write one result line through a temporary file and a rename.
    fn write_result(&self, key: &Key, output: &str, line: &str) -> Result<(), String> {
        let directory = self.result_dir(key);
        fs::create_dir_all(&directory)
            .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
        let target = directory.join(output);
        let temporary = directory.join(format!("{output}.tmp"));
        fs::write(&temporary, format!("{line}\n"))
            .and_then(|()| fs::rename(&temporary, &target))
            .map_err(|error| format!("cannot write {}: {error}", target.display()))
    }

    /// Delete the result directories no row names, and the files unfinished
    /// attempts left beside any result.
    fn sweep(&self) -> Result<(), String> {
        let results = self.store.join("results");
        for directory in version_dirs(&results) {
            let Ok(relative) = directory.strip_prefix(&results) else {
                continue;
            };
            let etag = relative
                .file_name()
                .map(|etag| etag.to_string_lossy().into_owned())
                .unwrap_or_default();
            let name = relative
                .parent()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            if !self.rows.contains_key(&(name, etag)) {
                remove_dir(&directory)?;
                continue;
            }
            for file in fs::read_dir(&directory).into_iter().flatten().flatten() {
                let unfinished = file.path().extension().is_some_and(|extension| {
                    extension == "tmp" || extension == "err" || extension == "in"
                });
                if unfinished {
                    let _ = fs::remove_file(file.path());
                }
            }
        }
        Ok(())
    }
}

fn clear_attempt(row: &mut PartitionVersion) {
    row.process_id = None;
    row.attempt_id = None;
    row.start_time = None;
}

/// A fresh attempt id: 64 random bits as hex.
fn attempt_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos()),
    );
    format!("{:016x}", hasher.finish())
}

/// The table a partition name is under.
fn parent_table(name: &str) -> &str {
    name.split_once("/partitions/")
        .map_or(name, |(table, _)| table)
}

/// The price a result file holds, if it holds one.
fn read_price(price: &Path) -> Option<pqbench::v1::PartitionPrice> {
    let text = fs::read_to_string(price).ok()?;
    serde_json::from_str(text.lines().next()?).ok()
}

/// The cheapest and dearest monthly cost a price result names.
fn cost_bounds(price: &Path) -> (Option<f64>, Option<f64>) {
    let costs: Vec<f64> = read_price(price)
        .map(|price| {
            price
                .storage_costs
                .iter()
                .map(|cost| cost.cost_per_month)
                .collect()
        })
        .unwrap_or_default();
    (
        costs.iter().copied().reduce(f64::min),
        costs.iter().copied().reduce(f64::max),
    )
}

/// Whether a price's files can be read without a restore: none is in an
/// archive class or access tier. Files with no class (not S3 objects) are
/// readable.
fn readable(price: &Path) -> bool {
    read_price(price).is_some_and(|price| {
        let archived = |class: &str| ARCHIVE_CLASSES.contains(&class);
        !price.storage_classes.iter().any(|class| archived(class))
            && !price.storage_costs.iter().any(|cost| {
                archived(&cost.storage_class)
                    || cost
                        .access_tier
                        .as_deref()
                        .is_some_and(|tier| ARCHIVE_TIERS.contains(&tier))
            })
    })
}

/// The newest version of one partition that reached `UP_TO_DATE`, while the
/// partition has a newer live version that has not.
fn kept_version<'a>(versions: &[(&'a Key, &PartitionVersion)]) -> Option<&'a Key> {
    let (key, row) = versions
        .iter()
        .filter(|(_, row)| row.up_to_date_time.is_some())
        .max_by_key(|(_, row)| row.create_time)?;
    let replaced_later = versions.iter().any(|(_, other)| {
        other.create_time > row.create_time
            && !matches!(other.state, State::Deleted | State::UpToDate)
    });
    replaced_later.then_some(*key)
}

/// The `{partition}/{etag}` directories under `results`: a partition name is
/// eight segments (`catalogs/…/partitions/{id}`), then the etag.
fn version_dirs(results: &Path) -> Vec<PathBuf> {
    let mut level = vec![results.to_path_buf()];
    for _ in 0..9 {
        level = level
            .iter()
            .flat_map(|directory| fs::read_dir(directory).into_iter().flatten().flatten())
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
    }
    level
}

pub(crate) fn remove_dir(directory: &Path) -> Result<(), String> {
    match fs::remove_dir_all(directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("cannot delete {}: {error}", directory.display())),
    }
}

fn read_checkpoint(path: &Path) -> Result<BTreeMap<Key, PartitionVersion>, String> {
    let mut rows = BTreeMap::new();
    let Ok(file) = File::open(path) else {
        return Ok(rows);
    };
    for line in BufReader::new(file).lines() {
        let mut row: PartitionVersion =
            serde_json::from_str(&line.map_err(|error| error.to_string())?)
                .map_err(|error| format!("bad checkpoint row: {error}"))?;
        let key = match row.name.rsplit_once("/versions/") {
            Some((name, etag)) if valid_key(name, etag) => (name.to_string(), etag.to_string()),
            _ => return Err(format!("bad checkpoint row: {} names no version", row.name)),
        };
        row.name = String::new();
        rows.insert(key, row);
    }
    Ok(rows)
}

/// A row as the [`PartitionVersion`] it is, its name put back.
pub(crate) fn version(key: &Key, row: &PartitionVersion) -> PartitionVersion {
    PartitionVersion {
        name: format!("{}/versions/{}", key.0, key.1),
        ..row.clone()
    }
}

/// A partition name of eight plain segments and a hex etag: a key that is
/// also a path under `results`, and cannot leave it.
fn valid_key(name: &str, etag: &str) -> bool {
    let segments: Vec<&str> = name.split('/').collect();
    segments.len() == 8
        && segments[6] == "partitions"
        && segments
            .iter()
            .all(|segment| !segment.is_empty() && *segment != "." && *segment != "..")
        && !etag.is_empty()
        && etag.chars().all(|character| character.is_ascii_hexdigit())
}

fn table_prefix(table: &str) -> String {
    format!("{}/partitions/", table.trim_end_matches('/'))
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARTITION: &str = "catalogs/c/schemas/s/tables/t/partitions/day%3D1";

    fn listing(etag: &str, file_count: u64) -> String {
        serde_json::json!({
            "name": PARTITION,
            "etag": etag,
            "file_count": file_count,
            "sample_file": {"name": "catalogs/c/schemas/s/tables/t/files/a.parquet"},
        })
        .to_string()
    }

    /// Price one version with a result naming `class` and `tier`, and return
    /// the state it lands in.
    fn priced(file_count: u64, class: &str, tier: Option<&str>) -> State {
        let directory = tempfile::tempdir().unwrap();
        let mut master = Master::open(directory.path(), false).unwrap();
        let key = master.found(&listing("aa", file_count), 1).unwrap();
        let run = master.plan(1).remove(0);
        master.start(&run, 1, 2);
        let price = serde_json::json!({
            "name": format!("{PARTITION}/price"),
            "storage_classes": [class],
            "storage_costs": [{"storage_class": class, "access_tier": tier,
                "dollars_per_gibibyte_per_month": 0.023, "cost_per_month": 1.0}],
        });
        fs::write(run.stdout.unwrap(), format!("{price}\n")).unwrap();
        master.finish(&key, Ok(()), 3);
        assert_eq!(master.rows[&key].highest_cost_per_month, Some(1.0));
        master.rows[&key].state
    }

    /// A price ends the version when there is nothing to read: no live file,
    /// or a class or access tier that needs a restore.
    #[test]
    fn a_price_ends_the_version_when_there_is_nothing_to_read() {
        assert_eq!(priced(1, "STANDARD_IA", None), State::Priced);
        assert_eq!(priced(1, "GLACIER_IR", None), State::Priced);
        assert_eq!(priced(1, "GLACIER", None), State::UpToDate);
        assert_eq!(priced(1, "DEEP_ARCHIVE", None), State::UpToDate);
        assert_eq!(
            priced(1, "INTELLIGENT_TIERING", Some("FREQUENT_ACCESS")),
            State::Priced
        );
        assert_eq!(
            priced(1, "INTELLIGENT_TIERING", Some("ARCHIVE_ACCESS")),
            State::UpToDate
        );
        assert_eq!(priced(0, "STANDARD", None), State::UpToDate);
    }

    /// A failed run puts the version back with its error, and after two it
    /// is not planned again.
    #[test]
    fn a_version_that_failed_twice_waits() {
        let directory = tempfile::tempdir().unwrap();
        let mut master = Master::open(directory.path(), false).unwrap();
        let key = master.found(&listing("aa", 1), 1).unwrap();
        for attempt in 0..2 {
            let run = master.plan(1).remove(0);
            master.start(&run, 1, 2);
            master.finish(&key, Err(format!("boom {attempt}")), 3);
            assert_eq!(master.rows[&key].state, State::Pending);
        }
        assert_eq!(master.rows[&key].failure_count, 2);
        assert_eq!(master.rows[&key].error.as_deref(), Some("boom 1"));
        assert!(master.plan(1).is_empty());
    }

    /// A restart puts a running version back, and deletes its attempt's files.
    #[test]
    fn a_restart_puts_running_versions_back() {
        let directory = tempfile::tempdir().unwrap();
        let mut master = Master::open(directory.path(), false).unwrap();
        let key = master.found(&listing("aa", 1), 1).unwrap();
        let run = master.plan(1).remove(0);
        master.start(&run, 1, 2);
        let output = run.stdout.clone().unwrap();
        fs::write(&output, "{}\n").unwrap();
        master.checkpoint().unwrap();
        drop(master);
        let master = Master::open(directory.path(), false).unwrap();
        assert_eq!(master.rows[&key].state, State::Pending);
        assert!(master.rows[&key].process_id.is_none());
        assert!(!output.exists());
    }

    /// A listing line that would name a path outside the store is refused.
    #[test]
    fn a_listing_that_leaves_the_store_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let mut master = Master::open(directory.path(), false).unwrap();
        let line = serde_json::json!({
            "name": "catalogs/c/schemas/s/tables/t/partitions/..",
            "etag": "aa",
        });
        assert!(master.found(&line.to_string(), 1).is_err());
        assert!(master.records("").unwrap().is_empty());
    }
}

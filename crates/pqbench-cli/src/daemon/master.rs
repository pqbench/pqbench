//! The master: one map of partition versions, kept in memory, and the calls
//! that move them between states.
//!
//! A row is a partition at one etag; a change to a partition is a new row and
//! the old one goes to `DELETED`. Every call is one atomic step: the master
//! takes one call at a time, renames the files it names, changes the rows,
//! and appends the changed rows to the edit log before it answers. The full
//! image is rewritten every hour or every million edits, and on stop (HDFS
//! keeps its namespace the same way); a restart loads the image and replays
//! the edits after it.
//!
//! The store, which only the master writes:
//!
//! ```text
//! {store}/master/image.ndjson     the map at one edit: a {"seq"} line, then the rows
//! {store}/master/edits.ndjson     the rows each later call changed
//! {store}/scan/{table}.ndjson     a scan job's listing of one table, before `scan`
//! {store}/results/{partition}/{etag}/{output}.ndjson
//! ```
//!
//! A job writes a command's standard output to a path the master gave it
//! (`….tmp` next to the result, or the scan file) and the master renames it
//! in, inside the commit, only while the row is still in the job's state.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How long the edit log may grow before the image is rewritten.
const IMAGE_PERIOD: Duration = Duration::from_secs(3600);

/// How many edits the log may hold before the image is rewritten.
const IMAGE_EDITS: u64 = 1_000_000;

/// A version that failed this many times in one step is left out of claims.
const MAX_FAILURES: u64 = 2;

/// Where a partition version is in the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum State {
    /// The scan no longer finds this version; its results wait for purge.
    Deleted,
    /// Found, not processed yet.
    Pending,
    Pricing,
    Priced,
    Bytemassing,
    Bytemassed,
    Linting,
    /// Every step done for this version.
    UpToDate,
}

impl State {
    /// The result a job in this state writes, by the command that writes it.
    fn output(self) -> Option<&'static str> {
        match self {
            State::Pricing => Some("price"),
            State::Bytemassing => Some("bytemass"),
            State::Linting => Some("list-suggestions"),
            _ => None,
        }
    }

    /// The state a version in this job state goes back to when the job
    /// fails or dies.
    fn source(self) -> Option<State> {
        match self {
            State::Pricing => Some(State::Pending),
            State::Bytemassing => Some(State::Priced),
            State::Linting => Some(State::Bytemassed),
            _ => None,
        }
    }
}

/// One partition version's row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Row {
    pub state: State,
    /// When the scan found this version, milliseconds since the Unix epoch.
    pub create_time: u64,
    /// When this version reached `UP_TO_DATE`.
    // aipnaming: allow(aip-140/prepositions)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub up_to_date_time: Option<u64>,
    /// The partition's first scan, carried from version to version.
    pub first_listed_time: u64,
    /// The partition's versions before this one.
    pub etag_change_count: u64,
    /// Failures of the current step on this version.
    pub failure_count: u64,
    /// The last failure's error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A row with its key, as the store and the socket carry it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub name: String,
    pub etag: String,
    #[serde(flatten)]
    pub row: Row,
}

/// One version a claim handed to a job: its listing, and where to write the
/// command's output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Claim {
    pub name: String,
    pub etag: String,
    /// The version's `list partitions` record.
    pub partition: Value,
    /// The version's other results so far, by output name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub results: BTreeMap<String, Value>,
    /// Where the job writes the command's standard output.
    pub path: PathBuf,
}

/// The rows one call changed, as one line of the edit log.
#[derive(Default, Serialize, Deserialize)]
struct Edit {
    seq: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    set: Vec<Entry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    remove: Vec<(String, String)>,
}

type Key = (String, String);

pub(crate) struct Master {
    store: PathBuf,
    rows: BTreeMap<Key, Row>,
    edit_log: BufWriter<File>,
    seq: u64,
    image_seq: u64,
    image_time: Instant,
}

impl Master {
    /// Open the store: load the image, replay the edits after it, then put
    /// back what a crash may have left. Rows in a job state return to the
    /// state before it (no job holds a claim across a master restart), and
    /// temporary files and result directories without a row are deleted.
    pub(crate) fn open(store: &Path) -> Result<Self, String> {
        for directory in ["master", "scan", "results"] {
            fs::create_dir_all(store.join(directory))
                .map_err(|error| format!("cannot create {}: {error}", store.display()))?;
        }
        let (mut rows, image_seq) = read_image(&store.join("master/image.ndjson"))?;
        let seq = replay(&store.join("master/edits.ndjson"), image_seq, &mut rows)?;
        let edits = OpenOptions::new()
            .create(true)
            .append(true)
            .open(store.join("master/edits.ndjson"))
            .map_err(|error| format!("cannot open the edit log: {error}"))?;
        let mut master = Master {
            store: store.to_path_buf(),
            rows,
            edit_log: BufWriter::new(edits),
            seq,
            image_seq,
            image_time: Instant::now(),
        };
        for state in [State::Pricing, State::Bytemassing, State::Linting] {
            master.revert(state)?;
        }
        let _ = fs::remove_dir_all(store.join("scan"));
        fs::create_dir_all(store.join("scan")).map_err(|error| error.to_string())?;
        master.sweep()?;
        master.write_image()?;
        Ok(master)
    }

    /// The directory the master keeps its image, edits and results in.
    pub(crate) fn store(&self) -> &Path {
        &self.store
    }

    /// Every row, or the rows of one table, in key order.
    pub(crate) fn list(&self, table: Option<&str>) -> Vec<Entry> {
        let prefix = table.map(table_prefix);
        self.rows
            .iter()
            .filter(|((name, _), _)| {
                prefix
                    .as_ref()
                    .is_none_or(|prefix| name.starts_with(prefix))
            })
            .map(|((name, etag), row)| entry(name, etag, row))
            .collect()
    }

    /// Where a scan job writes `table`'s `list partitions` output.
    pub(crate) fn scan_path(&self, table: &str) -> PathBuf {
        self.store
            .join("scan")
            .join(format!("{}.ndjson", escape(table)))
    }

    /// Apply one table's listing, the scan file the job wrote: a version the
    /// listing names and the map lacks is a new `PENDING` row (its history
    /// from the partition's newest version), a `DELETED` one found again is
    /// `PENDING`, and any other row of the table it does not name is
    /// `DELETED`. Every found version's listing line is renamed in.
    pub(crate) fn scan(&mut self, table: &str) -> Result<usize, String> {
        let path = self.scan_path(table);
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("cannot read the scan of {table}: {error}"))?;
        let _ = fs::remove_file(&path);
        let prefix = table_prefix(table);
        let now = now_ms();
        let mut found: BTreeSet<Key> = BTreeSet::new();
        let mut edit = Edit::default();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let record: Value =
                serde_json::from_str(line).map_err(|error| format!("bad listing line: {error}"))?;
            let (Some(name), Some(etag)) = (record["name"].as_str(), record["etag"].as_str())
            else {
                return Err("a listing line has no name or etag".into());
            };
            if !name.starts_with(&prefix) || !valid_key(name, etag) {
                return Err(format!("{name} at {etag} is not a partition of {table}"));
            }
            let key = (name.to_string(), etag.to_string());
            self.write_result(&key, "list-partitions", line)?;
            let row = match self.rows.get(&key) {
                Some(row) if row.state == State::Deleted => Some(Row {
                    state: State::Pending,
                    failure_count: 0,
                    error: None,
                    ..row.clone()
                }),
                Some(_) => None,
                None => Some(self.new_version(name, now)),
            };
            if let Some(row) = row {
                self.rows.insert(key.clone(), row.clone());
                edit.set.push(entry(&key.0, &key.1, &row));
            }
            found.insert(key);
        }
        let missing: Vec<Key> = self
            .rows
            .iter()
            .filter(|((name, _), row)| name.starts_with(&prefix) && row.state != State::Deleted)
            .map(|(key, _)| key.clone())
            .filter(|key| !found.contains(key))
            .collect();
        for key in missing {
            self.delete_version(&key, &mut edit);
        }
        self.log(edit)?;
        Ok(found.len())
    }

    /// After a complete catalog walk: every row of a table not in `tables`
    /// is `DELETED`.
    pub(crate) fn retain_tables(&mut self, tables: &[String]) -> Result<usize, String> {
        let prefixes: Vec<String> = tables.iter().map(|table| table_prefix(table)).collect();
        let gone: Vec<Key> = self
            .rows
            .iter()
            .filter(|((name, _), row)| {
                row.state != State::Deleted
                    && !prefixes.iter().any(|prefix| name.starts_with(prefix))
            })
            .map(|(key, _)| key.clone())
            .collect();
        let mut edit = Edit::default();
        for key in &gone {
            self.delete_version(key, &mut edit);
        }
        self.log(edit)?;
        Ok(gone.len())
    }

    /// Move up to `limit` rows from `from` to the job state `to` and hand
    /// them out, each with its listing and the path for its output. A row
    /// that failed [`MAX_FAILURES`] times stays where it is.
    pub(crate) fn claim(
        &mut self,
        from: State,
        to: State,
        limit: usize,
    ) -> Result<Vec<Claim>, String> {
        if to.source() != Some(from) {
            return Err(format!("cannot claim {from:?} into {to:?}"));
        }
        let output = to.output().expect("a job state has an output");
        let keys: Vec<Key> = self
            .rows
            .iter()
            .filter(|(_, row)| row.state == from && row.failure_count < MAX_FAILURES)
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect();
        let mut claims = Vec::with_capacity(keys.len());
        let mut edit = Edit::default();
        for key in keys {
            let directory = self.result_dir(&key);
            let mut results = BTreeMap::new();
            for name in ["list-partitions", "price", "bytemass", "list-suggestions"] {
                if let Ok(text) = fs::read_to_string(directory.join(format!("{name}.ndjson"))) {
                    if let Some(Ok(value)) = text.lines().next().map(serde_json::from_str::<Value>)
                    {
                        results.insert(name.to_string(), value);
                    }
                }
            }
            let Some(partition) = results.remove("list-partitions") else {
                continue;
            };
            let row = self.rows.get_mut(&key).expect("the claimed key exists");
            row.state = to;
            edit.set.push(entry(&key.0, &key.1, row));
            claims.push(Claim {
                path: directory.join(format!("{output}.ndjson.tmp")),
                name: key.0,
                etag: key.1,
                partition,
                results,
            });
        }
        self.log(edit)?;
        Ok(claims)
    }

    /// Finish one claimed version. While the row is still in the job state
    /// `from`, a success renames the job's output in (when it wrote one) and
    /// moves the row to `to`; an error moves it back to the state before
    /// `from` and counts the failure. Otherwise the call is dropped and the
    /// output deleted. Returns whether it applied.
    pub(crate) fn commit(
        &mut self,
        key: &Key,
        from: State,
        to: State,
        error: Option<String>,
    ) -> Result<bool, String> {
        let output = from
            .output()
            .ok_or_else(|| format!("{from:?} is not a job state"))?;
        let temporary = self.result_dir(key).join(format!("{output}.ndjson.tmp"));
        let current = self.rows.get(key).map(|row| row.state);
        if current != Some(from) {
            let _ = fs::remove_file(&temporary);
            return Ok(false);
        }
        let row = self.rows.get_mut(key).expect("the row exists");
        match error {
            None => {
                if temporary.exists() {
                    let target = temporary.with_extension("");
                    fs::rename(&temporary, &target).map_err(|error| {
                        format!("cannot rename {}: {error}", temporary.display())
                    })?;
                }
                row.state = to;
                row.failure_count = 0;
                row.error = None;
                if to == State::UpToDate {
                    row.up_to_date_time = Some(now_ms());
                }
            }
            Some(error) => {
                let _ = fs::remove_file(&temporary);
                row.state = from.source().expect("a job state has a source");
                row.failure_count += 1;
                row.error = Some(error);
            }
        }
        let edit = Edit {
            set: vec![entry(&key.0, &key.1, row)],
            ..Edit::default()
        };
        self.log(edit)?;
        Ok(true)
    }

    /// Move every row in the job state `state` back to the state before it
    /// and delete its temporary output: what a job does when it starts, since
    /// with one instance at a time only a dead run can have left them.
    pub(crate) fn revert(&mut self, state: State) -> Result<usize, String> {
        let source = state
            .source()
            .ok_or_else(|| format!("{state:?} is not a job state"))?;
        let output = state.output().expect("a job state has an output");
        let keys: Vec<Key> = self
            .rows
            .iter()
            .filter(|(_, row)| row.state == state)
            .map(|(key, _)| key.clone())
            .collect();
        let mut edit = Edit::default();
        for key in &keys {
            let _ = fs::remove_file(self.result_dir(key).join(format!("{output}.ndjson.tmp")));
            let row = self.rows.get_mut(key).expect("the key exists");
            row.state = source;
            edit.set.push(entry(&key.0, &key.1, row));
        }
        self.log(edit)?;
        Ok(keys.len())
    }

    /// Delete every `DELETED` version's results and row, except a kept one:
    /// a partition's newest version that reached `UP_TO_DATE` stays while a
    /// newer version of it is live and not `UP_TO_DATE` yet, so the last
    /// complete results are shown until the new ones replace them.
    pub(crate) fn purge(&mut self) -> Result<usize, String> {
        let mut by_name: BTreeMap<&str, Vec<(&Key, &Row)>> = BTreeMap::new();
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
        let mut edit = Edit::default();
        for key in &purged {
            remove_dir(&self.result_dir(key))?;
            self.rows.remove(key);
            edit.remove.push(key.clone());
        }
        self.log(edit)?;
        Ok(purged.len())
    }

    /// Write the image if the edit log is due, and on stop.
    pub(crate) fn tick(&mut self, stopping: bool) -> Result<(), String> {
        let due =
            self.seq - self.image_seq >= IMAGE_EDITS || self.image_time.elapsed() >= IMAGE_PERIOD;
        if self.seq > self.image_seq && (due || stopping) {
            self.write_image()?;
        }
        Ok(())
    }

    fn new_version(&self, name: &str, now: u64) -> Row {
        let newest = self
            .rows
            .range((name.to_string(), String::new())..)
            .take_while(|((other, _), _)| other == name)
            .map(|(_, row)| row)
            .max_by_key(|row| row.create_time);
        Row {
            state: State::Pending,
            create_time: now,
            up_to_date_time: None,
            first_listed_time: newest.map_or(now, |row| row.first_listed_time),
            etag_change_count: newest.map_or(0, |row| row.etag_change_count + 1),
            failure_count: 0,
            error: None,
        }
    }

    fn delete_version(&mut self, key: &Key, edit: &mut Edit) {
        let row = self.rows.get_mut(key).expect("the key exists");
        if let Some(output) = row.state.output() {
            let _ = fs::remove_file(
                self.store
                    .join("results")
                    .join(&key.0)
                    .join(&key.1)
                    .join(format!("{output}.ndjson.tmp")),
            );
        }
        row.state = State::Deleted;
        edit.set.push(entry(&key.0, &key.1, row));
    }

    fn result_dir(&self, key: &Key) -> PathBuf {
        self.store.join("results").join(&key.0).join(&key.1)
    }

    /// Write one result line through a temporary file and a rename.
    fn write_result(&self, key: &Key, output: &str, line: &str) -> Result<(), String> {
        let directory = self.result_dir(key);
        fs::create_dir_all(&directory)
            .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
        let target = directory.join(format!("{output}.ndjson"));
        let temporary = directory.join(format!("{output}.ndjson.tmp"));
        fs::write(&temporary, format!("{line}\n"))
            .and_then(|()| fs::rename(&temporary, &target))
            .map_err(|error| format!("cannot write {}: {error}", target.display()))
    }

    /// Append one call's changes to the edit log and flush it to disk.
    fn log(&mut self, mut edit: Edit) -> Result<(), String> {
        if edit.set.is_empty() && edit.remove.is_empty() {
            return Ok(());
        }
        self.seq += 1;
        edit.seq = self.seq;
        let mut line = serde_json::to_string(&edit).map_err(|error| error.to_string())?;
        line.push('\n');
        self.edit_log
            .write_all(line.as_bytes())
            .and_then(|()| self.edit_log.flush())
            .and_then(|()| self.edit_log.get_ref().sync_data())
            .map_err(|error| format!("cannot append to the edit log: {error}"))?;
        self.tick(false)
    }

    /// Write the whole map as the image at the current edit, then empty the
    /// edit log: a crash between the two replays nothing the image holds.
    fn write_image(&mut self) -> Result<(), String> {
        let path = self.store.join("master/image.ndjson");
        let temporary = path.with_extension("ndjson.new");
        let write = || -> std::io::Result<()> {
            let mut file = BufWriter::new(File::create(&temporary)?);
            serde_json::to_writer(&mut file, &serde_json::json!({ "seq": self.seq }))?;
            file.write_all(b"\n")?;
            for ((name, etag), row) in &self.rows {
                serde_json::to_writer(&mut file, &entry(name, etag, row))?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
            file.get_ref().sync_all()?;
            fs::rename(&temporary, &path)
        };
        write().map_err(|error| format!("cannot write the image: {error}"))?;
        self.edit_log
            .get_ref()
            .set_len(0)
            .map_err(|error| format!("cannot truncate the edit log: {error}"))?;
        self.image_seq = self.seq;
        self.image_time = Instant::now();
        Ok(())
    }

    /// Delete the result directories no row names, and temporary files left
    /// beside any result.
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
                if file
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "tmp")
                {
                    let _ = fs::remove_file(file.path());
                }
            }
        }
        Ok(())
    }
}

/// The newest version of one partition that reached `UP_TO_DATE`, while the
/// partition has a newer live version that has not.
fn kept_version<'a>(versions: &[(&'a Key, &Row)]) -> Option<&'a Key> {
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

fn remove_dir(directory: &Path) -> Result<(), String> {
    match fs::remove_dir_all(directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("cannot delete {}: {error}", directory.display())),
    }
}

fn read_image(path: &Path) -> Result<(BTreeMap<Key, Row>, u64), String> {
    let mut rows = BTreeMap::new();
    let Ok(file) = File::open(path) else {
        return Ok((rows, 0));
    };
    let mut lines = BufReader::new(file).lines();
    let seq = match lines.next() {
        Some(line) => {
            let header: Value = serde_json::from_str(&line.map_err(|error| error.to_string())?)
                .map_err(|error| format!("bad image header: {error}"))?;
            header["seq"].as_u64().unwrap_or(0)
        }
        None => 0,
    };
    for line in lines {
        let entry: Entry = serde_json::from_str(&line.map_err(|error| error.to_string())?)
            .map_err(|error| format!("bad image row: {error}"))?;
        rows.insert((entry.name, entry.etag), entry.row);
    }
    Ok((rows, seq))
}

/// Apply the edits after `image_seq`; a torn last line (a crash mid-append)
/// was never answered, so it is skipped.
fn replay(path: &Path, image_seq: u64, rows: &mut BTreeMap<Key, Row>) -> Result<u64, String> {
    let mut seq = image_seq;
    let Ok(file) = File::open(path) else {
        return Ok(seq);
    };
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|error| error.to_string())?;
        let Ok(edit) = serde_json::from_str::<Edit>(&line) else {
            break;
        };
        if edit.seq <= image_seq {
            continue;
        }
        for entry in edit.set {
            rows.insert((entry.name, entry.etag), entry.row);
        }
        for key in edit.remove {
            rows.remove(&key);
        }
        seq = edit.seq;
    }
    Ok(seq)
}

fn entry(name: &str, etag: &str, row: &Row) -> Entry {
    Entry {
        name: name.to_string(),
        etag: etag.to_string(),
        row: row.clone(),
    }
}

/// A partition name of eight plain segments and a hex etag: a key that is
/// also a path under `results`, and cannot leave it.
fn valid_key(name: &str, etag: &str) -> bool {
    let segments: Vec<&str> = name.split('/').collect();
    segments.len() == 8
        && segments
            .iter()
            .all(|segment| !segment.is_empty() && *segment != "." && *segment != "..")
        && !etag.is_empty()
        && etag.chars().all(|character| character.is_ascii_hexdigit())
}

fn table_prefix(table: &str) -> String {
    format!("{}/partitions/", table.trim_end_matches('/'))
}

/// A table name as one file name.
fn escape(table: &str) -> String {
    table.replace('%', "%25").replace('/', "%2F")
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = "catalogs/c/schemas/s/tables/t";

    fn scan(master: &mut Master, etag: &str) {
        let line = serde_json::json!({"name": format!("{TABLE}/partitions/day%3D1"), "etag": etag});
        fs::write(master.scan_path(TABLE), format!("{line}\n")).unwrap();
        master.scan(TABLE).unwrap();
    }

    /// A job's output for a version the scan replaced meanwhile is dropped:
    /// not renamed in, and its temporary file deleted.
    #[test]
    fn a_late_commit_for_a_replaced_version_is_dropped() {
        let directory = tempfile::tempdir().unwrap();
        let mut master = Master::open(directory.path()).unwrap();
        scan(&mut master, "aa");
        let claim = master
            .claim(State::Pending, State::Pricing, 10)
            .unwrap()
            .remove(0);
        fs::write(&claim.path, "{}\n").unwrap();
        scan(&mut master, "bb");
        let key = (claim.name.clone(), claim.etag.clone());
        assert!(!master
            .commit(&key, State::Pricing, State::Priced, None)
            .unwrap());
        assert!(!claim.path.exists());
        assert!(!claim.path.with_extension("").exists());
        assert_eq!(master.rows[&key].state, State::Deleted);
    }

    /// A restart returns the rows a job held to the state before it, and
    /// deletes what the job had written.
    #[test]
    fn a_restart_returns_claimed_versions() {
        let directory = tempfile::tempdir().unwrap();
        let mut master = Master::open(directory.path()).unwrap();
        scan(&mut master, "aa");
        let claim = master
            .claim(State::Pending, State::Pricing, 10)
            .unwrap()
            .remove(0);
        fs::write(&claim.path, "{}\n").unwrap();
        drop(master);
        let master = Master::open(directory.path()).unwrap();
        let key = (claim.name, claim.etag);
        assert_eq!(master.rows[&key].state, State::Pending);
        assert!(!claim.path.exists());
    }

    /// A listing line that would name a path outside the store is refused.
    #[test]
    fn a_scan_refuses_a_name_that_leaves_the_store() {
        let directory = tempfile::tempdir().unwrap();
        let mut master = Master::open(directory.path()).unwrap();
        let line = serde_json::json!({"name": format!("{TABLE}/partitions/.."), "etag": "aa"});
        fs::write(master.scan_path(TABLE), format!("{line}\n")).unwrap();
        assert!(master.scan(TABLE).is_err());
        assert!(master.list(None).is_empty());
    }
}

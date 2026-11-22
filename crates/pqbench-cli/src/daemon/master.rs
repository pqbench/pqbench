//! The master: one map of partition versions, kept in memory, and the calls
//! that move them between states.
//!
//! A row is a [`PartitionVersion`]: a partition at one etag; a change to a
//! partition is a new row and the old one goes to `DELETED`. Every call is one atomic step: the master
//! takes one call at a time, renames the files it names, and changes the
//! rows. The map is the state: the daemon writes it whole to a checkpoint
//! every few seconds while it changes, and on stop, and a restart loads the
//! checkpoint. A crash loses the calls since the last checkpoint; the jobs
//! redo that work, and a result they land again replaces the old one.
//!
//! The store, which only the master writes:
//!
//! ```text
//! {store}/master/checkpoint.ndjson  the rows, one PartitionVersion per line
//! {store}/scan/{table}.ndjson       a scan job's listing of one table, before `scan`
//! {store}/results/{partition}/{etag}/{output}.ndjson
//! ```
//!
//! A job writes a command's standard output to a path the master gave it
//! (`….tmp` next to the result, or the scan file) and the master renames it
//! in, inside the commit, only while the row is still in the job's state.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use pqbench::v1::partition_version::State;
use pqbench::v1::PartitionVersion;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A version that failed this many times in one step is left out of claims.
const MAX_FAILURES: u64 = 2;

/// The result a job in `state` writes, by the command that writes it.
fn output(state: State) -> Option<&'static str> {
    match state {
        State::Pricing => Some("price"),
        State::Bytemassing => Some("bytemass"),
        State::Linting => Some("list-suggestions"),
        _ => None,
    }
}

/// The state a version in the job state `state` goes back to when the job
/// fails or dies.
fn source(state: State) -> Option<State> {
    match state {
        State::Pricing => Some(State::Pending),
        State::Bytemassing => Some(State::Priced),
        State::Linting => Some(State::Bytemassed),
        _ => None,
    }
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

pub(crate) type Key = (String, String);

pub(crate) struct Master {
    store: PathBuf,
    /// The versions by partition and etag; a row's `name` is left empty, the
    /// key holds it, and [`version`] puts it back.
    rows: BTreeMap<Key, PartitionVersion>,
    /// Whether the rows changed since the last checkpoint.
    changed: bool,
}

impl Master {
    /// Open the store: load the checkpoint, then put back what a crash may
    /// have left. Rows in a job state return to the
    /// state before it (no job holds a claim across a master restart), and
    /// temporary files and result directories without a row are deleted.
    pub(crate) fn open(store: &Path) -> Result<Self, String> {
        for directory in ["master", "scan", "results"] {
            fs::create_dir_all(store.join(directory))
                .map_err(|error| format!("cannot create {}: {error}", store.display()))?;
        }
        let mut master = Master {
            store: store.to_path_buf(),
            rows: read_checkpoint(&store.join("master/checkpoint.ndjson"))?,
            changed: false,
        };
        for state in [State::Pricing, State::Bytemassing, State::Linting] {
            master.revert(state)?;
        }
        let _ = fs::remove_dir_all(store.join("scan"));
        fs::create_dir_all(store.join("scan")).map_err(|error| error.to_string())?;
        master.sweep()?;
        master.checkpoint()?;
        Ok(master)
    }

    /// The directory the master keeps its checkpoint and results in.
    pub(crate) fn store(&self) -> &Path {
        &self.store
    }

    /// The versions under `parent`, in key order: a catalog, schema, table or
    /// partition name, with `-` for any id (AIP-159), or every version when
    /// it is empty.
    pub(crate) fn list(&self, parent: &str) -> Result<Vec<PartitionVersion>, String> {
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
            .map(|(key, row)| version(key, row))
            .collect())
    }

    /// Every row by reference, in key order: a read that copies nothing.
    pub(crate) fn rows(&self) -> impl Iterator<Item = (&Key, &PartitionVersion)> {
        self.rows.iter()
    }

    /// Where a scan job writes `table`'s `list partitions` output.
    pub(crate) fn scan_path(&self, table: &str) -> PathBuf {
        self.store
            .join("scan")
            .join(format!("{}.ndjson", escape(table)))
    }

    /// Apply one table's listing, the scan file the job wrote: a version the
    /// listing names and the map lacks is a new `PENDING` row (its history
    /// from the partition's newest version, its file count and sizes from the
    /// listing, and its newest file's time), a `DELETED` one found again is
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
                Some(row) if row.state == State::Deleted => Some(PartitionVersion {
                    state: State::Pending,
                    failure_count: 0,
                    error: None,
                    ..row.clone()
                }),
                Some(_) => None,
                None => Some(PartitionVersion {
                    file_count: record["file_count"].as_u64().unwrap_or(0),
                    size_bytes: record["size_bytes"].as_u64().unwrap_or(0),
                    removed_size_bytes: record["removed_size_bytes"].as_u64(),
                    last_modification_time: record["last_file"]["modification_time"].as_u64(),
                    ..self.new_version(name, now)
                }),
            };
            if let Some(row) = row {
                self.rows.insert(key.clone(), row.clone());
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
            self.delete_version(&key);
        }
        self.changed = true;
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
        for key in &gone {
            self.delete_version(key);
        }
        self.changed = true;
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
        if source(to) != Some(from) {
            return Err(format!("cannot claim {from:?} into {to:?}"));
        }
        let output = output(to).expect("a job state has an output");
        let keys: Vec<Key> = self
            .rows
            .iter()
            .filter(|(_, row)| row.state == from && row.failure_count < MAX_FAILURES)
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect();
        let mut claims = Vec::with_capacity(keys.len());
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
            claims.push(Claim {
                path: directory.join(format!("{output}.ndjson.tmp")),
                name: key.0,
                etag: key.1,
                partition,
                results,
            });
        }
        self.changed = true;
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
        let output = output(from).ok_or_else(|| format!("{from:?} is not a job state"))?;
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
                    if from == State::Pricing {
                        (row.lowest_cost_per_month, row.highest_cost_per_month) =
                            cost_bounds(&target);
                    }
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
                row.state = source(from).expect("a job state has a source");
                row.failure_count += 1;
                row.error = Some(error);
            }
        }
        self.changed = true;
        Ok(true)
    }

    /// Move every row in the job state `state` back to the state before it
    /// and delete its temporary output: what a job does when it starts, since
    /// with one instance at a time only a dead run can have left them.
    pub(crate) fn revert(&mut self, state: State) -> Result<usize, String> {
        let source = source(state).ok_or_else(|| format!("{state:?} is not a job state"))?;
        let output = output(state).expect("a job state has an output");
        let keys: Vec<Key> = self
            .rows
            .iter()
            .filter(|(_, row)| row.state == state)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &keys {
            let _ = fs::remove_file(self.result_dir(key).join(format!("{output}.ndjson.tmp")));
            let row = self.rows.get_mut(key).expect("the key exists");
            row.state = source;
        }
        self.changed = true;
        Ok(keys.len())
    }

    /// Delete every `DELETED` version's results and row, except a kept one:
    /// a partition's newest version that reached `UP_TO_DATE` stays while a
    /// newer version of it is live and not `UP_TO_DATE` yet, so the last
    /// complete results are shown until the new ones replace them.
    pub(crate) fn purge(&mut self) -> Result<usize, String> {
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
        for key in &purged {
            remove_dir(&self.result_dir(key))?;
            self.rows.remove(key);
        }
        self.changed = true;
        Ok(purged.len())
    }

    /// Write every row to the checkpoint when they changed since the last
    /// one: a temporary file, synced, then renamed over it.
    pub(crate) fn checkpoint(&mut self) -> Result<(), String> {
        if !self.changed && self.store.join("master/checkpoint.ndjson").exists() {
            return Ok(());
        }
        let path = self.store.join("master/checkpoint.ndjson");
        let temporary = path.with_extension("ndjson.new");
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
        self.changed = false;
        Ok(())
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

    fn delete_version(&mut self, key: &Key) {
        let row = self.rows.get_mut(key).expect("the key exists");
        if let Some(output) = output(row.state) {
            let _ = fs::remove_file(
                self.store
                    .join("results")
                    .join(&key.0)
                    .join(&key.1)
                    .join(format!("{output}.ndjson.tmp")),
            );
        }
        row.state = State::Deleted;
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

/// The cheapest and dearest monthly cost a price result names.
fn cost_bounds(price: &Path) -> (Option<f64>, Option<f64>) {
    let costs: Vec<f64> = fs::read_to_string(price)
        .ok()
        .and_then(|text| {
            serde_json::from_str::<pqbench::v1::PartitionPrice>(text.lines().next()?).ok()
        })
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

fn remove_dir(directory: &Path) -> Result<(), String> {
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

    /// A restart returns the rows a job held at the last checkpoint to the
    /// state before it, and deletes what the job had written.
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
        master.checkpoint().unwrap();
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
        assert!(master.list("").unwrap().is_empty());
    }
}

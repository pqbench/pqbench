//! The master's store: the files the daemon keeps for the master, which only
//! the daemon writes.
//!
//! ```text
//! {store}/checkpoint.ndjson                  the map, one PartitionVersion per line
//! {store}/walk/                              the running walk's output and errors
//! {store}/results/{partition}/{etag}/{file}  a version's results
//! {store}/deleted/{partition}/{etag}/        results waiting to be removed
//! ```
//!
//! A command's output goes to `{file}.tmp` in its version's directory, and is
//! renamed in when the command succeeds. A purged version's directory is
//! renamed under `deleted/`, so dropping it from the map is one syscall, and
//! emptied later.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pqbench::v1::partition_version::State;
use pqbench::v1::PartitionVersion;
use serde_json::Value;

use super::master::{self, Key};

/// A version's listing: the walk's Partition record for it.
pub(crate) const LISTING: &str = "list-partitions.ndjson";

/// The copy of a version's sample file, while the transitions that read its
/// data run.
pub(crate) const SAMPLE: &str = "sample.parquet";

#[derive(Clone)]
pub(crate) struct Store {
    root: PathBuf,
    /// Exclusive process-lifetime lock on `{root}/store.lock`.
    #[allow(dead_code)]
    lock: Arc<File>,
}

impl Store {
    /// Open the store at `root`: acquire exclusive ownership first, then
    /// delete the walk's directory and the trash a previous run left.
    pub(crate) fn open(root: &Path) -> Result<Self, String> {
        fs::create_dir_all(root)
            .map_err(|error| format!("cannot create {}: {error}", root.display()))?;
        let lock = acquire_lock(root)?;
        let store = Store {
            root: root.to_path_buf(),
            lock: Arc::new(lock),
        };
        remove_dir(&store.walk_dir())?;
        store.empty_trash()?;
        let results = store.root.join("results");
        fs::create_dir_all(&results)
            .map_err(|error| format!("cannot create {}: {error}", results.display()))?;
        Ok(store)
    }

    /// The directory purged results wait in.
    pub(crate) fn trash_dir(&self) -> PathBuf {
        self.root.join("deleted")
    }

    /// The directory of the running walk's output and errors.
    pub(crate) fn walk_dir(&self) -> PathBuf {
        self.root.join("walk")
    }

    /// The directory of one version's results.
    pub(crate) fn version_dir(&self, key: &Key) -> PathBuf {
        self.root.join("results").join(&key.0).join(&key.1)
    }

    /// The versions the last checkpoint holds; none before the first.
    pub(crate) fn read_checkpoint(&self) -> Result<Vec<PartitionVersion>, String> {
        let file = match File::open(self.root.join("checkpoint.ndjson")) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(format!("cannot read the checkpoint: {error}")),
        };
        BufReader::new(file)
            .lines()
            .map(|line| {
                let line = line.map_err(|error| format!("cannot read the checkpoint: {error}"))?;
                serde_json::from_str(&line).map_err(|error| format!("bad checkpoint row: {error}"))
            })
            .collect()
    }

    /// Write the checkpoint: a temporary file, synced, then renamed over it.
    pub(crate) fn write_checkpoint(
        &self,
        versions: impl Iterator<Item = PartitionVersion>,
    ) -> Result<(), String> {
        let path = self.root.join("checkpoint.ndjson");
        let temporary = path.with_extension("ndjson.tmp");
        let write = || -> std::io::Result<()> {
            let mut file = BufWriter::new(File::create(&temporary)?);
            for version in versions {
                serde_json::to_writer(&mut file, &version)?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
            file.get_ref().sync_all()?;
            fs::rename(&temporary, &path)
        };
        write().map_err(|error| format!("cannot write the checkpoint: {error}"))
    }

    /// Store a new version's listing.
    pub(crate) fn write_listing(&self, key: &Key, line: &str) -> Result<(), String> {
        let directory = self.version_dir(key);
        fs::create_dir_all(&directory)
            .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
        fs::write(self.temporary(key, LISTING), format!("{line}\n"))
            .and_then(|()| fs::rename(self.temporary(key, LISTING), directory.join(LISTING)))
            .map_err(|error| format!("cannot store the listing of {}: {error}", key.0))
    }

    pub(crate) fn read_listing(&self, key: &Key) -> Result<String, String> {
        fs::read_to_string(self.version_dir(key).join(LISTING))
            .map(|text| text.trim().to_string())
            .map_err(|error| format!("cannot read the listing of {}: {error}", key.0))
    }

    /// Where a run writes `output` before it lands.
    pub(crate) fn temporary(&self, key: &Key, output: &str) -> PathBuf {
        self.version_dir(key).join(format!("{output}.tmp"))
    }

    /// Land a run's `output`: `stdout` when the run wrote its result there,
    /// else the file it wrote at [`Store::temporary`].
    pub(crate) fn land(
        &self,
        key: &Key,
        output: &str,
        stdout: Option<&[u8]>,
    ) -> Result<(), String> {
        let temporary = self.temporary(key, output);
        stdout
            .map_or(Ok(()), |bytes| fs::write(&temporary, bytes))
            .and_then(|()| fs::rename(&temporary, self.version_dir(key).join(output)))
            .map_err(|error| format!("cannot store {output}: {error}"))
    }

    /// Drop what a failed run left of `output`.
    pub(crate) fn discard(&self, key: &Key, output: &str) {
        let _ = fs::remove_file(self.temporary(key, output));
    }

    /// One version's results, in the order its transitions wrote them: its
    /// price, each suggestion, each column mass, then the listing.
    pub(crate) fn read_results(&self, key: &Key) -> Result<Vec<Value>, String> {
        let directory = self.version_dir(key);
        let mut results = Vec::new();
        for (file, all) in [
            ("price.ndjson", false),
            ("list-suggestions.ndjson", true),
            ("bytemass.ndjson", true),
            (LISTING, false),
        ] {
            let text = match fs::read_to_string(directory.join(file)) {
                Ok(text) => text,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("cannot read {file}: {error}")),
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
        Ok(results)
    }

    /// Move one version's results to the trash: one rename, so a purge never
    /// waits on deleting them.
    pub(crate) fn trash(&self, key: &Key) -> Result<(), String> {
        let target = self.trash_dir().join(&key.0).join(&key.1);
        let parent = target
            .parent()
            .expect("a version's trash path has a parent");
        fs::create_dir_all(parent)
            .and_then(|()| fs::rename(self.version_dir(key), &target))
            .map_err(|error| format!("cannot move the results of {} to the trash: {error}", key.0))
    }

    /// Delete what the trash holds.
    pub(crate) fn empty_trash(&self) -> Result<(), String> {
        remove_dir(&self.trash_dir())
    }

    /// Delete a version's copy of its sample file: it was for the
    /// transitions that read data, and a sample file can be large.
    pub(crate) fn remove_sample(&self, key: &Key) {
        let _ = fs::remove_file(self.version_dir(key).join(SAMPLE));
    }

    /// Delete the result directories of versions that are not in the map, the
    /// outputs unfinished runs left beside any result, and the sample copies
    /// no transition will read: what a crash left.
    pub(crate) fn sweep(&self, state: impl Fn(&Key) -> Option<State>) -> Result<(), String> {
        let results = self.root.join("results");
        for (key, directory) in version_dirs(&results) {
            let state = match state(&key) {
                Some(state) => state,
                None => {
                    remove_dir(&directory)?;
                    continue;
                }
            };
            if !master::needs_sample(state) {
                let _ = fs::remove_file(directory.join(SAMPLE));
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

/// The `{partition}/{etag}` directories under `results` and their keys: a
/// partition name is eight segments (`catalogs/…/partitions/{id}`), then the
/// etag.
fn version_dirs(results: &Path) -> Vec<(Key, PathBuf)> {
    let mut partitions = vec![results.to_path_buf()];
    for _ in 0..8 {
        partitions = children(&partitions);
    }
    let mut versions = Vec::new();
    for partition in partitions {
        let name = partition
            .strip_prefix(results)
            .expect("a partition directory is under results")
            .to_string_lossy()
            .into_owned();
        for directory in children(&[partition]) {
            let etag = directory
                .file_name()
                .expect("a version directory has a name")
                .to_string_lossy()
                .into_owned();
            versions.push(((name.clone(), etag), directory));
        }
    }
    versions
}

/// The directories directly under each of `parents`.
fn children(parents: &[PathBuf]) -> Vec<PathBuf> {
    parents
        .iter()
        .flat_map(|parent| fs::read_dir(parent).into_iter().flatten().flatten())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect()
}

/// Acquire an exclusive non-blocking OS lock on `{root}/store.lock`.
fn acquire_lock(root: &Path) -> Result<File, String> {
    let path = root.join("store.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    // SAFETY: `flock` on an open file descriptor is safe.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == -1 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Err(format!("store {} is already in use", root.display()));
        }
        return Err(format!("cannot lock {}: {error}", path.display()));
    }
    Ok(file)
}

fn remove_dir(directory: &Path) -> Result<(), String> {
    match fs::remove_dir_all(directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("cannot delete {}: {error}", directory.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sweep deletes the results no version names and unfinished outputs,
    /// and keeps the rest.
    #[test]
    fn a_sweep_keeps_only_live_results() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let partition = "catalogs/c/schemas/s/tables/t/partitions/day%3D1";
        let live = (partition.to_string(), "aa".to_string());
        let dead = (partition.to_string(), "bb".to_string());
        for key in [&live, &dead] {
            store.write_listing(key, "{}").unwrap();
            fs::write(store.temporary(key, "price.ndjson"), "{}").unwrap();
        }
        store
            .sweep(|key| (*key == live).then_some(State::UpToDate))
            .unwrap();
        assert_eq!(store.read_listing(&live).unwrap(), "{}");
        assert!(!store.temporary(&live, "price.ndjson").exists());
        assert!(!store.version_dir(&dead).exists());
    }

    /// A sweep deletes a finished version's sample copy; a version waiting
    /// for a transition that reads it keeps it.
    #[test]
    fn a_sweep_keeps_only_the_samples_a_transition_reads() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let partition = "catalogs/c/schemas/s/tables/t/partitions/day%3D1";
        let versions: Vec<(Key, State, bool)> = [
            (State::UpToDate, false),
            (State::Downloaded, true),
            (State::Bytemassed, true),
        ]
        .into_iter()
        .enumerate()
        .map(|(at, (state, kept))| ((partition.to_string(), format!("a{at}")), state, kept))
        .collect();
        for (key, _, _) in &versions {
            store.write_listing(key, "{}").unwrap();
            fs::write(store.version_dir(key).join(SAMPLE), "x").unwrap();
        }
        store
            .sweep(|key| {
                versions
                    .iter()
                    .find(|(other, _, _)| other == key)
                    .map(|(_, state, _)| *state)
            })
            .unwrap();
        for (key, _, kept) in &versions {
            assert_eq!(store.version_dir(key).join(SAMPLE).exists(), *kept);
        }
    }

    /// A checkpoint that exists but cannot be read fails the open: it is not
    /// an empty map, and reading it as one would sweep every result away.
    #[test]
    fn an_unreadable_checkpoint_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        fs::create_dir(directory.path().join("checkpoint.ndjson")).unwrap();
        assert!(store.read_checkpoint().is_err());
        let missing = Store::open(&directory.path().join("elsewhere")).unwrap();
        assert!(missing.read_checkpoint().unwrap().is_empty());
    }

    /// A result that exists but cannot be read fails, rather than passing as
    /// one transition that has not written yet.
    #[test]
    fn an_unreadable_result_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let key = (
            "catalogs/c/schemas/s/tables/t/partitions/day%3D1".to_string(),
            "aa".to_string(),
        );
        store.write_listing(&key, "{}").unwrap();
        fs::write(store.version_dir(&key).join("price.ndjson"), "{}\n").unwrap();
        assert_eq!(store.read_results(&key).unwrap().len(), 2);
        fs::remove_file(store.version_dir(&key).join("price.ndjson")).unwrap();
        fs::create_dir(store.version_dir(&key).join("price.ndjson")).unwrap();
        assert!(store.read_results(&key).is_err());
    }

    /// A trashed version is gone from its results and waits in the trash,
    /// which opening the store empties.
    #[test]
    fn a_trashed_version_waits_outside_results() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let key = (
            "catalogs/c/schemas/s/tables/t/partitions/day%3D1".to_string(),
            "aa".to_string(),
        );
        store.write_listing(&key, "{}").unwrap();
        store.trash(&key).unwrap();
        assert!(!store.version_dir(&key).exists());
        assert!(store.trash_dir().join(&key.0).join(&key.1).exists());
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        assert!(!store.trash_dir().exists());
    }

    /// A second open on the same store is rejected before touching its files,
    /// and dropping the first owner releases it.
    #[test]
    fn a_store_has_one_owner_at_a_time() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        fs::create_dir_all(store.walk_dir()).unwrap();
        fs::write(store.walk_dir().join("active"), "ok").unwrap();
        assert!(Store::open(directory.path()).is_err());
        assert!(store.walk_dir().join("active").exists());
        drop(store);
        assert!(Store::open(directory.path()).is_ok());
    }
}

//! The driver's store: the files the driver keeps, which only the driver
//! writes.
//!
//! ```text
//! {store}/checkpoint.ndjson.zst               the map, one Zstandard frame of NDJSON
//! {store}/walk/                              the running walk's output and errors
//! {store}/results/{partition}/{etag}/{file}  a version's results
//! {store}/deleted/{partition}/{etag}/        results waiting to be removed
//! ```
//!
//! A command's output goes to `{file}.tmp` in its version's directory, and is
//! renamed in when the command succeeds. A purged version's directory is
//! renamed under `deleted/`, so dropping it from the map is one syscall, and
//! emptied later.
//!
//! The checkpoint is made durable the same way: written to a temporary file,
//! synced, renamed over its name, and the store directory synced, so a crash
//! leaves either the old checkpoint or the new one, never half of either.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pqbench_model_v1::partition_version::State;
use pqbench_model_v1::{PartitionVersion, Suggestion};
use pqbench_parquet::codecs::{self, Codec};
use serde_json::value::RawValue;

use super::engine::{self, Key};

/// A version's listing: the walk's Partition record for it.
pub(crate) const LISTING: &str = "list-partitions.ndjson";

/// The copy of a version's sample file, while the transitions that read its
/// data run.
pub(crate) const SAMPLE: &str = "sample.parquet";

/// The price `get partition-price` writes.
pub(crate) const PRICE: &str = "price.ndjson";

/// The potential suggestions `list suggestions` writes.
pub(crate) const LIST_SUGGESTIONS: &str = "list-suggestions.ndjson";

/// The verified suggestions the driver measured for a version: the potential
/// ones against their control, and the partition rules that need no file.
pub(crate) const SUGGESTIONS: &str = "suggestions.ndjson";

/// A version's column masses, from `bytemass`.
pub(crate) const BYTEMASS: &str = "bytemass.ndjson";

/// The checkpoint's name under the store: one Zstandard frame of NDJSON, which
/// `zstdcat` reads.
const CHECKPOINT: &str = "checkpoint.ndjson.zst";

/// Where a checkpoint is assembled before the rename that lands it: never a
/// checkpoint, whatever a crash leaves there.
const TEMPORARY: &str = "checkpoint.ndjson.tmp";

/// How long acquiring the store's lock waits for a holder that is about to
/// go: long enough for a forked child to exec, short enough that a store held
/// for good still fails at once.
const LOCK_WAIT: Duration = Duration::from_millis(100);

/// The level a checkpoint is compressed at: the fastest one, since the map is
/// written every few minutes and read once, when the driver starts.
const CHECKPOINT_LEVEL: u8 = 1;

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
        store.remove_temporary()?;
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
        let path = self.root.join(CHECKPOINT);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(format!("cannot read {}: {error}", path.display())),
        };
        let text = codecs::decompress_frame(Codec::Zstd, &bytes)
            .map_err(|error| format!("cannot decompress {CHECKPOINT}: {error}"))?;
        let text =
            String::from_utf8(text).map_err(|error| format!("bad checkpoint text: {error}"))?;
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line).map_err(|error| format!("bad checkpoint row: {error}"))
            })
            .collect()
    }

    /// Write the checkpoint: a temporary file, synced, then renamed over it,
    /// with the store directory synced after the rename. This is the durable
    /// barrier, so it keeps its fsyncs; the listing writes below do not.
    pub(crate) fn write_checkpoint(
        &self,
        versions: impl Iterator<Item = PartitionVersion>,
    ) -> Result<(), String> {
        let mut text = Vec::new();
        for version in versions {
            serde_json::to_writer(&mut text, &version)
                .map_err(|error| format!("cannot write the checkpoint: {error}"))?;
            text.push(b'\n');
        }
        let bytes = codecs::compress_frame(Codec::Zstd, CHECKPOINT_LEVEL, &text)
            .map_err(|error| format!("cannot compress the checkpoint: {error}"))?;
        let temporary = self.temporary_checkpoint();
        let path = self.root.join(CHECKPOINT);
        let write = || -> std::io::Result<()> {
            let mut file = File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &path)
        };
        write().map_err(|error| format!("cannot write the checkpoint: {error}"))?;
        sync_directory(&self.root)
    }

    /// Where a checkpoint is assembled.
    fn temporary_checkpoint(&self) -> PathBuf {
        self.root.join(TEMPORARY)
    }

    /// Delete what a crash left of a checkpoint write.
    fn remove_temporary(&self) -> Result<(), String> {
        remove_file(&self.temporary_checkpoint())
    }

    /// Store a new version's listing: a temporary file renamed over the
    /// listing, so a reader never sees half of one. It is not synced: the
    /// checkpoint, not a listing, is the durable barrier, and a walk may store
    /// thousands of listings.
    pub(crate) fn write_listing(&self, key: &Key, line: &str) -> Result<(), String> {
        let directory = self.version_dir(key);
        fs::create_dir_all(&directory)
            .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
        let temporary = self.temporary(key, LISTING);
        let write = || -> std::io::Result<()> {
            let mut file = File::create(&temporary)?;
            file.write_all(format!("{line}\n").as_bytes())?;
            fs::rename(&temporary, directory.join(LISTING))
        };
        write().map_err(|error| format!("cannot store the listing of {}: {error}", key.0))
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
    /// else the file it wrote at [`Store::temporary`]. The temporary file is
    /// synced before the rename, so a power loss right after it cannot leave a
    /// zero-byte landed result.
    pub(crate) fn land(
        &self,
        key: &Key,
        output: &str,
        stdout: Option<&[u8]>,
    ) -> Result<(), String> {
        let temporary = self.temporary(key, output);
        let land = || -> std::io::Result<()> {
            if let Some(bytes) = stdout {
                let mut file = File::create(&temporary)?;
                file.write_all(bytes)?;
            }
            File::open(&temporary)?.sync_all()?;
            fs::rename(&temporary, self.version_dir(key).join(output))
        };
        land().map_err(|error| format!("cannot store {output}: {error}"))
    }

    /// Drop what a failed run left of `output`.
    pub(crate) fn discard(&self, key: &Key, output: &str) {
        let _ = fs::remove_file(self.temporary(key, output));
    }

    /// One version's results, in the order its transitions wrote them: its
    /// price, each suggestion, each column mass, then the listing.
    ///
    /// Each record stays the JSON line it was read from: the driver hands a
    /// version's results to a client verbatim, so parsing them into a tree here
    /// and serializing them again would be two passes for no one.
    pub(crate) fn read_results(&self, key: &Key) -> Result<Vec<Box<RawValue>>, String> {
        let directory = self.version_dir(key);
        let mut results = Vec::new();
        for (file, all) in [
            (PRICE, false),
            (LIST_SUGGESTIONS, true),
            (BYTEMASS, true),
            (LISTING, false),
        ] {
            let text = match fs::read_to_string(directory.join(file)) {
                Ok(text) => text,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("cannot read {file}: {error}")),
            };
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let record = RawValue::from_string(line.to_owned())
                    .map_err(|error| format!("bad result line in {file}: {error}"))?;
                results.push(record);
                if !all {
                    break;
                }
            }
        }
        Ok(results)
    }

    /// One result file's text, or `None` when the run has not written it.
    pub(crate) fn read_output(&self, key: &Key, output: &str) -> Result<Option<String>, String> {
        let path = self.version_dir(key).join(output);
        match fs::read_to_string(&path) {
            Ok(text) => Ok(Some(text)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("cannot read {}: {error}", path.display())),
        }
    }

    /// Land a version's verified suggestions, one Suggestion per line: written
    /// beside the file and renamed in, so a reader never sees half of them.
    pub(crate) fn write_suggestions(
        &self,
        key: &Key,
        suggestions: &[Suggestion],
    ) -> Result<(), String> {
        let mut text = Vec::new();
        for suggestion in suggestions {
            serde_json::to_writer(&mut text, suggestion)
                .map_err(|error| format!("cannot write a suggestion: {error}"))?;
            text.push(b'\n');
        }
        self.land(key, SUGGESTIONS, Some(&text))
    }

    /// The suggestions a version holds; empty when it has none yet.
    pub(crate) fn read_suggestions(&self, key: &Key) -> Result<Vec<Suggestion>, String> {
        let Some(text) = self.read_output(key, SUGGESTIONS)? else {
            return Ok(Vec::new());
        };
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|error| format!("bad suggestion of {}: {error}", key.0))
            })
            .collect()
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
            if !engine::needs_sample(state) {
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
/// partition name is `catalogs/…/partitions/{id}`, one segment per partition
/// column, and the etag is the directory below it.
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
            // A partition id may itself be several directories (`day=1/hours=05`),
            // and only the leaf is a version's: its key names it as the last
            // segment.
            collect_versions(name.clone(), directory, &mut versions);
        }
    }
    versions
}

/// Collect the versions under `directory`, whose partition name is `name`: a
/// directory with no child is a version's — its name is the etag — and one
/// with children continues the partition's id.
fn collect_versions(name: String, directory: PathBuf, versions: &mut Vec<(Key, PathBuf)>) {
    let segment = directory
        .file_name()
        .expect("a version or partition directory has a name")
        .to_string_lossy()
        .into_owned();
    let children = children(std::slice::from_ref(&directory));
    if children.is_empty() {
        versions.push(((name, segment), directory));
        return;
    }
    let name = format!("{name}/{segment}");
    for child in children {
        collect_versions(name.clone(), child, versions);
    }
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
///
/// A lock held for good means another driver owns the store. One that clears
/// within [`LOCK_WAIT`] was a descriptor a just-forked child inherited: `fork`
/// copies the whole descriptor table, and only `exec` closes the store's copy
/// (`O_CLOEXEC`), so the lock can look held for the few milliseconds between.
fn acquire_lock(root: &Path) -> Result<File, String> {
    let path = root.join("store.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(TryLockError::WouldBlock) => {
                return Err(format!("store {} is already in use", root.display()));
            }
            Err(TryLockError::Error(error)) => {
                return Err(format!("cannot lock {}: {error}", path.display()));
            }
        }
    }
}

fn remove_dir(directory: &Path) -> Result<(), String> {
    match fs::remove_dir_all(directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("cannot delete {}: {error}", directory.display())),
    }
}

fn remove_file(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("cannot delete {}: {error}", path.display())),
    }
}

/// Sync a directory's entries (a rename among them) to the disk.
fn sync_directory(directory: &Path) -> Result<(), String> {
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("cannot sync {}: {error}", directory.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two versions of one partition, as a checkpoint holds them.
    fn versions() -> Vec<PartitionVersion> {
        let partition = "catalogs/c/schemas/s/tables/t/partitions/day%3D1";
        ["aa", "bb"]
            .into_iter()
            .enumerate()
            .map(|(at, etag)| PartitionVersion {
                name: format!("{partition}/versions/{etag}"),
                state: State::Pending,
                etag_change_count: at as u64,
                ..PartitionVersion::default()
            })
            .collect()
    }

    /// A checkpoint round-trips under its own name, and is the standard
    /// Zstandard frame: its magic number, which is what `zstdcat` recognizes.
    #[test]
    fn a_checkpoint_round_trips_as_a_zstd_frame() {
        let versions = versions();
        let zstd = [0x28, 0xb5, 0x2f, 0xfd];
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        assert!(store.read_checkpoint().unwrap().is_empty());
        store
            .write_checkpoint(versions.clone().into_iter())
            .unwrap();
        assert_eq!(store.read_checkpoint().unwrap(), versions);
        let bytes = fs::read(directory.path().join(CHECKPOINT)).unwrap();
        assert!(bytes.starts_with(&zstd), "no Zstandard frame");
        assert!(!directory.path().join(TEMPORARY).exists());

        // The next open reads the map back: the checkpoint is the map's.
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.read_checkpoint().unwrap(), versions);
    }

    /// A half-written checkpoint a crash left is never read as one: opening
    /// the store deletes it, and the checkpoint that landed is the map.
    #[test]
    fn a_crashs_temporary_checkpoint_is_swept() {
        let directory = tempfile::tempdir().unwrap();
        {
            let store = Store::open(directory.path()).unwrap();
            store.write_checkpoint(versions().into_iter()).unwrap();
        }
        fs::write(directory.path().join(TEMPORARY), "half a row").unwrap();
        let store = Store::open(directory.path()).unwrap();
        assert!(!directory.path().join(TEMPORARY).exists());
        assert_eq!(store.read_checkpoint().unwrap(), versions());
    }

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
        fs::create_dir(directory.path().join(CHECKPOINT)).unwrap();
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

    /// A version's suggestions round-trip in one file, are renamed in rather
    /// than left half written, and go with the version's results.
    #[test]
    fn a_versions_suggestions_land_and_sweep_with_it() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let key = (
            "catalogs/c/schemas/s/tables/t/partitions/day%3D1".to_string(),
            "aa".to_string(),
        );
        store.write_listing(&key, "{}").unwrap();
        assert!(store.read_suggestions(&key).unwrap().is_empty());
        let suggestions = vec![
            Suggestion {
                name: format!("{}/suggestions/tombstones", key.0),
                rule: "tombstones".into(),
                verified: Some(true),
                savings_bytes: Some(2048),
                ..Suggestion::default()
            },
            Suggestion {
                name: format!("{}/suggestions/small-files", key.0),
                rule: "small-files".into(),
                verified: None,
                ..Suggestion::default()
            },
        ];
        store.write_suggestions(&key, &suggestions).unwrap();
        assert_eq!(store.read_suggestions(&key).unwrap(), suggestions);
        assert!(!store.temporary(&key, SUGGESTIONS).exists());
        let path = store.version_dir(&key).join(SUGGESTIONS);
        assert_eq!(fs::read_to_string(&path).unwrap().lines().count(), 2);

        store.trash(&key).unwrap();
        assert!(!path.exists());
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

#[cfg(test)]
mod multi_column_tests {
    use super::*;

    /// A partition id of several segments (`day=1/hours=05`) is a nested path
    /// under `results`, and its version is still found, swept and trashed as
    /// one: the id is not truncated to its first segment.
    #[test]
    fn a_multi_column_partition_round_trips_through_the_store() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let key = (
            "catalogs/c/schemas/s/tables/t/partitions/day%3D2026-01-01/hour%3D05".to_string(),
            "aa".to_string(),
        );
        store.write_listing(&key, "{}").unwrap();
        assert_eq!(store.read_listing(&key).unwrap(), "{}");

        let version = store.version_dir(&key);
        assert!(version.ends_with("partitions/day%3D2026-01-01/hour%3D05/aa"));
        assert_eq!(
            version_dirs(&directory.path().join("results")),
            vec![(key.clone(), version.clone())],
            "the nested partition id is not a version directory"
        );

        store
            .sweep(|found| (found == &key).then_some(State::UpToDate))
            .unwrap();
        assert!(version.exists(), "the sweep dropped a multi-column version");

        store.trash(&key).unwrap();
        assert!(!version.exists());
        assert!(store.trash_dir().join(&key.0).join(&key.1).exists());
    }
}

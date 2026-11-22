//! The master's store: the files the daemon keeps for the master, which only
//! the daemon writes.
//!
//! ```text
//! {store}/checkpoint.ndjson                  the map, one PartitionVersion per line
//! {store}/walk/                              the running walk's output and errors
//! {store}/results/{partition}/{etag}/{file}  a version's results
//! ```
//!
//! A command's output goes to `{file}.tmp` in its version's directory, and is
//! renamed in when the command succeeds.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use pqbench::v1::PartitionVersion;
use serde_json::Value;

use super::master::Key;

/// A version's listing: the walk's Partition record for it.
pub(crate) const LISTING: &str = "list-partitions.ndjson";

/// The copy of a version's sample file, while the transitions that read its
/// data run.
pub(crate) const SAMPLE: &str = "sample.parquet";

#[derive(Clone)]
pub(crate) struct Store {
    root: PathBuf,
}

impl Store {
    /// Open the store at `root`: create it, and delete the walk's directory,
    /// which only a running walk uses.
    pub(crate) fn open(root: &Path) -> Result<Self, String> {
        let store = Store {
            root: root.to_path_buf(),
        };
        remove_dir(&store.walk_dir())?;
        let results = store.root.join("results");
        fs::create_dir_all(&results)
            .map_err(|error| format!("cannot create {}: {error}", results.display()))?;
        Ok(store)
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
        let Ok(file) = File::open(self.root.join("checkpoint.ndjson")) else {
            return Ok(Vec::new());
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
        Ok(results)
    }

    /// Delete one version's results.
    pub(crate) fn remove_version(&self, key: &Key) -> Result<(), String> {
        remove_dir(&self.version_dir(key))
    }

    /// Delete a version's copy of its sample file: it was for the
    /// transitions that read data, and a sample file can be large.
    pub(crate) fn remove_sample(&self, key: &Key) {
        let _ = fs::remove_file(self.version_dir(key).join(SAMPLE));
    }

    /// Delete the result directories of versions that are not `live`, and
    /// the outputs unfinished runs left beside any result: what a crash
    /// left.
    pub(crate) fn sweep(&self, live: impl Fn(&Key) -> bool) -> Result<(), String> {
        let results = self.root.join("results");
        for directory in version_dirs(&results) {
            let Ok(relative) = directory.strip_prefix(&results) else {
                continue;
            };
            let name = |path: Option<&Path>| {
                path.map(|path| path.to_string_lossy().into_owned())
                    .unwrap_or_default()
            };
            let key = (
                name(relative.parent()),
                name(relative.file_name().map(Path::new)),
            );
            if !live(&key) {
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
        store.sweep(|key| *key == live).unwrap();
        assert_eq!(store.read_listing(&live).unwrap(), "{}");
        assert!(!store.temporary(&live, "price.ndjson").exists());
        assert!(!store.version_dir(&dead).exists());
    }
}

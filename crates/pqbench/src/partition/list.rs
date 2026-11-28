//! `partition::list`: a table's partitions, from one pass over its files.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::Write;

use futures_util::TryStreamExt;

use crate::file::sample;
use crate::table::SnapshotFile;
use crate::v1;
use crate::Lake;

/// The id of a table without partition columns: its one partition.
const UNPARTITIONED: &str = "unpartitioned";

/// The id segment Hive and Spark write for a null partition value.
const NULL_VALUE: &str = "__HIVE_DEFAULT_PARTITION__";

/// Errors listing a table's partitions.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The table or a file in it is not complete.
    Invalid(String),
    /// The table loader failed.
    Table(crate::table::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Invalid(message) => write!(f, "{message}"),
            Error::Table(_) => write!(f, "the table loader failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Table(error) => Some(error),
            Error::Invalid(_) => None,
        }
    }
}

/// List a table's partitions, sorted by name.
///
/// The snapshot's active files are read once — for Delta the checkpoint and
/// the commits after it, without the per-column statistics — and each file is
/// folded into its partition as it arrives, so the run holds one accumulator
/// per partition and one log batch, not the table's files. A table that fails
/// mid-read yields no partitions rather than partial ones.
///
/// The removed files are read first (for Delta the `remove` actions, a second
/// read of the log projected to them) and kept as a path hash, a partition and
/// a size each; an active file under the same path cancels its tombstone, so
/// a deletion-vector update is not counted as removed. A partition that holds
/// only removed files is listed with no files.
///
/// The delete files follow the active ones: a merge-on-read table's position
/// and equality deletes are counted in the partition they narrow, and mixed
/// into its etag, so compacting them changes the etag a caller caches.
///
/// The caller scopes `lake` with the table's lease (`Lake::with_lease`).
///
/// # Errors
/// Fails when the table names no storage root, cannot be loaded, or names a
/// file without a size.
pub async fn list(lake: &Lake, table: &v1::Table) -> Result<Vec<v1::Partition>, Error> {
    let uri = table.uri.as_deref().ok_or_else(|| {
        Error::Invalid("a table record has no storage path; run `pqbench get table` first".into())
    })?;
    let request = crate::table::LoadRequest::new(uri.to_string(), table.snapshot_version)
        .with_file_stats(false);
    let mut entries = crate::table::stream_snapshot_files(lake, &request)
        .await
        .map_err(Error::Table)?;
    let mut removed = Removed::default();
    let mut partitions: HashMap<BTreeMap<String, Option<String>>, Fold> = HashMap::new();
    while let Some(entry) = entries.try_next().await.map_err(Error::Table)? {
        match entry {
            SnapshotFile::Removed(file) => removed.insert(file),
            SnapshotFile::Active(file) => {
                let size_bytes = file.size_bytes.ok_or_else(|| {
                    let name = v1::name::table_file_name(&table.name, &file.path);
                    Error::Invalid(format!("the log gives no size for {name}"))
                })?;
                let v1::TableFile {
                    blob,
                    path,
                    modification_time,
                    partition_values,
                    stats,
                    ..
                } = file;
                let reference = v1::TableFile {
                    blob,
                    path,
                    size_bytes: Some(size_bytes),
                    modification_time,
                    ..v1::TableFile::default()
                };
                removed.cancel(&reference.path);
                let record_count = stats.map(|stats| stats.record_count);
                match partitions.get_mut(&partition_values) {
                    Some(fold) => fold.add(reference, record_count),
                    None => {
                        check_columns(&partition_values, &table.partition_columns)?;
                        let mut fold = Fold::default();
                        fold.add(reference, record_count);
                        partitions.insert(partition_values, fold);
                    }
                }
            }
            SnapshotFile::Delete(file) => match partitions.get_mut(&file.partition_values) {
                Some(fold) => fold.add_delete(&file),
                None => {
                    check_columns(&file.partition_values, &table.partition_columns)?;
                    let mut fold = Fold::default();
                    fold.add_delete(&file);
                    partitions.insert(file.partition_values, fold);
                }
            },
        }
    }
    for total in removed.totals() {
        check_columns(&total.values, &table.partition_columns)?;
        partitions.entry(total.values).or_default().add_removed(
            total.file_count,
            total.size_bytes,
            total.hash,
        );
    }
    let mut records: Vec<v1::Partition> = partitions
        .into_iter()
        .map(|(values, fold)| fold.finish(&table.name, values, &table.partition_columns, true))
        .collect();
    records.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Ok(records)
}

/// The removed files of a table, keyed by path hash: what is left of each once
/// the active files have cancelled the paths they still hold.
#[derive(Default)]
struct Removed {
    files: HashMap<u64, RemovedFile>,
    partitions: Vec<BTreeMap<String, Option<String>>>,
    index: HashMap<BTreeMap<String, Option<String>>, usize>,
}

struct RemovedFile {
    partition: usize,
    size_bytes: u64,
    live: bool,
}

impl Removed {
    fn insert(&mut self, file: v1::TableFile) {
        let hash = path_hash(&file.path);
        if self.files.contains_key(&hash) {
            // A path tombstoned twice (each of its deletion vectors, or both a
            // historical manifest and a delete entry) is one file.
            return;
        }
        let partition = match self.index.get(&file.partition_values) {
            Some(partition) => *partition,
            None => {
                let partition = self.partitions.len();
                self.partitions.push(file.partition_values.clone());
                self.index.insert(file.partition_values, partition);
                partition
            }
        };
        self.files.insert(
            hash,
            RemovedFile {
                partition,
                size_bytes: file.size_bytes.unwrap_or_default(),
                live: false,
            },
        );
    }

    fn cancel(&mut self, path: &str) {
        if let Some(entry) = self.files.get_mut(&path_hash(path)) {
            entry.live = true;
        }
    }

    /// Per partition: its values, and the count, bytes and identity hash of its
    /// removed files.
    fn totals(self) -> Vec<RemovedTotal> {
        let mut totals = vec![(0_u64, 0_u64, 0_u64); self.partitions.len()];
        for (path_hash, file) in self.files {
            if file.live {
                continue;
            }
            let total = &mut totals[file.partition];
            total.0 += 1;
            total.1 += file.size_bytes;
            total.2 ^= removed_hash(path_hash, file.size_bytes);
        }
        self.partitions
            .into_iter()
            .zip(totals)
            .filter(|(_, (file_count, _, _))| *file_count > 0)
            .map(|(values, (file_count, size_bytes, hash))| RemovedTotal {
                values,
                file_count,
                size_bytes,
                hash,
            })
            .collect()
    }
}

/// One partition's removed files, once the active ones cancelled the paths
/// they still hold.
struct RemovedTotal {
    values: BTreeMap<String, Option<String>>,
    file_count: u64,
    size_bytes: u64,
    /// The XOR of the files' hashes: their share of the partition's etag.
    hash: u64,
}

fn path_hash(path: &str) -> u64 {
    sample::hash(&[path.as_bytes()])
}

/// One partition's running totals and the files it keeps a reference to.
struct Fold {
    etag: u64,
    file_count: u64,
    size_bytes: u64,
    record_count: Option<u64>,
    smallest_file_size_bytes: Option<u64>,
    largest_file_size_bytes: Option<u64>,
    first_file: Option<v1::TableFile>,
    last_file: Option<v1::TableFile>,
    sample: Option<(u64, v1::TableFile)>,
    removed_file_count: u64,
    removed_size_bytes: u64,
    delete_file_count: u64,
    delete_size_bytes: u64,
}

impl Default for Fold {
    fn default() -> Self {
        Fold {
            etag: 0,
            file_count: 0,
            size_bytes: 0,
            record_count: Some(0),
            smallest_file_size_bytes: None,
            largest_file_size_bytes: None,
            first_file: None,
            last_file: None,
            sample: None,
            removed_file_count: 0,
            removed_size_bytes: 0,
            delete_file_count: 0,
            delete_size_bytes: 0,
        }
    }
}

impl Fold {
    fn add(&mut self, file: v1::TableFile, record_count: Option<u64>) {
        let size_bytes = file.size_bytes.unwrap_or_default();
        let hash = file_hash(&file);
        self.etag ^= hash;
        self.file_count += 1;
        self.size_bytes += size_bytes;
        self.record_count = self.record_count.zip(record_count).map(|(a, b)| a + b);
        self.smallest_file_size_bytes = Some(
            self.smallest_file_size_bytes
                .map_or(size_bytes, |smallest| smallest.min(size_bytes)),
        );
        self.largest_file_size_bytes = Some(
            self.largest_file_size_bytes
                .map_or(size_bytes, |largest| largest.max(size_bytes)),
        );
        if let Some(time) = file.modification_time {
            let key = (time, file.path.as_str());
            if self
                .first_file
                .as_ref()
                .is_none_or(|first| key < age(first))
            {
                self.first_file = Some(file.clone());
            }
            if self.last_file.as_ref().is_none_or(|last| key > age(last)) {
                self.last_file = Some(file.clone());
            }
        }
        let lower = self.sample.as_ref().is_none_or(|(sample_hash, sample)| {
            (hash, file.path.as_str()) < (*sample_hash, sample.path.as_str())
        });
        if lower {
            self.sample = Some((hash, file));
        }
    }

    /// Fold in the removed files the active ones did not cancel: their count,
    /// bytes, and identity, so a tombstone-only partition's etag says which
    /// tombstones it holds and changes when `VACUUM` drops them.
    fn add_removed(&mut self, file_count: u64, size_bytes: u64, hash: u64) {
        self.removed_file_count += file_count;
        self.removed_size_bytes += size_bytes;
        self.etag ^= hash;
    }

    /// Fold in one merge-on-read delete file: it is not a data file, so it
    /// counts nothing toward the partition's rows, but its bytes and identity
    /// are the partition's too, and compacting it has to change the etag.
    fn add_delete(&mut self, file: &v1::TableFile) {
        self.delete_file_count += 1;
        self.delete_size_bytes += file.size_bytes.unwrap_or_default();
        self.etag ^= delete_hash(file);
    }

    /// The partition's record. `counted` says whether the table's format
    /// records removed files; without it the removed totals are absent.
    fn finish(
        self,
        table: &str,
        partition_values: BTreeMap<String, Option<String>>,
        order: &[String],
        counted: bool,
    ) -> v1::Partition {
        let named = |mut file: v1::TableFile| {
            file.name = v1::name::table_file_name(table, &file.path);
            file
        };
        let has_deletes = self.delete_file_count > 0;
        v1::Partition {
            name: v1::name::partition_name(table, &partition_id(&partition_values, order)),
            partition_values,
            etag: format!("{:016x}", self.etag),
            file_count: self.file_count,
            size_bytes: self.size_bytes,
            record_count: self.record_count,
            smallest_file_size_bytes: self.smallest_file_size_bytes.unwrap_or_default(),
            largest_file_size_bytes: self.largest_file_size_bytes.unwrap_or_default(),
            first_file: self.first_file.map(named),
            last_file: self.last_file.map(named),
            sample_file: self.sample.map(|(_, file)| named(file)),
            removed_file_count: counted.then_some(self.removed_file_count),
            removed_size_bytes: counted.then_some(self.removed_size_bytes),
            delete_file_count: has_deletes.then_some(self.delete_file_count),
            delete_size_bytes: has_deletes.then_some(self.delete_size_bytes),
        }
    }
}

/// A dated file's place in time; ties fall to the path, so the pick does not
/// depend on the order the log yields files in.
fn age(file: &v1::TableFile) -> (u64, &str) {
    (
        file.modification_time.unwrap_or_default(),
        file.path.as_str(),
    )
}

/// A file's hash: its share of the etag, and the sample pick. Path and size,
/// so a rewrite under the same path with another size changes it too.
fn file_hash(file: &v1::TableFile) -> u64 {
    let size_bytes = file.size_bytes.unwrap_or_default().to_le_bytes();
    sample::hash(&[file.path.as_bytes(), &[0], &size_bytes])
}

/// A removed file's hash: its share of the etag. The domain tag keeps it
/// distinct from a live file's hash at the same path and size, so a partition
/// whose only difference is a tombstone never hashes the same.
fn removed_hash(path_hash: u64, size_bytes: u64) -> u64 {
    const TAG: &[u8] = b"removed";
    sample::hash(&[TAG, &path_hash.to_le_bytes(), &size_bytes.to_le_bytes()])
}

/// A delete file's hash: its share of the etag. Its own domain tag keeps it
/// distinct from a data file's hash at the same path and size, so replacing a
/// data file with a delete file at that path changes the etag.
fn delete_hash(file: &v1::TableFile) -> u64 {
    const TAG: &[u8] = b"delete";
    let size_bytes = file.size_bytes.unwrap_or_default().to_le_bytes();
    sample::hash(&[TAG, file.path.as_bytes(), &[0], &size_bytes])
}

/// A partition's name follows the table's partition column order, so a
/// record that does not name every column the log uses would name the same
/// partition differently.
fn check_columns(values: &BTreeMap<String, Option<String>>, order: &[String]) -> Result<(), Error> {
    match values.keys().find(|key| !order.contains(key)) {
        Some(key) => Err(Error::Invalid(format!(
            "the table record does not name the partition column {key}; run `pqbench get table` first"
        ))),
        None => Ok(()),
    }
}

/// The Hive-style path of a partition's values, `k=v/k2=v2`, in the table's
/// partition column order. `/`, `=`, `%` and control bytes in a key or value
/// are percent-escaped, so the id splits back unambiguously.
fn partition_id(values: &BTreeMap<String, Option<String>>, order: &[String]) -> String {
    if values.is_empty() {
        return UNPARTITIONED.to_string();
    }
    let mut id = String::new();
    for key in order.iter().filter(|key| values.contains_key(*key)) {
        if !id.is_empty() {
            id.push('/');
        }
        escape(key, &mut id);
        id.push('=');
        match values.get(key).and_then(Option::as_deref) {
            Some(value) => escape(value, &mut id),
            None => id.push_str(NULL_VALUE),
        }
    }
    id
}

fn escape(value: &str, out: &mut String) {
    for character in value.chars() {
        match character {
            '/' | '=' | '%' | '\u{0}'..='\u{1f}' | '\u{7f}' => {
                // No per-byte allocation: the hex digits go straight to `out`.
                let _ = write!(out, "%{:02X}", u32::from(character));
            }
            _ => out.push(character),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::v1::TableFile;

    use super::{partition_id, Fold, Removed, RemovedFile};

    fn values(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
            .collect()
    }

    #[test]
    fn an_id_follows_the_partition_column_order() {
        let order = ["hour".to_string(), "day".to_string()];
        let id = partition_id(&values(&[("day", Some("1")), ("hour", Some("2"))]), &order);
        assert_eq!(id, "hour=2/day=1");
    }

    #[test]
    fn an_id_escapes_separators_and_names_nulls() {
        let order = ["a".to_string(), "b".to_string()];
        let id = partition_id(&values(&[("a", Some("x/y=z%")), ("b", None)]), &order);
        assert_eq!(id, "a=x%2Fy%3Dz%25/b=__HIVE_DEFAULT_PARTITION__");
    }

    #[test]
    fn a_table_without_partition_values_has_one_partition() {
        assert_eq!(partition_id(&BTreeMap::new(), &[]), "unpartitioned");
    }

    /// A partition's etag covers its removed files: a tombstone-only partition
    /// whose tombstones change has a different etag, and vacuuming them returns
    /// the etag the partition had before the files were removed. A cached
    /// `tombstones` suggestion is therefore invalidated when `VACUUM` runs or
    /// more tombstones accumulate.
    #[test]
    fn removed_files_change_a_partitions_etag() {
        let live = TableFile {
            name: "files/live.parquet".into(),
            path: "live.parquet".into(),
            size_bytes: Some(100),
            ..TableFile::default()
        };
        let mut no_removed = Fold::default();
        no_removed.add(live.clone(), None);
        let live_etag = no_removed.etag;

        for (path_hash, size_bytes) in [(0x1234_u64, 200_u64), (0x9876, 300)] {
            let removed = Removed {
                files: std::collections::HashMap::from([(
                    path_hash,
                    RemovedFile {
                        partition: 0,
                        size_bytes,
                        live: false,
                    },
                )]),
                partitions: vec![BTreeMap::new()],
                index: std::collections::HashMap::new(),
            };
            let total = removed.totals().pop().unwrap();
            let mut fold = Fold::default();
            fold.add(live.clone(), None);
            fold.add_removed(total.file_count, total.size_bytes, total.hash);
            assert_ne!(
                fold.etag, live_etag,
                "a tombstone at {path_hash:x} did not change the etag"
            );
            assert_eq!(fold.removed_file_count, 1);
            assert_eq!(fold.removed_size_bytes, size_bytes);
        }

        // Vacuumed: a fresh fold without the removed files has the live etag
        // again, so the stale suggestion no longer matches.
        let mut vacuumed = Fold::default();
        vacuumed.add(live, None);
        assert_eq!(vacuumed.etag, live_etag);
    }
}

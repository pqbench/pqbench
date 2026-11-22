//! `partition::list`: a table's partitions, from one pass over its files.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use futures_util::TryStreamExt;

use crate::file::sample;
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
    let mut files = crate::table::stream_active_files(lake, &request)
        .await
        .map_err(Error::Table)?;
    let mut partitions: HashMap<BTreeMap<String, Option<String>>, Fold> = HashMap::new();
    while let Some(file) = files.try_next().await.map_err(Error::Table)? {
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
        let record_count = stats.map(|stats| stats.record_count);
        match partitions.get_mut(&partition_values) {
            Some(fold) => fold.add(reference, record_count),
            None => {
                check_columns(&partition_values, &table.partition_columns)?;
                partitions.insert(partition_values, Fold::new(reference, record_count));
            }
        }
    }
    let mut records: Vec<v1::Partition> = partitions
        .into_iter()
        .map(|(values, fold)| fold.finish(&table.name, values, &table.partition_columns))
        .collect();
    records.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Ok(records)
}

/// One partition's running totals and the files it keeps a reference to.
struct Fold {
    etag: u64,
    file_count: u64,
    size_bytes: u64,
    record_count: Option<u64>,
    smallest_file_size_bytes: u64,
    largest_file_size_bytes: u64,
    first_file: Option<v1::TableFile>,
    last_file: Option<v1::TableFile>,
    sample_file: v1::TableFile,
    sample_hash: u64,
}

impl Fold {
    fn new(file: v1::TableFile, record_count: Option<u64>) -> Self {
        let size_bytes = file.size_bytes.unwrap_or_default();
        let dated = file.modification_time.is_some();
        let hash = file_hash(&file);
        Fold {
            etag: hash,
            file_count: 1,
            size_bytes,
            record_count,
            smallest_file_size_bytes: size_bytes,
            largest_file_size_bytes: size_bytes,
            first_file: dated.then(|| file.clone()),
            last_file: dated.then(|| file.clone()),
            sample_hash: hash,
            sample_file: file,
        }
    }

    fn add(&mut self, file: v1::TableFile, record_count: Option<u64>) {
        let size_bytes = file.size_bytes.unwrap_or_default();
        let hash = file_hash(&file);
        self.etag ^= hash;
        self.file_count += 1;
        self.size_bytes += size_bytes;
        self.record_count = self.record_count.zip(record_count).map(|(a, b)| a + b);
        self.smallest_file_size_bytes = self.smallest_file_size_bytes.min(size_bytes);
        self.largest_file_size_bytes = self.largest_file_size_bytes.max(size_bytes);
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
        if (hash, file.path.as_str()) < (self.sample_hash, self.sample_file.path.as_str()) {
            self.sample_hash = hash;
            self.sample_file = file;
        }
    }

    fn finish(
        self,
        table: &str,
        partition_values: BTreeMap<String, Option<String>>,
        order: &[String],
    ) -> v1::Partition {
        let named = |mut file: v1::TableFile| {
            file.name = v1::name::table_file_name(table, &file.path);
            file
        };
        v1::Partition {
            name: v1::name::partition_name(table, &partition_id(&partition_values, order)),
            partition_values,
            etag: format!("{:016x}", self.etag),
            file_count: self.file_count,
            size_bytes: self.size_bytes,
            record_count: self.record_count,
            smallest_file_size_bytes: self.smallest_file_size_bytes,
            largest_file_size_bytes: self.largest_file_size_bytes,
            first_file: self.first_file.map(named),
            last_file: self.last_file.map(named),
            sample_file: Some(named(self.sample_file)),
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
                out.push_str(&format!("%{:02X}", u32::from(character)));
            }
            _ => out.push(character),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::partition_id;

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
}

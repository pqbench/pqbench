//! The only module that names the `deltalake` crate: snapshot resolution.
//!
//! [`load`] is plain async code; the caller owns the runtime. Drive it from a
//! current-thread runtime: delta-rs then selects its own executor instead of
//! borrowing the caller's, which its kernel can panic on after a failed load.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use deltalake::kernel::scalars::ScalarExt;
use deltalake::logstore::LogStore;
use deltalake::{DeltaTable, DeltaTableBuilder};
use futures::TryStreamExt;
use serde_json::{Map, Value};
use url::Url;

use crate::table::{
    add_partition_total, bytes_per_row, finish_partition_masses, FileStats, LoadEvent, LoadRequest,
    LogAction, LogCommit, PartitionMass, TableFile, TableFormat, TableInfo,
};

/// Errors resolving a snapshot through delta-rs.
#[derive(Debug)]
pub(super) struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "delta: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// Resolve the transaction log and the active files of a Delta table.
pub(super) async fn load(request: &LoadRequest) -> Result<TableInfo, Error> {
    visit_load(request, &mut |_| Ok(())).await
}

/// Resolve a Delta snapshot, visiting the header then each active file.
pub(super) async fn visit_load(
    request: &LoadRequest,
    visit: &mut impl FnMut(LoadEvent<'_>) -> Result<(), crate::table::Error>,
) -> Result<TableInfo, Error> {
    let table = open(&request.uri, request.snapshot_version, &request.env).await?;
    let snapshot = snapshot_meta(&table)?;
    let log = read_log(&table, snapshot.version).await?;
    let mut info = TableInfo::new(
        TableFormat::DELTA,
        request.uri.clone(),
        snapshot.version,
        snapshot.partition_columns,
        log,
        Vec::new(),
        request.env.clone(),
    );
    visit(LoadEvent::BEGIN { info: &info }).map_err(|error| Error(error.to_string()))?;
    let (files, partitions) = active_files(&table, request, visit).await?;
    if request.collect_files {
        info.files = files;
    }
    info.partitions = partitions;
    Ok(info)
}

async fn open(
    uri: &str,
    version: Option<u64>,
    env: &BTreeMap<String, String>,
) -> Result<DeltaTable, Error> {
    if uri.contains("://") {
        let url = Url::parse(uri).map_err(|e| Error(format!("invalid table URI: {e}")))?;
        if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|()| Error("invalid local table URI".into()))?;
            return load_local_table(&local_root(&path)?, version, env).await;
        }
        return load_table(url, version, env).await;
    }
    load_local_table(&local_root(Path::new(uri))?, version, env).await
}

fn local_root(path: &Path) -> Result<PathBuf, Error> {
    let root = path
        .canonicalize()
        .map_err(|e| Error(format!("cannot open table {}: {e}", path.display())))?;
    if !root.join("_delta_log").is_dir() {
        return Err(Error(format!("missing _delta_log in {}", root.display())));
    }
    Ok(root)
}

async fn load_local_table(
    root: &Path,
    version: Option<u64>,
    env: &BTreeMap<String, String>,
) -> Result<DeltaTable, Error> {
    let url = Url::from_directory_path(root)
        .map_err(|()| Error("cannot convert table path to a local file URL".into()))?;
    load_table(url, version, env).await
}

async fn load_table(
    url: Url,
    version: Option<u64>,
    env: &BTreeMap<String, String>,
) -> Result<DeltaTable, Error> {
    let mut builder = DeltaTableBuilder::from_url(url).map_err(delta_error)?;
    if !env.is_empty() {
        builder = builder.with_storage_options(env.clone().into_iter().collect());
    }
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    builder.load().await.map_err(delta_error)
}

struct SnapshotMeta {
    version: u64,
    partition_columns: Vec<String>,
}

fn snapshot_meta(table: &DeltaTable) -> Result<SnapshotMeta, Error> {
    let snapshot = table.snapshot().map_err(delta_error)?;
    Ok(SnapshotMeta {
        version: snapshot.version(),
        partition_columns: snapshot.metadata().partition_columns().to_vec(),
    })
}

async fn read_log(table: &DeltaTable, last_version: u64) -> Result<Vec<LogCommit>, Error> {
    let store = table.log_store();
    let mut commits = Vec::new();
    for version in 0..=last_version {
        let Some(bytes) = store
            .read_commit_entry(version)
            .await
            .map_err(delta_error)?
        else {
            continue;
        };
        commits.push(LogCommit {
            version,
            actions: parse_commit(&bytes, version)?,
        });
    }
    Ok(commits)
}

fn parse_commit(bytes: &[u8], version: u64) -> Result<Vec<LogAction>, Error> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error(format!("commit {version} is not UTF-8: {e}")))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| parse_action(line, version))
        .collect()
}

fn parse_action(line: &str, version: u64) -> Result<LogAction, Error> {
    let value: serde_json::Value = serde_json::from_str(line)
        .map_err(|e| Error(format!("cannot parse commit {version}: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| Error(format!("commit {version} action is not an object")))?;
    let (kind, body) = object
        .iter()
        .next()
        .ok_or_else(|| Error(format!("commit {version} action is empty")))?;
    Ok(LogAction {
        kind: kind.clone(),
        path: body
            .get("path")
            .and_then(|path| path.as_str())
            .map(str::to_owned),
    })
}

/// Resolve every active data file to a path `bytemass` can read, emitting each
/// file as the add-action stream yields it and summing per-partition totals.
async fn active_files(
    table: &DeltaTable,
    request: &LoadRequest,
    visit: &mut impl FnMut(LoadEvent<'_>) -> Result<(), crate::table::Error>,
) -> Result<(Vec<TableFile>, Vec<PartitionMass>), Error> {
    let root = if table.table_url().scheme() == "file" {
        Some(
            table
                .table_url()
                .to_file_path()
                .map_err(|()| Error("invalid local table URI".into()))?,
        )
    } else {
        None
    };
    let mut files = table.get_active_add_actions_by_partitions(&[]);
    let mut active = vec![];
    let mut totals = std::collections::BTreeMap::new();
    while let Some(file) = files.try_next().await.map_err(delta_error)? {
        let relative = file.path().to_string();
        let size = u64::try_from(file.size())
            .map_err(|_| Error(format!("invalid file size in log: {relative}")))?;
        let uri = match &root {
            Some(root) => local_uri(root, &relative)?,
            None => object_uri(table.table_url(), &relative)?,
        };
        let mut stats = file_stats(file.stats());
        if let Some(stats) = stats.as_mut() {
            stats.bytes_per_row = bytes_per_row(size, stats.num_records);
            if !request.file_stats {
                stats.min_values.clear();
                stats.max_values.clear();
                stats.null_count.clear();
                stats.tight_bounds = None;
            }
        }
        let table_file = TableFile {
            path: relative,
            uri,
            size_bytes: size,
            partition_values: partition_values(&file),
            stats,
        };
        add_partition_total(&mut totals, &table_file).map_err(|error| Error(error.to_string()))?;
        visit(LoadEvent::FILE { file: &table_file }).map_err(|error| Error(error.to_string()))?;
        if request.collect_files {
            active.push(table_file);
        }
    }
    Ok((active, finish_partition_masses(totals)))
}

fn partition_values(file: &deltalake::kernel::LogicalFileView) -> BTreeMap<String, Option<String>> {
    let Some(parsed) = file.partition_values() else {
        return BTreeMap::new();
    };
    parsed
        .fields()
        .iter()
        .zip(parsed.values())
        .map(|(field, value)| {
            let partition = if value.is_null() {
                None
            } else {
                Some(value.serialize())
            };
            (field.name().clone(), partition)
        })
        .collect()
}

/// Parse `add.stats`. Nested `nullCount` objects are flattened to dotted
/// keys. A malformed stats blob is omitted rather than failing the table.
fn file_stats(raw: Option<String>) -> Option<FileStats> {
    let raw = raw?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    let object = value.as_object()?;
    let num_records = object.get("numRecords").and_then(Value::as_u64)?;
    Some(FileStats {
        num_records,
        bytes_per_row: None,
        min_values: stat_object(object, "minValues"),
        max_values: stat_object(object, "maxValues"),
        null_count: flatten_null_counts(object.get("nullCount")),
        tight_bounds: object.get("tightBounds").and_then(Value::as_bool),
    })
}

fn stat_object(object: &Map<String, Value>, key: &str) -> BTreeMap<String, Value> {
    match object.get(key) {
        Some(Value::Object(entries)) => entries
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        _ => BTreeMap::new(),
    }
}

fn flatten_null_counts(value: Option<&Value>) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    flatten_counts(value, "", &mut out);
    out
}

fn flatten_counts(value: Option<&Value>, prefix: &str, out: &mut BTreeMap<String, u64>) {
    match value {
        Some(Value::Number(number)) => {
            if let Some(count) = number.as_u64() {
                if !prefix.is_empty() {
                    out.insert(prefix.to_string(), count);
                }
            }
        }
        Some(Value::Object(entries)) => {
            for (key, nested) in entries {
                let next = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_counts(Some(nested), &next, out);
            }
        }
        _ => {}
    }
}

/// A data path that stays inside the table: empty, absolute, or URI paths
/// are rejected here, once, for both local and object adapters.
fn relative_data_path(relative: &str) -> Result<&Path, Error> {
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.contains("://")
        || !path.components().all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(Error(format!(
            "only relative data paths inside the table are supported: {relative}"
        )));
    }
    Ok(path)
}

fn object_uri(base: &Url, relative: &str) -> Result<String, Error> {
    relative_data_path(relative)?;
    let mut directory = base.clone();
    if !directory.path().ends_with('/') {
        directory.set_path(&format!("{}/", directory.path()));
    }
    Ok(directory
        .join(relative)
        .map_err(|e| Error(format!("invalid active file path {relative}: {e}")))?
        .into())
}

/// Join a local active file without opening it. Size is the log's claim;
/// `bytemass` compares the measured size later.
fn local_uri(root: &Path, relative: &str) -> Result<String, Error> {
    let path = relative_data_path(relative)?;
    Ok(root.join(path).to_string_lossy().into_owned())
}

fn delta_error(error: deltalake::DeltaTableError) -> Error {
    Error(format!("cannot resolve snapshot: {error}"))
}

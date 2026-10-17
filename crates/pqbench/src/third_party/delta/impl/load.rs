//! The only module that names the `deltalake` crate: snapshot resolution.
//!
//! [`load`] is plain async code; the caller owns the runtime. Drive it from a
//! current-thread runtime: delta-rs then selects its own executor instead of
//! borrowing the caller's, which its kernel can panic on after a failed load.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use deltalake::kernel::scalars::ScalarExt;
use deltalake::kernel::Snapshot;
use deltalake::logstore::object_store::ObjectStoreExt;
use deltalake::logstore::{commit_uri_from_version, LogStore, LogStoreRef};
use deltalake::{DeltaTable, DeltaTableBuilder, ObjectStoreError};
use futures::{Stream, TryStreamExt};
use serde_json::{Map, Value};
use url::Url;

use crate::table::{
    add_partition_total, bytes_per_row, finish_partition_masses, Column, FileStats, LoadRequest,
    LogAction, LogCommit, PartitionMass, TableFile, TableFormat, TableInfo,
};
use crate::Lake;

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
pub(super) async fn load(lake: &Lake, request: &LoadRequest) -> Result<TableInfo, Error> {
    let store = open_store(lake, request)?;
    // A plain snapshot is lazy: it reads the log's actions, not its files, so
    // even a header read never materializes the active-file set. A loaded
    // `DeltaTable` would, at O(files) memory.
    let snapshot = Snapshot::try_new(store.as_ref(), request.snapshot_version)
        .await
        .map_err(delta_error)?;
    let meta = snapshot_meta(&snapshot);
    let mut info = TableInfo::new(
        TableFormat::DELTA,
        request.uri.clone(),
        meta.version,
        meta.partition_columns,
        Vec::new(),
        Vec::new(),
        request.env.clone(),
    );
    info.columns = meta.columns;
    info.delta_properties = meta.properties;
    if !request.require_files {
        return Ok(info);
    }
    info.log = read_log(&store, meta.version, request.commit_versions.as_ref()).await?;
    // A caller that names the commits wants the files the log itself names; the
    // snapshot's active-file replay is redundant then (and O(files)).
    if request.commit_versions.is_none() {
        let mut table = DeltaTable::new(store);
        table
            .load_version(meta.version)
            .await
            .map_err(delta_error)?;
        let (files, partitions) = active_files(&table, request).await?;
        info.files = files;
        info.partitions = partitions;
    }
    Ok(info)
}

/// The table's location as a URL: a URI as given, or a local directory made
/// absolute.
fn table_url(request: &LoadRequest) -> Result<Url, Error> {
    if request.uri.contains("://") {
        let url = Url::parse(&request.uri).map_err(|e| Error(format!("invalid table URI: {e}")))?;
        if url.scheme() != "file" {
            return Ok(url);
        }
        let path = url
            .to_file_path()
            .map_err(|()| Error("invalid local table URI".into()))?;
        return directory_url(&local_root(&path)?);
    }
    directory_url(&local_root(Path::new(&request.uri))?)
}

fn directory_url(root: &Path) -> Result<Url, Error> {
    Url::from_directory_path(root)
        .map_err(|()| Error("cannot convert table path to a local file URL".into()))
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

/// Resolve the table's log store, under the request's storage options and (for
/// S3) the shared client: no snapshot, so the open materializes no files.
fn open_store(lake: &Lake, request: &LoadRequest) -> Result<LogStoreRef, Error> {
    let url = table_url(request)?;
    let mut builder = DeltaTableBuilder::from_url(url.clone()).map_err(delta_error)?;
    if !request.env.is_empty() {
        builder = builder.with_storage_options(request.env.clone().into_iter().collect());
    }
    if let Some(store) = shared_store(lake, &url, request)? {
        builder = builder.with_storage_backend(store, url);
    }
    builder.build_storage().map_err(delta_error)
}

/// The store on the one shared HTTP client, for delta-rs to reuse instead of
/// building a fresh client per table (each build re-parses the platform CA
/// bundle). Only S3 reaches the S3 backend; other schemes build their own.
fn shared_store(
    lake: &Lake,
    url: &Url,
    request: &LoadRequest,
) -> Result<Option<Arc<dyn deltalake::ObjectStore>>, Error> {
    #[cfg(feature = "aws")]
    {
        if matches!(url.scheme(), "s3" | "s3a") {
            let options: Vec<(String, String)> = request.env.clone().into_iter().collect();
            let store = lake
                .stores()
                .root_store(url, &options)
                .map_err(|error| Error(error.to_string()))?;
            return Ok(Some(store));
        }
    }
    #[cfg(not(feature = "aws"))]
    let _ = (lake, url, request);
    Ok(None)
}

struct SnapshotMeta {
    version: u64,
    partition_columns: Vec<String>,
    columns: Vec<Column>,
    properties: BTreeMap<String, String>,
}

fn snapshot_meta(snapshot: &Snapshot) -> SnapshotMeta {
    SnapshotMeta {
        version: snapshot.version(),
        partition_columns: snapshot.metadata().partition_columns().to_vec(),
        columns: snapshot
            .schema()
            .fields()
            .map(|field| Column {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect(),
        properties: snapshot
            .metadata()
            .configuration()
            .clone()
            .into_iter()
            .collect(),
    }
}

async fn read_log(
    store: &LogStoreRef,
    last_version: u64,
    versions: Option<&BTreeSet<u64>>,
) -> Result<Vec<LogCommit>, Error> {
    if let Some(versions) = versions {
        // The named commits, in version order.
        let mut commits = Vec::new();
        for &version in versions {
            let Some(bytes) = store
                .read_commit_entry(version)
                .await
                .map_err(delta_error)?
            else {
                continue;
            };
            let parsed = parse_commit(&bytes, version)?;
            drop(bytes);
            commits.push(LogCommit {
                version,
                commit_time: parsed.commit_time,
                actions: parsed.actions,
            });
        }
        return Ok(commits);
    }
    // A JSON commit per version is contiguous up to log retention (the kernel's
    // log segment enforces no gaps), so reading newest-first and stopping at the
    // first retained-boundary hole reads every available commit and probes no
    // missing version — the difference between O(retained) and O(#versions).
    let mut commits = Vec::new();
    let mut version = last_version as i64;
    while version >= 0 {
        let index = version as u64;
        match store.read_commit_entry(index).await.map_err(delta_error)? {
            Some(bytes) => {
                let parsed = parse_commit(&bytes, index)?;
                drop(bytes);
                commits.push(LogCommit {
                    version: index,
                    commit_time: parsed.commit_time,
                    actions: parsed.actions,
                });
            }
            // A hole only above the retained boundary; the head itself may be
            // summarized by a checkpoint with no JSON left.
            None if !commits.is_empty() => break,
            None => {}
        }
        version -= 1;
    }
    commits.reverse();
    Ok(commits)
}

/// Stream the table's commits, newest first, without collecting them.
///
/// Each retained commit file is read one version at a time straight from the
/// log store, so nothing grows with the log's length and no active-file
/// metadata is materialized. This is the walk the library's `history` makes
/// internally; `DeltaTable::history` reaches it only through a loaded, and so
/// fully materialized, table.
pub(super) async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<LogCommit, Error>>, Error> {
    let store = open_store(lake, request)?;
    // A plain snapshot is lazy: it reads the table's metadata and lists the log
    // segment, and has no files of its own to materialize.
    let head = Snapshot::try_new(store.as_ref(), request.snapshot_version)
        .await
        .map_err(delta_error)?
        .version();
    Ok(commits_newest_first(store, head))
}

/// Stream the log's commits newest first, ending at the retention hole.
///
/// A version with no JSON file above the retained boundary is only a checkpoint
/// that outran the commits it summarizes, so the walk steps over it; the first
/// hole below a file that did read ends the stream. A commit the log does not
/// date has no time to contribute, so it is not yielded.
fn commits_newest_first(
    store: LogStoreRef,
    head: u64,
) -> impl Stream<Item = Result<LogCommit, Error>> {
    futures::stream::try_unfold((Some(head), false), move |(mut version, mut started)| {
        let store = store.clone();
        async move {
            loop {
                let Some(current) = version else {
                    return Ok(None);
                };
                version = current.checked_sub(1);
                let time = match commit_head(&store, current).await? {
                    CommitHead::Missing if started => return Ok(None),
                    CommitHead::Missing => continue,
                    CommitHead::Present(time) => {
                        started = true;
                        time
                    }
                };
                let Some(time) = time else {
                    continue;
                };
                return Ok(Some((
                    LogCommit {
                        version: current,
                        commit_time: Some(time),
                        actions: Vec::new(),
                    },
                    (version, started),
                )));
            }
        }
    })
}

/// Bytes of a commit file to read for its head: `commitInfo` is a few hundred
/// bytes and Delta writers put it first, so a prefix carries it.
const COMMIT_HEADER_BYTES: u64 = 4096;

/// What one log version's commit file says about itself.
#[derive(Debug, Clone, Copy)]
enum CommitHead {
    /// No JSON file at this version: log retention, or a commit that only a
    /// checkpoint still summarizes.
    Missing,
    /// The file is there; the time when the log records one.
    Present(Option<i64>),
}

/// The head of one log version, read as a prefix of its commit file.
///
/// Only a commit whose `commitInfo` is not first, or whose header runs past the
/// prefix, is read whole: the log's own order decides, and a read shorter than
/// the prefix is the whole file.
async fn commit_head(store: &LogStoreRef, version: u64) -> Result<CommitHead, Error> {
    let object = store.object_store(None);
    let path = commit_uri_from_version(Some(version));
    match object.get_range(&path, 0..COMMIT_HEADER_BYTES).await {
        Ok(prefix) => {
            if let Some(time) = parse_commit_time(&prefix) {
                return Ok(CommitHead::Present(Some(time)));
            }
            if (prefix.len() as u64) < COMMIT_HEADER_BYTES {
                return Ok(CommitHead::Present(None));
            }
        }
        Err(ObjectStoreError::NotFound { .. }) => return Ok(CommitHead::Missing),
        // Anything else is the whole read's to report.
        Err(_) => {}
    }
    match store
        .read_commit_entry(version)
        .await
        .map_err(delta_error)?
    {
        Some(bytes) => Ok(CommitHead::Present(parse_commit_time(&bytes))),
        None => Ok(CommitHead::Missing),
    }
}

/// `commitInfo.timestamp` of a commit, decoded from its own line: the actions
/// of a long commit are not parsed. A read that stops mid-line — a prefix, or a
/// log that does not end in a newline — simply holds no `commitInfo` yet.
fn parse_commit_time(bytes: &[u8]) -> Option<i64> {
    for line in bytes.split(|byte| *byte == b'\n') {
        let line = String::from_utf8_lossy(line);
        if !line.contains("\"commitInfo\"") {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        return value
            .get("commitInfo")
            .and_then(|info| info.get("timestamp"))
            .and_then(Value::as_i64);
    }
    None
}

/// The parts of a commit file a read keeps.
struct ParsedCommit {
    actions: Vec<LogAction>,
    commit_time: Option<i64>,
}

fn parse_commit(bytes: &[u8], version: u64) -> Result<ParsedCommit, Error> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error(format!("commit {version} is not UTF-8: {e}")))?;
    let mut actions = Vec::new();
    let mut commit_time = None;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| Error(format!("cannot parse commit {version}: {e}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| Error(format!("commit {version} action is not an object")))?;
        let (kind, body) = object
            .iter()
            .next()
            .ok_or_else(|| Error(format!("commit {version} action is empty")))?;
        if kind == "commitInfo" && commit_time.is_none() {
            commit_time = body.get("timestamp").and_then(serde_json::Value::as_i64);
        }
        actions.push(LogAction {
            kind: kind.clone(),
            path: body
                .get("path")
                .and_then(|path| path.as_str())
                .map(str::to_owned),
        });
    }
    Ok(ParsedCommit {
        actions,
        commit_time,
    })
}

/// Resolve every active data file to a path `bytemass` can read, collecting
/// each file as the add-action stream yields it and summing per-partition
/// totals.
async fn active_files(
    table: &DeltaTable,
    request: &LoadRequest,
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
        active.push(table_file);
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
    use crate::third_party::object_store::r#impl::error_chain;
    Error(format!("cannot resolve snapshot: {}", error_chain(&error)))
}

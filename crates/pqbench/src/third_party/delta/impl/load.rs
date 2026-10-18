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
use deltalake::logstore::object_store::path::Path as ObjectPath;
use deltalake::logstore::object_store::{ObjectStore, ObjectStoreExt};
use deltalake::logstore::{commit_uri_from_version, LogStore, LogStoreRef};
use deltalake::{DeltaTable, DeltaTableBuilder, ObjectStoreError};
use futures::{Stream, StreamExt, TryStreamExt};
use serde_json::{Map, Value};
use url::Url;

use crate::table::{
    add_partition_total, bytes_per_row, finish_partition_masses, FileStats, LoadRequest, LogCommit,
    PartitionMass, TableFile, TableFormat, TableInfo,
};
use crate::v1;
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
    info.log = read_log(
        &store,
        &request.uri,
        Some(meta.version),
        request.commit_versions.as_ref(),
    )
    .await?;
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
    columns: Vec<v1::DeclaredColumn>,
    properties: BTreeMap<String, String>,
}

fn snapshot_meta(snapshot: &Snapshot) -> SnapshotMeta {
    SnapshotMeta {
        version: snapshot.version(),
        partition_columns: snapshot.metadata().partition_columns().to_vec(),
        columns: snapshot
            .schema()
            .fields()
            .map(|field| v1::DeclaredColumn {
                column: field.name().clone(),
                data_type: crate::table::non_empty(&field.data_type().to_string()),
                nullable: field.is_nullable(),
                position: None,
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

/// Read the log's commits, actions and all, in version order.
///
/// The caller either names the commits it wants or takes them all; either way
/// the versions are a list, so the reads are a map over it and come back in
/// version order as they were asked for.
async fn read_log(
    store: &LogStoreRef,
    uri: &str,
    head: Option<u64>,
    versions: Option<&BTreeSet<u64>>,
) -> Result<Vec<LogCommit>, Error> {
    let versions: Vec<u64> = match versions {
        Some(named) => named.iter().copied().collect(),
        None => commit_versions(store, uri, head).await?,
    };
    let store = store.clone();
    // As in `stream_commits`: the versions are known, so every read goes at once
    // and the commits come back in version order.
    let commits = futures::future::try_join_all(versions.into_iter().map(|version| {
        let store = store.clone();
        async move {
            let Some(bytes) = store
                .read_commit_entry(version)
                .await
                .map_err(delta_error)?
            else {
                // Named but gone: the log kept no actions to read.
                return Ok(None);
            };
            let parsed = parse_commit(&bytes, version)?;
            drop(bytes);
            Ok(Some(LogCommit {
                version,
                commit_time: parsed.commit_time,
                commit_info: parsed.commit_info,
                actions: parsed.actions,
            }))
        }
    }))
    .await?;
    Ok(commits.into_iter().flatten().collect())
}

/// Stream the table's commits in version order, oldest first.
///
/// The log names its commits, so the versions come from one listing of it: every
/// retained version, none probed, nothing asked for below retention, no hole to
/// step over. Each version's time then comes from the commit itself, read with
/// the store's own concurrency rather than one round trip at a time, so the walk
/// costs the log's latency over the commits in reach and not the sum of it.
pub(super) async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<LogCommit, Error>>, Error> {
    let store = open_store(lake, request)?;
    let versions = commit_versions(&store, &request.uri, request.snapshot_version).await?;
    // The log names every commit, so no read waits on another: they all go at
    // once and are yielded in version order, newest first, as they land. A commit
    // the log does not date has no time to contribute, and one the listing named
    // that a read no longer finds has nothing left to read, so neither is yielded.
    Ok(futures::stream::iter(versions.into_iter().rev())
        .map(move |version| {
            let store = store.clone();
            async move {
                commit_head(&store, version)
                    .await
                    .map(|head| (version, head))
            }
        })
        .buffered(commit_read_width())
        .try_filter_map(|(version, head)| async move {
            Ok(match head {
                CommitHead::Present(Some(info)) => {
                    // A commit the log does not date has no window to fall in.
                    info.get("timestamp")
                        .and_then(Value::as_i64)
                        .map(|commit_time| LogCommit {
                            version,
                            commit_time: Some(commit_time),
                            commit_info: Some(info),
                            actions: Vec::new(),
                        })
                }
                CommitHead::Missing | CommitHead::Present(None) => None,
            })
        }))
}

/// Stream the files the requested commits added, in version order.
///
/// A commit's `add` actions name the files it wrote, so the read is the actions
/// themselves: no snapshot, no `_last_checkpoint`, no checkpoint, no active-file
/// replay. The versions come from the caller when it names them — `list files`
/// does, one window at a time — and from the log's own listing when it does not.
///
/// A version the listing names that a read no longer finds has nothing left to
/// read — retention kept no actions — and contributes no files.
pub(super) async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<TableFile, Error>>, Error> {
    let store = open_store(lake, request)?;
    let versions: Vec<u64> = match request.commit_versions.as_ref() {
        Some(named) => named.iter().copied().collect(),
        None => commit_versions(&store, &request.uri, request.snapshot_version).await?,
    };
    let uri = request.uri.clone();
    // Version order is the log's order, and `buffered` keeps it while bounding
    // how many commits are in flight at once; a commit's files are yielded as a
    // group once its own read lands.
    Ok(futures::stream::iter(versions)
        .map(move |version| {
            let store = store.clone();
            let uri = uri.clone();
            async move { commit_files(&store, &uri, version).await }
        })
        .buffered(commit_read_width())
        .map_ok(|files| futures::stream::iter(files.into_iter().map(Ok)))
        .try_flatten())
}

/// The files one commit's `add` actions name, in file order.
async fn commit_files(
    store: &LogStoreRef,
    uri: &str,
    version: u64,
) -> Result<Vec<TableFile>, Error> {
    let Some(bytes) = store
        .read_commit_entry(version)
        .await
        .map_err(delta_error)?
    else {
        // Named but gone: the log kept no actions to read.
        return Ok(Vec::new());
    };
    let parsed = parse_commit(&bytes, version)?;
    drop(bytes);
    parsed
        .actions
        .into_iter()
        .filter(|action| action.kind.as_deref() == Some("add"))
        .filter_map(|action| action.path)
        .map(|path| added_file(uri, path))
        .collect()
}

/// The table file one `add` action's path describes.
fn added_file(uri: &str, path: String) -> Result<TableFile, Error> {
    let file_uri = crate::table::join_uri(uri, &path).map_err(|error| Error(error.to_string()))?;
    Ok(TableFile::new(path, file_uri, 0))
}

/// The table's commit versions, oldest first, from the log's own listing.
///
/// A JSON commit is named `00000000000000000042.json` and nothing else is: a
/// checkpoint, a compiled log, a sidecar, and an archiver's droppings all have
/// other shapes, so the name alone names the commit. Retention removes a prefix,
/// so the retained versions are contiguous and a gap is a log that lost commits
/// — a read either side of it would report a table that never existed.
async fn commit_versions(
    store: &LogStoreRef,
    uri: &str,
    head: Option<u64>,
) -> Result<Vec<u64>, Error> {
    let log = ObjectPath::from("_delta_log");
    // Folded rather than collected: a version is eight bytes, the object listing
    // is not, and a log can name millions of files.
    let (mut versions, listed) = store
        .object_store(None)
        .list_with_offset(Some(&log), &log)
        .try_fold(
            (Vec::new(), false),
            |(mut versions, _), object| async move {
                let name = object.location.filename().unwrap_or_default();
                if let Some(version) = commit_file_version(name) {
                    versions.push(version);
                }
                Ok::<_, ObjectStoreError>((versions, true))
            },
        )
        .await
        .map_err(|error| Error(error.to_string()))?;
    if !listed {
        return Err(Error(format!("no _delta_log in {uri}")));
    }
    versions.retain(|version| head.is_none_or(|head| *version <= head));
    versions.sort_unstable();
    versions.dedup();
    if let Some(gap) = versions.windows(2).find(|pair| pair[1] > pair[0] + 1) {
        return Err(Error(format!(
            "the log misses commit {}: versions {} and {} are not contiguous",
            gap[0] + 1,
            gap[0],
            gap[1]
        )));
    }
    Ok(versions)
}

/// The version a commit file name carries, or `None` when the name is not a
/// commit's.
fn commit_file_version(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".json")?;
    let digits = stem.len() == 20 && stem.bytes().all(|byte| byte.is_ascii_digit());
    digits.then(|| stem.parse().ok()).flatten()
}

/// How many commit reads one walk keeps in flight: the store's own concurrency,
/// as delta-rs takes it for its own commit stream. Unbounded, every read's body
/// is resident at once — a million-commit log would be gigabytes — and windowed
/// it is flat in the log.
fn commit_read_width() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        * 4
}

/// How far into a commit file to read for its head, in stages: `commitInfo` is a
/// few hundred bytes and Delta writers put it first, so the first read carries
/// it for almost every commit; a commit whose header is longer is read further
/// — 4 KB covers the long ones — before the file is read whole.
const COMMIT_HEADER_STAGES: [u64; 2] = [1024, 4096];

/// What one log version's commit file says about itself.
#[derive(Debug, Clone)]
enum CommitHead {
    /// No JSON file at this version: log retention, or a commit that only a
    /// checkpoint still summarizes.
    Missing,
    /// The file is there; its `commitInfo`, when the log records one.
    Present(Option<Value>),
}

/// The head of one log version, read as a prefix of its commit file.
///
/// The prefix grows by stages and only then whole: the log's own order decides,
/// and a read shorter than the stage asked for is the whole file.
async fn commit_head(store: &LogStoreRef, version: u64) -> Result<CommitHead, Error> {
    let object = store.object_store(None);
    let path = commit_uri_from_version(Some(version));
    for end in COMMIT_HEADER_STAGES {
        match object.get_range(&path, 0..end).await {
            Ok(prefix) => {
                if let Some(info) = parse_commit_info(&prefix) {
                    return Ok(CommitHead::Present(Some(info)));
                }
                if (prefix.len() as u64) < end {
                    return Ok(CommitHead::Present(None));
                }
            }
            Err(ObjectStoreError::NotFound { .. }) => return Ok(CommitHead::Missing),
            // Anything else is the whole read's to report.
            Err(_) => break,
        }
    }
    match store
        .read_commit_entry(version)
        .await
        .map_err(delta_error)?
    {
        Some(bytes) => Ok(CommitHead::Present(parse_commit_info(&bytes))),
        None => Ok(CommitHead::Missing),
    }
}

/// A commit's `commitInfo` action, decoded from its own line: the actions of a
/// long commit are not parsed. A read that stops mid-line — a prefix, or a log
/// that does not end in a newline — simply holds no `commitInfo` yet.
fn parse_commit_info(bytes: &[u8]) -> Option<Value> {
    for line in bytes.split(|byte| *byte == b'\n') {
        let line = String::from_utf8_lossy(line);
        if !line.contains("\"commitInfo\"") {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        return value.get("commitInfo").cloned();
    }
    None
}

/// The parts of a commit file a read keeps.
struct ParsedCommit {
    actions: Vec<v1::LogAction>,
    commit_time: Option<i64>,
    commit_info: Option<serde_json::Value>,
}

fn parse_commit(bytes: &[u8], version: u64) -> Result<ParsedCommit, Error> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error(format!("commit {version} is not UTF-8: {e}")))?;
    let mut actions = Vec::new();
    let mut commit_time = None;
    let mut commit_info = None;
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
        if kind == "commitInfo" && commit_info.is_none() {
            commit_info = Some(body.clone());
            commit_time = body.get("timestamp").and_then(serde_json::Value::as_i64);
        }
        actions.push(v1::LogAction {
            kind: Some(kind.clone()),
            path: body
                .get("path")
                .and_then(|path| path.as_str())
                .map(str::to_owned),
        });
    }
    Ok(ParsedCommit {
        actions,
        commit_time,
        commit_info,
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
        let table_file = data_file(&root, table.table_url(), &file, request.file_stats)?;
        add_partition_total(&mut totals, &table_file).map_err(|error| Error(error.to_string()))?;
        active.push(table_file);
    }
    Ok((active, finish_partition_masses(totals)))
}

/// One add action as a `TableFile`, resolving its storage path and, unless
/// `keep_stats` is set, trimming the stats to the byte facts.
fn data_file(
    root: &Option<PathBuf>,
    table_url: &Url,
    file: &deltalake::kernel::LogicalFileView,
    keep_stats: bool,
) -> Result<TableFile, Error> {
    let relative = file.path().to_string();
    let size = u64::try_from(file.size())
        .map_err(|_| Error(format!("invalid file size in log: {relative}")))?;
    let uri = match root {
        Some(root) => local_uri(root, &relative)?,
        None => object_uri(table_url, &relative)?,
    };
    let mut stats = file_stats(file.stats());
    if let Some(stats) = stats.as_mut() {
        stats.bytes_per_row = bytes_per_row(size, stats.num_records);
        if !keep_stats {
            stats.min_values.clear();
            stats.max_values.clear();
            stats.null_count.clear();
            stats.tight_bounds = None;
        }
    }
    Ok(TableFile {
        path: relative,
        uri,
        size_bytes: size,
        partition_values: partition_values(file),
        stats,
    })
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

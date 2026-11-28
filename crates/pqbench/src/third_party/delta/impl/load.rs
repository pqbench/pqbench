//! The only module that names the `deltalake` crate: snapshot resolution.
//!
//! [`load`] is plain async code; the caller owns the runtime. Drive it from a
//! current-thread runtime: delta-rs then selects its own executor instead of
//! borrowing the caller's, which its kernel can panic on after a failed load.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
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

use crate::table::{relative_data_path, Error, LoadRequest, PartitionMass, TableInfo};
use crate::v1;
use crate::Lake;

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
        v1::TableFormat::Delta,
        request.uri.clone(),
        meta.version,
        meta.partition_columns,
        Vec::new(),
        Vec::new(),
    );
    info.columns = meta.columns;
    info.delta_properties = meta.properties;
    info.create_time = meta.create_time;
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
        let url = Url::parse(&request.uri).map_err(Error::Uri)?;
        if url.scheme() != "file" {
            return Ok(url);
        }
        let path = url
            .to_file_path()
            .map_err(|()| Error::Invalid("invalid local table URI".into()))?;
        return directory_url(&local_root(&path)?);
    }
    directory_url(&local_root(Path::new(&request.uri))?)
}

fn directory_url(root: &Path) -> Result<Url, Error> {
    Url::from_directory_path(root)
        .map_err(|()| Error::Invalid("cannot convert table path to a local file URL".into()))
}

fn local_root(path: &Path) -> Result<PathBuf, Error> {
    let root = path.canonicalize().map_err(|e| Error::Io {
        path: path.display().to_string(),
        source: e,
    })?;
    if !root.join("_delta_log").is_dir() {
        return Err(Error::Invalid(format!(
            "missing _delta_log in {}",
            root.display()
        )));
    }
    Ok(root)
}

/// Resolve the table's log store, under the request's storage options and (for
/// S3) the shared client: no snapshot, so the open materializes no files.
fn open_store(lake: &Lake, request: &LoadRequest) -> Result<LogStoreRef, Error> {
    let url = table_url(request)?;
    let mut builder = DeltaTableBuilder::from_url(url.clone()).map_err(delta_error)?;
    if !lake.options().is_empty() {
        builder = builder.with_storage_options(lake.options().iter().cloned().collect());
    }
    if let Some(store) = shared_store(lake, &url)? {
        builder = builder.with_storage_backend(store, url);
    }
    builder.build_storage().map_err(delta_error)
}

/// The store on the one shared HTTP client, for delta-rs to reuse instead of
/// building a fresh client per table (each build re-parses the platform CA
/// bundle). Only S3 reaches the S3 backend; other schemes build their own.
fn shared_store(lake: &Lake, url: &Url) -> Result<Option<Arc<dyn deltalake::ObjectStore>>, Error> {
    #[cfg(feature = "aws")]
    {
        if matches!(url.scheme(), "s3" | "s3a") {
            let store = lake
                .stores()
                .root_store(url, lake.options())
                .map_err(|error| Error::Storage(Box::new(error)))?;
            return Ok(Some(store));
        }
    }
    #[cfg(not(feature = "aws"))]
    let _ = (lake, url);
    Ok(None)
}

struct SnapshotMeta {
    version: u64,
    create_time: Option<u64>,
    partition_columns: Vec<String>,
    columns: Vec<v1::DeclaredColumn>,
    properties: BTreeMap<String, String>,
}

fn snapshot_meta(snapshot: &Snapshot) -> SnapshotMeta {
    SnapshotMeta {
        version: snapshot.version(),
        create_time: snapshot
            .metadata()
            .created_time()
            .and_then(|time| u64::try_from(time).ok()),
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
) -> Result<Vec<v1::Commit>, Error> {
    let versions: Vec<u64> = match versions {
        Some(named) => named.iter().copied().collect(),
        None => commit_versions(store, uri, head).await?,
    };
    let store = store.clone();
    let table = v1::name::local_table(uri);
    // As in `stream_commits`: the versions are known, so every read goes at once
    // and the commits come back in version order.
    let commits = futures::future::try_join_all(versions.into_iter().map(|version| {
        let store = store.clone();
        let table = table.clone();
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
            Ok(Some(v1::Commit {
                name: v1::name::commit_name(&table, &version.to_string()),
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

/// Stream the table's commits, newest first.
///
/// The log names its commits, so the versions come from one listing of it: every
/// retained version, none probed, nothing asked for below retention, no hole to
/// step over. Each version's time then comes from the commit itself, read a
/// batch at a time rather than one round trip at a time, so the walk costs the
/// log's latency once per batch and not the sum over the commits in reach — and
/// a batch is only started when the consumer asks for another item, so a stalled
/// consumer leaves no read in flight.
pub(super) async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<v1::Commit, Error>>, Error> {
    let store = open_store(lake, request)?;
    let versions = commit_versions(&store, &request.uri, request.snapshot_version).await?;
    let table = v1::name::local_table(&request.uri);
    // A commit the log does not date has no time to contribute, and one the
    // listing named that a read no longer finds has nothing left to read, so
    // neither is yielded.
    let batches = commit_batches(versions.into_iter().rev());
    Ok(futures::stream::iter(batches)
        .then(move |chunk| {
            let store = store.clone();
            async move {
                futures::future::try_join_all(chunk.into_iter().map(|version| {
                    let store = store.clone();
                    async move {
                        commit_head(&store, version)
                            .await
                            .map(|head| (version, head))
                    }
                }))
                .await
            }
        })
        .map_ok(|chunk| futures::stream::iter(chunk.into_iter().map(Ok)))
        .try_flatten()
        .try_filter_map(move |(version, head)| {
            let table = table.clone();
            async move {
                Ok(match head {
                    CommitHead::Present(Some(info)) => {
                        // A commit the log does not date has no window to fall in.
                        info.get("timestamp")
                            .and_then(Value::as_i64)
                            .map(|commit_time| v1::Commit {
                                name: v1::name::commit_name(&table, &version.to_string()),
                                version,
                                commit_time: Some(commit_time),
                                commit_info: Some(info),
                                actions: Vec::new(),
                            })
                    }
                    CommitHead::Missing | CommitHead::Present(None) => None,
                })
            }
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
) -> Result<impl Stream<Item = Result<v1::TableFile, Error>>, Error> {
    let store = open_store(lake, request)?;
    let versions: Vec<u64> = match request.commit_versions.as_ref() {
        Some(named) => named.iter().copied().collect(),
        None => commit_versions(&store, &request.uri, request.snapshot_version).await?,
    };
    let uri = request.uri.clone();
    // Version order is the log's order, and a batch keeps it while bounding how
    // many commits are in flight at once; a commit's files are yielded as a
    // group once its own read lands.
    let batches = commit_batches(versions);
    Ok(futures::stream::iter(batches)
        .then(move |chunk| {
            let store = store.clone();
            let uri = uri.clone();
            async move {
                futures::future::try_join_all(chunk.into_iter().map(|version| {
                    let store = store.clone();
                    let uri = uri.clone();
                    async move { commit_files(&store, &uri, version).await }
                }))
                .await
            }
        })
        .map_ok(|files| futures::stream::iter(files.into_iter().flatten().map(Ok)))
        .try_flatten())
}

/// The files one commit's `add` actions name, in file order.
async fn commit_files(
    store: &LogStoreRef,
    uri: &str,
    version: u64,
) -> Result<Vec<v1::TableFile>, Error> {
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
        .filter_map(|action| action.path.map(|path| (path, action.size_bytes)))
        .map(|(path, size)| added_file(uri, path, size))
        .collect()
}

/// The table file one `add` action describes: its path and the size the log
/// declares.
fn added_file(uri: &str, path: String, size_bytes: Option<u64>) -> Result<v1::TableFile, Error> {
    let file_uri = crate::table::join_uri(uri, &path)?;
    Ok(v1::TableFile {
        blob: Some(v1::name::blob_name(&file_uri)),
        path,
        size_bytes,
        ..v1::TableFile::default()
    })
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
        .map_err(|error| Error::Storage(Box::new(error)))?;
    if !listed {
        return Err(Error::Invalid(format!("no _delta_log in {uri}")));
    }
    versions.retain(|version| head.is_none_or(|head| *version <= head));
    versions.sort_unstable();
    versions.dedup();
    if let Some(gap) = versions.windows(2).find(|pair| pair[1] > pair[0] + 1) {
        return Err(Error::Invalid(format!(
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

/// The versions to read together, in the order given: `commit_read_width()` of
/// them per batch, so a walk costs the log's latency once per batch rather than
/// once per commit. A batch is the unit the consumer drives — the next one is
/// not started until it asks for it.
fn commit_batches(versions: impl IntoIterator<Item = u64>) -> Vec<Vec<u64>> {
    let versions: Vec<u64> = versions.into_iter().collect();
    versions
        .chunks(commit_read_width())
        .map(<[u64]>::to_vec)
        .collect()
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
        .map_err(|e| Error::Invalid(format!("commit {version} is not UTF-8: {e}")))?;
    let mut actions = Vec::new();
    let mut commit_time = None;
    let mut commit_info = None;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line).map_err(Error::Json)?;
        let object = value
            .as_object()
            .ok_or_else(|| Error::Invalid(format!("commit {version} action is not an object")))?;
        let (kind, body) = object
            .iter()
            .next()
            .ok_or_else(|| Error::Invalid(format!("commit {version} action is empty")))?;
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
            size_bytes: body.get("size").and_then(serde_json::Value::as_u64),
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
) -> Result<(Vec<v1::TableFile>, Vec<PartitionMass>), Error> {
    let root = local_table_root(table.table_url())?;
    let mut files = table.get_active_add_actions_by_partitions(&[]);
    let mut active = vec![];
    let mut totals = std::collections::BTreeMap::new();
    while let Some(file) = files.try_next().await.map_err(delta_error)? {
        let table_file = data_file(&root, table.table_url(), &file, request.file_stats)?;
        add_partition_total(&mut totals, &table_file)?;
        active.push(table_file);
    }
    Ok((active, finish_partition_masses(totals)))
}

/// Fold one active file into its partition's running totals.
fn add_partition_total(
    groups: &mut BTreeMap<BTreeMap<String, Option<String>>, PartitionTotals>,
    file: &v1::TableFile,
) -> Result<(), Error> {
    let entry = groups
        .entry(file.partition_values.clone())
        .or_insert(PartitionTotals {
            file_count: 0,
            size: 0,
            num_records: Some(0),
        });
    entry.file_count += 1;
    entry.size = entry
        .size
        .checked_add(file.size_bytes.unwrap_or(0))
        .ok_or_else(|| Error::Overflow {
            file: file.name.clone(),
            kind: "byte",
        })?;
    entry.num_records =
        match (entry.num_records, file.stats.as_ref()) {
            (Some(sum), Some(stats)) => Some(sum.checked_add(stats.record_count).ok_or_else(
                || Error::Overflow {
                    file: file.name.clone(),
                    kind: "record",
                },
            )?),
            _ => None,
        };
    Ok(())
}

/// One `PartitionMass` per partition's running totals, in partition order.
fn finish_partition_masses(
    groups: BTreeMap<BTreeMap<String, Option<String>>, PartitionTotals>,
) -> Vec<PartitionMass> {
    groups
        .into_iter()
        .map(|(values, totals)| PartitionMass {
            values,
            file_count: totals.file_count,
            size: totals.size,
            bytes_per_row: totals
                .num_records
                .and_then(|records| bytes_per_row(totals.size, records)),
            num_records: totals.num_records,
        })
        .collect()
}

/// The running totals of one partition's active files.
struct PartitionTotals {
    file_count: usize,
    size: u64,
    // aipnaming: allow(aip-141/count-suffix)
    num_records: Option<u64>,
}

/// Bytes per row, or `None` for a file that records no rows.
fn bytes_per_row(size: u64, num_records: u64) -> Option<f64> {
    if num_records == 0 {
        None
    } else {
        Some(size as f64 / num_records as f64)
    }
}

/// Stream the snapshot's active files, newest first, without collecting them.
///
/// The read is the lazy snapshot's own scan, not a `DeltaTable`: the table
/// materializes the snapshot through `EagerSnapshot`, which replays every add
/// action into resident record batches, so the set would be resident once in
/// delta and again in the caller. The lazy scan also materializes each file's
/// `numRecords` from its stats JSON, so a log whose stats are not JSON fails
/// here where [`load`] (the materialized path) omits them.
///
/// The stream owns the snapshot and the store it reads from: `file_views`
/// borrows the snapshot, so one task owns both and hands each file over a
/// one-slot channel, running at most one file ahead of the caller.
pub(super) async fn stream_active_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::FileStream<'static>, Error> {
    let store = open_store(lake, request)?;
    let snapshot = Snapshot::try_new(store.as_ref(), request.snapshot_version)
        .await
        .map_err(delta_error)?;
    let table_url = store.root_url().clone();
    let root = local_table_root(&table_url)?;
    let file_stats = request.file_stats;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let mut files = snapshot.file_views(store.as_ref(), None);
        loop {
            let file = match files.try_next().await.map_err(delta_error) {
                Ok(Some(file)) => file,
                Ok(None) => break,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    break;
                }
            };
            let file = match data_file(&root, &table_url, &file, file_stats) {
                Ok(file) => file,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    break;
                }
            };
            if sender.send(Ok(file)).await.is_err() {
                break;
            }
        }
    });
    Ok(
        futures_util::stream::try_unfold(receiver, |mut receiver| async move {
            match receiver.recv().await {
                Some(file) => Ok(Some((file?, receiver))),
                None => Ok(None),
            }
        })
        .boxed(),
    )
}

/// The snapshot's `remove` actions, one batch at a time: the kernel reads the
/// log segment (checkpoint and commits) on a blocking thread, projected to the
/// three fields a removed file needs, and hands each batch over a channel.
pub(super) async fn stream_removed_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::FileStream<'static>, Error> {
    use delta_kernel::schema::{DataType, MapType, StructField, StructType};

    let store = open_store(lake, request)?;
    let engine = store.engine(None);
    let mut root = store.root_url().clone();
    if !root.path().ends_with('/') {
        root.set_path(&format!("{}/", root.path()));
    }
    let version = request.snapshot_version;
    let remove = StructType::try_new([
        StructField::not_null("path", DataType::STRING),
        StructField::nullable(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
        ),
        StructField::nullable("size", DataType::LONG),
    ])
    .map_err(kernel_error)?;
    let schema = Arc::new(
        StructType::try_new([StructField::nullable("remove", remove)]).map_err(kernel_error)?,
    );
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Vec<v1::TableFile>, Error>>(2);
    tokio::task::spawn_blocking(move || {
        let read = || -> Result<(), Error> {
            let mut builder = delta_kernel::Snapshot::builder_for(root);
            if let Some(version) = version {
                builder = builder.at_version(version);
            }
            let snapshot = builder.build(engine.as_ref()).map_err(kernel_error)?;
            let batches = snapshot
                .log_segment()
                .read_actions(engine.as_ref(), schema)
                .map_err(kernel_error)?;
            for batch in batches {
                let actions = batch.map_err(kernel_error)?.actions;
                let data = delta_kernel::engine::arrow_data::ArrowEngineData::try_from_engine_data(
                    actions,
                )
                .map_err(kernel_error)?;
                if sender
                    .blocking_send(Ok(removed_files(data.record_batch())?))
                    .is_err()
                {
                    break;
                }
            }
            Ok(())
        };
        if let Err(error) = read() {
            let _ = sender.blocking_send(Err(error));
        }
    });
    // The kernel hands over a batch at a time; the stream yields them one file
    // at a time, holding only the batch it is working through.
    let pending: std::vec::IntoIter<v1::TableFile> = Vec::new().into_iter();
    Ok(futures_util::stream::try_unfold(
        (receiver, pending),
        |(mut receiver, mut pending)| async move {
            loop {
                if let Some(file) = pending.next() {
                    return Ok(Some((file, (receiver, pending))));
                }
                let Some(files) = receiver.recv().await else {
                    return Ok(None);
                };
                pending = files?.into_iter();
            }
        },
    )
    .boxed())
}

/// The removed files in one batch of `remove` actions; other rows are null.
fn removed_files(
    batch: &deltalake::arrow::array::RecordBatch,
) -> Result<Vec<v1::TableFile>, Error> {
    use deltalake::arrow::array::{Array, AsArray};
    use deltalake::arrow::datatypes::Int64Type;

    let invalid = || Error::Invalid("the log's remove actions have an unexpected shape".into());
    let remove = batch.column(0).as_struct_opt().ok_or_else(invalid)?;
    let paths = remove
        .column(0)
        .as_string_opt::<i32>()
        .ok_or_else(invalid)?;
    let values = remove.column(1).as_map_opt().ok_or_else(invalid)?;
    let sizes = remove
        .column(2)
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(invalid)?;
    let keys = values.keys().as_string_opt::<i32>().ok_or_else(invalid)?;
    let entries = values.values().as_string_opt::<i32>().ok_or_else(invalid)?;
    let mut files = Vec::new();
    for row in 0..remove.len() {
        if remove.is_null(row) {
            continue;
        }
        let path = percent_encoding::percent_decode_str(paths.value(row))
            .decode_utf8_lossy()
            .into_owned();
        let mut partition_values = BTreeMap::new();
        if !values.is_null(row) {
            for entry in values.value_offsets()[row]..values.value_offsets()[row + 1] {
                let entry = entry as usize;
                let value = (!entries.is_null(entry)).then(|| entries.value(entry).to_string());
                partition_values.insert(keys.value(entry).to_string(), value);
            }
        }
        files.push(v1::TableFile {
            path,
            size_bytes: (!sizes.is_null(row))
                .then(|| u64::try_from(sizes.value(row)).ok())
                .flatten(),
            partition_values,
            ..v1::TableFile::default()
        });
    }
    Ok(files)
}

fn kernel_error(error: delta_kernel::Error) -> Error {
    Error::Snapshot(Box::new(error))
}

/// The table's local filesystem root, when it is a `file://` table.
fn local_table_root(table_url: &Url) -> Result<Option<PathBuf>, Error> {
    if table_url.scheme() != "file" {
        return Ok(None);
    }
    let path = table_url
        .to_file_path()
        .map_err(|()| Error::Invalid("invalid local table URI".into()))?;
    Ok(Some(path))
}

/// One add action as a [`v1::TableFile`], resolving its storage path and, unless
/// `keep_stats` is set, trimming the stats to the byte facts.
fn data_file(
    root: &Option<PathBuf>,
    table_url: &Url,
    file: &deltalake::kernel::LogicalFileView,
    keep_stats: bool,
) -> Result<v1::TableFile, Error> {
    let relative = file.path().to_string();
    let size = u64::try_from(file.size())
        .map_err(|_| Error::Invalid(format!("invalid file size in log: {relative}")))?;
    let uri = match root {
        Some(root) => local_uri(root, &relative)?,
        None => object_uri(table_url, &relative)?,
    };
    // Without the per-column maps, the record count comes from the parsed
    // stats column; `stats()` would render each row back to JSON to parse it.
    let record_count = if keep_stats {
        None
    } else {
        file.num_records()
            .and_then(|count| u64::try_from(count).ok())
    };
    let mut stats = match record_count {
        Some(record_count) => Some(v1::FileStats {
            record_count,
            ..v1::FileStats::default()
        }),
        None => file_stats(file.stats()),
    };
    if let Some(stats) = stats.as_mut() {
        stats.bytes_per_row = bytes_per_row(size, stats.record_count);
        if !keep_stats {
            stats.first_values.clear();
            stats.last_values.clear();
            stats.null_count.clear();
            stats.tight_bounds = None;
        }
    }
    Ok(v1::TableFile {
        blob: Some(v1::name::blob_name(&uri)),
        path: relative,
        size_bytes: Some(size),
        modification_time: u64::try_from(file.modification_time()).ok(),
        partition_values: partition_values(file),
        stats,
        ..v1::TableFile::default()
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
fn file_stats(raw: Option<String>) -> Option<v1::FileStats> {
    let raw = raw?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    let object = value.as_object()?;
    let num_records = object.get("numRecords").and_then(Value::as_u64)?;
    Some(v1::FileStats {
        record_count: num_records,
        bytes_per_row: None,
        first_values: stat_object(object, "minValues"),
        last_values: stat_object(object, "maxValues"),
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

fn object_uri(base: &Url, relative: &str) -> Result<String, Error> {
    relative_data_path(relative)?;
    let mut directory = base.clone();
    if !directory.path().ends_with('/') {
        directory.set_path(&format!("{}/", directory.path()));
    }
    Ok(directory.join(relative).map_err(Error::Uri)?.into())
}

/// Join a local active file without opening it. Size is the log's claim;
/// `bytemass` compares the measured size later.
fn local_uri(root: &Path, relative: &str) -> Result<String, Error> {
    let path = relative_data_path(relative)?;
    Ok(root.join(path).to_string_lossy().into_owned())
}

fn delta_error(error: deltalake::DeltaTableError) -> Error {
    Error::Snapshot(Box::new(error))
}

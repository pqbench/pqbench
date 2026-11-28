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
use futures::stream::{self, Stream};
use futures::{StreamExt, TryStreamExt};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::Deserialize;
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
    Ok(futures::stream::iter(versions.into_iter().rev())
        .chunks(commit_read_width())
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
    Ok(futures::stream::iter(versions)
        .chunks(commit_read_width())
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
/// it is flat in the log. A batch is the unit the consumer drives: the next one
/// is not started until it asks for it.
fn commit_read_width() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        * 4
}

/// The items a task sends over `channel`, in order: `poll_fn` polls the
/// receiver and `try_flatten` yields each sent batch's items, so the sender's
/// drop ends the stream and a sent error is one `Err` item.
fn channel_stream<T, I>(
    mut receiver: tokio::sync::mpsc::Receiver<Result<I, Error>>,
) -> impl Stream<Item = Result<T, Error>> + Send + 'static
where
    I: IntoIterator<Item = Result<T, Error>> + Send + 'static,
    I::IntoIter: Send,
    T: Send + 'static,
{
    stream::poll_fn(move |context| receiver.poll_recv(context))
        .map_ok(stream::iter)
        .try_flatten()
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
    /// What a line has to hold to be the commit's own action.
    const MARKER: &[u8] = br#""commitInfo""#;
    for line in bytes.split(|byte| *byte == b'\n') {
        // The marker is looked for in the bytes: decoding every line of a long
        // commit to text to reject it would cost more than the parse.
        if !line.windows(MARKER.len()).any(|window| window == MARKER) {
            continue;
        }
        let Ok(Action::CommitInfo(info)) = serde_json::from_slice::<Action>(line) else {
            continue;
        };
        return Some(info);
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
        let action: Action = serde_json::from_slice(line.as_bytes())
            .map_err(|error| Error::Invalid(format!("commit {version}: {error}")))?;
        if let Action::CommitInfo(info) = &action {
            if commit_info.is_none() {
                commit_time = info.get("timestamp").and_then(Value::as_i64);
                commit_info = Some(info.clone());
            }
        }
        let body = action.body();
        actions.push(v1::LogAction {
            kind: Some(action.kind().to_owned()),
            path: body.and_then(|body| body.path.clone()),
            size_bytes: body.and_then(|body| body.size),
        });
    }
    Ok(ParsedCommit {
        actions,
        commit_time,
        commit_info,
    })
}

/// One action of a commit file.
///
/// Delta writes one action per line and the line's one key names the kind. The
/// protocol fixes the kinds this reader knows, so they are typed variants; a
/// kind a future protocol adds (a checkpoint's `checkpointMetadata`, a
/// sidecar's metadata) keeps its own name in [`Action::Unknown`] rather than
/// failing the commit, so `list commits` stays forward-compatible and the
/// action's kind survives.
#[derive(Debug)]
enum Action {
    /// A file the commit wrote.
    Add(ActionBody),
    /// A file the commit removed.
    Remove(ActionBody),
    /// A change-data-capture file the commit wrote.
    Cdc(ActionBody),
    /// The table's metadata: the snapshot describes it already.
    Metadata(IgnoredAny),
    /// The table's protocol: the snapshot describes it already.
    Protocol(IgnoredAny),
    /// A transaction identifier.
    Txn(IgnoredAny),
    /// The commit's own record: its time, and what the writer reports.
    CommitInfo(Value),
    /// A domain's metadata.
    DomainMetadata(IgnoredAny),
    /// A checkpoint's sidecar file.
    Sidecar(ActionBody),
    /// An action this reader does not know, under the kind the log spelled.
    Unknown(String),
}

impl<'de> Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ActionVisitor;

        impl<'de> Visitor<'de> for ActionVisitor {
            type Value = Action;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Delta log action")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Action, A::Error>
            where
                A: MapAccess<'de>,
            {
                let Some(kind) = map.next_key::<String>()? else {
                    return Err(serde::de::Error::custom("a Delta log action has no kind"));
                };
                let action = match kind.as_str() {
                    "add" => Action::Add(map.next_value()?),
                    "remove" => Action::Remove(map.next_value()?),
                    "cdc" => Action::Cdc(map.next_value()?),
                    "metaData" => Action::Metadata(map.next_value()?),
                    "protocol" => Action::Protocol(map.next_value()?),
                    "txn" => Action::Txn(map.next_value()?),
                    "commitInfo" => Action::CommitInfo(map.next_value()?),
                    "domainMetadata" => Action::DomainMetadata(map.next_value()?),
                    "sidecar" => Action::Sidecar(map.next_value()?),
                    _ => {
                        let _: IgnoredAny = map.next_value()?;
                        Action::Unknown(kind)
                    }
                };
                // An action line is one entry; anything after it is not part of
                // the action and is skipped.
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(action)
            }
        }

        deserializer.deserialize_map(ActionVisitor)
    }
}

impl Action {
    /// The kind as the log spells it.
    fn kind(&self) -> &str {
        match self {
            Self::Add(_) => "add",
            Self::Remove(_) => "remove",
            Self::Cdc(_) => "cdc",
            Self::Metadata(_) => "metaData",
            Self::Protocol(_) => "protocol",
            Self::Txn(_) => "txn",
            Self::CommitInfo(_) => "commitInfo",
            Self::DomainMetadata(_) => "domainMetadata",
            Self::Sidecar(_) => "sidecar",
            Self::Unknown(kind) => kind,
        }
    }

    /// The action's body, when its kind has one a log record reads.
    fn body(&self) -> Option<&ActionBody> {
        match self {
            Self::Add(body) | Self::Remove(body) | Self::Cdc(body) | Self::Sidecar(body) => {
                Some(body)
            }
            _ => None,
        }
    }
}

/// The part of an action's body a log record keeps.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActionBody {
    /// The data path the action names, when it names one.
    path: Option<String>,
    /// The size the action declares, when it declares one.
    size: Option<u64>,
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
        let outcome = loop {
            let file = match files.try_next().await {
                Ok(Some(file)) => file,
                Ok(None) => break Ok(()),
                Err(error) => break Err(delta_error(error)),
            };
            let file = match data_file(&root, &table_url, &file, file_stats) {
                Ok(file) => file,
                Err(error) => break Err(error),
            };
            if sender.send(Ok(std::iter::once(Ok(file)))).await.is_err() {
                break Ok(());
            }
        };
        if let Err(error) = outcome {
            let _ = sender.send(Err(error)).await;
        }
    });
    Ok(channel_stream(receiver).boxed())
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
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
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
                let files = removed_files(data.record_batch())?.into_iter().map(Ok);
                if sender.blocking_send(Ok(files)).is_err() {
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
    Ok(channel_stream(receiver).boxed())
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
        None => file_stats(file.stats(), keep_stats),
    };
    if let Some(stats) = stats.as_mut() {
        stats.bytes_per_row = bytes_per_row(size, stats.record_count);
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

/// Parse `add.stats` into the facts `keep_stats` asks for: the record count
/// always, and the per-column maps only when the caller keeps them. Nested
/// `nullCount` objects are flattened to dotted keys. A malformed stats blob is
/// omitted rather than failing the table.
fn file_stats(raw: Option<String>, keep_stats: bool) -> Option<v1::FileStats> {
    // The blob is the log's own bytes: parse it where it lies rather than
    // through a copy.
    let raw = raw?.into_bytes();
    if !keep_stats {
        // Only the count is kept, so read only the count: a full value tree for
        // a stats blob would hold every bound just to drop it.
        let counted: CountOnly = serde_json::from_slice(&raw).ok()?;
        return Some(v1::FileStats {
            record_count: counted.record_count,
            ..v1::FileStats::default()
        });
    }
    let value: Value = serde_json::from_slice(&raw).ok()?;
    let object = value.as_object()?;
    let record_count = object.get("numRecords").and_then(Value::as_u64)?;
    Some(v1::FileStats {
        record_count,
        bytes_per_row: None,
        first_values: stat_object(object, "minValues"),
        last_values: stat_object(object, "maxValues"),
        null_count: flatten_null_counts(object.get("nullCount")),
        tight_bounds: object.get("tightBounds").and_then(Value::as_bool),
    })
}

/// The one field a stats blob is read for when the per-column maps are not
/// kept.
#[derive(Deserialize)]
struct CountOnly {
    #[serde(rename = "numRecords")]
    record_count: u64,
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

#[cfg(test)]
mod tests {
    use super::{file_stats, parse_commit, Action};

    /// A future protocol's action keeps its own kind instead of failing the
    /// commit, while a known action stays typed.
    #[test]
    fn an_unknown_action_keeps_its_kind() {
        let line = br#"{"checkpointMetadata":{"version":3,"tags":{}}}"#;
        let action: Action = serde_json::from_slice(line).expect("an unknown action parses");
        assert_eq!(action.kind(), "checkpointMetadata");
        assert!(action.body().is_none());

        let commit = parse_commit(line, 7).expect("the commit parses");
        assert_eq!(commit.actions.len(), 1);
        assert_eq!(
            commit.actions[0].kind.as_deref(),
            Some("checkpointMetadata")
        );
        assert!(commit.actions[0].path.is_none());

        let add = parse_commit(br#"{"add":{"path":"a.parquet","size":12}}"#, 8)
            .expect("a known action parses");
        assert_eq!(add.actions[0].kind.as_deref(), Some("add"));
        assert_eq!(add.actions[0].path.as_deref(), Some("a.parquet"));
        assert_eq!(add.actions[0].size_bytes, Some(12));
    }

    /// The record count is read without building the full stats tree when the
    /// per-column maps are not kept, and the maps are read when they are.
    #[test]
    fn file_stats_reads_only_what_the_caller_keeps() {
        let raw = Some(
            r#"{"numRecords":3,"minValues":{"name":"alice"},"maxValues":{"name":"carol"},
                "nullCount":{"name":0},"tightBounds":true}"#
                .to_string(),
        );
        let counted = file_stats(raw.clone(), false).expect("the count parses");
        assert_eq!(counted.record_count, 3);
        assert!(counted.first_values.is_empty());
        assert!(counted.last_values.is_empty());
        assert!(counted.null_count.is_empty());

        let kept = file_stats(raw, true).expect("the stats parse");
        assert_eq!(kept.record_count, 3);
        assert_eq!(kept.first_values["name"], "alice");
        assert_eq!(kept.last_values["name"], "carol");
        assert_eq!(kept.null_count["name"], 0);
        assert_eq!(kept.tight_bounds, Some(true));

        assert!(file_stats(Some("not json".to_string()), false).is_none());
        assert!(file_stats(None, true).is_none());
    }
}

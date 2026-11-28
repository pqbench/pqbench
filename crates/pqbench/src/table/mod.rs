//! The table command: one table's record and the tables in a schema.
//!
//! [`get`] reads one table's record from a catalog (Unity or Iceberg REST),
//! without files, so the cost is O(1) in files. [`list`] lists the tables in a
//! schema, one `Table` each — the record [`get`] enriches.
//!
//! [`detect`] names the format from on-disk markers before any format-specific
//! loader runs. [`load`] then fetches the table metadata. For Delta that is the
//! transaction log plus the resolved active files; for Iceberg, the metadata
//! JSON and Avro manifests. Measurement is a later step: pipe the record to
//! `bytemass`.
//!
//! Enable the `delta` feature to load Delta logs. That feature requires Rust
//! 1.94.1 or newer because of the Delta snapshot dependencies. Iceberg needs
//! `iceberg` (`iceberg-s3` for S3).

pub mod get;
pub mod list;

mod iceberg;
mod selection;
pub use selection::FileSelection;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use futures_util::stream::{BoxStream, StreamExt};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::third_party::delta;
use crate::third_party::object_store;
use crate::v1;
use crate::Lake;

/// Errors detecting a table format or loading its metadata.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The location could not be opened.
    Missing(String),
    /// No supported table format was found at the location.
    Unrecognized(String),
    /// A path, URI, or metadata value was invalid.
    Invalid(String),
    /// A URI could not be parsed.
    Uri(url::ParseError),
    /// A local filesystem call failed.
    Io {
        /// The path involved.
        path: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
    /// A log record or metadata document was not valid JSON.
    Json(serde_json::Error),
    /// An Avro manifest could not be read.
    Avro(crate::third_party::avro::Error),
    /// The object store or a backend store failed.
    Storage(Box<dyn std::error::Error + Send + Sync>),
    /// The snapshot could not be resolved by the format's own loader.
    Snapshot(Box<dyn std::error::Error + Send + Sync>),
    /// A partition total exceeded `u64`.
    Overflow {
        /// The file whose partition total overflowed.
        file: String,
        /// Which total overflowed (`"byte"`, `"record"`).
        kind: &'static str,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Missing(location) => write!(f, "cannot open table {location}"),
            Error::Unrecognized(location) => write!(
                f,
                "unrecognized table format at {location}; supported formats: delta, iceberg"
            ),
            Error::Invalid(message) => write!(f, "{message}"),
            Error::Uri(_) => write!(f, "invalid table URI"),
            Error::Io { path, .. } => write!(f, "cannot read {path}"),
            Error::Json(_) => write!(f, "the document was not valid JSON"),
            Error::Avro(_) => write!(f, "an Avro manifest could not be read"),
            Error::Storage(_) => write!(f, "object storage failed"),
            Error::Snapshot(_) => write!(f, "the snapshot could not be resolved"),
            Error::Overflow { file, kind } => {
                write!(f, "partition {kind} total overflowed for {file}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Uri(error) => Some(error),
            Error::Io { source, .. } => Some(source),
            Error::Json(error) => Some(error),
            Error::Avro(error) => Some(error),
            Error::Storage(error) => Some(error.as_ref()),
            Error::Snapshot(error) => Some(error.as_ref()),
            Error::Missing(_)
            | Error::Unrecognized(_)
            | Error::Invalid(_)
            | Error::Overflow { .. } => None,
        }
    }
}

impl Error {
    /// Wrap a message as a table error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

/// A table's commits, newest first, from [`stream_commits`].
///
/// The stream owns the log it reads, so it borrows nothing from the caller.
pub type CommitStream<'a> = BoxStream<'a, Result<v1::Commit, Error>>;

/// A table's data files, in log order, from [`stream_files`].
///
/// Each file carries its path relative to the table's storage root; naming it
/// under a table is the caller's, which knows the table's resource name.
///
/// The stream owns the log it reads, so it borrows nothing from the caller.
pub type FileStream<'a> = BoxStream<'a, Result<v1::TableFile, Error>>;

/// One file of a snapshot, classified for [`crate::partition::list`].
///
/// Any `Removed` entry for a path arrives before a live `Active` entry for the
/// same path, so a caller cancels tombstones as active files stream past.
pub(crate) enum SnapshotFile {
    /// An active data file in the snapshot.
    Active(v1::TableFile),
    /// A removed or superseded data file still retained on storage.
    Removed(v1::TableFile),
    /// An active merge-on-read delete file in the snapshot.
    Delete(v1::TableFile),
}

pub(crate) type SnapshotStream<'a> = BoxStream<'a, Result<SnapshotFile, Error>>;

/// Byte mass of one partition, summed from the active files' log statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct PartitionMass {
    /// Partition column values. Empty for an unpartitioned table.
    pub values: BTreeMap<String, Option<String>>,
    /// Active files in this partition.
    pub file_count: usize,
    /// Sum of log file sizes, in bytes.
    pub size: u64,
    /// Sum of `numRecords` when every file in the partition has statistics.
    // aipnaming: allow(aip-141/count-suffix)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_records: Option<u64>,
    /// [`Self::size`] divided by [`Self::num_records`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_row: Option<f64>,
}

/// A table's record: format, log, and the files the snapshot names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TableInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_selection: Option<FileSelection>,
    /// Detected table format.
    pub format: v1::TableFormat,
    /// Table root as given (path or URI).
    pub uri: String,
    /// Table's full name (`catalog.schema.table`), when a catalog named it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Snapshot version the files belong to.
    pub snapshot_version: u64,
    /// When the table was created, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_time: Option<u64>,
    /// Partition columns live in the log and need not occupy Parquet columns.
    pub partition_columns: Vec<String>,
    /// Table schema columns, when the metadata carries them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<v1::DeclaredColumn>,
    /// Delta table properties: the log's `metaData.configuration` plus the
    /// catalog record's values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub delta_properties: BTreeMap<String, String>,
    /// Iceberg table properties from the metadata JSON, or the catalog
    /// response that carries it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub iceberg_properties: BTreeMap<String, String>,
    /// Every available JSON commit, in version order. Checkpoint-only versions
    /// that have no remaining JSON file are omitted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub log: Vec<v1::Commit>,
    /// Active data files after replaying the log to `snapshot_version`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<v1::TableFile>,
    /// Per-partition totals from Delta add statistics. Empty for Iceberg and
    /// for a Delta snapshot with no active files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<PartitionMass>,
}

impl TableInfo {
    /// Assemble a table record from its parts.
    #[must_use]
    pub fn new(
        format: v1::TableFormat,
        uri: impl Into<String>,
        snapshot_version: u64,
        partition_columns: Vec<String>,
        log: Vec<v1::Commit>,
        files: Vec<v1::TableFile>,
    ) -> Self {
        Self {
            file_selection: None,
            format,
            uri: uri.into(),
            name: String::new(),
            snapshot_version,
            create_time: None,
            partition_columns,
            columns: Vec::new(),
            delta_properties: BTreeMap::new(),
            iceberg_properties: BTreeMap::new(),
            log,
            files,
            partitions: Vec::new(),
        }
    }
}

impl TableInfo {
    /// The wire [`v1::Table`] this record describes: what `table::get` emits.
    /// The reader, which holds the lease, keeps it off the record.
    #[must_use]
    pub fn resource(&self, catalog: &str, schema: &str, table: &str) -> v1::Table {
        let mut properties = self.delta_properties.clone();
        properties.extend(self.iceberg_properties.clone());
        v1::Table {
            name: v1::name::table(catalog, schema, table),
            display_name: non_empty(table),
            uri: non_empty(&self.uri),
            format: Some(self.format),
            snapshot_version: Some(self.snapshot_version),
            create_time: self.create_time,
            partition_columns: self.partition_columns.clone(),
            declared_columns: self
                .columns
                .iter()
                .enumerate()
                .map(|(position, column)| v1::DeclaredColumn {
                    position: Some(position as u64),
                    ..column.clone()
                })
                .collect(),
            properties,
            file_count: non_empty_len(&self.files),
            partition_count: non_empty_len(&self.partitions),
            ..v1::Table::default()
        }
    }
}

/// `Some` unless the text is empty.
pub(crate) fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

/// The length of a non-empty collection.
fn non_empty_len<T>(items: &[T]) -> Option<u64> {
    (!items.is_empty()).then_some(items.len() as u64)
}

/// Arguments for [`load`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LoadRequest {
    /// Local table directory, table URI (`file://`, `s3://`, ...), or Iceberg
    /// metadata JSON.
    pub uri: String,
    /// Snapshot version; `None` selects the latest.
    pub snapshot_version: Option<u64>,
    /// When false, omit first/last/null maps (keep `record_count` / `bytes_per_row`).
    pub file_stats: bool,
    /// When false, read only the snapshot metadata: no commit log, no active
    /// files.
    // aipnaming: allow(aip-140/verbs)
    pub require_files: bool,
    /// Read only these commit versions; `None` reads every commit. Naming the
    /// commits says the log's own file actions are what the caller wants, so
    /// the snapshot's active files are not replayed.
    pub commit_versions: Option<BTreeSet<u64>>,
}

impl LoadRequest {
    /// Build a load request.
    #[must_use]
    pub fn new(uri: impl Into<String>, snapshot_version: Option<u64>) -> Self {
        Self {
            uri: uri.into(),
            snapshot_version,
            file_stats: true,
            require_files: true,
            commit_versions: None,
        }
    }

    /// Omit heavy min/max/null maps from each file.
    // aipnaming: allow(aip-136/method-prepositions)
    #[must_use]
    pub fn with_file_stats(mut self, file_stats: bool) -> Self {
        self.file_stats = file_stats;
        self
    }

    /// Read only the snapshot metadata: no commit log, no active files. This is
    /// `without_files()` for Delta and a metadata-JSON-only read for Iceberg,
    /// so the cost is O(1) in files.
    // aipnaming: allow(aip-136/method-prepositions)
    #[must_use]
    pub fn without_files(mut self) -> Self {
        self.require_files = false;
        self
    }

    /// Read only these commit versions. The caller wants the files the log
    /// itself names, so the snapshot replay is skipped.
    // aipnaming: allow(aip-136/method-prepositions)
    #[must_use]
    pub fn with_commits(mut self, versions: impl IntoIterator<Item = u64>) -> Self {
        self.commit_versions = Some(versions.into_iter().collect());
        self
    }
}

/// Name the table format from well-known markers. Does not load the log.
///
/// Delta wins when `_delta_log` is present. Iceberg is named from
/// `metadata/version-hint.text`, `metadata/*.metadata.json`, or a
/// `.metadata.json` path. An unrecognized location is an error.
///
/// # Errors
/// Fails when the location cannot be opened, a remote probe fails for a reason
/// other than a missing marker, or no supported format is present.
#[must_use = "detecting a format has no effect unless the result is used"]
pub async fn detect(lake: &Lake, uri: &str) -> Result<v1::TableFormat, Error> {
    if is_local(uri) {
        local_format(&local_path(uri)?)?.ok_or_else(|| unrecognized(uri))
    } else {
        remote_format(lake, uri)
            .await?
            .ok_or_else(|| unrecognized(uri))
    }
}

/// Load table metadata: detect the format, then fetch the log.
///
/// For Delta this is every remaining JSON commit plus the active files of the
/// requested snapshot. Iceberg reads metadata JSON and Avro manifests.
///
/// # Errors
/// As [`detect`], plus format-specific load failures. Delta needs the `delta`
/// feature (`delta-s3` for S3). Iceberg needs `iceberg` (`iceberg-s3` for S3).
#[must_use = "loading a table has no effect unless the result is used"]
pub async fn load(lake: &Lake, request: &LoadRequest) -> Result<TableInfo, Error> {
    let format = detect(lake, &request.uri).await?;
    match format {
        v1::TableFormat::Delta => delta::load(lake, request).await,
        v1::TableFormat::Iceberg => iceberg::load(lake, request).await,
        _ => Err(unrecognized(&request.uri)),
    }
}

/// Stream the requested snapshot's active files, without collecting them.
///
/// This is the active set [`load`] returns in [`TableInfo::files`], handed over
/// as it arrives instead of kept, so a caller that writes each file as it comes
/// holds one file rather than the table's. The stream owns the snapshot it
/// reads, so it borrows nothing from the caller and nothing grows with the
/// table's size.
///
/// # Errors
/// As [`detect`], plus format-specific load failures. A failure raised after
/// the first file arrives as a stream item.
pub async fn stream_active_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<FileStream<'static>, Error> {
    let format = detect(lake, &request.uri).await?;
    match format {
        v1::TableFormat::Delta => delta::stream_active_files(lake, request).await,
        v1::TableFormat::Iceberg => iceberg::stream_active_files(lake, request).await,
        _ => Err(unrecognized(&request.uri)),
    }
}

/// Stream the files the requested snapshot removed but still holds, without
/// collecting them: for Delta, the `remove` actions whose tombstones the
/// table's deleted-file retention has not expired, billed until a vacuum
/// deletes them; for Iceberg, the data files the snapshot marks deleted plus
/// the data files only a retained older snapshot still names, billed until an
/// expire removes them.
///
/// A removed path can still be live (a deletion-vector update removes and
/// re-adds it); drop the paths [`stream_active_files`] yields to count the
/// bytes only the tombstones hold.
///
/// # Errors
/// As [`stream_active_files`].
pub async fn stream_removed_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<Option<FileStream<'static>>, Error> {
    match detect(lake, &request.uri).await? {
        v1::TableFormat::Delta => Ok(Some(delta::stream_removed_files(lake, request).await?)),
        v1::TableFormat::Iceberg => Ok(Some(iceberg::stream_removed_files(lake, request).await?)),
        _ => Err(unrecognized(&request.uri)),
    }
}

/// Stream the merge-on-read delete files the requested snapshot still applies,
/// without collecting them: for Iceberg, the active entries of its delete
/// manifests (position deletes, equality deletes, and v3 deletion vectors),
/// whatever format the writer chose. `None` for a format whose deletes are not
/// stored as files (Delta applies them when it writes).
///
/// These are not data: their size is storage a rewrite removes, and their rows
/// narrow the data files they reference.
///
/// # Errors
/// As [`stream_active_files`].
pub async fn stream_delete_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<Option<FileStream<'static>>, Error> {
    match detect(lake, &request.uri).await? {
        v1::TableFormat::Delta => Ok(None),
        v1::TableFormat::Iceberg => Ok(Some(iceberg::stream_delete_files(lake, request).await?)),
        _ => Err(unrecognized(&request.uri)),
    }
}

/// Stream a snapshot's removed, active, and delete files in one pass.
///
/// For Delta this chains the `remove` stream ahead of the `add` stream under
/// one format check; for Iceberg it reads the metadata JSON, the manifest list,
/// and each manifest at most once.
pub(crate) async fn stream_snapshot_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<SnapshotStream<'static>, Error> {
    match detect(lake, &request.uri).await? {
        v1::TableFormat::Delta => {
            let removed = delta::stream_removed_files(lake, request)
                .await?
                .map_ok(SnapshotFile::Removed);
            let active = delta::stream_active_files(lake, request)
                .await?
                .map_ok(SnapshotFile::Active);
            Ok(removed.chain(active).boxed())
        }
        v1::TableFormat::Iceberg => iceberg::stream_snapshot_files(lake, request).await,
        _ => Err(unrecognized(&request.uri)),
    }
}

/// Stream a table's commits, newest first, without collecting them.
///
/// The stream owns the log it reads, so it borrows nothing from the caller and
/// nothing grows with the log's length; it reaches as far back as the log's
/// retention allows.
///
/// # Errors
/// As [`detect`], plus format-specific read failures. A failure raised after the
/// first commit arrives as a stream item.
pub async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<CommitStream<'static>, Error> {
    let format = detect(lake, &request.uri).await?;
    match format {
        v1::TableFormat::Delta => Ok(delta::stream_commits(lake, request).await?.boxed()),
        v1::TableFormat::Iceberg => Ok(iceberg::stream_commits(lake, request).await?.boxed()),
        _ => Err(unrecognized(&request.uri)),
    }
}

/// Stream the files the requested commits added, in log order, without
/// collecting them.
///
/// The files are the commits' own `add` actions — the **added** set, not the
/// table's active files — so Delta reads them from the commits themselves and no
/// snapshot, checkpoint or active-file replay is involved. Iceberg attaches a
/// snapshot's files only when that snapshot is resolved, so its files come from
/// the selected snapshot's manifests.
///
/// # Errors
/// As [`detect`], plus format-specific read failures. A failure raised after the
/// first file arrives as a stream item.
pub async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<FileStream<'static>, Error> {
    let format = detect(lake, &request.uri).await?;
    match format {
        v1::TableFormat::Delta => Ok(delta::stream_files(lake, request).await?.boxed()),
        v1::TableFormat::Iceberg => Ok(iceberg::stream_files(lake, request).await?.boxed()),
        _ => Err(unrecognized(&request.uri)),
    }
}

/// Local markers `lake` and `table` share. `None` is unrecognized, not an error.
pub(crate) fn local_format(path: &Path) -> Result<Option<v1::TableFormat>, Error> {
    if !path.exists() {
        return Err(Error::Missing(path.display().to_string()));
    }
    if path.is_file() {
        return Ok(is_metadata_json_path(path).then_some(v1::TableFormat::Iceberg));
    }
    if path.join("_delta_log").is_dir() {
        return Ok(Some(v1::TableFormat::Delta));
    }
    let metadata = path.join("metadata");
    if metadata.join("version-hint.text").is_file() {
        return Ok(Some(v1::TableFormat::Iceberg));
    }
    if has_metadata_json(&metadata)? {
        return Ok(Some(v1::TableFormat::Iceberg));
    }
    Ok(None)
}

/// Remote markers `lake` and `table` share. A `.metadata.json` URI is Iceberg;
/// otherwise Delta is `_delta_log/_last_checkpoint` or commit `0`, and Iceberg
/// is `metadata/version-hint.text`.
pub(crate) async fn remote_format(
    lake: &Lake,
    uri: &str,
) -> Result<Option<v1::TableFormat>, Error> {
    if uri
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.ends_with(".metadata.json"))
    {
        return Ok(Some(v1::TableFormat::Iceberg));
    }
    if probe(lake, uri, "_delta_log/_last_checkpoint").await?
        || probe(lake, uri, "_delta_log/00000000000000000000.json").await?
    {
        return Ok(Some(v1::TableFormat::Delta));
    }
    if probe(lake, uri, "metadata/version-hint.text").await? {
        return Ok(Some(v1::TableFormat::Iceberg));
    }
    Ok(None)
}

async fn probe(lake: &Lake, uri: &str, relative: &str) -> Result<bool, Error> {
    let child = join_uri(uri, relative)?;
    let reader = object_store::open(lake, &child).map_err(|e| Error::Storage(Box::new(e)))?;
    reader
        .exists()
        .await
        .map_err(|e| Error::Storage(Box::new(e)))
}

/// A data path that stays inside the table: empty, absolute, or URI paths
/// are rejected here, once, for both the local and the object adapters.
pub(crate) fn relative_data_path(relative: &str) -> Result<&Path, Error> {
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.contains("://")
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(Error::Invalid(format!(
            "only relative data paths inside the table are supported: {relative}"
        )));
    }
    Ok(path)
}

/// Resolve a path relative to a table root, either local or a URI.
pub(crate) fn join_uri(base: &str, relative: &str) -> Result<String, Error> {
    // An absolute location is already the answer: a Delta log names a file
    // relative to the table root, but an Iceberg manifest names it absolute.
    if relative.contains("://") || relative.starts_with('/') {
        return Ok(relative.to_string());
    }
    if is_local(base) {
        return Ok(local_path(base)?
            .join(relative)
            .to_string_lossy()
            .into_owned());
    }
    let mut url = Url::parse(base).map_err(Error::Uri)?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url.join(relative).map_err(Error::Uri)?.into())
}

/// Whether a table location names the local filesystem.
pub(crate) fn is_local(uri: &str) -> bool {
    !uri.contains("://") || uri.starts_with("file://")
}

/// The filesystem path of a local table location.
pub(crate) fn local_path(uri: &str) -> Result<PathBuf, Error> {
    if uri.starts_with("file://") {
        Url::parse(uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .ok_or_else(|| Error::Invalid("invalid local table URI".into()))
    } else {
        Ok(PathBuf::from(uri))
    }
}

fn is_metadata_json_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".metadata.json"))
}

fn has_metadata_json(metadata: &Path) -> Result<bool, Error> {
    if !metadata.exists() || !metadata.is_dir() {
        return Ok(false);
    }
    let entries = std::fs::read_dir(metadata).map_err(|e| Error::Io {
        path: metadata.display().to_string(),
        source: e,
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::Io {
            path: metadata.display().to_string(),
            source: e,
        })?;
        if entry.path().is_file() && is_metadata_json_path(&entry.path()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn unrecognized(location: impl std::fmt::Display) -> Error {
    Error::Unrecognized(location.to_string())
}

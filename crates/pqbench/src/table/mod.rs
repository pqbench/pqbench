//! Table-format discovery and metadata.
//!
//! [`detect`] names the format from on-disk markers before any format-specific
//! loader runs. [`load`] then fetches the table metadata. For Delta that is the
//! transaction log plus the resolved active files; for Iceberg, the metadata
//! JSON and Avro manifests. Measurement is a later step: pipe the document to
//! `bytemass`.
//!
//! Enable the `delta` feature to load Delta logs. That feature requires Rust
//! 1.91.1 or newer because of the Delta snapshot dependencies. Iceberg needs
//! `iceberg` (`iceberg-s3` for S3).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::object_store;
use crate::pattern::Selection;

pub mod delta;
#[cfg(feature = "iceberg")]
pub mod iceberg;

/// Errors detecting a table format or loading its metadata.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Wrap a message as a table error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// On-disk table formats `pqbench table` can name. Detection runs before load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableFormat {
    /// Zero value; not a detected format.
    #[serde(rename = "unspecified")]
    UNSPECIFIED,
    #[serde(rename = "delta")]
    DELTA,
    #[serde(rename = "iceberg")]
    ICEBERG,
}

impl TableFormat {
    /// Wire name of the format (`delta`, `iceberg`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UNSPECIFIED => "unspecified",
            Self::DELTA => "delta",
            Self::ICEBERG => "iceberg",
        }
    }
}

/// One action from a commit file, in file order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LogAction {
    /// Action kind as stored (`add`, `remove`, `metaData`, ...).
    pub kind: String,
    /// Data path when the action names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// One commit from the table log, as stored on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LogCommit {
    /// Commit version.
    pub version: u64,
    /// Actions from the commit file, in file order.
    pub actions: Vec<LogAction>,
}

/// One data file the current snapshot treats as active.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TableFile {
    /// Path as recorded in the log, relative to the table root.
    pub path: String,
    /// URI or filesystem path `bytemass` should read.
    pub uri: String,
    /// Size the log claims, in bytes.
    pub size: u64,
    /// Log modification time as RFC3339 UTC, when the format records one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_time: Option<String>,
    /// Snapshot version that added this file, when the log records one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<u64>,
    /// Partition values from the Delta add action. Empty when unpartitioned.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_values: BTreeMap<String, Option<String>>,
    /// Statistics from the Delta `add.stats` JSON. Absent when the log has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<FileStats>,
}

/// Statistics copied from a Delta add action. Values are the log's claim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct FileStats {
    /// `numRecords` from `add.stats`.
    pub num_records: u64,
    /// On-disk file size divided by [`Self::num_records`]. Absent at zero rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_row: Option<f64>,
    /// Per-column minimum from `minValues`, JSON as stored (nested structs stay
    /// objects).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub min_values: BTreeMap<String, Value>,
    /// Per-column maximum from `maxValues`, JSON as stored.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub max_values: BTreeMap<String, Value>,
    /// Per-column `nullCount`. Nested struct columns are flattened to dotted
    /// keys (`struct.inner.x`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub null_count: BTreeMap<String, u64>,
    /// `tightBounds` when the log sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tight_bounds: Option<bool>,
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_records: Option<u64>,
    /// [`Self::size`] divided by [`Self::num_records`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_row: Option<f64>,
}

impl TableFile {
    /// Assemble a table file from its parts.
    #[must_use]
    pub fn new(path: impl Into<String>, uri: impl Into<String>, size: u64) -> Self {
        Self {
            path: path.into(),
            uri: uri.into(),
            size,
            last_modified_time: None,
            snapshot_version: None,
            partition_values: BTreeMap::new(),
            stats: None,
        }
    }
}

/// A versioned table document: format, log, and the files the snapshot names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TableInfo {
    /// Document kind; always `pqbench.table`.
    pub kind: String,
    /// Document version; currently `1`.
    pub version: u32,
    /// Detected table format.
    pub format: TableFormat,
    /// Table root as given (path or URI).
    pub uri: String,
    /// Snapshot version the files belong to.
    pub snapshot_version: u64,
    /// Snapshot creation time as RFC3339 UTC, when the log records one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_time: Option<String>,
    /// File-selection options applied while loading this snapshot.
    #[serde(default, skip_serializing_if = "Selection::is_default")]
    pub selection: Selection,
    /// Partition columns live in the log and need not occupy Parquet columns.
    pub partition_columns: Vec<String>,
    /// Every available JSON commit, in version order. Checkpoint-only versions
    /// that have no remaining JSON file are omitted.
    pub log: Vec<LogCommit>,
    /// Active data files after replaying the log to `snapshot_version`.
    pub files: Vec<TableFile>,
    /// Per-partition totals from Delta add statistics. Empty for Iceberg and
    /// for a Delta snapshot with no active files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<PartitionMass>,
    /// Storage options from the producer. A pipe to `bytemass` reuses them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl TableInfo {
    /// Assemble a table document from its parts.
    #[must_use]
    pub fn new(
        format: TableFormat,
        uri: impl Into<String>,
        snapshot_version: u64,
        partition_columns: Vec<String>,
        log: Vec<LogCommit>,
        files: Vec<TableFile>,
        env: BTreeMap<String, String>,
    ) -> Self {
        Self {
            kind: "pqbench.table".into(),
            version: 1,
            format,
            uri: uri.into(),
            snapshot_version,
            snapshot_time: None,
            selection: Selection::default(),
            partition_columns,
            log,
            files,
            partitions: Vec::new(),
            env,
        }
    }

    /// Total size the log claims for the active files.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|file| file.size).sum()
    }
}

/// Arguments for [`load`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LoadRequest {
    /// Local table directory, table URI (`file://`, `s3://`, ...), or Iceberg
    /// metadata JSON.
    pub uri: String,
    /// Snapshot version; `None` selects the latest.
    pub version: Option<u64>,
    /// Storage options (`AWS_*` names), copied onto the document.
    pub env: BTreeMap<String, String>,
    /// Exclude files or snapshots by time and version.
    pub selection: Selection,
    /// When false, omit min/max/null maps (keep `num_records` / `bytes_per_row`).
    pub file_stats: bool,
    /// When false, do not retain active files on the returned document.
    /// [`visit_load`] still emits each file; partition totals are kept.
    pub collect_files: bool,
}

impl LoadRequest {
    /// Build a load request. `env` is passed to the storage backend.
    #[must_use]
    pub fn new(
        uri: impl Into<String>,
        version: Option<u64>,
        env: BTreeMap<String, String>,
    ) -> Self {
        Self {
            uri: uri.into(),
            version,
            env,
            selection: Selection::default(),
            file_stats: true,
            collect_files: true,
        }
    }

    /// Set file and snapshot exclude options.
    #[must_use]
    pub fn with_selection(mut self, selection: Selection) -> Self {
        self.selection = selection;
        self
    }

    /// Omit heavy min/max/null maps from each file.
    #[must_use]
    pub fn with_file_stats(mut self, file_stats: bool) -> Self {
        self.file_stats = file_stats;
        self
    }

    /// Drop the file list from the returned document after visiting each file.
    #[must_use]
    pub fn with_collect_files(mut self, collect_files: bool) -> Self {
        self.collect_files = collect_files;
        self
    }
}

/// One step of [`visit_load`]. `BEGIN` is the snapshot and log; `FILE` is
/// each active file as it is resolved.
pub enum LoadEvent<'a> {
    /// Snapshot header and log. `files` is empty.
    BEGIN {
        /// Table document without active files.
        info: &'a TableInfo,
    },
    /// One active file, in log order.
    FILE {
        /// File just resolved from the snapshot.
        file: &'a TableFile,
    },
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
pub async fn detect(uri: &str, env: &BTreeMap<String, String>) -> Result<TableFormat, Error> {
    if is_local(uri) {
        local_format(&local_path(uri)?)?.ok_or_else(|| unrecognized(uri))
    } else {
        remote_format(uri, env)
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
/// feature (`delta-s3` for S3). Iceberg needs `iceberg` (`iceberg-s3` for S3).
#[must_use = "loading a table has no effect unless the result is used"]
pub async fn load(request: &LoadRequest) -> Result<TableInfo, Error> {
    visit_load(request, |_| Ok(())).await
}

/// Load a table, calling `visit` as the snapshot and each file are known.
///
/// Delta files are visited from the add-action stream. Iceberg files are
/// visited after the manifests are read. When [`LoadRequest::collect_files`]
/// is false the returned document keeps partition totals and drops `files`.
///
/// # Errors
/// Same as [`load`].
pub async fn visit_load(
    request: &LoadRequest,
    mut visit: impl FnMut(LoadEvent<'_>) -> Result<(), Error>,
) -> Result<TableInfo, Error> {
    let format = detect(&request.uri, &request.env).await?;
    let mut info = match format {
        TableFormat::DELTA => delta::visit_load(request, &mut visit).await?,
        TableFormat::ICEBERG => visit_iceberg(request, &mut visit).await?,
        TableFormat::UNSPECIFIED => return Err(Error("unrecognized table format".into())),
    };
    info.selection = request.selection.clone();
    if request.collect_files {
        apply_file_selection(&mut info)?;
        if info.format == TableFormat::DELTA {
            info.partitions = partition_masses(&info.files)?;
        }
    }
    Ok(info)
}

/// Drop files excluded by path, modified time, or add version.
///
/// # Errors
/// Fails when a filter is set and a file does not record that field, or when
/// nothing remains.
pub fn apply_file_selection(info: &mut TableInfo) -> Result<(), Error> {
    let selection = &info.selection;
    let path_filter = !selection.include.is_empty() || !selection.exclude.is_empty();
    if !path_filter && !selection.modified_time() && !selection.add_version() {
        return Ok(());
    }
    let mut kept = Vec::new();
    for file in info.files.drain(..) {
        if path_filter
            && !crate::pattern::keep(&file.path, &selection.include, &selection.exclude)
                .map_err(|error| Error(error.to_string()))?
        {
            continue;
        }
        if !keep_file(&file, selection)? {
            continue;
        }
        kept.push(file);
    }
    if kept.is_empty() {
        return Err(Error("no files remained after exclude".into()));
    }
    info.files = kept;
    Ok(())
}

/// Sum log sizes and `numRecords` by partition value.
///
/// `num_records` and `bytes_per_row` are set only when every file in the
/// partition has add statistics. A zero record count leaves `bytes_per_row`
/// unset.
///
/// # Errors
/// Fails when a partition's record count overflows `u64`.
pub fn partition_masses(files: &[TableFile]) -> Result<Vec<PartitionMass>, Error> {
    let mut groups = BTreeMap::new();
    for file in files {
        add_partition_total(&mut groups, file)?;
    }
    Ok(finish_partition_masses(groups))
}

pub(crate) fn add_partition_total(
    groups: &mut BTreeMap<BTreeMap<String, Option<String>>, PartitionTotals>,
    file: &TableFile,
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
        .checked_add(file.size)
        .ok_or_else(|| Error(format!("partition byte total overflowed for {}", file.path)))?;
    entry.num_records = match (entry.num_records, file.stats.as_ref()) {
        (Some(sum), Some(stats)) => Some(sum.checked_add(stats.num_records).ok_or_else(|| {
            Error(format!(
                "partition record total overflowed for {}",
                file.path
            ))
        })?),
        _ => None,
    };
    Ok(())
}

pub(crate) fn finish_partition_masses(
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

pub(crate) struct PartitionTotals {
    file_count: usize,
    size: u64,
    num_records: Option<u64>,
}

pub(crate) fn bytes_per_row(size: u64, num_records: u64) -> Option<f64> {
    if num_records == 0 {
        None
    } else {
        Some(size as f64 / num_records as f64)
    }
}

pub(crate) fn keep_file(file: &TableFile, selection: &Selection) -> Result<bool, Error> {
    if let Some(before) = &selection.exclude_modified_before {
        let time = file_modified_millis(file)?;
        if time < object_store::parse_rfc3339_millis(before).map_err(|e| Error(e.to_string()))? {
            return Ok(false);
        }
    }
    if let Some(after) = &selection.exclude_modified_after {
        let time = file_modified_millis(file)?;
        if time > object_store::parse_rfc3339_millis(after).map_err(|e| Error(e.to_string()))? {
            return Ok(false);
        }
    }
    if let Some(before) = selection.exclude_version_before {
        let version = file.snapshot_version.ok_or_else(|| {
            Error(format!(
                "file {} has no add version; cannot apply --exclude-version-before",
                file.path
            ))
        })?;
        if version < before {
            return Ok(false);
        }
    }
    if let Some(after) = selection.exclude_version_after {
        let version = file.snapshot_version.ok_or_else(|| {
            Error(format!(
                "file {} has no add version; cannot apply --exclude-version-after",
                file.path
            ))
        })?;
        if version > after {
            return Ok(false);
        }
    }
    Ok(true)
}

fn file_modified_millis(file: &TableFile) -> Result<i64, Error> {
    let time = file.last_modified_time.as_deref().ok_or_else(|| {
        Error(format!(
            "file {} has no log modification time; cannot apply --exclude-modified-*",
            file.path
        ))
    })?;
    object_store::parse_rfc3339_millis(time).map_err(|e| Error(e.to_string()))
}

/// Check that `value` is `YYYY-MM-DD` or RFC3339 UTC.
///
/// # Errors
/// Fails when the instant cannot be parsed.
pub fn validate_time(value: &str) -> Result<(), Error> {
    object_store::parse_rfc3339_millis(value)
        .map(|_| ())
        .map_err(|error| Error(error.to_string()))
}

/// Whether a snapshot timestamp (epoch milliseconds) is kept.
#[cfg(any(feature = "delta", feature = "iceberg"))]
pub(crate) fn keep_snapshot_time(millis: i64, selection: &Selection) -> Result<bool, Error> {
    if let Some(before) = &selection.exclude_snapshot_before {
        if millis < object_store::parse_rfc3339_millis(before).map_err(|e| Error(e.to_string()))? {
            return Ok(false);
        }
    }
    if let Some(after) = &selection.exclude_snapshot_after {
        if millis > object_store::parse_rfc3339_millis(after).map_err(|e| Error(e.to_string()))? {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn visit_iceberg(
    request: &LoadRequest,
    visit: &mut impl FnMut(LoadEvent<'_>) -> Result<(), Error>,
) -> Result<TableInfo, Error> {
    let mut info = load_iceberg(request).await?;
    info.selection = request.selection.clone();
    apply_file_selection(&mut info)?;
    let files = std::mem::take(&mut info.files);
    visit(LoadEvent::BEGIN { info: &info })?;
    let mut groups = BTreeMap::new();
    for file in &files {
        add_partition_total(&mut groups, file)?;
        visit(LoadEvent::FILE { file })?;
    }
    if request.collect_files {
        info.files = files;
    } else {
        info.partitions = finish_partition_masses(groups);
    }
    Ok(info)
}

async fn load_iceberg(request: &LoadRequest) -> Result<TableInfo, Error> {
    #[cfg(feature = "iceberg")]
    {
        iceberg::load(request)
            .await
            .map_err(|error| Error(error.to_string()))
    }
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = request;
        Err(Error(
            "iceberg tables require the `iceberg` feature (`iceberg-s3` for S3)".into(),
        ))
    }
}
/// Local markers `lake` and `table` share. `None` is unrecognized, not an error.
pub(crate) fn local_format(path: &Path) -> Result<Option<TableFormat>, Error> {
    if !path.exists() {
        return Err(Error(format!("cannot open table {}", path.display())));
    }
    if path.is_file() {
        return Ok(is_metadata_json_path(path).then_some(TableFormat::ICEBERG));
    }
    if path.join("_delta_log").is_dir() {
        return Ok(Some(TableFormat::DELTA));
    }
    if path.join("metadata").join("version-hint.text").is_file() {
        return Ok(Some(TableFormat::ICEBERG));
    }
    if has_metadata_json(&path.join("metadata"))? {
        return Ok(Some(TableFormat::ICEBERG));
    }
    Ok(None)
}

/// Remote markers `lake` and `table` share. A `.metadata.json` URI is Iceberg;
/// otherwise Delta is `_delta_log/_last_checkpoint` or commit `0`, and Iceberg
/// is `metadata/version-hint.text`.
pub(crate) async fn remote_format(
    uri: &str,
    env: &BTreeMap<String, String>,
) -> Result<Option<TableFormat>, Error> {
    if uri
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.ends_with(".metadata.json"))
    {
        return Ok(Some(TableFormat::ICEBERG));
    }
    let options: Vec<(String, String)> = env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if probe(uri, "_delta_log/_last_checkpoint", &options).await?
        || probe(uri, "_delta_log/00000000000000000000.json", &options).await?
    {
        return Ok(Some(TableFormat::DELTA));
    }
    if probe(uri, "metadata/version-hint.text", &options).await? {
        return Ok(Some(TableFormat::ICEBERG));
    }
    Ok(None)
}

async fn probe(uri: &str, relative: &str, options: &[(String, String)]) -> Result<bool, Error> {
    let child = join_uri(uri, relative)?;
    let reader = object_store::open(&child, options).map_err(|e| Error(e.to_string()))?;
    reader.exists().await.map_err(|e| Error(e.to_string()))
}

fn join_uri(base: &str, relative: &str) -> Result<String, Error> {
    let mut url = Url::parse(base).map_err(|e| Error(format!("invalid table URI: {e}")))?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url
        .join(relative)
        .map_err(|e| Error(format!("invalid table path {relative}: {e}")))?
        .into())
}

fn is_local(uri: &str) -> bool {
    !uri.contains("://") || uri.starts_with("file://")
}

fn local_path(uri: &str) -> Result<PathBuf, Error> {
    if uri.starts_with("file://") {
        Url::parse(uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .ok_or_else(|| Error("invalid local table URI".into()))
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
    if !metadata.exists() {
        return Ok(false);
    }
    if !metadata.is_dir() {
        return Ok(false);
    }
    let entries = std::fs::read_dir(metadata)
        .map_err(|e| Error(format!("cannot read {}: {e}", metadata.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error(format!("cannot read {}: {e}", metadata.display())))?;
        if !entry.path().is_file() {
            continue;
        }
        if is_metadata_json_path(&entry.path()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn unrecognized(location: impl std::fmt::Display) -> Error {
    Error(format!(
        "unrecognized table format at {location}; supported formats: delta, iceberg"
    ))
}

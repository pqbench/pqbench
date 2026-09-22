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
use url::Url;

use crate::third_party::delta;
use crate::third_party::object_store;

#[cfg(feature = "iceberg")]
pub mod iceberg;

/// Errors detecting a table format or loading its metadata.
#[derive(Debug)]
pub struct Error(pub(crate) String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// On-disk table formats `pqbench table` can name. Detection runs before load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TableFormat {
    /// Zero value; not a detected format.
    #[serde(rename = "unspecified")]
    UNSPECIFIED,
    #[serde(rename = "delta")]
    DELTA,
    #[serde(rename = "iceberg")]
    ICEBERG,
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
    pub size_bytes: u64,
}

impl TableFile {
    /// Assemble a table file from its parts.
    #[must_use]
    pub fn new(path: impl Into<String>, uri: impl Into<String>, size_bytes: u64) -> Self {
        Self {
            path: path.into(),
            uri: uri.into(),
            size_bytes,
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
    #[serde(rename = "version")]
    pub document_version: u32,
    /// Detected table format.
    pub format: TableFormat,
    /// Table root as given (path or URI).
    pub uri: String,
    /// Snapshot version the files belong to.
    pub snapshot_version: u64,
    /// Partition columns live in the log and need not occupy Parquet columns.
    pub partition_columns: Vec<String>,
    /// Every available JSON commit, in version order. Checkpoint-only versions
    /// that have no remaining JSON file are omitted.
    pub log: Vec<LogCommit>,
    /// Active data files after replaying the log to `snapshot_version`.
    pub files: Vec<TableFile>,
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
            document_version: 1,
            format,
            uri: uri.into(),
            snapshot_version,
            partition_columns,
            log,
            files,
            env,
        }
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
    pub snapshot_version: Option<u64>,
    /// Storage options (`AWS_*` names), copied onto the document.
    pub env: BTreeMap<String, String>,
}

impl LoadRequest {
    /// Build a load request. `env` is passed to the storage backend.
    #[must_use]
    pub fn new(
        uri: impl Into<String>,
        snapshot_version: Option<u64>,
        env: BTreeMap<String, String>,
    ) -> Self {
        Self {
            uri: uri.into(),
            snapshot_version,
            env,
        }
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
pub async fn detect(uri: &str, env: &BTreeMap<String, String>) -> Result<TableFormat, Error> {
    if is_local(uri) {
        detect_local(&local_path(uri)?)
    } else {
        detect_remote(uri, env).await
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
pub async fn load(request: &LoadRequest) -> Result<TableInfo, Error> {
    let format = detect(&request.uri, &request.env).await?;
    match format {
        TableFormat::DELTA => delta::load(request).await,
        TableFormat::ICEBERG => load_iceberg(request).await,
        TableFormat::UNSPECIFIED => Err(Error("unrecognized table format".into())),
    }
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

fn detect_local(path: &Path) -> Result<TableFormat, Error> {
    if !path.exists() {
        return Err(Error(format!("cannot open table {}", path.display())));
    }
    if path.is_file() {
        if is_metadata_json_path(path) {
            return Ok(TableFormat::ICEBERG);
        }
        return Err(unrecognized(path.display()));
    }
    if path.join("_delta_log").is_dir() {
        return Ok(TableFormat::DELTA);
    }
    if path.join("metadata").join("version-hint.text").is_file()
        || has_metadata_json(&path.join("metadata"))
    {
        return Ok(TableFormat::ICEBERG);
    }
    Err(unrecognized(path.display()))
}

async fn detect_remote(uri: &str, env: &BTreeMap<String, String>) -> Result<TableFormat, Error> {
    let options: Vec<(String, String)> = env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if probe(uri, "_delta_log/_last_checkpoint", &options).await?
        || probe(uri, "_delta_log/00000000000000000000.json", &options).await?
    {
        return Ok(TableFormat::DELTA);
    }
    if probe(uri, "metadata/version-hint.text", &options).await? {
        return Ok(TableFormat::ICEBERG);
    }
    Err(unrecognized(uri))
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

fn is_metadata_json(uri: &str) -> bool {
    uri.rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.ends_with(".metadata.json"))
}

fn is_metadata_json_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".metadata.json"))
}

fn has_metadata_json(metadata: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(metadata) else {
        return false;
    };
    entries
        .flatten()
        .any(|entry| is_metadata_json_path(&entry.path()))
}

fn unrecognized(location: impl std::fmt::Display) -> Error {
    Error(format!(
        "unrecognized table format at {location}; supported formats: delta, iceberg"
    ))
}
